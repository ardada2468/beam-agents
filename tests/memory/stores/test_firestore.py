"""Offline call-shape tests for the Firestore `MemoryStore`.

Covers the client-side half of "The Firestore store guards upserts with a
transaction" without an emulator, through a faked `google.cloud.firestore`
module injected at the constructor's lazy-import seam — so the suite runs (and
is coverage-counted) in the `ci`/`quality` lanes, where the real client is not
installed. Pinned here: the document-ID encoding invariants that would have
caught defect D-3 the day it was written (spec: "A hierarchical key
round-trips"), the guard's client-side compare branches, search request
shaping (spec: "Prefix search is unaffected by document-ID encoding"), decode
paths, and both `close` variants. The fakes never evaluate transaction
semantics — the emulator conformance suite (`test_firestore_emulator.py`,
`-m integration`) stays the interchangeability authority.
"""

from __future__ import annotations

import re
import sys
import types
from typing import TYPE_CHECKING, TypeVar
from urllib.parse import unquote

import pytest
from hypothesis import given
from hypothesis import strategies as st

from beam_agents.memory.stores.base import _encode_envelope
from beam_agents.memory.stores.firestore import FirestoreMemoryStore

from ._conformance import ENTITY_A, ENTITY_B, a_record

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

_T = TypeVar("_T")

COLLECTION = "agent-memories"


# -- The faked client surface --------------------------------------------------


class _FakeSnapshot:
    """A document snapshot: `None` fields means the document is absent."""

    def __init__(self, fields: dict[str, object] | None) -> None:
        self._fields = fields

    @property
    def exists(self) -> bool:
        return self._fields is not None

    def get(self, field: str) -> object:
        assert self._fields is not None, "get() on an absent snapshot"
        return self._fields[field]


class _FakeTransaction:
    """Records the `set` calls the guarded upsert issues."""

    def __init__(self) -> None:
        self.sets: list[tuple[str, dict[str, object]]] = []

    def set(self, doc_ref: _FakeDocRef, payload: dict[str, object]) -> None:
        self.sets.append((doc_ref.doc_id, payload))


class _FakeDocRef:
    def __init__(self, doc_id: str, collection: _FakeCollection) -> None:
        self.doc_id = doc_id
        self._collection = collection

    async def get(self, transaction: object | None = None) -> _FakeSnapshot:
        self._collection.gets.append((self.doc_id, transaction))
        return _FakeSnapshot(self._collection.documents.get(self.doc_id))


class _FakeFieldFilter:
    """Stands in for `google.cloud.firestore_v1.base_query.FieldFilter`."""

    def __init__(self, field_path: str, op_string: str, value: object) -> None:
        self.field_path = field_path
        self.op_string = op_string
        self.value = value


class _FakeQuery:
    """Records the filter/order/limit chain and streams scripted snapshots."""

    def __init__(self, collection: _FakeCollection) -> None:
        self._collection = collection
        self.filters: list[_FakeFieldFilter] = []
        self.order_by_fields: list[str] = []
        self.limit_value: int | None = None

    def where(self, *, filter: _FakeFieldFilter) -> _FakeQuery:
        self.filters.append(filter)
        return self

    def order_by(self, field: str) -> _FakeQuery:
        self.order_by_fields.append(field)
        return self

    def limit(self, count: int) -> _FakeQuery:
        self.limit_value = count
        return self

    async def stream(self) -> AsyncIterator[_FakeSnapshot]:
        for fields in self._collection.stream_results:
            yield _FakeSnapshot(fields)


class _FakeCollection:
    def __init__(self, name: str) -> None:
        self.name = name
        # doc_id -> stored fields; the scripted "server" state reads come from.
        self.documents: dict[str, dict[str, object]] = {}
        self.gets: list[tuple[str, object | None]] = []
        self.queries: list[_FakeQuery] = []
        self.stream_results: list[dict[str, object]] = []

    def document(self, doc_id: str) -> _FakeDocRef:
        return _FakeDocRef(doc_id, self)

    def where(self, *, filter: _FakeFieldFilter) -> _FakeQuery:
        query = _FakeQuery(self)
        self.queries.append(query)
        return query.where(filter=filter)


class _FakeAsyncClient:
    """`google.cloud.firestore.AsyncClient` stand-in with a coroutine `close`."""

    def __init__(self, *, project: str) -> None:
        self.project = project
        self.collections: dict[str, _FakeCollection] = {}
        self.transactions: list[_FakeTransaction] = []
        self.closed = False

    def collection(self, name: str) -> _FakeCollection:
        return self.collections.setdefault(name, _FakeCollection(name))

    def transaction(self) -> _FakeTransaction:
        transaction = _FakeTransaction()
        self.transactions.append(transaction)
        return transaction

    async def close(self) -> None:
        self.closed = True


class _FakeSyncCloseClient(_FakeAsyncClient):
    """The sync-`close` client variant the store must tolerate."""

    def close(self) -> None:  # type: ignore[override]
        self.closed = True


def _fake_async_transactional(
    func: Callable[[object], Awaitable[_T]],
) -> Callable[[object], Awaitable[_T]]:
    """`firestore.async_transactional` stand-in: run the body once, no retries.

    Deliberately no commit/contention semantics — the transaction's atomicity
    is the emulator suite's to verify (design D2); offline we pin only what
    the store's own body does inside it.
    """

    async def _run(transaction: object) -> _T:
        return await func(transaction)

    return _run


def _install_fake_firestore(
    monkeypatch: pytest.MonkeyPatch, *, client_cls: type[_FakeAsyncClient] = _FakeAsyncClient
) -> None:
    """Satisfy both of the store's lazy imports with the fakes above.

    Works whether or not the real client is installed: the `sys.modules`
    entries win for the dotted imports, and the `google.cloud` attribute is
    re-pointed for the environments where the emulator suite's collection
    already bound the real submodule there.
    """
    fake_firestore = types.ModuleType("google.cloud.firestore")
    setattr(fake_firestore, "AsyncClient", client_cls)  # noqa: B010
    setattr(fake_firestore, "async_transactional", _fake_async_transactional)  # noqa: B010
    fake_v1 = types.ModuleType("google.cloud.firestore_v1")
    fake_base_query = types.ModuleType("google.cloud.firestore_v1.base_query")
    setattr(fake_base_query, "FieldFilter", _FakeFieldFilter)  # noqa: B010

    monkeypatch.setitem(sys.modules, "google.cloud.firestore", fake_firestore)
    monkeypatch.setitem(sys.modules, "google.cloud.firestore_v1", fake_v1)
    monkeypatch.setitem(sys.modules, "google.cloud.firestore_v1.base_query", fake_base_query)
    google_cloud = sys.modules.get("google.cloud")
    if google_cloud is not None:
        monkeypatch.setattr(google_cloud, "firestore", fake_firestore, raising=False)


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch) -> tuple[FirestoreMemoryStore, _FakeAsyncClient]:
    _install_fake_firestore(monkeypatch)
    built = FirestoreMemoryStore("proj", COLLECTION)
    # Widened to `object` first: in a full environment mypy types `_client` as
    # the real `AsyncClient`, and narrowing that directly against the fake
    # would read as an impossible intersection.
    client: object = built._client
    assert isinstance(client, _FakeAsyncClient)
    return built, client


def _collection_of(client: _FakeAsyncClient) -> _FakeCollection:
    return client.collections[COLLECTION]


# -- Scenario: A hierarchical key round-trips (the D-3 invariants) ------------


def test_the_doc_id_percent_encodes_the_path_separator() -> None:
    # The exact D-3 shape: `/` in a key must not become a document-path
    # separator, so it is encoded and the ID stays a single path element.
    assert FirestoreMemoryStore._doc_id(ENTITY_A, "case/2") == ENTITY_A.hex() + "#case%2F2"


@pytest.mark.parametrize("key", ["case/2", "a#b", "a%2Fb", "100% sure", "café/☕", "a?b=c&d"])
def test_the_doc_id_round_trips_every_conformance_shaped_key(key: str) -> None:
    doc_id = FirestoreMemoryStore._doc_id(ENTITY_A, key)
    entity_hex, _, encoded_key = doc_id.partition("#")

    assert entity_hex == ENTITY_A.hex()
    assert unquote(encoded_key) == key


@given(key=st.text(min_size=1, max_size=64))
def test_no_key_can_put_a_path_separator_or_reserved_form_in_the_doc_id(key: str) -> None:
    # Property form of the D-3 lesson: for ANY key the store accepts, the
    # document ID is one path element and never a Firestore reserved form.
    doc_id = FirestoreMemoryStore._doc_id(ENTITY_A, key)

    assert "/" not in doc_id
    assert doc_id not in (".", "..")
    assert not re.fullmatch(r"__.*__", doc_id)


@given(
    keys=st.lists(st.text(min_size=1, max_size=32), min_size=2, max_size=2, unique=True),
    entity=st.sampled_from([ENTITY_A, ENTITY_B]),
)
def test_distinct_keys_yield_distinct_doc_ids(keys: list[str], entity: bytes) -> None:
    # Injectivity: percent-encoding with `safe=""` encodes `%` itself, so two
    # different keys can never collide on one document.
    assert FirestoreMemoryStore._doc_id(entity, keys[0]) != FirestoreMemoryStore._doc_id(
        entity, keys[1]
    )


def test_distinct_entities_yield_distinct_doc_ids_for_the_same_key() -> None:
    assert FirestoreMemoryStore._doc_id(ENTITY_A, "profile") != FirestoreMemoryStore._doc_id(
        ENTITY_B, "profile"
    )


# -- Requirement: Save is an idempotent upsert guarded by seq -----------------


async def test_a_save_against_an_absent_document_applies_and_writes_the_payload(
    store: tuple[FirestoreMemoryStore, _FakeAsyncClient],
) -> None:
    fs, client = store
    record = a_record("case/2", seq=7)

    applied = await fs.save(record)

    assert applied
    (transaction,) = client.transactions
    (written_doc_id, payload) = transaction.sets[0]
    assert written_doc_id == ENTITY_A.hex() + "#case%2F2"
    # The key FIELD carries the key verbatim — that is what search orders and
    # range-scans over, so prefix semantics survive the ID encoding.
    assert payload == {
        "entity": ENTITY_A.hex(),
        "key": "case/2",
        "seq": 7,
        "rec": _encode_envelope(record),
    }


async def test_a_stale_seq_refuses_and_writes_nothing(
    store: tuple[FirestoreMemoryStore, _FakeAsyncClient],
) -> None:
    # Scenario: A stale seq cannot regress a newer row — the client-side
    # compare branch that decides it.
    fs, client = store
    doc_id = FirestoreMemoryStore._doc_id(ENTITY_A, "profile")
    _collection_of(client).documents[doc_id] = {"seq": 7}

    applied = await fs.save(a_record("profile", seq=5))

    assert not applied
    (transaction,) = client.transactions
    assert transaction.sets == []


async def test_an_equal_seq_applies(
    store: tuple[FirestoreMemoryStore, _FakeAsyncClient],
) -> None:
    # Scenario: Replayed flush converges on the identical row — the `>=` half
    # of the guard rule (`<` refuses, so equal must pass).
    fs, client = store
    record = a_record("profile", seq=5)
    doc_id = FirestoreMemoryStore._doc_id(ENTITY_A, "profile")
    _collection_of(client).documents[doc_id] = {"seq": 5}

    applied = await fs.save(record)

    assert applied
    assert client.transactions[0].sets[0][1]["rec"] == _encode_envelope(record)


async def test_the_guarded_read_runs_inside_the_transaction(
    store: tuple[FirestoreMemoryStore, _FakeAsyncClient],
) -> None:
    # The read-compare-write is atomic only if the read is transactional; a
    # plain `get` would reintroduce the client-side race the spec forbids.
    fs, client = store

    await fs.save(a_record("profile", seq=1))

    ((_, transaction_used),) = _collection_of(client).gets
    assert transaction_used is client.transactions[0]


# -- Scenario: Load returns the saved record or None --------------------------


async def test_load_returns_none_for_an_absent_document(
    store: tuple[FirestoreMemoryStore, _FakeAsyncClient],
) -> None:
    fs, client = store

    assert await fs.load(ENTITY_A, "missing") is None
    expected_doc_id = FirestoreMemoryStore._doc_id(ENTITY_A, "missing")
    assert _collection_of(client).gets == [(expected_doc_id, None)]


async def test_load_decodes_the_envelope_via_the_encoded_doc_id(
    store: tuple[FirestoreMemoryStore, _FakeAsyncClient],
) -> None:
    fs, client = store
    record = a_record("case/2", seq=3)
    doc_id = FirestoreMemoryStore._doc_id(ENTITY_A, "case/2")
    _collection_of(client).documents[doc_id] = {"rec": _encode_envelope(record)}

    loaded = await fs.load(ENTITY_A, "case/2")

    # The loaded key is `case/2`, not an encoded form: the envelope stores it
    # verbatim, so the ID encoding never leaks into results.
    assert loaded == record


# -- Scenario: Prefix search is unaffected by document-ID encoding ------------


async def test_search_shapes_an_entity_scoped_ordered_bounded_range_query(
    store: tuple[FirestoreMemoryStore, _FakeAsyncClient],
) -> None:
    fs, client = store
    collection = _collection_of(client)
    for key in ("case/1", "case/2"):
        record = a_record(key, value=key.encode())
        collection.stream_results.append({"rec": _encode_envelope(record)})

    results = await fs.search(ENTITY_A, "case/", limit=2)

    (query,) = collection.queries
    assert [(f.field_path, f.op_string, f.value) for f in query.filters] == [
        ("entity", "==", ENTITY_A.hex()),
        ("key", ">=", "case/"),
        ("key", "<", "case/￿"),
    ]
    assert query.order_by_fields == ["key"]
    assert query.limit_value == 2
    assert [r.key for r in results] == ["case/1", "case/2"]
    assert all(r.entity_key == ENTITY_A for r in results)


async def test_an_empty_prefix_query_still_scopes_and_bounds(
    store: tuple[FirestoreMemoryStore, _FakeAsyncClient],
) -> None:
    # The requirement's empty-prefix clause: match all of the entity's
    # records, still entity-scoped and still bounded.
    fs, client = store

    await fs.search(ENTITY_A, "", limit=5)

    (query,) = _collection_of(client).queries
    assert [(f.field_path, f.op_string, f.value) for f in query.filters] == [
        ("entity", "==", ENTITY_A.hex()),
        ("key", ">=", ""),
        ("key", "<", "￿"),
    ]
    assert query.limit_value == 5


# -- Close tolerates the client's sync/async variants -------------------------


async def test_close_awaits_a_coroutine_close(
    store: tuple[FirestoreMemoryStore, _FakeAsyncClient],
) -> None:
    fs, client = store

    await fs.close()

    assert client.closed


async def test_close_tolerates_a_sync_close(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_firestore(monkeypatch, client_cls=_FakeSyncCloseClient)
    fs = FirestoreMemoryStore("proj", COLLECTION)

    await fs.close()

    client: object = fs._client
    assert isinstance(client, _FakeSyncCloseClient)
    assert client.closed
