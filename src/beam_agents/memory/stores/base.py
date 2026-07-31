"""The `MemoryStore` ABC, record envelope, URI factory, and in-memory store.

The long-term memory tier's backend-independent core (see
``openspec/changes/add-longterm-memory-stores/design.md``): one record type,
one deterministic envelope encoding every backend stores byte-identically
(design D2), one definition of the seq-guard rule "apply iff incoming ``seq``
is >= the stored one" (D1/D3), argument validation ahead of any I/O, the
import-free URI grammar and ``build_memory_store`` factory mirroring the
effector's ``build_dedup_store`` (D6), and the in-memory reference
implementation the offline conformance suite runs against.

Unlike the effector's ``DedupStore`` — a pure structural seam where a Protocol
is right — :class:`MemoryStore` is an ``abc.ABC``: the base class owns the
correctness-bearing shared behavior (validation, envelope codec, the guard
rule's reference definition) so a backend only implements storage primitives
and the semantics live once (design D1).

Backend client libraries are imported inside the backend constructors, never
here: ``import beam_agents.memory.stores`` must succeed with none of them
installed.
"""

from __future__ import annotations

import abc
import struct
from collections.abc import Callable
from dataclasses import dataclass

from beam_agents._protos import LongTermRecord

__all__ = [
    "InMemoryMemoryStore",
    "MemoryRecord",
    "MemoryStore",
    "build_memory_store",
    "parse_memory_store_uri",
]

# The grammar every recognized URI documents in its errors; any other
# ``scheme://`` URI is handed to SQLAlchemy whole, which is the authority on
# its own URL grammar (design D6).
_GRAMMARS = (
    "memory://",
    "redis://<url>",
    "bigtable://<project>/<instance>/<table>",
    "firestore://<project>/<collection>",
    "<sqlalchemy-async-url> (e.g. postgresql+asyncpg://... or sqlite+aiosqlite:///...)",
)


@dataclass(frozen=True, slots=True)
class MemoryRecord:
    """One long-term row: the record type `save`/`load`/`search` traffic in.

    ``entity_key`` scopes the row to one keyed agent stream; it is the
    backend's row/document key, not part of the stored envelope. ``seq`` and
    ``updated_at_ms`` are stamped from the staging activation's frozen scope,
    which is what makes a replayed flush byte-identical.
    """

    entity_key: bytes
    key: str
    value: bytes
    seq: int
    updated_at_ms: int


def _encode_seq(seq: int) -> bytes:
    """Encode ``seq`` as 8 big-endian bytes so byte order matches numeric order.

    The comparable-bytes trick proven for the dedup store's lease expiry: it is
    what lets Bigtable's ``ValueRangeFilter`` and Redis's Lua byte compare
    evaluate the numeric guard lexicographically.
    """
    if seq < 0:
        raise ValueError(f"seq must be non-negative, got {seq}")
    return struct.pack(">Q", seq)


def _decode_seq(encoded: bytes) -> int:
    """Inverse of :func:`_encode_seq` (reads the first 8 bytes)."""
    return int(struct.unpack(">Q", encoded[:8])[0])


def _encode_envelope(record: MemoryRecord) -> bytes:
    """The deterministic `LongTermRecord` bytes every backend stores verbatim."""
    return LongTermRecord(
        state_schema_version=1,
        key=record.key,
        value=record.value,
        seq=record.seq,
        updated_at_ms=record.updated_at_ms,
    ).SerializeToString(deterministic=True)


def _decode_envelope(entity_key: bytes, encoded: bytes) -> MemoryRecord:
    """Rebuild the record from its envelope bytes and the row's entity key."""
    message = LongTermRecord()
    message.ParseFromString(encoded)
    return MemoryRecord(
        entity_key=entity_key,
        key=message.key,
        value=message.value,
        seq=message.seq,
        updated_at_ms=message.updated_at_ms,
    )


def _seq_guard_applies(incoming_seq: int, stored_seq: int | None) -> bool:
    """The one definition of the upsert guard: apply iff ``incoming >= stored``.

    ``>=`` (not ``>``) is deliberate: an equal-seq save is a replayed
    activation legitimately rewriting its own byte-identical row and must be
    accepted; the strict half is what keeps a delayed duplicate flush of seq
    ``N`` from regressing a row already at ``N+1`` (design D3).
    """
    return stored_seq is None or incoming_seq >= stored_seq


class MemoryStore(abc.ABC):
    """Async long-term memory store: ``load``/``save``/``search``/``close``.

    The public methods own argument validation and delegate to the abstract
    storage primitives (``_load``/``_save``/``_search``), so every backend
    inherits one contract and only supplies its own atomic mechanics.
    """

    async def load(self, entity_key: bytes, key: str) -> MemoryRecord | None:
        """The stored record for ``(entity_key, key)``, or ``None``."""
        _require_key(key)
        return await self._load(entity_key, key)

    async def save(self, record: MemoryRecord) -> bool:
        """Seq-guarded idempotent upsert; ``True`` iff the write applied.

        Applies iff :func:`_seq_guard_applies` holds for the row's stored seq,
        enforced atomically by the backend's own primitive; a lower incoming
        seq leaves the row unchanged and returns ``False``.
        """
        _require_key(record.key)
        if record.seq < 0:
            raise ValueError(f"record seq must be non-negative, got {record.seq}")
        return await self._save(record)

    async def search(self, entity_key: bytes, prefix: str, limit: int) -> list[MemoryRecord]:
        """At most ``limit`` of the entity's records whose key starts with
        ``prefix``, ordered by key ascending. An empty prefix matches all of
        the entity's records, still bounded — an unbounded scan inside an
        activation is a latency hazard, so the API refuses to express one.
        """
        if limit <= 0:
            raise ValueError(f"search limit must be positive, got {limit}")
        return await self._search(entity_key, prefix, limit)

    async def close(self) -> None:
        """Release backend clients. Default: nothing to release."""
        return None

    # -- storage primitives (backend-supplied) --------------------------------

    @abc.abstractmethod
    async def _load(self, entity_key: bytes, key: str) -> MemoryRecord | None: ...

    @abc.abstractmethod
    async def _save(self, record: MemoryRecord) -> bool: ...

    @abc.abstractmethod
    async def _search(self, entity_key: bytes, prefix: str, limit: int) -> list[MemoryRecord]: ...


def _require_key(key: str) -> None:
    if not key:
        raise ValueError("record key must be a non-empty string")


def _wall_clock_ms() -> int:
    import time

    return int(time.time() * 1000)


class InMemoryMemoryStore(MemoryStore):
    """Process-local `MemoryStore` for tests and single-worker development.

    Implements the full contract with no external process. Rows hold the same
    envelope bytes a real backend would store, so the byte-identity contract is
    exercised even offline. The injectable ``clock`` mirrors the dedup store's
    seam (nothing expires in v0, but the seam is part of the reference
    surface). Not shared across processes: two workers pointed at it converge
    independently, which is why real deployments use a networked backend.
    """

    def __init__(self, *, clock: Callable[[], int] = _wall_clock_ms) -> None:
        self.clock = clock
        # entity_key -> key -> (seq, envelope bytes)
        self._rows: dict[bytes, dict[str, tuple[int, bytes]]] = {}

    async def _load(self, entity_key: bytes, key: str) -> MemoryRecord | None:
        stored = self._rows.get(entity_key, {}).get(key)
        if stored is None:
            return None
        return _decode_envelope(entity_key, stored[1])

    async def _save(self, record: MemoryRecord) -> bool:
        entity_rows = self._rows.setdefault(record.entity_key, {})
        stored = entity_rows.get(record.key)
        if not _seq_guard_applies(record.seq, stored[0] if stored is not None else None):
            return False
        entity_rows[record.key] = (record.seq, _encode_envelope(record))
        return True

    async def _search(self, entity_key: bytes, prefix: str, limit: int) -> list[MemoryRecord]:
        entity_rows = self._rows.get(entity_key, {})
        matching = sorted(key for key in entity_rows if key.startswith(prefix))
        return [_decode_envelope(entity_key, entity_rows[key][1]) for key in matching[:limit]]


# Segment counts the recognized multi-part grammars require.
_BIGTABLE_URI_SEGMENTS = 3  # <project>/<instance>/<table>
_FIRESTORE_URI_SEGMENTS = 2  # <project>/<collection>


def parse_memory_store_uri(
    uri: str, *, field: str = "longterm_memory"
) -> tuple[str, tuple[str, ...]]:
    """Validate the store-URI grammar without importing any client library.

    Returns ``(scheme, parts)`` for :func:`build_memory_store`. Recognized
    schemes get their grammar checked here, at configuration-construction
    time; any other ``scheme://`` URI is accepted as a SQLAlchemy async URL
    and fully parsed only at store construction, where SQLAlchemy itself is
    the authority on its own grammar (design D6). Raises an actionable
    ``ValueError`` naming ``field`` for a malformed URI.
    """
    scheme, sep, rest = uri.partition("://")
    if not sep or not scheme:
        raise ValueError(
            f"{field}: {uri!r} is not a store URI; expected one of: " + ", ".join(_GRAMMARS)
        )
    if scheme == "memory":
        return scheme, ()
    if scheme == "redis":
        # The whole URI is the client's; Redis URLs carry auth/db/query parts
        # this grammar has no business re-validating.
        return scheme, (uri,)
    if scheme == "bigtable":
        parts = tuple(rest.split("/"))
        if len(parts) != _BIGTABLE_URI_SEGMENTS or not all(parts):
            raise ValueError(
                f"{field}: malformed Bigtable URI {uri!r}; "
                "expected bigtable://<project>/<instance>/<table>"
            )
        return scheme, parts
    if scheme == "firestore":
        parts = tuple(rest.split("/"))
        if len(parts) != _FIRESTORE_URI_SEGMENTS or not all(parts):
            raise ValueError(
                f"{field}: malformed Firestore URI {uri!r}; "
                "expected firestore://<project>/<collection>"
            )
        return scheme, parts
    # Anything else is a SQLAlchemy async URL, passed through whole.
    return scheme, (uri,)


def build_memory_store(scheme: str, parts: tuple[str, ...]) -> MemoryStore:
    """Construct the store a parsed URI names.

    Called once per DoFn instance at ``setup()``; the client import happens
    inside the chosen store's constructor, never here.
    """
    if scheme == "memory":
        return InMemoryMemoryStore()
    if scheme == "redis":
        from beam_agents.memory.stores import redis as redis_module

        return redis_module.RedisMemoryStore(parts[0])
    if scheme == "bigtable":
        from beam_agents.memory.stores import bigtable as bigtable_module

        project, instance, table = parts
        return bigtable_module.BigtableMemoryStore(project, instance, table)
    if scheme == "firestore":
        from beam_agents.memory.stores import firestore as firestore_module

        project, collection = parts
        return firestore_module.FirestoreMemoryStore(project, collection)
    from beam_agents.memory.stores import sql as sql_module

    return sql_module.SqlMemoryStore(parts[0])


def _missing_client_error(store: str, client: str, cause: ImportError) -> ImportError:
    """The actionable constructor-time error for an absent optional client."""
    return ImportError(
        f"{store} requires the {client!r} client library, which is not installed; "
        "install the 'memory-stores' extra (pip install 'beam-agents[memory-stores]') "
        "or add the client to your environment"
    )
