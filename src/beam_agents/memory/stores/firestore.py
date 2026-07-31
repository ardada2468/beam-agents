"""`MemoryStore` over Firestore: transactional seq-guarded upserts.

One document per ``(entity_key, key)`` — ID ``hex(entity_key) + "#" +
quote(key)`` — under the configured collection, carrying the entity hex, the
record key, the native ``seq``, and the ``rec`` envelope bytes.

The record key is **percent-encoded into the document ID**: Firestore treats
``/`` inside a document ID as a path separator, so a hierarchical key like
``case/2`` would otherwise address the three-element path
``<collection>/<hex>#case/2`` and be rejected as an invalid document
reference. Hierarchical keys are ordinary — the shared store conformance
suite uses them — and the Redis and Bigtable backends accept them, so
encoding here is what keeps the three backends interchangeable. Only the
document ID is encoded; the ``key`` FIELD stores the key verbatim, which is
what ``search`` orders and range-scans over, so prefix semantics are
unaffected. The ``hex#`` prefix additionally guarantees the ID can never
collide with Firestore's reserved forms (``.``, ``..``, ``__.*__``).

Firestore has no CAS
primitive, so the transaction *is* the atomic guard: ``save`` runs the
read-compare-write inside one (design D8). ``search`` is an ordered range
query over the entity's keys (``key >= prefix`` and ``key < prefix +
"\\uffff"``) bounded by ``limit`` (D7).

The client library is imported inside the constructor: it belongs to the
optional ``memory-stores`` extra.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast
from urllib.parse import quote

from beam_agents.memory.stores.base import (
    MemoryRecord,
    MemoryStore,
    _decode_envelope,
    _encode_envelope,
    _missing_client_error,
)

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = [
    "FirestoreMemoryStore",
]

_ENTITY_FIELD = "entity"
_KEY_FIELD = "key"
_SEQ_FIELD = "seq"
_RECORD_FIELD = "rec"


class FirestoreMemoryStore(MemoryStore):
    """`MemoryStore` over Firestore; see the module docstring for the layout."""

    def __init__(self, project: str, collection: str) -> None:
        try:
            from google.cloud import firestore
        except ImportError as exc:
            raise _missing_client_error(
                "FirestoreMemoryStore", "google-cloud-firestore", exc
            ) from exc

        self._firestore = firestore
        self._client = firestore.AsyncClient(project=project)
        self._collection = self._client.collection(collection)

    @staticmethod
    def _doc_id(entity_key: bytes, key: str) -> str:
        # `safe=""` so `/` is encoded too — that is the whole point (see the
        # module docstring). The key FIELD keeps the unencoded value.
        return f"{entity_key.hex()}#{quote(key, safe='')}"

    async def _load(self, entity_key: bytes, key: str) -> MemoryRecord | None:
        snapshot = await self._collection.document(self._doc_id(entity_key, key)).get()
        if not snapshot.exists:
            return None
        return _decode_envelope(entity_key, bytes(snapshot.get(_RECORD_FIELD)))

    async def _save(self, record: MemoryRecord) -> bool:
        doc_ref = self._collection.document(self._doc_id(record.entity_key, record.key))
        payload = {
            _ENTITY_FIELD: record.entity_key.hex(),
            _KEY_FIELD: record.key,
            _SEQ_FIELD: record.seq,
            _RECORD_FIELD: _encode_envelope(record),
        }
        transaction = self._client.transaction()

        @self._firestore.async_transactional
        async def _guarded_upsert(transaction: object) -> bool:
            snapshot = await doc_ref.get(transaction=transaction)
            if snapshot.exists and record.seq < int(snapshot.get(_SEQ_FIELD)):
                return False
            transaction.set(doc_ref, payload)
            return True

        applied = await _guarded_upsert(transaction)
        return bool(applied)

    async def _search(self, entity_key: bytes, prefix: str, limit: int) -> list[MemoryRecord]:
        from google.cloud.firestore_v1.base_query import FieldFilter

        query = (
            self._collection.where(filter=FieldFilter(_ENTITY_FIELD, "==", entity_key.hex()))
            .where(filter=FieldFilter(_KEY_FIELD, ">=", prefix))
            .where(filter=FieldFilter(_KEY_FIELD, "<", prefix + "\uffff"))
            .order_by(_KEY_FIELD)
            .limit(limit)
        )
        records: list[MemoryRecord] = []
        async for snapshot in query.stream():
            records.append(_decode_envelope(entity_key, bytes(snapshot.get(_RECORD_FIELD))))
        return records

    async def close(self) -> None:
        """Close the Firestore client, tolerating its sync/async ``close`` variants."""
        import inspect

        # AsyncClient.close is a coroutine in current client versions; the
        # awaitable check keeps this correct across the sync/async variants.
        #
        # The `cast` is load-bearing for typechecking, not for runtime. When the
        # `integration`/`memory-stores` clients ARE installed, mypy resolves the
        # real `AsyncClient.close`, which is itself untyped, and `--strict`
        # rejects the call with `no-untyped-call`. When they are NOT installed
        # (the selection every typecheck lane uses today) the attribute is `Any`
        # and the call is silently fine — so a `# type: ignore[no-untyped-call]`
        # would be flagged as an unused ignore in exactly the lane that runs the
        # gate. Casting to a typed callable is correct in both environments.
        close = cast("Callable[[], object]", self._client.close)
        result = close()
        if inspect.isawaitable(result):
            await result
