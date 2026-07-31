"""Building and encoding the ``StateSnapshot`` the export route emits.

Two pure functions, kept out of ``core/dofn.py`` so the DoFn's export branch is
a read-five-cells-and-yield and nothing else:

- :func:`build_snapshot` assembles one snapshot from state a caller already
  read. It copies the blobs **verbatim** — no migration, no re-versioning, no
  reordering — because interpreting old bytes is the loader's job
  (``beam_agents.replay``), and an export that quietly rewrote them would make
  the snapshot a claim about the exporting binary rather than a record of what
  is committed.
- :func:`serialize_snapshot` is the message-bus encoding, keyed by
  ``entity_key`` for the same reason ``serialize_trace_event`` is: one key's
  records keep their relative order through a single partition.

Everything here is replay-deterministic: ``snapshot_at_ms`` is the export
request's event time, never a wall-clock reading, so a retried bundle re-emits
byte-identical bytes.

Importing this module has no side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from beam_agents._protos import StateSnapshot
from beam_agents.core.migration import CURRENT_STATE_SCHEMA_VERSION

if TYPE_CHECKING:
    from collections.abc import Iterable

    from beam_agents._protos import Continuation, LlmCacheBlob, MemoryBlob, ToolIntent

__all__ = ["build_snapshot", "serialize_snapshot"]


def build_snapshot(
    *,
    entity_key: bytes,
    seq: int,
    snapshot_at_ms: int,
    request_id: str,
    memory_blob: MemoryBlob | None,
    cache_blob: LlmCacheBlob | None,
    continuation: Continuation | None,
    pending: Iterable[ToolIntent],
) -> StateSnapshot:
    """Assemble one ``StateSnapshot`` from already-read keyed state.

    ``None`` blobs (a key the runtime has never written) become the message's
    empty defaults, so a snapshot of an unknown key is a valid, empty answer
    rather than an error. ``continuation=None`` leaves the field **absent**:
    presence is what distinguishes "not suspended" from "suspended at step 0".

    ``state_schema_version`` stamps the exporting binary's version; the
    embedded blobs keep whatever version they were committed with.
    """
    snapshot = StateSnapshot(
        state_schema_version=CURRENT_STATE_SCHEMA_VERSION,
        entity_key=entity_key,
        seq=seq,
        snapshot_at_ms=snapshot_at_ms,
        request_id=request_id,
    )
    if memory_blob is not None:
        snapshot.memory.CopyFrom(memory_blob)
    if cache_blob is not None:
        snapshot.llm_cache.CopyFrom(cache_blob)
    if continuation is not None:
        snapshot.continuation.CopyFrom(continuation)
    snapshot.pending.extend(pending)
    return snapshot


def serialize_snapshot(snapshot: StateSnapshot) -> tuple[bytes, bytes]:
    """Encode for a message-bus sink: ``(entity_key, deterministic proto bytes)``."""
    return snapshot.entity_key, snapshot.SerializeToString(deterministic=True)
