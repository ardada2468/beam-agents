"""The normalized row vocabulary every ingest path decodes into.

This is the contract between the decoders (``_ingest.py``), the store
(``_store.py``), and the query layer (``_queries.py``). Five sources deliver the
same three protos in five encodings — native framed bytes, OTLP spans, Kafka
messages, BigQuery rows, and replay-bundle files — and design D7 puts exactly
one normalizer between all of them and the store. These are the types that
normalizer produces.

Identifiers are hex strings rather than ``bytes``. The runtime's own BigQuery
encoder already made that choice (``observability/exporters.py``) for the same
reason it applies here: a hex ID can be indexed, joined, put in a URL, and
displayed without a decode step, and SQLite has no first-class blob comparison
worth the trouble.

``provenance`` records which ingest path a record arrived by, because the paths
are not equally complete: OTLP cannot carry ``ACTIVATION_START`` at all
(``observability/otlp.py`` drops it — it shares a span ID with
``ACTIVATION_END``), so an activation assembled only from OTLP is missing the
event that distinguishes a fresh attempt from a resume. The UI is required to
say so rather than present that activation as a complete record.

Importing this module has no side effects.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = [
    "PROVENANCE",
    "PROVENANCE_BIGQUERY",
    "PROVENANCE_BUNDLE",
    "PROVENANCE_KAFKA",
    "PROVENANCE_NATIVE",
    "PROVENANCE_OTLP",
    "ErrorRow",
    "EventRow",
    "RecordBatch",
    "SnapshotRow",
]

# The ingest path a record arrived by. Not decoration: `PROVENANCE_OTLP` marks a
# record whose encoding is known-lossy, and the UI keys its incomplete-provenance
# warning off it.
PROVENANCE_NATIVE = "native"
PROVENANCE_OTLP = "otlp"
PROVENANCE_KAFKA = "kafka"
PROVENANCE_BIGQUERY = "bigquery"
PROVENANCE_BUNDLE = "bundle"

PROVENANCE = (
    PROVENANCE_NATIVE,
    PROVENANCE_OTLP,
    PROVENANCE_KAFKA,
    PROVENANCE_BIGQUERY,
    PROVENANCE_BUNDLE,
)


@dataclass(frozen=True, slots=True)
class EventRow:
    """One ``TraceEvent``, normalized.

    ``(trace_id, span_id, event_type)`` is the identity tuple — the dedup key
    ``docs/traces.md`` publishes for at-least-once trace delivery, and the key
    the store upserts on (design D5). Everything else is payload and may be
    merged by a later, richer copy of the same event.

    ``start_ms`` and ``end_ms`` are equal for every event the runtime produces:
    spans are zero-width by design (``add-trace-events`` D7) because measuring
    elapsed time would need a wall-clock read in the hot path. Nothing
    downstream may treat their difference as a duration.
    """

    trace_id: str
    span_id: str
    parent_span_id: str
    entity_key: str
    seq: int
    step_index: int
    event_type: str
    start_ms: int
    end_ms: int
    attributes: Mapping[str, str] = field(default_factory=dict)
    provenance: str = PROVENANCE_NATIVE


@dataclass(frozen=True, slots=True)
class ErrorRow:
    """One ``ActivationErrorRecord``, normalized.

    ``reason`` is drawn from the runtime's closed vocabulary
    (``core/dofn.py``, ``hitl.py``), which is what makes grouping by it a real
    navigation axis rather than a string histogram. ``entity_key`` is present on
    every error; ``seq`` is not, because several reasons fire from timer
    callbacks that have no activation, so it is parsed from ``detail`` when the
    reason carries it and left ``None`` otherwise.
    """

    entity_key: str
    reason: str
    detail: str
    event_time_ms: int
    seq: int | None = None
    provenance: str = PROVENANCE_NATIVE


@dataclass(frozen=True, slots=True)
class SnapshotRow:
    """One ``StateSnapshot``, normalized to its summary plus the original bytes.

    The snapshot's state is opaque by contract — ``DefaultSinkResolver`` refuses
    a BigQuery sink for ``snapshots_to`` for exactly that reason — so this keeps
    the countable metadata as columns and the image itself as ``raw``, which is
    what the replay CLI needs back unchanged.
    """

    entity_key: str
    seq: int
    snapshot_at_ms: int
    state_schema_version: int
    request_id: str = ""
    memory_entries: int = 0
    memory_bytes: int = 0
    llm_cache_entries: int = 0
    pending_intent_ids: tuple[str, ...] = ()
    continuation_step_index: int | None = None
    continuation_deadline_ms: int | None = None
    continuation_adapter: str = ""
    raw: bytes = b""
    provenance: str = PROVENANCE_NATIVE


@dataclass(frozen=True, slots=True)
class RecordBatch:
    """What one decode produces and one store write consumes.

    A batch is the unit of idempotency: writing it twice leaves the store in the
    state writing it once does, so a retried POST, a re-read BigQuery window,
    and a Kafka consumer restarting from an earlier offset are all safe.
    """

    events: tuple[EventRow, ...] = ()
    errors: tuple[ErrorRow, ...] = ()
    snapshots: tuple[SnapshotRow, ...] = ()

    def __bool__(self) -> bool:
        """Return whether the batch carries any record at all."""
        return bool(self.events or self.errors or self.snapshots)

    def __len__(self) -> int:
        """Return the total number of records across all three kinds."""
        return len(self.events) + len(self.errors) + len(self.snapshots)
