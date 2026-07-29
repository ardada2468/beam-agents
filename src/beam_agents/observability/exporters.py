"""Encoders that make ``.traces`` deliverable to a configured sink.

``.traces`` is a ``PCollection[TraceEvent]``, and none of the write transforms
``DefaultSinkResolver`` resolves accept a proto message: Kafka and Pub/Sub want
bytes, BigQuery wants a row mapping. These are the two encodings, kept beside
the trace vocabulary rather than in the transform so the sink wiring stays a
one-line ``beam.Map`` (design D9).

Both are deterministic. Trace records are emitted at-least-once — a retried
bundle re-emits byte-identical events — and downstream dedup only collapses
them if the encoding does not reorder the ``attributes`` map.

Importing this module has no side effects.
"""

from __future__ import annotations

import datetime
from typing import Any

from beam_agents._protos import TraceEvent

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)

# The trace table's layout, published beside the row encoder so the pair can be
# asserted equal in one test: every key `trace_event_to_row` produces appears
# here with the matching type/mode, and nothing is declared that the encoder
# does not produce. `event_time` (not `start_ms`) is the partition column
# because BigQuery cannot column-partition on an INT64 epoch-millis field, and
# ingestion-time partitioning would decouple partitions from event semantics
# under replay/backfill (design D6).
TRACE_TABLE_SCHEMA: dict[str, Any] = {
    "fields": [
        {"name": "trace_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "span_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "parent_span_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "entity_key", "type": "STRING", "mode": "NULLABLE"},
        {"name": "seq", "type": "INT64", "mode": "NULLABLE"},
        {"name": "step_index", "type": "INT64", "mode": "NULLABLE"},
        {"name": "event_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "start_ms", "type": "INT64", "mode": "NULLABLE"},
        {"name": "end_ms", "type": "INT64", "mode": "NULLABLE"},
        {"name": "event_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {
            "name": "attributes",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "key", "type": "STRING", "mode": "NULLABLE"},
                {"name": "value", "type": "STRING", "mode": "NULLABLE"},
            ],
        },
    ]
}


def serialize_trace_event(event: TraceEvent) -> tuple[bytes, bytes]:
    """Encode for a message-bus sink: ``(entity_key, deterministic proto bytes)``.

    Keyed by ``entity_key`` so one key's trace records keep their relative
    order through a single partition, the same reason ``WriteIntents`` keys its
    outbox writes.
    """
    return event.entity_key, event.SerializeToString(deterministic=True)


def trace_event_to_row(event: TraceEvent) -> dict[str, Any]:
    """Encode for a BigQuery sink: a flat row with hex IDs and key/value attributes.

    IDs are hex rather than raw bytes so a table can be clustered and joined on
    ``trace_id`` without a decode step, and ``event_type`` is its enum *name* so
    a query does not have to carry the numbering. Attributes are sorted by key,
    which keeps two encodings of one event equal. ``event_time`` is ``start_ms``
    re-expressed as an RFC 3339 UTC timestamp — a pure derivation, present so
    the table can be day-partitioned on a TIMESTAMP column (design D6).
    """
    return {
        "trace_id": event.trace_id.hex(),
        "span_id": event.span_id.hex(),
        "parent_span_id": event.parent_span_id.hex(),
        "entity_key": event.entity_key.hex(),
        "seq": event.seq,
        "step_index": event.step_index,
        "event_type": TraceEvent.EventType.Name(event.event_type),
        "start_ms": event.start_ms,
        "end_ms": event.end_ms,
        "event_time": (_EPOCH + datetime.timedelta(milliseconds=event.start_ms)).isoformat(),
        "attributes": [
            {"key": key, "value": event.attributes[key]} for key in sorted(event.attributes)
        ],
    }
