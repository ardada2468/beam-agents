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

from typing import Any

from beam_agents._protos import TraceEvent


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
    which keeps two encodings of one event equal.
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
        "attributes": [
            {"key": key, "value": event.attributes[key]} for key in sorted(event.attributes)
        ],
    }
