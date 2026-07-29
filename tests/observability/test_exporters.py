"""Trace-event serialization for the `.traces` sink path.

`.traces` carries `TraceEvent` protos, and none of the write transforms
`DefaultSinkResolver` resolves accept a proto message, so these encoders are
what make a configured `traces_to` reachable at all.
"""

from __future__ import annotations

from beam_agents._protos import TraceEvent
from beam_agents.observability import serialize_trace_event, trace_event_to_row

_EVENT = TraceEvent(
    trace_id=bytes(range(16)),
    span_id=bytes(range(8)),
    parent_span_id=bytes(range(8, 16)),
    entity_key=b"key-1",
    seq=7,
    step_index=2,
    event_type=TraceEvent.LLM_CALL,
    attributes={"gen_ai.request.model": "m-1", "beam_agents.cache_hit": "true"},
    start_ms=1_700_000_000_000,
    end_ms=1_700_000_000_000,
)


# --- Requirement: The traces output is deliverable to a configured sink ------


def test_message_bus_encoding_is_keyed_deterministic_bytes() -> None:
    # Scenario: A message-bus traces sink receives keyed deterministic bytes.
    key, payload = serialize_trace_event(_EVENT)

    assert key == b"key-1"
    decoded = TraceEvent()
    decoded.ParseFromString(payload)
    assert decoded == _EVENT


def test_message_bus_encoding_is_stable_across_calls() -> None:
    # Deterministic serialization matters here specifically: `attributes` is a
    # proto map, whose encoding order is otherwise undefined, and identical
    # events must produce identical bytes for downstream dedup to work.
    assert serialize_trace_event(_EVENT) == serialize_trace_event(_EVENT)


def test_bigquery_encoding_is_a_row_not_a_proto() -> None:
    # Scenario: A BigQuery traces sink receives rows, not protos.
    row = trace_event_to_row(_EVENT)

    assert row["trace_id"] == bytes(range(16)).hex()
    assert row["span_id"] == bytes(range(8)).hex()
    assert row["parent_span_id"] == bytes(range(8, 16)).hex()
    assert row["entity_key"] == b"key-1".hex()
    assert row["seq"] == 7
    assert row["step_index"] == 2
    assert row["event_type"] == "LLM_CALL"
    assert row["start_ms"] == 1_700_000_000_000
    assert row["end_ms"] == 1_700_000_000_000
    assert row["attributes"] == [
        {"key": "beam_agents.cache_hit", "value": "true"},
        {"key": "gen_ai.request.model", "value": "m-1"},
    ]


def test_bigquery_attribute_order_is_stable() -> None:
    # Sorted by key, so two encodings of one event are equal and a row diff is
    # readable.
    assert trace_event_to_row(_EVENT) == trace_event_to_row(_EVENT)


def test_an_empty_event_encodes_without_correlation_ids() -> None:
    # A bare event (nothing correlated, no attributes) must still round-trip:
    # hex of empty bytes is the empty string, not a crash.
    row = trace_event_to_row(TraceEvent())
    assert row["trace_id"] == ""
    assert row["event_type"] == "EVENT_TYPE_UNSPECIFIED"
    assert row["attributes"] == []
