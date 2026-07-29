"""Dead-letter encoding for the `.errors` sink path.

`.errors` carries `ActivationError` dataclasses, and none of the write
transforms `DefaultSinkResolver` resolves accept one, so these encoders are
what make a configured `errors_to` reachable at all — the same gap
`observability/exporters.py` closes for `.traces`.

The message-bus encoding wraps the record in an `AgentEnvelope`, which is what
makes the errors topic a valid `RunAgent` input stream (see `docs/errors.md`).
"""

from __future__ import annotations

import json

from beam_agents._protos import ActivationErrorRecord, AgentEnvelope, ToolIntent
from beam_agents.core.dofn import (
    REASON_ERROR,
    REASON_INTENT_DEAD_LETTER,
    REASON_ORPHANED,
    ActivationError,
)
from beam_agents.core.error_records import (
    activation_error_to_row,
    intent_dead_letter_to_error,
    serialize_error_envelope,
)

_ERROR = ActivationError(
    entity_key=b"key-1",
    reason=REASON_ERROR,
    detail="RuntimeError('boom') failed_at_step=2 after=ACTIVATION_START",
    event_time_ms=1_700_000_000_000,
)


# --- Requirement: Errors encode as AgentEnvelope-wrapped records --------------


def test_message_bus_encoding_round_trips_through_agent_envelope() -> None:
    # Scenario: Encoded record round-trips through AgentEnvelope.
    key, payload = serialize_error_envelope(_ERROR)

    assert key == b"key-1"
    envelope = AgentEnvelope()
    envelope.ParseFromString(payload)
    assert envelope.entity_key == b"key-1"
    assert envelope.event_time_ms == 1_700_000_000_000
    # The record travels as opaque `external_event` bytes: the errors topic is
    # shaped exactly like any other event stream entering RunAgent.
    assert envelope.WhichOneof("payload") == "external_event"

    record = ActivationErrorRecord()
    record.ParseFromString(envelope.external_event)
    assert record.entity_key == b"key-1"
    assert record.reason == REASON_ERROR
    assert record.detail == "RuntimeError('boom') failed_at_step=2 after=ACTIVATION_START"
    assert record.event_time_ms == 1_700_000_000_000


def test_message_bus_encoding_is_stable_across_calls() -> None:
    # Scenario: Encoding is deterministic. A retried bundle re-emits the same
    # dead letter; downstream dedup only collapses the two if their bytes match.
    assert serialize_error_envelope(_ERROR) == serialize_error_envelope(_ERROR)


def test_message_bus_encoding_keys_by_entity_key() -> None:
    # Keyed like the intents and traces writes, for the same reason: one key's
    # records keep their relative order through a single partition.
    other = ActivationError(b"key-2", REASON_ORPHANED, "no_continuation:ghost", 5)

    assert serialize_error_envelope(other)[0] == b"key-2"


def test_an_empty_detail_encodes_without_a_placeholder() -> None:
    # The timeout route carries no detail; the encoded record must say so
    # rather than invent a string a consumer would read as a real cause.
    _, payload = serialize_error_envelope(ActivationError(b"k", "activation_timeout"))

    envelope = AgentEnvelope()
    envelope.ParseFromString(payload)
    record = ActivationErrorRecord()
    record.ParseFromString(envelope.external_event)
    assert record.detail == ""
    assert record.event_time_ms == 0


# --- Requirement: Intent dead letters unify into the error record schema ------


def test_an_intent_dead_letter_maps_onto_the_shared_error_shape() -> None:
    # Scenario: A dead-lettered intent reaches the errors sink as a unified
    # record. The mapping is what lets one encoder cover both error streams.
    intent = ToolIntent(
        intent_id="intent-1",
        entity_key=b"key-1",
        seq=4,
        tool_name="http.post",
        created_at_ms=1_700_000_000_000,
    )

    error = intent_dead_letter_to_error(((b"key-1", intent), "boom"))

    assert error.entity_key == b"key-1"
    assert error.reason == REASON_INTENT_DEAD_LETTER
    # The intent's own serialization is what failed, so its identifying fields
    # travel as JSON in `detail` rather than being re-serialized.
    assert json.loads(error.detail) == {
        "reason": "boom",
        "intent_id": "intent-1",
        "seq": 4,
        "tool_name": "http.post",
    }
    # The intent's deterministic stamp, so a replay produces the same record.
    assert error.event_time_ms == 1_700_000_000_000


def test_a_mapped_intent_dead_letter_encodes_like_any_other_error() -> None:
    # Scenario: Dead letters and activation errors share one sink schema.
    intent = ToolIntent(intent_id="i", entity_key=b"k", seq=1, tool_name="t", created_at_ms=9)

    key, payload = serialize_error_envelope(intent_dead_letter_to_error(((b"k", intent), "boom")))

    assert key == b"k"
    envelope = AgentEnvelope()
    envelope.ParseFromString(payload)
    record = ActivationErrorRecord()
    record.ParseFromString(envelope.external_event)
    assert record.reason == REASON_INTENT_DEAD_LETTER
    assert record.event_time_ms == 9


def test_mapping_an_intent_dead_letter_is_deterministic() -> None:
    # The JSON detail must not vary between two runs of the same failure, or
    # the encoded records stop being byte-identical on replay.
    element = ((b"k", ToolIntent(intent_id="i", seq=2, tool_name="t")), "boom")

    assert intent_dead_letter_to_error(element) == intent_dead_letter_to_error(element)


# --- Requirement: Errors encode as row mappings for BigQuery sinks ------------


def test_bigquery_encoding_is_a_row_not_a_dataclass() -> None:
    # Scenario: Row carries all triage fields.
    row = activation_error_to_row(_ERROR)

    assert row == {
        "entity_key": b"key-1".hex(),
        "reason": REASON_ERROR,
        "detail": "RuntimeError('boom') failed_at_step=2 after=ACTIVATION_START",
        "event_time_ms": 1_700_000_000_000,
    }


def test_bigquery_encoding_is_stable_across_calls() -> None:
    assert activation_error_to_row(_ERROR) == activation_error_to_row(_ERROR)


def test_an_empty_key_encodes_to_an_empty_hex_string() -> None:
    # Hex of empty bytes is the empty string, not a crash.
    assert activation_error_to_row(ActivationError(b"", "r"))["entity_key"] == ""
