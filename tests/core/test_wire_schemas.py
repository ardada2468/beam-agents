"""Wire-schema round-trip and forward-compat tests for beam_agents.v1.

Covers the `wire-schemas` capability scenarios: importability, per-message
round-trips, oneof exclusivity, and unknown-field forward compatibility.
"""

from __future__ import annotations

import beam_agents._protos as protos
from beam_agents._protos import (
    ActivationErrorRecord,
    AgentEnvelope,
    Continuation,
    LlmCacheBlob,
    MemoryBlob,
    ToolIntent,
    ToolResult,
    TraceEvent,
)


def _varint(value: int) -> bytes:
    """Minimal protobuf base-128 varint encoder (test helper)."""
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _unknown_field(field_number: int, value: int) -> bytes:
    """Encode a varint field (wire type 0) for a field number our schema lacks."""
    tag = (field_number << 3) | 0
    return _varint(tag) + _varint(value)


# --- Requirement: Proto package and committed generation ---------------------


def test_all_nine_message_classes_importable() -> None:
    # Scenario: Bindings are importable from the installed package.
    for name in (
        "MemoryBlob",
        "ToolIntent",
        "ToolResult",
        "TraceEvent",
        "AgentEnvelope",
        "Continuation",
        "LlmCacheBlob",
        "ActivationErrorRecord",
        "LongTermRecord",
    ):
        assert hasattr(protos, name)
    assert set(protos.__all__) == {
        "MemoryBlob",
        "ToolIntent",
        "ToolResult",
        "TraceEvent",
        "AgentEnvelope",
        "Continuation",
        "LlmCacheBlob",
        "ActivationErrorRecord",
        "LongTermRecord",
    }


# --- Requirement: MemoryBlob carries versioned, LRU-orderable memory ---------


def test_memory_blob_entries_round_trip_in_insertion_order() -> None:
    # Scenario: Entries round-trip in insertion order.
    blob = MemoryBlob(state_schema_version=1, total_value_bytes=3)
    for i, key in enumerate(("a", "b", "c")):
        blob.entries.add(key=key, value=key.encode(), last_access_ms=1000 + i)

    parsed = MemoryBlob()
    parsed.ParseFromString(blob.SerializeToString())

    assert [e.key for e in parsed.entries] == ["a", "b", "c"]
    assert [e.value for e in parsed.entries] == [b"a", b"b", b"c"]
    assert [e.last_access_ms for e in parsed.entries] == [1000, 1001, 1002]
    assert parsed.state_schema_version == 1


def test_memory_blob_schema_version_zero_is_distinguishable() -> None:
    # Scenario: Schema version defaults are explicit.
    versioned = MemoryBlob(state_schema_version=1)
    unversioned = MemoryBlob()

    parsed_unversioned = MemoryBlob()
    parsed_unversioned.ParseFromString(unversioned.SerializeToString())

    assert versioned.state_schema_version == 1
    assert parsed_unversioned.state_schema_version == 0


# --- Requirement: LlmCacheBlob carries versioned, LRU-orderable cache entries -


def test_llm_cache_entries_round_trip_in_insertion_order() -> None:
    # Scenario: Cache entries round-trip in insertion order.
    blob = LlmCacheBlob(state_schema_version=1, total_response_bytes=6)
    for i, key in enumerate(("a", "b", "c")):
        blob.entries.add(
            cache_key=key,
            response=key.encode(),
            response_digest=(key * 2).encode(),
            created_at_ms=1000 + i,
            last_access_ms=2000 + i,
            digest_only=False,
        )

    parsed = LlmCacheBlob()
    parsed.ParseFromString(blob.SerializeToString())

    assert [e.cache_key for e in parsed.entries] == ["a", "b", "c"]
    assert [e.response for e in parsed.entries] == [b"a", b"b", b"c"]
    assert [e.response_digest for e in parsed.entries] == [b"aa", b"bb", b"cc"]
    assert [e.created_at_ms for e in parsed.entries] == [1000, 1001, 1002]
    assert [e.last_access_ms for e in parsed.entries] == [2000, 2001, 2002]
    assert parsed.state_schema_version == 1
    assert parsed.total_response_bytes == 6


def test_llm_cache_digest_only_entry_representable() -> None:
    # Scenario: Digest-only entries are representable.
    digest = bytes(range(32))
    blob = LlmCacheBlob(state_schema_version=1)
    blob.entries.add(
        cache_key="k",
        response=b"",
        response_digest=digest,
        created_at_ms=5,
        last_access_ms=5,
        digest_only=True,
    )

    parsed = LlmCacheBlob()
    parsed.ParseFromString(blob.SerializeToString())

    entry = parsed.entries[0]
    assert entry.digest_only is True
    assert entry.response == b""
    assert entry.response_digest == digest


# --- Requirement: ToolIntent carries deterministic identity and expiry -------


def test_tool_intent_all_fields_round_trip() -> None:
    # Scenario: All identity fields round-trip (incl. exact args_json).
    args_json = '{"a":1,"b":"two","nested":{"z":true}}'
    intent = ToolIntent(
        intent_id="11111111-2222-5333-8444-555555555555",
        entity_key=b"\x00\x01\xff",
        seq=42,
        step_index=7,
        tool_name="http.post",
        args_json=args_json,
        created_at_ms=1_700_000_000_000,
        expires_at_ms=1_700_000_060_000,
        attempt=2,
        kind=ToolIntent.TOOL,
    )

    parsed = ToolIntent()
    parsed.ParseFromString(intent.SerializeToString())

    assert parsed == intent
    assert parsed.args_json == args_json
    assert parsed.kind == ToolIntent.TOOL


def test_tool_intent_expiry_representable() -> None:
    # Scenario: Expiry is representable for fail-closed enforcement.
    intent = ToolIntent(expires_at_ms=1_700_000_060_000)
    parsed = ToolIntent()
    parsed.ParseFromString(intent.SerializeToString())
    assert parsed.expires_at_ms == 1_700_000_060_000


def test_approval_intent_is_distinguishable_without_reading_tool_name() -> None:
    # Scenario: Approval intents are distinguishable on the wire.
    approval = ToolIntent(intent_id="a", tool_name="approval", kind=ToolIntent.APPROVAL)
    tool = ToolIntent(intent_id="t", tool_name="approval", kind=ToolIntent.TOOL)

    parsed_approval = ToolIntent()
    parsed_approval.ParseFromString(approval.SerializeToString())
    parsed_tool = ToolIntent()
    parsed_tool.ParseFromString(tool.SerializeToString())

    # Same tool_name on both: only `kind` separates them.
    assert parsed_approval.kind == ToolIntent.APPROVAL
    assert parsed_tool.kind == ToolIntent.TOOL
    assert parsed_approval.tool_name == parsed_tool.tool_name


def test_intent_without_kind_reads_as_unspecified_tool_call() -> None:
    # Scenario: An intent written before kind existed reads as a tool call.
    # An unset proto3 scalar is absent from the wire, so bytes produced before
    # `kind` existed are exactly these bytes.
    legacy = ToolIntent(intent_id="legacy", tool_name="http.post").SerializeToString()

    parsed = ToolIntent()
    parsed.ParseFromString(legacy)

    assert parsed.kind == ToolIntent.TOOL_KIND_UNSPECIFIED
    assert parsed.kind != ToolIntent.APPROVAL


# --- Requirement: ToolResult correlates outcomes with terminal statuses ------


def test_tool_result_every_status_round_trips() -> None:
    # Scenario: Every status value is representable.
    statuses = [
        ToolResult.OK,
        ToolResult.ERROR,
        ToolResult.EXPIRED,
        ToolResult.REJECTED,
    ]
    for status in statuses:
        result = ToolResult(intent_id="i", status=status, payload=b"p")
        parsed = ToolResult()
        parsed.ParseFromString(result.SerializeToString())
        assert parsed.status == status

    unset = ToolResult()
    assert unset.status == ToolResult.STATUS_UNSPECIFIED


# --- Requirement: TraceEvent aligns with OTel GenAI conventions --------------


def test_trace_event_genai_attributes_round_trip() -> None:
    # Scenario: GenAI attributes survive round-trip.
    event = TraceEvent(
        trace_id=b"\x01" * 16,
        span_id=b"\x02" * 8,
        parent_span_id=b"\x03" * 8,
        entity_key=b"user-1",
        seq=1,
        step_index=0,
        event_type=TraceEvent.LLM_CALL,
        start_ms=1_700_000_000_000,
        end_ms=1_700_000_000_500,
    )
    event.attributes["gen_ai.request.model"] = "claude-opus-4-8"
    event.attributes["gen_ai.usage.input_tokens"] = "1234"

    parsed = TraceEvent()
    parsed.ParseFromString(event.SerializeToString())

    assert parsed.event_type == TraceEvent.LLM_CALL
    assert dict(parsed.attributes) == {
        "gen_ai.request.model": "claude-opus-4-8",
        "gen_ai.usage.input_tokens": "1234",
    }


# --- Requirement: AgentEnvelope is the single keyed input type ---------------


def test_agent_envelope_oneof_is_exclusive() -> None:
    # Scenario: Exactly one payload variant is set.
    envelope = AgentEnvelope(entity_key=b"k", event_time_ms=5)
    envelope.tool_result.intent_id = "i"
    assert envelope.WhichOneof("payload") == "tool_result"

    envelope.approval.approved = True
    assert envelope.WhichOneof("payload") == "approval"
    assert not envelope.HasField("tool_result")


def test_agent_envelope_all_three_variants_round_trip() -> None:
    # Scenario: All three variants round-trip.
    external = AgentEnvelope(entity_key=b"k", event_time_ms=1, external_event=b"raw-bytes")
    result_env = AgentEnvelope(
        entity_key=b"k",
        event_time_ms=2,
        tool_result=ToolResult(intent_id="i", status=ToolResult.OK),
    )
    approval_env = AgentEnvelope(
        entity_key=b"k",
        event_time_ms=3,
        approval=AgentEnvelope.Approval(
            intent_id="i", approved=True, approver="alice", decided_at_ms=99
        ),
    )

    for env, expected_case in (
        (external, "external_event"),
        (result_env, "tool_result"),
        (approval_env, "approval"),
    ):
        parsed = AgentEnvelope()
        parsed.ParseFromString(env.SerializeToString())
        assert parsed.WhichOneof("payload") == expected_case
        assert parsed == env


# --- Requirement: ActivationErrorRecord carries dead-letter triage fields -----


def test_activation_error_record_all_fields_round_trip() -> None:
    # Scenario: All fields round-trip.
    record = ActivationErrorRecord(
        entity_key=b"\x00key\xff",
        reason="activation_error",
        detail="ValueError('boom') at step 2",
        event_time_ms=1_700_000_000_123,
    )

    parsed = ActivationErrorRecord()
    parsed.ParseFromString(record.SerializeToString())

    assert parsed.entity_key == b"\x00key\xff"
    assert parsed.reason == "activation_error"
    assert parsed.detail == "ValueError('boom') at step 2"
    assert parsed.event_time_ms == 1_700_000_000_123


def test_activation_error_record_deterministic_serialization_is_byte_stable() -> None:
    # Scenario: Deterministic serialization is byte-stable.
    record = ActivationErrorRecord(
        entity_key=b"k",
        reason="orphaned_result",
        detail="unknown_intent",
        event_time_ms=42,
    )

    assert record.SerializeToString(deterministic=True) == record.SerializeToString(
        deterministic=True
    )


def test_activation_error_record_travels_inside_agent_envelope() -> None:
    # Scenario: All fields round-trip -- the errors-sink convention carries the
    # record as opaque `external_event` bytes, leaving AgentEnvelope unchanged.
    record = ActivationErrorRecord(
        entity_key=b"k", reason="activation_timeout", detail="", event_time_ms=7
    )
    envelope = AgentEnvelope(
        entity_key=b"k",
        event_time_ms=7,
        external_event=record.SerializeToString(deterministic=True),
    )

    parsed_envelope = AgentEnvelope()
    parsed_envelope.ParseFromString(envelope.SerializeToString(deterministic=True))
    parsed_record = ActivationErrorRecord()
    parsed_record.ParseFromString(parsed_envelope.external_event)

    assert parsed_envelope.WhichOneof("payload") == "external_event"
    assert parsed_record == record


# --- Requirement: Continuation persists framework-opaque resume state --------


def test_continuation_round_trips_with_byte_identical_snapshot() -> None:
    # Scenario: Suspension state round-trips.
    snapshot = bytes(range(256)) * 4  # arbitrary opaque bytes incl. 0x00
    cont = Continuation(
        state_schema_version=1,
        seq=10,
        step_index=3,
        pending_intent_ids=["a", "b", "c"],
        adapter="langgraph",
        snapshot=snapshot,
        suspended_at_ms=1_700_000_000_000,
        deadline_ms=1_700_000_600_000,
    )

    parsed = Continuation()
    parsed.ParseFromString(cont.SerializeToString())

    assert parsed == cont
    assert parsed.snapshot == snapshot
    assert list(parsed.pending_intent_ids) == ["a", "b", "c"]


def test_continuation_escalations_round_trips_and_defaults_to_zero() -> None:
    # Scenario: Escalation count round-trips and defaults to zero.
    legacy = Continuation(seq=10, deadline_ms=1_700_000_600_000).SerializeToString()
    parsed_legacy = Continuation()
    parsed_legacy.ParseFromString(legacy)
    assert parsed_legacy.escalations == 0

    escalated = Continuation(seq=10, deadline_ms=1_700_000_600_000, escalations=3)
    parsed = Continuation()
    parsed.ParseFromString(escalated.SerializeToString())
    assert parsed.escalations == 3


# --- Requirement: Schema evolution is additive and golden-blob guarded -------
# (forward-compat halves of the unknown-field scenarios)


def test_unknown_fields_are_tolerated_on_decode() -> None:
    # Scenario: Unknown fields are tolerated on decode.
    known = ToolIntent(intent_id="keep-me", seq=7, tool_name="t")
    wire = known.SerializeToString() + _unknown_field(500, 42)

    parsed = ToolIntent()
    parsed.ParseFromString(wire)  # must not raise

    assert parsed.intent_id == "keep-me"
    assert parsed.seq == 7
    assert parsed.tool_name == "t"


def test_unknown_fields_survive_re_encode() -> None:
    # Scenario: Unknown fields survive re-encode.
    unknown = _unknown_field(500, 42)
    wire = ToolIntent(intent_id="keep-me").SerializeToString() + unknown

    parsed = ToolIntent()
    parsed.ParseFromString(wire)
    reencoded = parsed.SerializeToString()

    assert unknown in reencoded


def test_llm_cache_blob_tolerates_and_preserves_unknown_fields() -> None:
    # Scenario: Unknown fields are tolerated on decode / survive re-encode,
    # extended to the newly added LlmCacheBlob type.
    known = LlmCacheBlob(state_schema_version=1, total_response_bytes=3)
    known.entries.add(cache_key="k", response=b"abc", digest_only=False)
    unknown = _unknown_field(500, 42)
    wire = known.SerializeToString() + unknown

    parsed = LlmCacheBlob()
    parsed.ParseFromString(wire)  # must not raise

    assert parsed.state_schema_version == 1
    assert parsed.entries[0].cache_key == "k"
    assert parsed.entries[0].response == b"abc"
    assert unknown in parsed.SerializeToString()
