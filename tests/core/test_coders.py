"""Coder tests for the `proto-coders` capability.

Covers determinism (byte-identical repeated encoding, map insertion-order
independence), lossless round-trips, explicit/idempotent registration with no
import side effects, and end-to-end flow through a Beam shuffle boundary.
"""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import Mock

import apache_beam as beam
import pytest
from apache_beam.coders.coders import PickleCoder
from apache_beam.coders.typecoders import registry as coder_registry

# Aliased: the bare name "TestPipeline" would be mis-collected by pytest.
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from google.protobuf.message import Message
from hypothesis import given
from hypothesis import strategies as st

from beam_agents._protos import (
    AgentEnvelope,
    Continuation,
    LlmCacheBlob,
    MemoryBlob,
    ToolIntent,
    ToolResult,
    TraceEvent,
)
from beam_agents.core.coders import (
    MESSAGE_TYPES,
    DeterministicProtoCoder,
    register_coders,
)

# --- Hypothesis strategies for the seven message types -------------------------

# Protobuf strings must be valid UTF-8; the codec restriction excludes surrogates.
_text = st.text(alphabet=st.characters(codec="utf-8"), max_size=16)
_bytes = st.binary(max_size=32)
_int64 = st.integers(min_value=-(2**63), max_value=2**63 - 1)
_uint32 = st.integers(min_value=0, max_value=2**32 - 1)


@st.composite
def _memory_blobs(draw: st.DrawFn) -> MemoryBlob:
    blob = MemoryBlob(
        state_schema_version=draw(_uint32),
        total_value_bytes=draw(_int64),
    )
    for key, value, last_access in draw(st.lists(st.tuples(_text, _bytes, _int64), max_size=4)):
        blob.entries.add(key=key, value=value, last_access_ms=last_access)
    return blob


@st.composite
def _tool_intents(draw: st.DrawFn) -> ToolIntent:
    return ToolIntent(
        intent_id=draw(_text),
        entity_key=draw(_bytes),
        seq=draw(_int64),
        step_index=draw(_uint32),
        tool_name=draw(_text),
        args_json=draw(_text),
        created_at_ms=draw(_int64),
        expires_at_ms=draw(_int64),
        attempt=draw(_uint32),
    )


@st.composite
def _tool_results(draw: st.DrawFn) -> ToolResult:
    return ToolResult(
        intent_id=draw(_text),
        entity_key=draw(_bytes),
        seq=draw(_int64),
        status=draw(
            st.sampled_from(
                [
                    ToolResult.STATUS_UNSPECIFIED,
                    ToolResult.OK,
                    ToolResult.ERROR,
                    ToolResult.EXPIRED,
                    ToolResult.REJECTED,
                ]
            )
        ),
        payload=draw(_bytes),
        error_message=draw(_text),
        completed_at_ms=draw(_int64),
    )


@st.composite
def _trace_events(draw: st.DrawFn) -> TraceEvent:
    event = TraceEvent(
        trace_id=draw(_bytes),
        span_id=draw(_bytes),
        parent_span_id=draw(_bytes),
        entity_key=draw(_bytes),
        seq=draw(_int64),
        step_index=draw(_uint32),
        event_type=draw(
            st.sampled_from(
                [
                    TraceEvent.EVENT_TYPE_UNSPECIFIED,
                    TraceEvent.ACTIVATION_START,
                    TraceEvent.LLM_CALL,
                    TraceEvent.TOOL_CALL,
                    TraceEvent.INTENT_EMITTED,
                    TraceEvent.ACTIVATION_END,
                    TraceEvent.ERROR,
                ]
            )
        ),
        start_ms=draw(_int64),
        end_ms=draw(_int64),
    )
    for key, value in draw(st.dictionaries(_text, _text, max_size=5)).items():
        event.attributes[key] = value
    return event


@st.composite
def _agent_envelopes(draw: st.DrawFn) -> AgentEnvelope:
    envelope = AgentEnvelope(entity_key=draw(_bytes), event_time_ms=draw(_int64))
    variant = draw(st.sampled_from(["none", "external", "result", "approval"]))
    if variant == "external":
        envelope.external_event = draw(_bytes)
    elif variant == "result":
        envelope.tool_result.CopyFrom(draw(_tool_results()))
    elif variant == "approval":
        envelope.approval.intent_id = draw(_text)
        envelope.approval.approved = draw(st.booleans())
        envelope.approval.approver = draw(_text)
        envelope.approval.decided_at_ms = draw(_int64)
    return envelope


@st.composite
def _llm_cache_blobs(draw: st.DrawFn) -> LlmCacheBlob:
    blob = LlmCacheBlob(
        state_schema_version=draw(_uint32),
        total_response_bytes=draw(_int64),
    )
    for cache_key, response, digest, created, accessed, digest_only in draw(
        st.lists(
            st.tuples(_text, _bytes, _bytes, _int64, _int64, st.booleans()),
            max_size=4,
        )
    ):
        blob.entries.add(
            cache_key=cache_key,
            response=response,
            response_digest=digest,
            created_at_ms=created,
            last_access_ms=accessed,
            digest_only=digest_only,
        )
    return blob


@st.composite
def _continuations(draw: st.DrawFn) -> Continuation:
    return Continuation(
        state_schema_version=draw(_uint32),
        seq=draw(_int64),
        step_index=draw(_uint32),
        pending_intent_ids=draw(st.lists(_text, max_size=4)),
        adapter=draw(_text),
        snapshot=draw(_bytes),
        suspended_at_ms=draw(_int64),
        deadline_ms=draw(_int64),
    )


_STRATEGIES = {
    MemoryBlob: _memory_blobs(),
    ToolIntent: _tool_intents(),
    ToolResult: _tool_results(),
    TraceEvent: _trace_events(),
    AgentEnvelope: _agent_envelopes(),
    Continuation: _continuations(),
    LlmCacheBlob: _llm_cache_blobs(),
}
_ANY_MESSAGE = st.one_of(*_STRATEGIES.values())


def _sample_pair(message_type: type[Message]) -> tuple[Message, Message]:
    """Two distinct concrete instances of a message type for pipeline tests."""
    trace = TraceEvent(seq=5, event_type=TraceEvent.LLM_CALL)
    trace.attributes["gen_ai.request.model"] = "claude-opus-4-8"
    samples: dict[type[Message], tuple[Message, Message]] = {
        MemoryBlob: (
            MemoryBlob(state_schema_version=1),
            MemoryBlob(state_schema_version=1, total_value_bytes=4),
        ),
        ToolIntent: (
            ToolIntent(intent_id="a", tool_name="t", seq=1),
            ToolIntent(intent_id="b", tool_name="t", seq=2),
        ),
        ToolResult: (
            ToolResult(intent_id="a", status=ToolResult.OK),
            ToolResult(intent_id="b", status=ToolResult.ERROR, error_message="boom"),
        ),
        TraceEvent: (TraceEvent(seq=1), trace),
        AgentEnvelope: (
            AgentEnvelope(entity_key=b"k", external_event=b"e"),
            AgentEnvelope(entity_key=b"k", tool_result=ToolResult(intent_id="a")),
        ),
        Continuation: (
            Continuation(state_schema_version=1, adapter="langgraph", snapshot=b"s1"),
            Continuation(state_schema_version=1, adapter="langgraph", snapshot=b"s2"),
        ),
        LlmCacheBlob: (
            LlmCacheBlob(
                state_schema_version=1,
                entries=[LlmCacheBlob.LlmCacheEntry(cache_key="a", response=b"r1")],
            ),
            LlmCacheBlob(
                state_schema_version=1,
                entries=[
                    LlmCacheBlob.LlmCacheEntry(
                        cache_key="b", response=b"", digest_only=True, response_digest=b"d"
                    )
                ],
            ),
        ),
    }
    return samples[message_type]


@pytest.fixture
def clean_registry() -> Iterator[None]:
    """Snapshot and restore the global coder registry so tests are isolated."""
    saved = dict(coder_registry._coders)
    try:
        yield
    finally:
        coder_registry._coders = saved


# --- Coverage: the module supports exactly the seven wire/state messages -----


def test_message_types_covers_all_seven_wire_messages() -> None:
    # Independent of coders.MESSAGE_TYPES iteration order: the module must
    # support exactly the seven wire/state message classes, including the
    # replay-cache blob.
    assert set(MESSAGE_TYPES) == {
        MemoryBlob,
        ToolIntent,
        ToolResult,
        TraceEvent,
        AgentEnvelope,
        Continuation,
        LlmCacheBlob,
    }


# --- Requirement: Deterministic proto coder ----------------------------------


@given(message=_ANY_MESSAGE)
def test_repeated_encoding_is_byte_identical(message: Message) -> None:
    # Scenario: Repeated encoding is byte-identical.
    coder = DeterministicProtoCoder(type(message))
    # Encode a second, distinct object of equal value to exercise the coder,
    # not just re-serialization of one object.
    twin = type(message)()
    twin.CopyFrom(message)
    assert coder.encode(message) == coder.encode(twin)


def test_encoding_requests_deterministic_protobuf_serialization() -> None:
    message = Mock(spec=Message)
    message.SerializeToString.return_value = b"wire"

    assert DeterministicProtoCoder(ToolIntent).encode(message) == b"wire"
    message.SerializeToString.assert_called_once_with(deterministic=True)


def test_map_insertion_order_does_not_affect_encoding() -> None:
    # Scenario: Map insertion order does not affect encoding.
    items = [
        ("gen_ai.request.model", "claude-opus-4-8"),
        ("gen_ai.usage.input_tokens", "1234"),
        ("gen_ai.usage.output_tokens", "567"),
        ("gen_ai.response.id", "resp-abc"),
    ]
    forward = TraceEvent()
    for key, value in items:
        forward.attributes[key] = value
    reverse = TraceEvent()
    for key, value in reversed(items):
        reverse.attributes[key] = value

    coder = DeterministicProtoCoder(TraceEvent)
    assert coder.encode(forward) == coder.encode(reverse)


# --- Requirement: Lossless round-trip ----------------------------------------


@given(message=_ANY_MESSAGE)
def test_round_trip_equality(message: Message) -> None:
    # Scenario: Round-trip equality for all seven types (incl. defaults, oneof, bytes).
    coder = DeterministicProtoCoder(type(message))
    assert coder.decode(coder.encode(message)) == message


@pytest.mark.parametrize("message_type", MESSAGE_TYPES)
def test_default_instance_round_trips(message_type: type[Message]) -> None:
    # Empty/default message is a valid round-trip case.
    coder = DeterministicProtoCoder(message_type)
    empty = message_type()
    assert coder.encode(empty) == b""
    assert coder.decode(coder.encode(empty)) == empty


# --- Requirement: Coder advertises determinism -------------------------------


@pytest.mark.parametrize("message_type", MESSAGE_TYPES)
def test_coder_reports_deterministic(message_type: type[Message]) -> None:
    assert DeterministicProtoCoder(message_type).is_deterministic() is True


# --- Coder identity: equality, hashing, and type-hint construction -----------


def test_coder_equality_and_hash_track_message_type() -> None:
    same_a = DeterministicProtoCoder(ToolIntent)
    same_b = DeterministicProtoCoder(ToolIntent)
    other = DeterministicProtoCoder(ToolResult)

    assert same_a == same_b
    assert hash(same_a) == hash(same_b) == hash(ToolIntent)
    assert hash(other) == hash(ToolResult)
    assert hash(same_a) != hash(other)
    assert same_a != other
    assert same_a != "not a coder"


def test_from_type_hint_builds_coder_for_message_type() -> None:
    coder = DeterministicProtoCoder.from_type_hint(ToolIntent, coder_registry)
    assert isinstance(coder, DeterministicProtoCoder)
    assert coder.to_type_hint() is ToolIntent


def test_from_type_hint_rejects_non_message_type() -> None:
    with pytest.raises(ValueError, match="Expected a subclass"):
        # Deliberately wrong type to exercise the runtime guard.
        DeterministicProtoCoder.from_type_hint(int, coder_registry)  # type: ignore[arg-type]


# --- Requirement: Explicit registration, no import side effects --------------


def test_import_alone_does_not_register(clean_registry: None) -> None:
    # Scenario: Import alone does not register.
    # `beam_agents.core.coders` is already imported at module top; without
    # calling register_coders(), the registry must not resolve our coder.
    for message_type in MESSAGE_TYPES:
        resolved = coder_registry.get_coder(message_type)
        assert not isinstance(resolved, DeterministicProtoCoder)


def test_registry_resolves_deterministic_coder_after_registration(
    clean_registry: None,
) -> None:
    # Scenario: Registry resolves the deterministic coder after registration.
    register_coders()
    for message_type in MESSAGE_TYPES:
        resolved = coder_registry.get_coder(message_type)
        assert isinstance(resolved, DeterministicProtoCoder)
        assert not isinstance(resolved, PickleCoder)
        assert resolved.is_deterministic() is True


def test_double_registration_is_harmless(clean_registry: None) -> None:
    # Scenario: Double registration is harmless.
    register_coders()
    register_coders()
    for message_type in MESSAGE_TYPES:
        assert isinstance(coder_registry.get_coder(message_type), DeterministicProtoCoder)


# --- Requirement: Pipeline elements never fall back to pickle -----------------


@pytest.mark.parametrize("message_type", MESSAGE_TYPES)
def test_message_flows_through_shuffle_boundary(
    message_type: type[Message], clean_registry: None
) -> None:
    # Scenario: Elements round-trip through a TestPipeline (GroupByKey shuffle).
    register_coders()
    a, b = _sample_pair(message_type)
    # Registered coder is the deterministic proto coder, not a pickle fallback.
    assert isinstance(coder_registry.get_coder(message_type), DeterministicProtoCoder)

    with BeamTestPipeline() as pipeline:
        grouped = (
            pipeline
            | beam.Create([(b"key", a), (b"key", b)])
            | beam.GroupByKey()
            | beam.FlatMap(lambda kv: list(kv[1]))
        )
        assert_that(grouped, equal_to([a, b]))


def test_message_works_as_group_by_key_key(clean_registry: None) -> None:
    # Scenario: Message type works as a GroupByKey key.
    register_coders()
    intent = ToolIntent(intent_id="k", tool_name="http.post", seq=1)

    with BeamTestPipeline() as pipeline:
        totals = (
            pipeline
            | beam.Create([(intent, 1), (intent, 1), (intent, 1)])
            | beam.GroupByKey()
            | beam.MapTuple(lambda key, values: (key.intent_id, sum(values)))
        )
        assert_that(totals, equal_to([("k", 3)]))
