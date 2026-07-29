"""Per-call trace emission tests for the `model-facade` capability.

Covers: a successful call stages exactly one `LLM_CALL` `TraceEvent` with
usage/attempt/model attributes, a cache hit records the hit outcome with zero
attempts, and a call that ultimately raises still stages a failure trace.
"""

from __future__ import annotations

import pytest

from beam_agents._protos import TraceEvent
from beam_agents.model import FakeLLM, LlmRequest, ProviderTimeout, raise_error, respond_with
from beam_agents.model.facade import RetryPolicy

from ._facade_helpers import make_facade

_REQUEST = LlmRequest(
    model_id="m-1", messages=[{"role": "user"}], tools_schema=[], sampling_params={}
)


# --- Requirement: Per-call trace emission ------------------------------------


async def test_a_successful_call_emits_one_llm_call_trace() -> None:
    # Scenario: A successful call emits one LLM_CALL trace.
    fake = FakeLLM([(lambda _r: True, respond_with(b"hello"))])
    facade, staging = make_facade(fake)

    await facade.complete(_REQUEST, entity_key=b"key-1", seq=3, step_index=1)

    assert len(staging.trace_events) == 1
    event = staging.trace_events[0]
    assert event.event_type == TraceEvent.LLM_CALL
    assert event.entity_key == b"key-1"
    assert event.seq == 3
    assert event.step_index == 1
    assert event.attributes["gen_ai.request.model"] == "m-1"
    assert event.attributes["gen_ai.usage.input_tokens"] == "5"  # len(b"hello")
    assert event.attributes["gen_ai.usage.output_tokens"] == "5"
    assert event.attributes["beam_agents.cache_hit"] == "false"
    assert event.attributes["beam_agents.attempts"] == "1"
    assert "error.type" not in event.attributes


async def test_a_cache_hit_is_recorded_in_the_trace_attributes() -> None:
    # Scenario: A cache hit is recorded in the trace attributes.
    fake = FakeLLM([(lambda _r: True, respond_with(b"hello"))])
    facade, staging = make_facade(fake)

    await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)
    await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)

    assert len(staging.trace_events) == 2
    hit_event = staging.trace_events[1]
    assert hit_event.attributes["beam_agents.cache_hit"] == "true"
    assert hit_event.attributes["beam_agents.attempts"] == "0"


async def test_a_cache_hit_reports_the_stored_responses_real_token_counts() -> None:
    # Scenario: A cache hit reports the stored response's real token counts.
    # The stored bytes carry the usage the provider reported when the call was
    # first served, so a hit has no excuse to report them as absent or zero.
    fake = FakeLLM([(lambda _r: True, respond_with(b"0123456789"))])
    facade, staging = make_facade(fake)

    await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)
    await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)

    miss, hit = staging.trace_events
    assert miss.attributes["gen_ai.usage.input_tokens"] == "10"  # len(b"0123456789")
    assert hit.attributes["gen_ai.usage.input_tokens"] == "10"
    assert hit.attributes["gen_ai.usage.output_tokens"] == "10"
    # Real tokens, already paid for: reported, but not billed again.
    assert miss.attributes["beam_agents.billed"] == "true"
    assert hit.attributes["beam_agents.billed"] == "false"


async def test_unknown_usage_is_omitted_rather_than_reported_as_zero() -> None:
    # Scenario: Unknown usage is omitted rather than reported as zero.
    # Nothing was decoded here, so "0" would be a lie a consumer summing the
    # attribute cannot detect.
    fake = FakeLLM([(lambda _r: True, raise_error(ProviderTimeout()))])
    facade, staging = make_facade(
        fake, retry_policy=RetryPolicy(max_attempts=1, base_ms=1, max_ms=1)
    )

    with pytest.raises(ProviderTimeout):
        await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)

    event = staging.trace_events[0]
    assert "gen_ai.usage.input_tokens" not in event.attributes
    assert "gen_ai.usage.output_tokens" not in event.attributes
    assert event.attributes["beam_agents.billed"] == "false"


async def test_the_facade_stages_uncorrelated_events() -> None:
    # Scenario: The facade stages uncorrelated events. Identity is the staging
    # sink's job (design D3), so `complete` carries no correlation arguments
    # and the event leaves here bare.
    fake = FakeLLM([(lambda _r: True, respond_with(b"hello"))])
    facade, staging = make_facade(fake)

    await facade.complete(_REQUEST, entity_key=b"key-1", seq=3, step_index=1)

    event = staging.trace_events[0]
    assert event.trace_id == b""
    assert event.span_id == b""
    assert event.parent_span_id == b""
    assert event.attributes["gen_ai.operation.name"] == "chat"


async def test_a_failed_call_still_emits_a_trace() -> None:
    # Scenario: A failed call still emits a trace.
    fake = FakeLLM([(lambda _r: True, raise_error(ProviderTimeout()))])
    facade, staging = make_facade(
        fake, retry_policy=RetryPolicy(max_attempts=2, base_ms=1, max_ms=1)
    )

    with pytest.raises(ProviderTimeout):
        await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)

    assert len(staging.trace_events) == 1
    event = staging.trace_events[0]
    assert event.attributes["beam_agents.attempts"] == "2"
    assert event.attributes["error.type"] == "ProviderTimeout"
