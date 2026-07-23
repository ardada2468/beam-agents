"""Token usage accounting tests for the `model-facade` capability.

Covers: cache-missing calls accumulate billed usage, a cache hit reports usage
on its result without billing it, and a failed call stages no usage.
"""

from __future__ import annotations

import pytest

from beam_agents.model import FakeLLM, LlmRequest, ProviderTimeout, raise_error, respond_with
from beam_agents.model.facade import RetryPolicy

from ._facade_helpers import decode_len_based, make_facade

_REQUEST = LlmRequest(
    model_id="m-1", messages=[{"role": "user"}], tools_schema=[], sampling_params={}
)


# --- Requirement: Token usage accounting -------------------------------------


async def test_provider_calls_accumulate_billed_usage() -> None:
    # Scenario: A provider call accumulates billed usage.
    fake = FakeLLM(
        [
            (lambda r: r.messages == [{"role": "a"}], respond_with(b"aaaa")),
            (lambda r: r.messages == [{"role": "b"}], respond_with(b"bb")),
        ]
    )
    facade, staging = make_facade(fake)

    result_a = await facade.complete(
        LlmRequest(model_id="m-1", messages=[{"role": "a"}], tools_schema=[], sampling_params={}),
        entity_key=b"key-1",
        seq=0,
        step_index=0,
    )
    result_b = await facade.complete(
        LlmRequest(model_id="m-1", messages=[{"role": "b"}], tools_schema=[], sampling_params={}),
        entity_key=b"key-1",
        seq=1,
        step_index=0,
    )

    expected_a = decode_len_based(b"aaaa").usage
    expected_b = decode_len_based(b"bb").usage
    assert result_a.usage == expected_a
    assert result_b.usage == expected_b

    assert staging.usages == [expected_a, expected_b]
    total_prompt = sum(u.prompt_tokens for u in staging.usages)
    assert total_prompt == expected_a.prompt_tokens + expected_b.prompt_tokens


async def test_a_cache_hit_reports_but_does_not_bill_usage() -> None:
    # Scenario: A cache hit reports but does not bill usage.
    fake = FakeLLM([(lambda _r: True, respond_with(b"payload"))])
    facade, staging = make_facade(fake)

    await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)
    assert len(staging.usages) == 1

    cached_result = await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)

    assert cached_result.cache_hit is True
    assert cached_result.usage == decode_len_based(b"payload").usage
    assert len(staging.usages) == 1  # unchanged: no billed entry for the hit


async def test_a_failed_call_stages_no_usage() -> None:
    # Scenario: no billed usage is recorded when the transport ultimately fails.
    fake = FakeLLM([(lambda _r: True, raise_error(ProviderTimeout()))])
    facade, staging = make_facade(
        fake, retry_policy=RetryPolicy(max_attempts=1, base_ms=1, max_ms=1)
    )

    with pytest.raises(ProviderTimeout):
        await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)

    assert staging.usages == []
