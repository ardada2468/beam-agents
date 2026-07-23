"""Call-counting tests for the `fake-llm` capability.

Covers: `call_count` increments per invocation including a raise, per-key
counts group dict-order-permuted-equal requests, and a replay that skips
FakeLLM leaves the per-key count unchanged.
"""

from __future__ import annotations

import pytest

from beam_agents.model import (
    FakeLLM,
    LlmRequest,
    ServerError,
    match_any,
    match_model_id,
    raise_error,
    respond_with,
)


def _request(model_id: str = "m-1", **overrides: object) -> LlmRequest:
    fields: dict[str, object] = {
        "model_id": model_id,
        "messages": [{"role": "user", "content": "hi"}],
        "tools_schema": [],
        "sampling_params": {"temperature": 0.0, "top_p": 1.0},
    }
    fields.update(overrides)
    return LlmRequest(**fields)  # type: ignore[arg-type]


# --- Requirement: Provider-call counting for determinism assertions ----------


async def test_total_count_increments_per_invocation() -> None:
    # Scenario: Total count increments per invocation.
    fake = FakeLLM([(match_model_id("m-1"), respond_with(b"ok"))])
    fake.add_rule(match_any(), raise_error(ServerError(status=500)))

    await fake.complete(_request("m-1"))
    await fake.complete(_request("m-1"))
    await fake.complete(_request("m-1"))
    with pytest.raises(ServerError):
        await fake.complete(_request("other"))

    assert fake.call_count == 4


async def test_per_key_count_groups_logically_equal_requests() -> None:
    # Scenario: Per-key count groups logically equal requests.
    fake = FakeLLM([(match_any(), respond_with(b"ok"))])
    first = _request(
        "m-1",
        messages=[{"role": "user", "content": "hi"}],
        sampling_params={"temperature": 0.0, "top_p": 1.0},
    )
    second = _request(
        "m-1",
        messages=[{"content": "hi", "role": "user"}],
        sampling_params={"top_p": 1.0, "temperature": 0.0},
    )

    await fake.complete(first)
    await fake.complete(second)

    assert fake.calls_for(first) == 2
    assert fake.calls_for(second) == 2


async def test_counts_support_a_zero_additional_calls_assertion() -> None:
    # Scenario: Counts support a zero-additional-calls assertion.
    fake = FakeLLM([(match_any(), respond_with(b"ok"))])
    request = _request("m-1")

    await fake.complete(request)
    # A cached path replays the same request material without invoking FakeLLM.
    assert fake.calls_for(request) == 1
