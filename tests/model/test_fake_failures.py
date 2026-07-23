"""Injectable-failure tests for the `fake-llm` capability.

Covers: a rule raises a configured `ServerError`, and fail-twice-then-succeed
yields two `RateLimitError`s then the response, recording all three.
"""

from __future__ import annotations

import pytest

from beam_agents.model import (
    FakeLLM,
    LlmRequest,
    RateLimitError,
    ServerError,
    fail_then_succeed,
    match_any,
    raise_error,
)

_REQUEST = LlmRequest(model_id="m-1", messages=[], tools_schema=[], sampling_params={})


# --- Requirement: Injectable failures -----------------------------------------


async def test_rule_raises_a_configured_provider_error() -> None:
    # Scenario: Rule raises a configured provider error.
    fake = FakeLLM([(match_any(), raise_error(ServerError(status=503)))])

    with pytest.raises(ServerError) as excinfo:
        await fake.complete(_REQUEST)

    assert excinfo.value.status == 503


async def test_fail_n_times_then_succeed() -> None:
    # Scenario: Fail N times then succeed.
    fake = FakeLLM(
        [
            (
                match_any(),
                fail_then_succeed(
                    error=RateLimitError(retry_after_ms=100), times=2, payload=b"succeeded"
                ),
            )
        ]
    )

    with pytest.raises(RateLimitError):
        await fake.complete(_REQUEST)
    with pytest.raises(RateLimitError):
        await fake.complete(_REQUEST)
    response = await fake.complete(_REQUEST)

    assert response.response == b"succeeded"
    assert fake.requests == (_REQUEST, _REQUEST, _REQUEST)
