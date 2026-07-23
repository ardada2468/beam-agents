"""Fail-closed tests for the `fake-llm` capability.

Covers: an unmatched request raises a descriptive non-`ProviderError` naming
the request, and an empty FakeLLM raises on its first call.
"""

from __future__ import annotations

import pytest

from beam_agents.model import (
    FakeLLM,
    LlmRequest,
    ProviderError,
    UnmatchedRequestError,
    match_model_id,
    respond_with,
)

_REQUEST = LlmRequest(model_id="unscripted", messages=[], tools_schema=[], sampling_params={})


# --- Requirement: Unmatched requests fail closed -----------------------------


async def test_no_matching_rule_raises() -> None:
    # Scenario: No matching rule raises.
    fake = FakeLLM([(match_model_id("other"), respond_with(b"ok"))])

    with pytest.raises(UnmatchedRequestError) as excinfo:
        await fake.complete(_REQUEST)

    assert excinfo.value.request == _REQUEST
    assert "unscripted" in str(excinfo.value)
    assert not isinstance(excinfo.value, ProviderError)


async def test_empty_fake_llm_raises_on_first_call() -> None:
    # Scenario: Empty FakeLLM raises on first call.
    fake = FakeLLM()

    with pytest.raises(UnmatchedRequestError):
        await fake.complete(_REQUEST)
