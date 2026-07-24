"""Nightly-only smoke tests against real provider endpoints.

Covers the `model-providers` requirement "Live-endpoint verification is
nightly-smoke only": these tests carry `-m smoke`, are excluded from
`make test-unit`, and skip when the relevant provider credential is absent
(local runs and PRs never hit a live endpoint).
"""

from __future__ import annotations

import os

import pytest

from beam_agents.model.anthropic import AnthropicProvider
from beam_agents.model.anthropic import decode as anthropic_decode
from beam_agents.model.client import LlmRequest
from beam_agents.model.openai_compat import OpenAICompatProvider
from beam_agents.model.openai_compat import decode as openai_decode

pytestmark = pytest.mark.smoke

_MESSAGES = [{"role": "user", "content": "Say the single word: pong"}]


@pytest.mark.skipif(not os.environ.get("ANTHROPIC_API_KEY"), reason="ANTHROPIC_API_KEY not set")
async def test_anthropic_live_call_returns_a_decodable_response() -> None:
    provider = AnthropicProvider(
        api_key=os.environ["ANTHROPIC_API_KEY"],
        anthropic_version="2023-06-01",
    )
    request = LlmRequest(
        model_id="claude-3-5-haiku-latest",
        messages=_MESSAGES,
        tools_schema=None,
        sampling_params={"max_tokens": 16},
    )

    response = await provider.complete(request)
    decoded = anthropic_decode(response.response)

    assert decoded.text
    assert decoded.usage.total_tokens > 0


@pytest.mark.skipif(not os.environ.get("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
async def test_openai_live_call_returns_a_decodable_response() -> None:
    provider = OpenAICompatProvider(api_key=os.environ["OPENAI_API_KEY"])
    request = LlmRequest(
        model_id="gpt-4o-mini",
        messages=_MESSAGES,
        tools_schema=None,
        sampling_params={"max_tokens": 16},
    )

    response = await provider.complete(request)
    decoded = openai_decode(response.response)

    assert decoded.text
    assert decoded.usage.total_tokens > 0
