"""Tests for the `model-providers` requirement "Providers use worker-local
shared httpx pools on the bridge loop": lazy client construction on first
`complete`, pool reuse across calls, and composition with `AsyncBridge`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

import httpx
import pytest

from beam_agents.core.bridge import AsyncBridge
from beam_agents.model.anthropic import AnthropicProvider
from beam_agents.model.client import LlmRequest
from beam_agents.model.openai_compat import OpenAICompatProvider

_MESSAGES = [{"role": "user", "content": "hi"}]

_ANTHROPIC_BODY: dict[str, object] = {
    "content": [{"type": "text", "text": "hi"}],
    "usage": {"input_tokens": 1, "output_tokens": 1},
}
_OPENAI_BODY: dict[str, object] = {
    "choices": [{"message": {"content": "hi"}}],
    "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
}

_Provider = AnthropicProvider | OpenAICompatProvider
_ProviderFactory = Callable[[httpx.AsyncBaseTransport], _Provider]


def _make_anthropic(transport: httpx.AsyncBaseTransport) -> _Provider:
    return AnthropicProvider(api_key="k", transport=transport)


def _make_openai_compat(transport: httpx.AsyncBaseTransport) -> _Provider:
    return OpenAICompatProvider(api_key="k", transport=transport)


_PROVIDER_CASES: list[tuple[_ProviderFactory, dict[str, object]]] = [
    (_make_anthropic, _ANTHROPIC_BODY),
    (_make_openai_compat, _OPENAI_BODY),
]


def _handler(body: dict[str, object]) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body)

    return handler


def _client_of(provider: _Provider) -> httpx.AsyncClient | None:
    """Read the private `_client` field via a fresh call each time so mypy
    never narrows it as a persistently-tracked attribute across awaits.
    """
    return cast("httpx.AsyncClient | None", getattr(provider, "_client"))  # noqa: B009


@pytest.mark.parametrize(("provider_factory", "body"), _PROVIDER_CASES)
async def test_client_is_lazy_and_reused_across_calls(
    provider_factory: _ProviderFactory, body: dict[str, object]
) -> None:
    # Scenario: The async client is reused across calls.
    provider = provider_factory(httpx.MockTransport(_handler(body)))

    assert _client_of(provider) is None  # not built at construction time

    request = LlmRequest(model_id="m", messages=_MESSAGES, tools_schema=None, sampling_params={})
    await provider.complete(request)
    first_client = _client_of(provider)
    assert first_client is not None

    await provider.complete(request)
    assert _client_of(provider) is first_client


@pytest.mark.parametrize(("provider_factory", "body"), _PROVIDER_CASES)
def test_provider_complete_runs_through_the_bridge_loop(
    provider_factory: _ProviderFactory, body: dict[str, object]
) -> None:
    # Scenario: composes with the one-loop-per-DoFn async bridge.
    provider = provider_factory(httpx.MockTransport(_handler(body)))
    bridge = AsyncBridge()
    bridge.start()
    try:
        request = LlmRequest(
            model_id="m", messages=_MESSAGES, tools_schema=None, sampling_params={}
        )

        async def call() -> bytes:
            response = await provider.complete(request)
            return response.response

        result = bridge.run(call, timeout_s=5.0)
        assert result
    finally:
        bridge.stop()
