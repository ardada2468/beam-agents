"""Tests for the OpenAI-compatible `LLMClient` (`beam_agents.model.openai_compat`).

Offline only: HTTP is faked via `httpx.MockTransport`.
"""

from __future__ import annotations

import json
from collections.abc import Callable

import httpx
import pytest

from beam_agents.model.client import LlmRequest, ProviderRequestError
from beam_agents.model.openai_compat import OpenAICompatProvider
from beam_agents.model.openai_compat import _decode as openai_decode

_MESSAGES = [{"role": "user", "content": "hi"}]

_SUCCESS_BODY = {
    "id": "chatcmpl_1",
    "object": "chat.completion",
    "choices": [
        {
            "index": 0,
            "message": {"role": "assistant", "content": "hello there"},
            "finish_reason": "stop",
        }
    ],
    "usage": {"prompt_tokens": 12, "completion_tokens": 5, "total_tokens": 17},
}

_Handler = Callable[[httpx.Request], httpx.Response]


def _provider(
    handler: _Handler, *, base_url: str = "https://api.openai.com/v1"
) -> OpenAICompatProvider:
    transport = httpx.MockTransport(handler)
    return OpenAICompatProvider(api_key="sk-test", base_url=base_url, transport=transport)


# --- Requirement: OpenAI-compatible provider is a conforming non-streaming ---


async def test_success_returns_raw_body_as_opaque_bytes() -> None:
    # Scenario: A successful call returns the raw body as opaque response bytes.
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json=_SUCCESS_BODY)

    provider = _provider(handler)
    request = LlmRequest(
        model_id="gpt-x", messages=_MESSAGES, tools_schema=None, sampling_params={}
    )

    response = await provider.complete(request)

    assert len(calls) == 1
    assert json.loads(response.response) == _SUCCESS_BODY
    sent = calls[0]
    assert str(sent.url) == "https://api.openai.com/v1/chat/completions"
    assert sent.headers["authorization"] == "Bearer sk-test"
    body = json.loads(sent.content)
    assert body["stream"] is False


async def test_base_url_selects_the_endpoint() -> None:
    # Scenario: Base URL selects the endpoint (e.g. a vLLM server).
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json=_SUCCESS_BODY)

    provider = _provider(handler, base_url="http://localhost:8000/v1")
    request = LlmRequest(
        model_id="local-model", messages=_MESSAGES, tools_schema=None, sampling_params={}
    )

    await provider.complete(request)

    assert str(calls[0].url) == "http://localhost:8000/v1/chat/completions"


# --- Requirement: Each provider ships a Decode for token usage and text -----


def test_decode_extracts_usage_and_text() -> None:
    # Scenario: OpenAI-compatible _decode extracts usage and text.
    decoded = openai_decode(json.dumps(_SUCCESS_BODY).encode())

    assert decoded.usage.prompt_tokens == 12
    assert decoded.usage.completion_tokens == 5
    assert decoded.usage.total_tokens == 17
    assert decoded.text == "hello there"


def test_decode_is_pure_over_cached_bytes() -> None:
    payload = json.dumps(_SUCCESS_BODY).encode()

    first = openai_decode(payload)
    second = openai_decode(payload)

    assert first == second


# --- Requirement: HTTP outcomes map onto the retryable/non-retryable taxonomy


async def test_undecodable_success_body_maps_to_non_retryable_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"unexpected": "shape"})

    provider = _provider(handler)
    request = LlmRequest(
        model_id="gpt-x", messages=_MESSAGES, tools_schema=None, sampling_params={}
    )

    with pytest.raises(ProviderRequestError):
        await provider.complete(request)
