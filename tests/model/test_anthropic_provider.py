"""Tests for the Anthropic `LLMClient` (`beam_agents.model.anthropic`).

Offline only: HTTP is faked via `httpx.MockTransport`, per the
`model-providers` requirement that taxonomy/decode behavior is verifiable
without a live endpoint.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Callable

import httpx
import pytest

from beam_agents.model.anthropic import AnthropicProvider
from beam_agents.model.anthropic import decode as anthropic_decode
from beam_agents.model.client import LlmRequest, ProviderRequestError

_MESSAGES = [{"role": "user", "content": "hi"}]

_SUCCESS_BODY = {
    "id": "msg_1",
    "type": "message",
    "role": "assistant",
    "content": [{"type": "text", "text": "hello there"}],
    "model": "claude-x",
    "usage": {"input_tokens": 12, "output_tokens": 5},
}

_Handler = Callable[[httpx.Request], httpx.Response]


def _provider(handler: _Handler) -> AnthropicProvider:
    return AnthropicProvider(
        api_key="sk-test",
        anthropic_version="2023-06-01",
        transport=httpx.MockTransport(handler),
    )


# --- Requirement: Anthropic provider is a conforming non-streaming LLMClient -


async def test_success_returns_raw_body_as_opaque_bytes() -> None:
    # Scenario: A successful call returns the raw body as opaque response bytes.
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json=_SUCCESS_BODY)

    provider = _provider(handler)
    request = LlmRequest(
        model_id="claude-x", messages=_MESSAGES, tools_schema=None, sampling_params={}
    )

    response = await provider.complete(request)

    assert len(calls) == 1
    assert json.loads(response.response) == _SUCCESS_BODY


async def test_headers_and_stream_false_and_single_post() -> None:
    # Scenario: The request is single-shot and non-streaming.
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json=_SUCCESS_BODY)

    provider = _provider(handler)
    request = LlmRequest(
        model_id="claude-x", messages=_MESSAGES, tools_schema=None, sampling_params={}
    )

    await provider.complete(request)

    assert len(calls) == 1
    sent = calls[0]
    assert sent.headers["x-api-key"] == "sk-test"
    assert sent.headers["anthropic-version"] == "2023-06-01"
    body = json.loads(sent.content)
    assert body["stream"] is False


def test_credentials_are_not_on_llm_request() -> None:
    # Scenario: Credentials are provider state, not request material.
    field_names = {f.name for f in dataclasses.fields(LlmRequest)}
    assert "api_key" not in field_names
    assert "credentials" not in field_names


# --- Requirement: Each provider ships a Decode for token usage and text -----


def test_decode_extracts_usage_and_text() -> None:
    # Scenario: Anthropic decode extracts usage and text.
    decoded = anthropic_decode(json.dumps(_SUCCESS_BODY).encode())

    assert decoded.usage.prompt_tokens == 12
    assert decoded.usage.completion_tokens == 5
    assert decoded.usage.total_tokens == 17
    assert decoded.text == "hello there"


def test_decode_is_pure_over_cached_bytes() -> None:
    # Scenario: Decode is pure over the cached bytes.
    payload = json.dumps(_SUCCESS_BODY).encode()

    first = anthropic_decode(payload)
    second = anthropic_decode(payload)

    assert first == second


# --- Requirement: HTTP outcomes map onto the retryable/non-retryable taxonomy


async def test_undecodable_success_body_maps_to_non_retryable_error() -> None:
    # Scenario: An undecodable success body maps to the non-retryable error.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"unexpected": "shape"})

    provider = _provider(handler)
    request = LlmRequest(
        model_id="claude-x", messages=_MESSAGES, tools_schema=None, sampling_params={}
    )

    with pytest.raises(ProviderRequestError):
        await provider.complete(request)
