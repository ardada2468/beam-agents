"""Spec: adk-adapter / Requirement: Model calls route through the runtime
LLMClient.

Scenarios: Recognized client is replay-cached across retries; Unrecognized
client warns once and falls back.

The recognized model is a test double shaped like `google.adk.models.Gemini`
over `google.genai.Client`: `api_client._api_client._async_httpx_client` is the
`httpx.AsyncClient` the adapter must rewire. The double's own transport is a
tripwire — if a request ever reaches it, routing failed.
"""

from __future__ import annotations

import asyncio
import logging
from unittest import mock

import httpx
import pytest

pytest.importorskip("google.adk")

from google.adk.agents import LlmAgent
from google.adk.models.base_llm import BaseLlm

from beam_agents.adapters.adk import AdkAgent
from beam_agents.adapters.adk.transport import _find_async_client, _install_transport
from beam_agents.core.agent import Complete
from beam_agents.model.fake import FakeLLM, match_any, respond_with
from tests.adapters._helpers import make_ctx
from tests.adapters.adk._helpers import (
    RecognizedGenaiModel,
    SdkClient,
    UnrecognizedModel,
)


def _tripwire(hits: list[httpx.Request]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        hits.append(request)
        return httpx.Response(200, json={"answer": "from-upstream"})

    return httpx.MockTransport(handler)


def _recognized(hits: list[httpx.Request]) -> RecognizedGenaiModel:
    return RecognizedGenaiModel(model="m-1", api_client=SdkClient(_tripwire(hits)))


def _agent_over(model: BaseLlm) -> AdkAgent:
    return AdkAgent(LlmAgent(name="probe", model=model), chat_models=[model])


async def test_recognized_client_is_replay_cached_across_retries() -> None:
    # Scenario: Recognized client is replay-cached across retries — the HTTP
    # call is served through call_model (FakeLLM), the double's own transport is
    # never reached, and a second execution from identical committed state makes
    # zero provider calls and produces identical output.
    upstream: list[httpx.Request] = []
    model = _recognized(upstream)
    fake = FakeLLM([(match_any(), respond_with(b'{"answer": "forty-two"}'))])
    agent = _agent_over(model)

    first_ctx = make_ctx(event=b"meaning?", seq=9, provider=fake)
    first = await agent(first_ctx)
    assert isinstance(first, Complete)
    assert first.output == b"forty-two"
    assert fake.call_count == 1
    assert upstream == [], "the model's own transport must never be reached"

    # Bundle retry: identical committed state (same cache blob), fresh context.
    retry_ctx = make_ctx(event=b"meaning?", seq=9, provider=fake, cache_blob=first_ctx.cache_blob())
    retry = await agent(retry_ctx)

    assert isinstance(retry, Complete)
    assert retry.output == first.output
    assert fake.call_count == 1, "the retry must be served entirely from the replay cache"
    assert upstream == []


async def test_unrecognized_model_warns_once_and_falls_back(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Scenario: Unrecognized client warns once and falls back — direct provider
    # calls, exactly one warning naming the model class, fallback counter
    # incremented, run completes normally.
    upstream: list[httpx.Request] = []
    model = UnrecognizedModel(model="m-2", hidden=httpx.AsyncClient(transport=_tripwire(upstream)))
    fake = FakeLLM()  # must never be consulted

    with mock.patch("beam_agents.adapters.adk.transport.Metrics") as metrics:
        agent = _agent_over(model)
        with caplog.at_level(logging.WARNING, logger="beam_agents.adapters.adk.transport"):
            for seq in (11, 12):
                ctx = make_ctx(event=b"hi", seq=seq, provider=fake)
                outcome = await agent(ctx)
                assert isinstance(outcome, Complete)
                assert outcome.output == b"from-upstream"

    assert len(upstream) == 2, "unrecognized model calls go directly to the provider"
    assert fake.call_count == 0
    # Scoped to the adapter's own logger: ADK's telemetry emits unrelated
    # warnings of its own under this caplog level.
    warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and r.name == "beam_agents.adapters.adk.transport"
    ]
    assert len(warnings) == 1, "exactly one warning per agent instance"
    assert "UnrecognizedModel" in warnings[0].getMessage()
    assert "replay" in warnings[0].getMessage().lower()
    fallback_calls = [c for c in metrics.counter.call_args_list if "transport_fallback" in str(c)]
    assert fallback_calls, "the fallback must increment the transport_fallback counter"


def test_recognition_probes_the_google_genai_layout() -> None:
    # The recognition table's ADK-specific entry: a genai Client held at
    # `api_client`, and a genai Client passed directly.
    client = SdkClient(httpx.MockTransport(lambda r: httpx.Response(200)))
    model = RecognizedGenaiModel(model="m", api_client=client)
    assert _find_async_client(model) is client._api_client._async_httpx_client
    assert _find_async_client(client) is client._api_client._async_httpx_client
    assert _find_async_client(object()) is None


def test_recognition_survives_a_model_whose_api_client_raises() -> None:
    # `Gemini.api_client` is a cached_property that *constructs* a genai client
    # and raises without credentials; an unconstructable client must read as
    # "unrecognized" (warning fallback), never as an activation failure.
    class _Exploding:
        @property
        def api_client(self) -> object:
            raise ValueError("No API key was provided")

    assert _find_async_client(_Exploding()) is None


def test_the_transport_passes_through_outside_an_activation() -> None:
    # Outside an activation the wrapped original transport handles the request
    # untouched: a shim tool or client exercised in a plain ADK runner still
    # reaches its real provider.
    upstream: list[httpx.Request] = []
    model = _recognized(upstream)
    assert _install_transport(model) is True

    async def call() -> httpx.Response:
        client = _find_async_client(model)
        assert client is not None
        return await client.post("https://provider.example/v1/chat", json={"model": "m-1"})

    response = asyncio.run(call())
    assert response.json() == {"answer": "from-upstream"}
    assert len(upstream) == 1
