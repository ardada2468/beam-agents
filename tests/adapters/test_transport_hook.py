"""Spec: langgraph-adapter / Requirement: Chat model calls route through the
runtime LLMClient.

The recognized chat model is a test double shaped like the langchain-anthropic /
langchain-openai stack: a model object holding an SDK client whose `_client` is
an `httpx.AsyncClient`. The double's own transport is a tripwire — if a request
ever reaches it, routing failed.
"""

from __future__ import annotations

import json
import logging
from unittest import mock

import httpx
import pytest

pytest.importorskip("langgraph")

from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from beam_agents.adapters.langgraph import LangGraphAgent
from beam_agents.core.agent import Complete
from beam_agents.model.fake import FakeLLM, match_any, respond_with
from tests.adapters._helpers import make_ctx


class _SdkClient:
    """Shaped like `openai.AsyncOpenAI` / `anthropic.AsyncAnthropic`: the SDK
    object whose `_client` is the httpx.AsyncClient the adapter must rewire."""

    def __init__(self, transport: httpx.AsyncBaseTransport) -> None:
        self._client = httpx.AsyncClient(transport=transport)


class _RecognizedChatModel:
    """Shaped like `langchain_openai.ChatOpenAI`: exposes `root_async_client`."""

    def __init__(self, transport: httpx.AsyncBaseTransport) -> None:
        self.root_async_client = _SdkClient(transport)


class _UnrecognizedChatModel:
    """No httpx client at any attribute path the adapter recognizes."""

    def __init__(self, transport: httpx.AsyncBaseTransport) -> None:
        self._hidden = httpx.AsyncClient(transport=transport)


class _State(TypedDict, total=False):
    prompt: str
    answer: str


def _graph_calling(model: _RecognizedChatModel | _UnrecognizedChatModel) -> StateGraph:
    graph: StateGraph = StateGraph(_State)

    async def call_model(state: _State) -> _State:
        client = (
            model.root_async_client._client
            if isinstance(model, _RecognizedChatModel)
            else model._hidden
        )
        response = await client.post(
            "https://provider.example/v1/chat",
            json={
                "model": "m-1",
                "messages": [{"role": "user", "content": state["prompt"]}],
                "temperature": 0,
            },
        )
        return {"answer": response.json()["content"]}

    graph.add_node("call_model", call_model)
    graph.add_edge(START, "call_model")
    graph.add_edge("call_model", END)
    return graph


def _tripwire(hits: list[httpx.Request]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        hits.append(request)
        return httpx.Response(200, json={"content": "from-upstream"})

    return httpx.MockTransport(handler)


async def test_recognized_client_is_replay_cached_across_retries() -> None:
    # Scenario: Recognized client is replay-cached across retries — the HTTP
    # call is served through call_model (FakeLLM), the double's own transport
    # is never reached, and a second execution from identical committed state
    # makes zero provider calls and produces identical output.
    upstream: list[httpx.Request] = []
    model = _RecognizedChatModel(_tripwire(upstream))
    fake = FakeLLM([(match_any(), respond_with(b'{"content": "forty-two"}'))])
    agent = LangGraphAgent(_graph_calling(model), chat_models=[model])

    first_ctx = make_ctx(event=json.dumps({"prompt": "meaning?"}).encode(), seq=9, provider=fake)
    first = await agent(first_ctx)
    assert isinstance(first, Complete)
    assert json.loads(first.output)["answer"] == "forty-two"
    assert fake.call_count == 1
    assert upstream == [], "the model's own transport must never be reached"

    # Bundle retry: identical committed state (same cache blob), fresh context.
    retry_ctx = make_ctx(
        event=json.dumps({"prompt": "meaning?"}).encode(),
        seq=9,
        provider=fake,
        cache_blob=first_ctx.cache_blob(),
    )
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
    # incremented, graph completes normally.
    upstream: list[httpx.Request] = []
    model = _UnrecognizedChatModel(_tripwire(upstream))
    fake = FakeLLM()  # must never be consulted

    with mock.patch("beam_agents.adapters.langgraph.transport.Metrics") as metrics:
        agent = LangGraphAgent(_graph_calling(model), chat_models=[model])
        with caplog.at_level(logging.WARNING, logger="beam_agents.adapters.langgraph.transport"):
            for seq in (11, 12):
                ctx = make_ctx(event=json.dumps({"prompt": "hi"}).encode(), seq=seq, provider=fake)
                outcome = await agent(ctx)
                assert isinstance(outcome, Complete)
                assert json.loads(outcome.output)["answer"] == "from-upstream"

    assert len(upstream) == 2, "unrecognized model calls go directly to the provider"
    assert fake.call_count == 0
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1, "exactly one warning per agent instance"
    assert "_UnrecognizedChatModel" in warnings[0].getMessage()
    assert "replay" in warnings[0].getMessage().lower()
    fallback_calls = [c for c in metrics.counter.call_args_list if "transport_fallback" in str(c)]
    assert fallback_calls, "the fallback must increment the transport_fallback counter"
