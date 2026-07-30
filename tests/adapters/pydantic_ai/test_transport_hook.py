"""Spec: pydantic-ai-adapter / Requirement: Model requests route through the
runtime LLMClient.

The recognized model is shaped like the framework's real provider models: a
``client`` property holding an SDK object whose ``_client`` is an
``httpx.AsyncClient``. The double's own transport is a tripwire — if a request
ever reaches it, routing failed.
"""

from __future__ import annotations

import logging
from unittest import mock

import httpx
import pytest

pytest.importorskip("pydantic_ai")

from pydantic_ai import Agent

from beam_agents.adapters.pydantic_ai import PydanticAIAgent
from beam_agents.core.agent import Complete
from beam_agents.model.fake import FakeLLM, match_any, respond_with
from tests.adapters.pydantic_ai._helpers import (
    RecognizedModel,
    UnrecognizedModel,
    make_ctx,
    tripwire,
)


async def test_recognized_model_is_replay_cached_across_retries() -> None:
    # Scenario: Recognized model is replay-cached across retries — the HTTP call
    # is served through call_model (FakeLLM), the model's own transport is never
    # reached, and a second execution from identical committed state makes zero
    # provider calls and produces identical output.
    upstream: list[httpx.Request] = []
    model = RecognizedModel("m-cached", tripwire(upstream))
    fake = FakeLLM([(match_any(), respond_with(b'{"answer": "forty-two"}'))])
    agent = PydanticAIAgent(Agent(model))

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
    # Scenario: Unrecognized model warns once and falls back — direct provider
    # calls, exactly one warning naming the model class, fallback counter
    # incremented, runs complete normally.
    upstream: list[httpx.Request] = []
    model = UnrecognizedModel("m-direct", tripwire(upstream))
    fake = FakeLLM()  # must never be consulted

    with mock.patch("beam_agents.adapters.pydantic_ai.transport.Metrics") as metrics:
        agent = PydanticAIAgent(Agent(model))
        logger = "beam_agents.adapters.pydantic_ai.transport"
        with caplog.at_level(logging.WARNING, logger=logger):
            for seq in (11, 12):
                ctx = make_ctx(event=b"hi", seq=seq, provider=fake)
                outcome = await agent(ctx)
                assert isinstance(outcome, Complete)
                assert outcome.output == b"from-upstream"

    assert len(upstream) == 2, "unrecognized model calls go directly to the provider"
    assert fake.call_count == 0
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1, "exactly one warning per agent instance"
    assert "UnrecognizedModel" in warnings[0].getMessage()
    assert "replay" in warnings[0].getMessage().lower()
    fallback_calls = [c for c in metrics.counter.call_args_list if "transport_fallback" in str(c)]
    assert fallback_calls, "the fallback must increment the transport_fallback counter"


async def test_transport_installation_is_idempotent() -> None:
    # Installing twice must not stack transports (the hook runs once per agent
    # instance, but a model shared between adapters must stay sane).
    from beam_agents.adapters.pydantic_ai.transport import install_transport  # noqa: PLC0415

    model = RecognizedModel("m-idem", tripwire())
    assert install_transport(model) is True
    first = model.client._client._transport
    assert install_transport(model) is True
    assert model.client._client._transport is first
