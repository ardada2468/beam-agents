"""Injectable-latency tests for the `fake-llm` capability.

Covers: the injected delay hook is awaited exactly once with the configured
`latency_ms`, and the real `asyncio.sleep` path is cancellable so a slow
response never resolves.
"""

from __future__ import annotations

import asyncio

import pytest

from beam_agents.model import FakeLLM, LlmRequest, match_any, respond_with

_REQUEST = LlmRequest(model_id="m-1", messages=[], tools_schema=[], sampling_params={})


# --- Requirement: Injectable latency ------------------------------------------


async def test_latency_is_applied_through_the_injected_hook() -> None:
    # Scenario: Latency is applied through the injected hook.
    calls: list[int] = []

    async def recording_delay(ms: int) -> None:
        calls.append(ms)

    fake = FakeLLM([(match_any(), respond_with(b"ok", latency_ms=250))], delay=recording_delay)

    response = await fake.complete(_REQUEST)

    assert calls == [250]
    assert response.response == b"ok"


async def test_latency_can_outlast_an_activation_deadline() -> None:
    # Scenario: Latency can outlast an activation deadline.
    fake = FakeLLM([(match_any(), respond_with(b"ok", latency_ms=10_000))])

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(fake.complete(_REQUEST), timeout=0.01)
