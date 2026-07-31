"""Shared test doubles for `agent-context` capability tests.

Not collected by pytest (module name doesn't match `test_*`).
"""

from __future__ import annotations

import random

from beam_agents.core.context import AgentContext
from beam_agents.hitl import DEFAULT_APPROVAL_CHANNEL, DEFAULT_INTENT_TTL_MS
from beam_agents.memory import Memory
from beam_agents.model import CircuitBreaker, FakeLLM, LLMClient, ReplayCache, RetryPolicy
from beam_agents.model.facade import Decode, DecodedResponse, Sleep, TokenUsage
from beam_agents.tools import ToolRegistry


def decode_len_based(response: bytes) -> DecodedResponse:
    """Trivial provider decode: usage derived from payload length, mirroring
    the model-facade capability's own test helper.
    """
    text = response.decode("utf-8")
    n = len(response)
    return DecodedResponse(
        usage=TokenUsage(prompt_tokens=n, completion_tokens=n, total_tokens=2 * n), text=text
    )


class RecordingSleep:
    """Awaitable `Sleep` double that records requested delays instead of waiting."""

    def __init__(self) -> None:
        self.calls: list[int] = []

    async def __call__(self, ms: int) -> None:
        self.calls.append(ms)


def make_context(
    *,
    entity_key: bytes = b"key-1",
    seq: int = 0,
    now_ms: int = 1_000_000,
    memory: Memory | None = None,
    replay_cache: ReplayCache | None = None,
    provider: LLMClient | None = None,
    tool_registry: ToolRegistry | None = None,
    rng: random.Random | None = None,
    sleep: Sleep | None = None,
    breaker: CircuitBreaker | None = None,
    retry_policy: RetryPolicy | None = None,
    decode: Decode | None = None,
    intent_ttl_ms: int = DEFAULT_INTENT_TTL_MS,
    approval_channel: str = DEFAULT_APPROVAL_CHANNEL,
    max_tokens_per_activation: int | None = None,
) -> AgentContext:
    """Build an `AgentContext` with sensible defaults for tests that don't
    care about a particular knob.
    """
    return AgentContext(
        entity_key=entity_key,
        seq=seq,
        now_ms=now_ms,
        memory=memory if memory is not None else Memory(now_ms=now_ms),
        replay_cache=replay_cache if replay_cache is not None else ReplayCache(now_ms=now_ms),
        provider=provider if provider is not None else FakeLLM(),
        rng=rng if rng is not None else random.Random(0),
        sleep=sleep if sleep is not None else RecordingSleep(),
        breaker=breaker
        if breaker is not None
        else CircuitBreaker(endpoint="test", threshold=1_000, cooldown_ms=1_000),
        retry_policy=retry_policy
        if retry_policy is not None
        else RetryPolicy(max_attempts=3, base_ms=100, max_ms=1_000),
        decode=decode if decode is not None else decode_len_based,
        tool_registry=tool_registry if tool_registry is not None else ToolRegistry(),
        intent_ttl_ms=intent_ttl_ms,
        approval_channel=approval_channel,
        max_tokens_per_activation=max_tokens_per_activation,
    )
