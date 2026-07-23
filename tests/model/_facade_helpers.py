"""Shared test doubles for `model-facade` capability tests.

Not collected by pytest (module name doesn't match `test_*`).
"""

from __future__ import annotations

import random

from beam_agents._protos import TraceEvent
from beam_agents.model import (
    CircuitBreaker,
    DecodedResponse,
    LLMClient,
    LlmFacade,
    ReplayCache,
    TokenUsage,
)
from beam_agents.model.facade import Decode, RetryPolicy, Sleep


def decode_len_based(response: bytes) -> DecodedResponse:
    """Trivial provider decode: usage derived from payload length, text is the
    payload itself (so `output_schema` tests can feed JSON payloads through).
    """
    text = response.decode("utf-8")
    n = len(response)
    return DecodedResponse(
        usage=TokenUsage(prompt_tokens=n, completion_tokens=n, total_tokens=2 * n), text=text
    )


class RecordingStaging:
    """Records every staged trace event and accumulated usage, in call order."""

    def __init__(self) -> None:
        self.trace_events: list[TraceEvent] = []
        self.usages: list[TokenUsage] = []

    def stage_trace_event(self, event: TraceEvent) -> None:
        self.trace_events.append(event)

    def accumulate_usage(self, usage: TokenUsage) -> None:
        self.usages.append(usage)


class RecordingSleep:
    """Awaitable `Sleep` double that records requested delays instead of waiting."""

    def __init__(self) -> None:
        self.calls: list[int] = []

    async def __call__(self, ms: int) -> None:
        self.calls.append(ms)


class MaxJitterRandom(random.Random):
    """A `random.Random` whose `uniform` always returns the upper bound.

    Makes jittered-backoff assertions exact instead of range-based, while still
    satisfying the facade's `random.Random`-typed `rng` injection point.
    """

    def uniform(self, a: float, b: float) -> float:
        return b


def make_facade(
    provider: LLMClient,
    *,
    now_ms: int = 1_000_000,
    retry_policy: RetryPolicy | None = None,
    sleep: Sleep | None = None,
    rng: random.Random | None = None,
    breaker: CircuitBreaker | None = None,
    decode: Decode | None = None,
    staging: RecordingStaging | None = None,
) -> tuple[LlmFacade, RecordingStaging]:
    """Build an `LlmFacade` with sensible defaults for tests that don't care
    about a particular knob, plus the `RecordingStaging` sink so assertions can
    inspect staged trace/usage effects.
    """
    staging = staging if staging is not None else RecordingStaging()
    facade = LlmFacade(
        provider,
        ReplayCache(now_ms=now_ms),
        now_ms=now_ms,
        rng=rng if rng is not None else random.Random(0),
        sleep=sleep if sleep is not None else RecordingSleep(),
        breaker=breaker
        if breaker is not None
        else CircuitBreaker(endpoint="test", threshold=1_000, cooldown_ms=1_000),
        retry_policy=retry_policy
        if retry_policy is not None
        else RetryPolicy(max_attempts=3, base_ms=100, max_ms=1_000),
        decode=decode if decode is not None else decode_len_based,
        staging=staging,
    )
    return facade, staging
