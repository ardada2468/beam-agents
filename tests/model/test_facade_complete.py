"""End-to-end `LlmFacade.complete` tests for the `model-facade` capability.

Covers: a successful call returns a fully populated `FacadeResult`, no path
(hit/miss/retry/open-breaker) touches wall-clock time or unseeded randomness,
and re-running `complete` for the same key replays byte-identically under
bundle retry with zero additional provider calls and identical staged effects
(correctness invariant 3).
"""

from __future__ import annotations

import random
import time

import pytest

from beam_agents.model import (
    CircuitBreaker,
    CircuitOpenError,
    FakeLLM,
    LlmRequest,
    ServerError,
    fail_then_succeed,
    respond_with,
)
from beam_agents.model.facade import RetryPolicy

from ._facade_helpers import decode_len_based, make_facade

_REQUEST = LlmRequest(
    model_id="m-1", messages=[{"role": "user"}], tools_schema=[], sampling_params={}
)


# --- Requirement: Resilient async facade over a provider LLMClient ----------


async def test_facade_returns_a_structured_result_for_a_successful_call() -> None:
    # Scenario: Facade returns a structured result for a successful call.
    fake = FakeLLM([(lambda _r: True, respond_with(b"payload"))])
    facade, _ = make_facade(fake)

    result = await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)

    assert result.response.response == b"payload"
    assert result.cache_hit is False
    assert result.attempts == 1
    assert result.usage == decode_len_based(b"payload").usage


async def test_facade_never_touches_wall_clock_or_unseeded_randomness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Scenario: Facade never touches wall-clock or unseeded randomness.
    def _forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("facade touched a forbidden non-deterministic source")

    # Only the wall-clock epoch read and the *module-level* (un-seeded) random
    # functions are forbidden. `time.monotonic` is left untouched because the
    # asyncio event loop itself relies on it for scheduling — patching it would
    # break `await` machinery unrelated to the facade under test.
    monkeypatch.setattr(time, "time", _forbidden)
    monkeypatch.setattr(random, "random", _forbidden)
    monkeypatch.setattr(random, "uniform", _forbidden)

    # Miss then hit.
    fake = FakeLLM([(lambda r: r.messages == [{"role": "hit"}], respond_with(b"hit-me"))])
    facade, _ = make_facade(fake)
    hit_request = LlmRequest(
        model_id="m-1", messages=[{"role": "hit"}], tools_schema=[], sampling_params={}
    )
    await facade.complete(hit_request, entity_key=b"k", seq=0, step_index=0)
    await facade.complete(hit_request, entity_key=b"k", seq=0, step_index=0)

    # Retry path: fails once, then succeeds, exercising the injected rng/sleep.
    retry_fake = FakeLLM(
        [
            (
                lambda r: r.messages == [{"role": "retry"}],
                fail_then_succeed(error=ServerError(status=503), times=1, payload=b"retried"),
            )
        ]
    )
    retry_facade, _ = make_facade(
        retry_fake, retry_policy=RetryPolicy(max_attempts=2, base_ms=10, max_ms=100)
    )
    retry_request = LlmRequest(
        model_id="m-1", messages=[{"role": "retry"}], tools_schema=[], sampling_params={}
    )
    await retry_facade.complete(retry_request, entity_key=b"k", seq=0, step_index=0)

    # Open-breaker path.
    breaker = CircuitBreaker(endpoint="test", threshold=1, cooldown_ms=100_000_000)
    breaker.record_failure(now_ms=0)
    open_facade, _ = make_facade(FakeLLM([]), breaker=breaker)
    open_request = LlmRequest(
        model_id="m-1", messages=[{"role": "open"}], tools_schema=[], sampling_params={}
    )
    with pytest.raises(CircuitOpenError):
        await open_facade.complete(open_request, entity_key=b"k", seq=0, step_index=0)


# --- Requirement: Replay-cache integration short-circuits provider calls ----
# (semantics gate: bundle-retry determinism, correctness invariant 3)


@pytest.mark.semantics
async def test_determinism_under_bundle_retry() -> None:
    # Scenario: a replayed bundle hits the cache and makes zero additional
    # provider calls, with identical staged effects on every replay.
    fake = FakeLLM([(lambda _r: True, respond_with(b"deterministic"))])
    facade, staging = make_facade(fake)

    first = await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)
    assert fake.call_count == 1

    # Simulate N bundle retries: the same activation call repeated.
    for _ in range(5):
        replay = await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)
        assert fake.call_count == 1  # zero additional provider calls
        assert replay.response.response == first.response.response
        assert replay.usage == first.usage
        assert replay.cache_hit is True

    # Every replay staged a trace with the same cache-hit/attempts shape.
    hit_traces = staging.trace_events[1:]
    assert len(hit_traces) == 5
    for event in hit_traces:
        assert event.attributes["beam_agents.cache_hit"] == "true"
        assert event.attributes["beam_agents.attempts"] == "0"
