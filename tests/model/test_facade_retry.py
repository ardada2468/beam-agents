"""Retry-policy and jittered-backoff tests for the `model-facade` capability.

Covers: a transient failure is retried to success, the attempt cap re-raises
the last error, a non-`ProviderError` is not retried, backoff follows
`min(base*2^(attempt-1), max)`, Retry-After is honored as a floor, and no
sleep is awaited after the final attempt.
"""

from __future__ import annotations

import pytest

from beam_agents.model import (
    FakeLLM,
    LlmRequest,
    ProviderTimeout,
    RateLimitError,
    ServerError,
    UnmatchedRequestError,
    fail_then_succeed,
    match_any,
    raise_error,
)
from beam_agents.model.facade import RetryPolicy

from ._facade_helpers import MaxJitterRandom, RecordingSleep, make_facade

_REQUEST = LlmRequest(
    model_id="m-1", messages=[{"role": "user"}], tools_schema=[], sampling_params={}
)


# --- Requirement: Typed retry classification with a bounded attempt cap -----


async def test_transient_failure_is_retried_to_success() -> None:
    # Scenario: A transient failure is retried to success.
    fake = FakeLLM(
        [
            (
                match_any(),
                fail_then_succeed(error=ServerError(status=503), times=1, payload=b"ok"),
            )
        ]
    )
    sleep = RecordingSleep()
    facade, _ = make_facade(
        fake, retry_policy=RetryPolicy(max_attempts=2, base_ms=100, max_ms=1_000), sleep=sleep
    )

    result = await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)

    assert result.response.response == b"ok"
    assert result.attempts == 2
    assert len(sleep.calls) == 1


async def test_attempts_are_capped_and_last_error_is_reraised() -> None:
    # Scenario: Attempts are capped and the last error is re-raised.
    fake = FakeLLM([(match_any(), raise_error(ProviderTimeout()))])
    sleep = RecordingSleep()
    facade, _ = make_facade(
        fake, retry_policy=RetryPolicy(max_attempts=3, base_ms=10, max_ms=100), sleep=sleep
    )

    with pytest.raises(ProviderTimeout):
        await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)

    assert fake.call_count == 3
    assert len(sleep.calls) == 2  # no sleep after the final (3rd) attempt


async def test_rate_limit_429_is_retried_per_policy_then_surfaced_typed() -> None:
    # A 429 (RateLimitError) is retried up to the policy cap like any other
    # retryable ProviderError, and the *exact* typed error re-surfaces at the
    # cap rather than being converted to a generic exception.
    fake = FakeLLM([(match_any(), raise_error(RateLimitError(retry_after_ms=50)))])
    sleep = RecordingSleep()
    facade, _ = make_facade(
        fake, retry_policy=RetryPolicy(max_attempts=3, base_ms=10, max_ms=100), sleep=sleep
    )

    with pytest.raises(RateLimitError) as excinfo:
        await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)

    assert fake.call_count == 3  # retried per policy (3 attempts)
    assert len(sleep.calls) == 2  # backoff honored between attempts, not after the cap
    assert excinfo.value.retry_after_ms == 50  # the exact typed error, not wrapped/genericized


async def test_non_retryable_error_is_not_retried() -> None:
    # Scenario: A non-retryable error is not retried.
    fake = FakeLLM([])  # no rules -> raises UnmatchedRequestError, not a ProviderError
    sleep = RecordingSleep()
    facade, _ = make_facade(fake, sleep=sleep)

    with pytest.raises(UnmatchedRequestError):
        await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)

    assert fake.call_count == 1
    assert sleep.calls == []


# --- Requirement: Jittered exponential backoff honoring Retry-After ---------


async def test_backoff_grows_and_stays_within_the_cap() -> None:
    # Scenario: Backoff grows and stays within the cap.
    fake = FakeLLM([(match_any(), raise_error(ServerError(status=503)))])
    sleep = RecordingSleep()
    facade, _ = make_facade(
        fake,
        retry_policy=RetryPolicy(max_attempts=5, base_ms=100, max_ms=1_000),
        sleep=sleep,
        rng=MaxJitterRandom(0),  # uniform() always returns its upper bound
    )

    with pytest.raises(ServerError):
        await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)

    # attempt n's pre-jitter delay is min(100 * 2**(n-1), 1000); no delay after
    # the 5th (final) attempt.
    assert sleep.calls == [100, 200, 400, 800]


async def test_retry_after_is_a_floor_on_the_delay() -> None:
    # Scenario: Retry-After is a floor on the delay.
    fake = FakeLLM(
        [
            (
                match_any(),
                fail_then_succeed(
                    error=RateLimitError(retry_after_ms=1_500), times=1, payload=b"ok"
                ),
            )
        ]
    )
    sleep = RecordingSleep()
    facade, _ = make_facade(
        fake,
        retry_policy=RetryPolicy(max_attempts=2, base_ms=10, max_ms=50),
        sleep=sleep,
        rng=MaxJitterRandom(0),  # would otherwise jitter up to only 10ms
    )

    result = await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)

    assert result.response.response == b"ok"
    assert sleep.calls == [1_500]


async def test_no_sleep_after_the_final_attempt() -> None:
    # Scenario: No sleep after the final attempt.
    fake = FakeLLM([(match_any(), raise_error(ProviderTimeout()))])
    sleep = RecordingSleep()
    facade, _ = make_facade(
        fake, retry_policy=RetryPolicy(max_attempts=1, base_ms=100, max_ms=1_000), sleep=sleep
    )

    with pytest.raises(ProviderTimeout):
        await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)

    assert sleep.calls == []
