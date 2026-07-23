"""Replay-cache integration tests for the `model-facade` capability.

Covers: a cache hit incurs no provider call, a cache miss calls the provider
and stages the response, a cache hit replays even while the breaker is open,
and the `output_schema` perturbs the cache key.
"""

from __future__ import annotations

from pydantic import BaseModel

from beam_agents.model import CircuitBreaker, FakeLLM, LlmRequest, respond_with

from ._facade_helpers import make_facade

_REQUEST = LlmRequest(
    model_id="m-1", messages=[{"role": "user"}], tools_schema=[], sampling_params={}
)


class _Answer(BaseModel):
    text: str


# --- Requirement: Replay-cache integration short-circuits provider calls ----


async def test_a_cache_hit_incurs_no_provider_call() -> None:
    # Scenario: A cache hit incurs no provider call.
    fake = FakeLLM([(lambda _r: True, respond_with(b"first response"))])
    facade, _ = make_facade(fake)

    first = await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)
    assert fake.call_count == 1
    assert first.cache_hit is False
    assert first.attempts == 1

    second = await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)

    assert fake.call_count == 1  # no additional provider call
    assert second.cache_hit is True
    assert second.attempts == 0
    assert second.response.response == b"first response"


async def test_a_cache_miss_calls_the_provider_and_stages_the_response() -> None:
    # Scenario: A cache miss calls the provider and stages the response.
    fake = FakeLLM([(lambda _r: True, respond_with(b"stage-me"))])
    facade, _ = make_facade(fake)

    result = await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)

    assert fake.call_count == 1
    assert result.cache_hit is False
    assert result.response.response == b"stage-me"

    # The staged response is now replayable with no additional provider call.
    replayed = await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)
    assert fake.call_count == 1
    assert replayed.response.response == b"stage-me"


async def test_a_cache_hit_replays_even_while_the_breaker_is_open() -> None:
    # Scenario: A cache hit replays even while the breaker is open.
    fake = FakeLLM([(lambda _r: True, respond_with(b"cached"))])
    breaker = CircuitBreaker(endpoint="test", threshold=1, cooldown_ms=1_000_000)
    facade, _ = make_facade(fake, breaker=breaker)

    await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)
    assert fake.call_count == 1

    breaker.record_failure(now_ms=0)  # trips the breaker OPEN (threshold=1)

    result = await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)

    assert result.cache_hit is True
    assert result.response.response == b"cached"
    assert fake.call_count == 1  # still no provider call


async def test_the_output_schema_perturbs_the_cache_key() -> None:
    # Scenario: The output schema perturbs the cache key.
    fake = FakeLLM([(lambda _r: True, respond_with(b'{"text": "hi"}'))])
    facade, _ = make_facade(fake)

    without_schema = await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)
    with_schema = await facade.complete(
        _REQUEST, entity_key=b"key-1", seq=0, step_index=0, output_schema=_Answer
    )

    assert without_schema.cache_hit is False
    assert with_schema.cache_hit is False  # did not alias the schema-less entry
    assert fake.call_count == 2
    assert with_schema.parsed == _Answer(text="hi")
