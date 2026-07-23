"""Behavior tests for the async LLM client facade."""

from __future__ import annotations

import pytest

from beam_agents.model import (
    CircuitBreakerPolicy,
    CircuitOpenError,
    FakeLLM,
    LlmClientFacade,
    LlmRequest,
    LlmResponse,
    OutputSchemaError,
    RateLimitError,
    ReplayCache,
    RetryPolicy,
    ServerError,
    TraceEvent,
    fail_then_succeed,
    match_any,
    raise_error,
    respond_with,
)


class _Clock:
    def __init__(self, now: int) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now

    def advance(self, ms: int) -> None:
        self.now += ms


class _StubClient:
    def __init__(self, responses: list[LlmResponse]) -> None:
        self._responses = responses
        self.call_count = 0

    async def complete(self, request: LlmRequest) -> LlmResponse:
        del request
        self.call_count += 1
        return self._responses[self.call_count - 1]


def _request() -> LlmRequest:
    return LlmRequest(
        model_id="model-a",
        messages=[{"role": "user", "content": "hi"}],
        tools_schema=[{"name": "lookup"}],
        output_schema={
            "type": "object",
            "properties": {"answer": {"type": "string"}},
            "required": ["answer"],
            "additionalProperties": False,
        },
        sampling_params={"temperature": 0.0},
    )


async def _no_sleep(ms: int) -> None:
    del ms


async def test_cache_hit_bypasses_provider_and_preserves_usage_provenance() -> None:
    cache = ReplayCache(now_ms=1_000)
    fake = FakeLLM(
        [
            (
                match_any(),
                respond_with(b'{"answer":"ok","usage":{"input_tokens":3,"output_tokens":4}}'),
            )
        ]
    )
    facade = LlmClientFacade(fake, replay_cache=cache, now_ms=lambda: 1_000, sleep_ms=_no_sleep)

    first = await facade.complete(_request(), entity_key=b"k1", seq=1)
    second = await facade.complete(_request(), entity_key=b"k1", seq=1)

    assert first.from_replay_cache is False
    assert second.from_replay_cache is True
    assert second.input_tokens == 3
    assert second.output_tokens == 4
    assert second.total_tokens == 7
    assert fake.call_count == 1


async def test_retry_after_takes_precedence_over_backoff() -> None:
    sleeps: list[int] = []

    async def sleep_ms(ms: int) -> None:
        sleeps.append(ms)

    fake = FakeLLM(
        [
            (
                match_any(),
                fail_then_succeed(
                    error=RateLimitError(retry_after_ms=2_500),
                    times=1,
                    payload=b'{"answer":"ok"}',
                ),
            )
        ]
    )
    facade = LlmClientFacade(
        fake,
        retry_policy=RetryPolicy(
            max_attempts=3, base_delay_ms=100, max_delay_ms=5_000, jitter_ratio=0.5
        ),
        now_ms=lambda: 0,
        sleep_ms=sleep_ms,
        rand=lambda: 0.99,
    )

    result = await facade.complete(_request(), entity_key=b"k1", seq=1)
    assert result.response == b'{"answer":"ok"}'
    assert sleeps == [2_500]


async def test_non_retryable_server_error_fails_fast_without_sleep() -> None:
    sleeps: list[int] = []

    async def sleep_ms(ms: int) -> None:
        sleeps.append(ms)

    fake = FakeLLM([(match_any(), raise_error(ServerError(status=400)))])
    facade = LlmClientFacade(fake, now_ms=lambda: 0, sleep_ms=sleep_ms)

    with pytest.raises(ServerError) as excinfo:
        await facade.complete(_request(), entity_key=b"k1", seq=1)

    assert excinfo.value.status == 400
    assert fake.call_count == 1
    assert sleeps == []


async def test_jittered_backoff_runs_when_retry_after_is_missing() -> None:
    sleeps: list[int] = []

    async def sleep_ms(ms: int) -> None:
        sleeps.append(ms)

    fake = FakeLLM(
        [
            (
                match_any(),
                fail_then_succeed(
                    error=ServerError(status=503),
                    times=1,
                    payload=b'{"answer":"ok"}',
                ),
            )
        ]
    )
    facade = LlmClientFacade(
        fake,
        retry_policy=RetryPolicy(
            max_attempts=3, base_delay_ms=100, max_delay_ms=1_000, jitter_ratio=0.5
        ),
        now_ms=lambda: 0,
        sleep_ms=sleep_ms,
        rand=lambda: 0.5,
    )

    await facade.complete(_request(), entity_key=b"k1", seq=1)
    assert sleeps == [125]


async def test_per_endpoint_circuit_breaker_short_circuits_and_recovers() -> None:
    clock = _Clock(1_000)
    fake = FakeLLM(
        [
            (
                match_any(),
                fail_then_succeed(
                    error=ServerError(status=503), times=1, payload=b'{"answer":"ok"}'
                ),
            )
        ]
    )
    facade = LlmClientFacade(
        fake,
        now_ms=clock,
        sleep_ms=_no_sleep,
        retry_policy=RetryPolicy(max_attempts=1),
        circuit_breaker_policy=CircuitBreakerPolicy(failure_threshold=1, cooldown_ms=1_000),
    )
    request = _request()

    with pytest.raises(ServerError):
        await facade.complete(request, entity_key=b"k1", seq=1)

    with pytest.raises(CircuitOpenError):
        await facade.complete(request, entity_key=b"k1", seq=2)
    assert fake.call_count == 1

    clock.advance(1_000)
    result = await facade.complete(request, entity_key=b"k1", seq=3)
    assert result.response == b'{"answer":"ok"}'
    assert fake.call_count == 2


async def test_output_schema_validation_rejects_invalid_payload() -> None:
    stub = _StubClient([LlmResponse(b'{"wrong":"shape"}')])
    facade = LlmClientFacade(stub, now_ms=lambda: 0, sleep_ms=_no_sleep)

    with pytest.raises(OutputSchemaError):
        await facade.complete(_request(), entity_key=b"k1", seq=1)


async def test_trace_emission_includes_retry_and_completion_points() -> None:
    traces: list[TraceEvent] = []

    def emit(event: TraceEvent) -> None:
        traces.append(event)

    fake = FakeLLM(
        [
            (
                match_any(),
                fail_then_succeed(
                    error=RateLimitError(retry_after_ms=1), times=1, payload=b'{"answer":"ok"}'
                ),
            )
        ]
    )
    facade = LlmClientFacade(
        fake,
        now_ms=lambda: 0,
        sleep_ms=_no_sleep,
        emit_trace=emit,
    )
    await facade.complete(_request(), entity_key=b"k1", seq=1)

    names = [event.name for event in traces]
    assert "completion_start" in names
    assert "cache_miss" in names
    assert "provider_attempt_error" in names
    assert "retry_scheduled" in names
    assert "provider_attempt_success" in names
    assert names[-1] == "completion_end"
