"""Resilient async facade over a provider `LLMClient`.

See :mod:`beam_agents.model` for the capability overview and the change design
(``openspec/changes/async-llmclient-facade/design.md``) for the load-bearing
decisions: a rich per-call `FacadeResult` (D1), provider neutrality via an
injected `decode` callable (D2), `output_schema` folded into the effective
request before cache-key hashing (D3), full-jitter exponential backoff with a
Retry-After floor (D4), a worker-local per-endpoint `CircuitBreaker` singleton
checked after the cache but before the provider (D5), staged-only trace/usage
effects (D6), and module/export layout (D7).

Importing this module has no side effects.
"""

from __future__ import annotations

import enum
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ValidationError

from beam_agents._protos import TraceEvent
from beam_agents.model.client import (
    LLMClient,
    LlmRequest,
    LlmResponse,
    ProviderError,
    RateLimitError,
)
from beam_agents.model.replay_cache import ReplayCache, compute_cache_key

Sleep = Callable[[int], Awaitable[None]]

# Fixed key under which the output_schema's JSON Schema is folded into the
# effective request's sampling_params, so a schema change perturbs the cache
# key (D3) without widening the provider-neutral LlmRequest shape.
_OUTPUT_SCHEMA_PARAM_KEY = "response_schema"


@dataclass(frozen=True, slots=True)
class TokenUsage:
    """Per-call token counts, decoded from the provider's opaque response bytes."""

    prompt_tokens: int
    completion_tokens: int
    total_tokens: int


@dataclass(frozen=True, slots=True)
class DecodedResponse:
    """Provider-specific decode of `LlmResponse.response`: usage plus JSON text.

    Keeps `facade.py` provider-neutral (D2): every real provider ships its own
    `Decode` callable, and this module never parses provider-shaped bytes itself.
    """

    usage: TokenUsage
    text: str


Decode = Callable[[bytes], DecodedResponse]


@dataclass(frozen=True, slots=True)
class FacadeResult:
    """Everything a caller needs from one `LlmFacade.complete` call."""

    response: LlmResponse
    parsed: BaseModel | None
    usage: TokenUsage
    cache_hit: bool
    attempts: int


class CircuitOpenError(Exception):
    """Raised when a per-endpoint `CircuitBreaker` is `OPEN` and fails a call closed.

    Deliberately not a `ProviderError`: the endpoint was never contacted, so this
    is neither a transport failure the `RetryPolicy` should retry nor a failure
    the breaker itself should count against its own threshold.
    """

    def __init__(self, endpoint: str) -> None:
        super().__init__(f"circuit open for endpoint {endpoint!r}")
        self.endpoint = endpoint


class OutputSchemaError(Exception):
    """Raised when a response fails `output_schema` JSON parsing/validation.

    Deliberately not a `ProviderError`: the provider transport already
    succeeded, so this is a caller/provider output-contract mismatch, not a
    retryable transport fault — the transport `RetryPolicy` must not retry it.
    """


class CircuitState(enum.Enum):
    """Breaker states: `CLOSED` (pass-through), `OPEN` (fail fast), `HALF_OPEN` (one trial)."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Bounds and shape for the facade's jittered exponential backoff.

    `max_attempts` caps total provider attempts; the pre-jitter delay for
    attempt *n* is ``min(base_ms * 2**(n-1), max_ms)`` (D4).
    """

    max_attempts: int
    base_ms: int
    max_ms: int


@runtime_checkable
class StagingSink(Protocol):
    """Injected sink for trace/usage effects, staged like every other activation
    effect (D6): a failed/timed-out activation contributes nothing because the
    caller only commits staged effects on success.
    """

    def stage_trace_event(self, event: TraceEvent) -> None: ...

    def accumulate_usage(self, usage: TokenUsage) -> None: ...


class CircuitBreaker:
    """Worker-local, per-endpoint breaker (D5): never keyed Beam state.

    Consecutive retryable transport failures reaching `threshold` trip the
    breaker `OPEN`; after `cooldown_ms` (measured against an injected
    `now_ms`) it allows one `HALF_OPEN` trial call. A success resets it to
    `CLOSED`; a failure returns it to `OPEN`.
    """

    def __init__(self, *, endpoint: str, threshold: int, cooldown_ms: int) -> None:
        self._endpoint = endpoint
        self._threshold = threshold
        self._cooldown_ms = cooldown_ms
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._opened_at_ms: int | None = None

    @property
    def state(self) -> CircuitState:
        return self._state

    def before_call(self, now_ms: int) -> None:
        """Raise `CircuitOpenError` while `OPEN`; otherwise let the call through.

        A cooldown-elapsed `OPEN` breaker transitions to `HALF_OPEN` and admits
        this one trial call.
        """
        if self._state is CircuitState.OPEN:
            assert self._opened_at_ms is not None
            if now_ms - self._opened_at_ms < self._cooldown_ms:
                raise CircuitOpenError(self._endpoint)
            self._state = CircuitState.HALF_OPEN

    def record_success(self) -> None:
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._opened_at_ms = None

    def record_failure(self, now_ms: int) -> None:
        if self._state is CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            self._opened_at_ms = now_ms
            return
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._threshold:
            self._state = CircuitState.OPEN
            self._opened_at_ms = now_ms


def _fold_output_schema(request: LlmRequest, output_schema: type[BaseModel] | None) -> LlmRequest:
    """Build the effective request the provider is called with and the cache is
    keyed on: `output_schema`'s JSON Schema folded into `sampling_params` (D3).
    """
    if output_schema is None:
        return request
    schema_dict = output_schema.model_json_schema()
    sampling_params = request.sampling_params
    folded: object
    if isinstance(sampling_params, dict):
        folded = {**sampling_params, _OUTPUT_SCHEMA_PARAM_KEY: schema_dict}
    else:
        folded = {"params": sampling_params, _OUTPUT_SCHEMA_PARAM_KEY: schema_dict}
    return replace(request, sampling_params=folded)


class LlmFacade:
    """Resilient async facade: cache -> breaker -> retry -> decode -> usage ->
    output-schema parse -> trace, behind a single `complete` call.

    Constructed per activation from the wrapped provider `LLMClient`, the
    activation's `ReplayCache`, and every non-determinism source injected
    (`now_ms`, `rng`, `sleep`) so a replayed bundle behaves identically. The
    `CircuitBreaker` is a worker-local singleton shared across activations
    (D5); everything else is activation-scoped.
    """

    def __init__(
        self,
        provider: LLMClient,
        replay_cache: ReplayCache,
        *,
        now_ms: int,
        rng: random.Random,
        sleep: Sleep,
        breaker: CircuitBreaker,
        retry_policy: RetryPolicy,
        decode: Decode,
        staging: StagingSink,
    ) -> None:
        self._provider = provider
        self._replay_cache = replay_cache
        self._now_ms = now_ms
        self._rng = rng
        self._sleep = sleep
        self._breaker = breaker
        self._retry_policy = retry_policy
        self._decode = decode
        self._staging = staging

    async def complete(
        self,
        request: LlmRequest,
        *,
        entity_key: bytes,
        seq: int,
        step_index: int,
        output_schema: type[BaseModel] | None = None,
    ) -> FacadeResult:
        effective_request = _fold_output_schema(request, output_schema)
        cache_key = compute_cache_key(
            effective_request.model_id,
            effective_request.messages,
            effective_request.tools_schema,
            effective_request.sampling_params,
            entity_key,
            seq,
        )

        # Cache-first: a live hit returns with zero provider calls and never
        # consults the breaker, so replay works even while the endpoint is
        # unhealthy (correctness invariant 3).
        cached = self._replay_cache.get(cache_key)
        if cached is not None and not cached.digest_only:
            return self._finish(
                cached.response,
                output_schema=output_schema,
                entity_key=entity_key,
                seq=seq,
                step_index=step_index,
                model_id=effective_request.model_id,
                cache_hit=True,
                attempts=0,
            )

        try:
            self._breaker.before_call(self._now_ms)
        except CircuitOpenError as exc:
            self._stage_trace(
                model_id=effective_request.model_id,
                entity_key=entity_key,
                seq=seq,
                step_index=step_index,
                cache_hit=False,
                attempts=0,
                usage=None,
                error=exc,
            )
            raise

        response, attempts, error = await self._call_with_retry(effective_request)
        if error is not None:
            self._breaker.record_failure(self._now_ms)
            self._stage_trace(
                model_id=effective_request.model_id,
                entity_key=entity_key,
                seq=seq,
                step_index=step_index,
                cache_hit=False,
                attempts=attempts,
                usage=None,
                error=error,
            )
            raise error
        assert response is not None

        self._breaker.record_success()
        self._replay_cache.put(cache_key, response.response)

        return self._finish(
            response.response,
            output_schema=output_schema,
            entity_key=entity_key,
            seq=seq,
            step_index=step_index,
            model_id=effective_request.model_id,
            cache_hit=False,
            attempts=attempts,
            response_override=response,
        )

    async def _call_with_retry(
        self, request: LlmRequest
    ) -> tuple[LlmResponse | None, int, ProviderError | None]:
        """Transport-only retry loop: classifies by `ProviderError` subclass,
        never string-matches. Returns the outcome rather than raising so the
        caller can record breaker/trace state before propagating a failure.
        """
        for attempt in range(1, self._retry_policy.max_attempts + 1):
            try:
                response = await self._provider.complete(request)
            except ProviderError as exc:
                if attempt == self._retry_policy.max_attempts:
                    return None, attempt, exc
                await self._sleep(self._compute_delay_ms(attempt, exc))
                continue
            return response, attempt, None
        raise AssertionError("unreachable: retry loop always returns")

    def _compute_delay_ms(self, attempt: int, error: ProviderError) -> int:
        pre_jitter = min(
            self._retry_policy.base_ms * (2 ** (attempt - 1)), self._retry_policy.max_ms
        )
        delay_ms = int(self._rng.uniform(0, pre_jitter)) if pre_jitter > 0 else 0
        if isinstance(error, RateLimitError) and error.retry_after_ms is not None:
            delay_ms = max(delay_ms, error.retry_after_ms)
        return delay_ms

    def _finish(
        self,
        response_bytes: bytes,
        *,
        output_schema: type[BaseModel] | None,
        entity_key: bytes,
        seq: int,
        step_index: int,
        model_id: str,
        cache_hit: bool,
        attempts: int,
        response_override: LlmResponse | None = None,
    ) -> FacadeResult:
        decoded = self._decode(response_bytes)
        if not cache_hit:
            self._staging.accumulate_usage(decoded.usage)

        parsed: BaseModel | None = None
        if output_schema is not None:
            try:
                parsed = output_schema.model_validate_json(decoded.text)
            except (ValidationError, ValueError) as exc:
                self._stage_trace(
                    model_id=model_id,
                    entity_key=entity_key,
                    seq=seq,
                    step_index=step_index,
                    cache_hit=cache_hit,
                    attempts=attempts,
                    usage=decoded.usage,
                    error=exc,
                )
                raise OutputSchemaError(str(exc)) from exc

        self._stage_trace(
            model_id=model_id,
            entity_key=entity_key,
            seq=seq,
            step_index=step_index,
            cache_hit=cache_hit,
            attempts=attempts,
            usage=decoded.usage,
            error=None,
        )
        response = (
            response_override if response_override is not None else LlmResponse(response_bytes)
        )
        return FacadeResult(
            response=response,
            parsed=parsed,
            usage=decoded.usage,
            cache_hit=cache_hit,
            attempts=attempts,
        )

    def _stage_trace(
        self,
        *,
        model_id: str,
        entity_key: bytes,
        seq: int,
        step_index: int,
        cache_hit: bool,
        attempts: int,
        usage: TokenUsage | None,
        error: BaseException | None,
    ) -> None:
        attributes = {
            "gen_ai.request.model": model_id,
            "gen_ai.usage.input_tokens": str(usage.prompt_tokens if usage is not None else 0),
            "gen_ai.usage.output_tokens": str(usage.completion_tokens if usage is not None else 0),
            "beam_agents.cache_hit": "true" if cache_hit else "false",
            "beam_agents.attempts": str(attempts),
            "beam_agents.circuit_state": self._breaker.state.value,
        }
        if error is not None:
            attributes["error.type"] = type(error).__name__
        event = TraceEvent(
            entity_key=entity_key,
            seq=seq,
            step_index=step_index,
            event_type=TraceEvent.LLM_CALL,
            attributes=attributes,
            start_ms=self._now_ms,
            end_ms=self._now_ms,
        )
        self._staging.stage_trace_event(event)
