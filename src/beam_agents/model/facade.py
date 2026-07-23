"""Async facade around ``LLMClient`` with retry, circuit breaking, cache, and traces."""

from __future__ import annotations

import json
import random
import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from beam_agents.model.client import (
    CircuitOpenError,
    LLMClient,
    LlmRequest,
    LlmResponse,
    OutputSchemaError,
    ProviderError,
    ProviderTimeout,
    RateLimitError,
    ServerError,
)
from beam_agents.model.replay_cache import ReplayCache, compute_cache_key

_RETRYABLE_5XX_MIN = 500
_RETRYABLE_5XX_MAX = 599


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay_ms: int = 250
    max_delay_ms: int = 5_000
    jitter_ratio: float = 0.25


@dataclass(frozen=True, slots=True)
class CircuitBreakerPolicy:
    failure_threshold: int = 3
    cooldown_ms: int = 30_000


@dataclass(frozen=True, slots=True)
class TraceEvent:
    name: str
    attributes: dict[str, str]


@dataclass(slots=True)
class _CircuitState:
    failures: int = 0
    opened_at_ms: int | None = None
    half_open: bool = False


class LlmClientFacade:
    """Provider-neutral completion facade around an ``LLMClient``."""

    def __init__(
        self,
        client: LLMClient,
        *,
        replay_cache: ReplayCache | None = None,
        retry_policy: RetryPolicy | None = None,
        circuit_breaker_policy: CircuitBreakerPolicy | None = None,
        endpoint_key: Callable[[LlmRequest], str] | None = None,
        now_ms: Callable[[], int] | None = None,
        sleep_ms: Callable[[int], Awaitable[None]] | None = None,
        rand: Callable[[], float] | None = None,
        emit_trace: Callable[[TraceEvent], None] | None = None,
    ) -> None:
        self._client = client
        self._replay_cache = replay_cache
        self._retry_policy = retry_policy or RetryPolicy()
        self._breaker_policy = circuit_breaker_policy or CircuitBreakerPolicy()
        self._endpoint_key = endpoint_key or (lambda request: request.model_id)
        self._now_ms = now_ms or (lambda: 0)
        self._sleep_ms = sleep_ms or _default_sleep_ms
        self._rand = rand or random.random
        self._emit_trace = emit_trace
        self._circuits: dict[str, _CircuitState] = {}

    async def complete(self, request: LlmRequest, *, entity_key: bytes, seq: int) -> LlmResponse:
        normalized = _normalize_request(request)
        endpoint = self._endpoint_key(normalized)
        self._emit(
            "completion_start",
            endpoint_key=endpoint,
            model_id=normalized.model_id,
            seq=str(seq),
        )
        self._assert_circuit_allows_call(endpoint)

        cache_key = compute_cache_key(
            normalized.model_id,
            normalized.messages,
            normalized.tools_schema,
            normalized.output_schema,
            normalized.sampling_params,
            entity_key=entity_key,
            seq=seq,
        )
        if self._replay_cache is not None:
            cached = self._replay_cache.get(cache_key)
            if cached is not None:
                self._emit("cache_hit", endpoint_key=endpoint, cache_key=cache_key)
                usage = _extract_usage(cached.response)
                response = LlmResponse(
                    cached.response,
                    input_tokens=usage[0],
                    output_tokens=usage[1],
                    total_tokens=usage[2],
                    from_replay_cache=True,
                    _source_response_digest=cached.response_digest,
                )
                self._emit("completion_end", endpoint_key=endpoint, cache_status="hit")
                return response
        self._emit("cache_miss", endpoint_key=endpoint, cache_key=cache_key)

        attempt = 0
        while True:
            attempt += 1
            self._emit(
                "provider_attempt_start",
                endpoint_key=endpoint,
                model_id=normalized.model_id,
                attempt=str(attempt),
            )
            try:
                provider_response = await self._client.complete(normalized)
                self._on_success(endpoint)
                self._validate_output_schema(provider_response.response, normalized.output_schema)
                if self._replay_cache is not None:
                    self._replay_cache.put(cache_key, provider_response.response)
                usage = _coalesce_usage(provider_response, provider_response.response)
                response = LlmResponse(
                    provider_response.response,
                    input_tokens=usage[0],
                    output_tokens=usage[1],
                    total_tokens=usage[2],
                    from_replay_cache=False,
                )
                self._emit("provider_attempt_success", endpoint_key=endpoint, attempt=str(attempt))
                self._emit("completion_end", endpoint_key=endpoint, cache_status="miss")
                return response
            except ProviderError as error:
                self._emit(
                    "provider_attempt_error",
                    endpoint_key=endpoint,
                    attempt=str(attempt),
                    error_type=type(error).__name__,
                )
                retryable = _is_retryable(error)
                if retryable:
                    self._on_retryable_failure(endpoint)
                if (
                    not retryable
                    or attempt >= self._retry_policy.max_attempts
                    or self._is_circuit_open(endpoint)
                ):
                    self._emit(
                        "completion_error", endpoint_key=endpoint, error_type=type(error).__name__
                    )
                    raise

                delay_ms = _retry_delay_ms(error, attempt, self._retry_policy, self._rand)
                self._emit(
                    "retry_scheduled",
                    endpoint_key=endpoint,
                    attempt=str(attempt),
                    retry_delay_ms=str(delay_ms),
                )
                await self._sleep_ms(delay_ms)

    def _emit(self, name: str, **attrs: str) -> None:
        if self._emit_trace is None:
            return
        self._emit_trace(TraceEvent(name=name, attributes=attrs))

    def _state_for(self, endpoint_key: str) -> _CircuitState:
        state = self._circuits.get(endpoint_key)
        if state is None:
            state = _CircuitState()
            self._circuits[endpoint_key] = state
        return state

    def _assert_circuit_allows_call(self, endpoint_key: str) -> None:
        state = self._state_for(endpoint_key)
        if state.opened_at_ms is None:
            return
        now = self._now_ms()
        elapsed = now - state.opened_at_ms
        if elapsed < self._breaker_policy.cooldown_ms:
            retry_after_ms = self._breaker_policy.cooldown_ms - elapsed
            self._emit(
                "circuit_short_circuit",
                endpoint_key=endpoint_key,
                retry_after_ms=str(retry_after_ms),
            )
            raise CircuitOpenError(endpoint_key, retry_after_ms)
        state.half_open = True

    def _on_success(self, endpoint_key: str) -> None:
        state = self._state_for(endpoint_key)
        state.failures = 0
        state.opened_at_ms = None
        state.half_open = False

    def _on_retryable_failure(self, endpoint_key: str) -> None:
        state = self._state_for(endpoint_key)
        if state.half_open:
            state.opened_at_ms = self._now_ms()
            state.failures = self._breaker_policy.failure_threshold
            state.half_open = False
            return
        state.failures += 1
        if state.failures >= self._breaker_policy.failure_threshold:
            state.opened_at_ms = self._now_ms()

    def _is_circuit_open(self, endpoint_key: str) -> bool:
        state = self._state_for(endpoint_key)
        if state.opened_at_ms is None:
            return False
        return self._now_ms() - state.opened_at_ms < self._breaker_policy.cooldown_ms

    @staticmethod
    def _validate_output_schema(response: bytes, output_schema: object | None) -> None:
        if output_schema is None:
            return
        try:
            payload = json.loads(response.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise OutputSchemaError("response is not valid utf-8 JSON") from error
        _validate_schema_value(payload, output_schema)


async def _default_sleep_ms(ms: int) -> None:
    await _sleep_ms_impl(ms)


async def _sleep_ms_impl(ms: int) -> None:
    # Isolated to keep the default path import-light for tests that inject sleep.
    await asyncio.sleep(ms / 1000)


def _normalize_request(request: LlmRequest) -> LlmRequest:
    return LlmRequest(
        model_id=request.model_id,
        messages=_canonicalize_json_like(request.messages),
        tools_schema=_canonicalize_json_like(request.tools_schema),
        output_schema=_canonicalize_json_like(request.output_schema),
        sampling_params=_canonicalize_json_like(request.sampling_params),
    )


def _canonicalize_json_like(value: object) -> object:
    canonical = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return json.loads(canonical)


def _is_retryable(error: ProviderError) -> bool:
    if isinstance(error, (RateLimitError, ProviderTimeout)):
        return True
    if isinstance(error, ServerError):
        return _RETRYABLE_5XX_MIN <= error.status <= _RETRYABLE_5XX_MAX
    return False


def _retry_delay_ms(
    error: ProviderError,
    attempt: int,
    policy: RetryPolicy,
    rand: Callable[[], float],
) -> int:
    if (
        isinstance(error, RateLimitError)
        and error.retry_after_ms is not None
        and error.retry_after_ms > 0
    ):
        return min(error.retry_after_ms, policy.max_delay_ms)
    exp = policy.base_delay_ms * (2 ** max(attempt - 1, 0))
    bounded = min(exp, policy.max_delay_ms)
    jitter = int(bounded * policy.jitter_ratio * rand())
    return min(bounded + jitter, policy.max_delay_ms)


def _coalesce_usage(
    response: LlmResponse, payload: bytes
) -> tuple[int | None, int | None, int | None]:
    extracted = _extract_usage(payload)
    input_tokens = response.input_tokens if response.input_tokens is not None else extracted[0]
    output_tokens = response.output_tokens if response.output_tokens is not None else extracted[1]
    total_tokens = response.total_tokens if response.total_tokens is not None else extracted[2]
    if total_tokens is None and input_tokens is not None and output_tokens is not None:
        total_tokens = input_tokens + output_tokens
    return input_tokens, output_tokens, total_tokens


def _extract_usage(payload: bytes) -> tuple[int | None, int | None, int | None]:
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, None, None
    if not isinstance(parsed, dict):
        return None, None, None
    usage_raw = parsed.get("usage")
    if not isinstance(usage_raw, dict):
        return None, None, None
    in_tokens = _as_int(usage_raw.get("input_tokens"))
    out_tokens = _as_int(usage_raw.get("output_tokens"))
    total_tokens = _as_int(usage_raw.get("total_tokens"))
    if total_tokens is None and in_tokens is not None and out_tokens is not None:
        total_tokens = in_tokens + out_tokens
    return in_tokens, out_tokens, total_tokens


def _as_int(value: object) -> int | None:
    if isinstance(value, int):
        return value
    return None


def _validate_schema_value(value: object, schema: object) -> None:  # noqa: PLR0912
    if not isinstance(schema, dict):
        raise OutputSchemaError("output_schema must be an object schema")

    expected_type = schema.get("type")
    if isinstance(expected_type, str) and not _matches_type(value, expected_type):
        raise OutputSchemaError(f"schema type mismatch: expected {expected_type}")

    enum_values = schema.get("enum")
    if isinstance(enum_values, list) and value not in enum_values:
        raise OutputSchemaError("schema enum mismatch")

    if expected_type == "object":
        if not isinstance(value, dict):
            raise OutputSchemaError("schema type mismatch: expected object")
        properties = schema.get("properties", {})
        if not isinstance(properties, dict):
            raise OutputSchemaError("schema properties must be an object")
        required = schema.get("required", [])
        if not isinstance(required, list):
            raise OutputSchemaError("schema required must be an array")
        required_keys = [item for item in required if isinstance(item, str)]
        for key in required_keys:
            if key not in value:
                raise OutputSchemaError(f"missing required property: {key}")
        for key, child in properties.items():
            if isinstance(key, str) and key in value:
                _validate_schema_value(value[key], child)
        if schema.get("additionalProperties") is False:
            unknown = set(value.keys()) - {k for k in properties if isinstance(k, str)}
            if unknown:
                raise OutputSchemaError(f"unknown property: {next(iter(unknown))}")
        return

    if expected_type == "array":
        if not isinstance(value, list):
            raise OutputSchemaError("schema type mismatch: expected array")
        item_schema = schema.get("items")
        if item_schema is None:
            return
        for item in value:
            _validate_schema_value(item, item_schema)


def _matches_type(value: object, expected: str) -> bool:  # noqa: PLR0911
    if expected == "object":
        return isinstance(value, dict)
    if expected == "array":
        return isinstance(value, list)
    if expected == "string":
        return isinstance(value, str)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, int | float) and not isinstance(value, bool)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "null":
        return value is None
    return True
