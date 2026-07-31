"""Deterministic in-process `LLMClient` test double.

See :mod:`beam_agents.model` for the capability overview and the change design
(``openspec/changes/add-fake-llm-provider/design.md``) for the load-bearing
decisions: ordered first-match-wins rules with fail-closed unmatched requests
(D4), injected-hook latency with no wall clock (D5), recording before latency
and failure (D6), per-key counting via ``compute_cache_key``'s request
derivation (D7), and module layout/exports (D8).

Importing this module has no side effects.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass

from beam_agents.model.client import LlmRequest, LlmResponse, ProviderError
from beam_agents.model.replay_cache import compute_cache_key

__all__ = [
    "Behavior",
    "FakeLLM",
    "Matcher",
    "UnmatchedRequestError",
    "fail_then_succeed",
    "match_any",
    "match_contains",
    "match_model_id",
    "raise_error",
    "respond_with",
]

Matcher = Callable[[LlmRequest], bool]


class Behavior:
    """A rule's serving behavior: yields response bytes or raises a `ProviderError`."""

    latency_ms: int

    def serve(self) -> bytes:
        """The response bytes this behavior yields, or the error it raises."""
        raise NotImplementedError


@dataclass(slots=True)
class _RespondBehavior(Behavior):
    payload: bytes
    latency_ms: int = 0

    def serve(self) -> bytes:
        return self.payload


@dataclass(slots=True)
class _RaiseBehavior(Behavior):
    error: ProviderError
    latency_ms: int = 0

    def serve(self) -> bytes:
        raise self.error


class _FailThenSucceedBehavior(Behavior):
    def __init__(
        self, *, error: ProviderError, times: int, payload: bytes, latency_ms: int = 0
    ) -> None:
        self._error = error
        self._times = times
        self._payload = payload
        self.latency_ms = latency_ms
        self._failures_served = 0

    def serve(self) -> bytes:
        if self._failures_served < self._times:
            self._failures_served += 1
            raise self._error
        return self._payload


def respond_with(payload: bytes, *, latency_ms: int = 0) -> Behavior:
    """Behavior constructor: serve `payload` verbatim as the response bytes."""
    return _RespondBehavior(payload=payload, latency_ms=latency_ms)


def raise_error(error: ProviderError, *, latency_ms: int = 0) -> Behavior:
    """Behavior constructor: raise the given `ProviderError` instead of responding."""
    return _RaiseBehavior(error=error, latency_ms=latency_ms)


def fail_then_succeed(
    *, error: ProviderError, times: int, payload: bytes, latency_ms: int = 0
) -> Behavior:
    """Behavior constructor: raise `error` for the first `times` matching calls,
    then serve `payload` on every call after.
    """
    return _FailThenSucceedBehavior(
        error=error, times=times, payload=payload, latency_ms=latency_ms
    )


def match_model_id(model_id: str) -> Matcher:
    """Convenience matcher: matches requests with the given `model_id`."""
    return lambda request: request.model_id == model_id


def match_contains(needle: str) -> Matcher:
    """Convenience matcher: matches requests whose material contains `needle`."""
    return lambda request: needle in repr(request)


def match_any() -> Matcher:
    """Convenience matcher: matches every request."""
    return lambda _request: True


@dataclass(frozen=True, slots=True)
class _Rule:
    matcher: Matcher
    behavior: Behavior


class UnmatchedRequestError(Exception):
    """Raised when no registered rule matches a request.

    Deliberately not a `ProviderError`: a missing test script is an authoring
    bug, not a simulated provider failure, and must not be swallowed by a
    caller's `except ProviderError` retry handling.
    """

    def __init__(self, request: LlmRequest) -> None:
        super().__init__(f"FakeLLM: no rule matched request {request!r}")
        self.request = request


async def _default_delay(ms: int) -> None:
    await asyncio.sleep(ms / 1000)


class FakeLLM:
    """Deterministic, offline `LLMClient` scripted by ordered `(matcher, behavior)` rules.

    Constructible with no arguments (empty script). Every `complete` call
    records the request and increments call counters before matching, applying
    any configured latency, and serving or raising the matched behavior. An
    unmatched request fails closed with `UnmatchedRequestError`.
    """

    def __init__(
        self,
        rules: Sequence[tuple[Matcher, Behavior]] = (),
        *,
        delay: Callable[[int], Awaitable[None]] | None = None,
    ) -> None:
        self._rules: list[_Rule] = [_Rule(matcher, behavior) for matcher, behavior in rules]
        self._delay = delay if delay is not None else _default_delay
        self._log: list[LlmRequest] = []
        self._counts: dict[str, int] = {}

    def add_rule(self, matcher: Matcher, behavior: Behavior) -> None:
        """Append a rule to the end of the script (lowest match priority)."""
        self._rules.append(_Rule(matcher, behavior))

    @property
    def requests(self) -> tuple[LlmRequest, ...]:
        """Read-only, call-ordered log of every recorded `LlmRequest`."""
        return tuple(self._log)

    @property
    def call_count(self) -> int:
        """Total number of `complete` invocations, success or raise."""
        return len(self._log)

    def calls_for(self, request: LlmRequest) -> int:
        """Invocation count for requests logically equal to `request`."""
        return self._counts.get(_request_key(request), 0)

    async def complete(self, request: LlmRequest) -> LlmResponse:
        """Serve ``request`` from the scripted behaviors, recording it first.

        Raises :class:`UnmatchedRequestError` when no matcher applies: a
        test that reaches an unscripted request has a gap, not a default.
        """
        self._log.append(request)
        key = _request_key(request)
        self._counts[key] = self._counts.get(key, 0) + 1

        behavior = self._match(request)
        if behavior.latency_ms:
            await self._delay(behavior.latency_ms)
        return LlmResponse(behavior.serve())

    def _match(self, request: LlmRequest) -> Behavior:
        for rule in self._rules:
            if rule.matcher(request):
                return rule.behavior
        raise UnmatchedRequestError(request)


def _request_key(request: LlmRequest) -> str:
    """Canonical per-request key, derived the same way `compute_cache_key`
    derives its request portion (D7): the activation-scoped `entity_key`/`seq`
    are held constant so only the request material perturbs the key.
    """
    return compute_cache_key(
        request.model_id,
        request.messages,
        request.tools_schema,
        request.sampling_params,
        entity_key=b"",
        seq=0,
    )
