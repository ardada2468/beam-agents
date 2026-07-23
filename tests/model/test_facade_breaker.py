"""Circuit-breaker tests for the `model-facade` capability.

Covers: consecutive failures trip the breaker open, an open breaker fast-fails
without a provider call, cooldown elapses into a half-open trial, a half-open
success resets to closed, and a half-open failure re-opens the breaker.
"""

from __future__ import annotations

import pytest

from beam_agents.model import CircuitBreaker, CircuitOpenError, CircuitState

# --- Requirement: Per-endpoint circuit breaker -------------------------------


def test_consecutive_failures_trip_the_breaker_open() -> None:
    # Scenario: Consecutive failures trip the breaker open.
    breaker = CircuitBreaker(endpoint="anthropic", threshold=3, cooldown_ms=10_000)

    breaker.record_failure(now_ms=1_000)
    breaker.record_failure(now_ms=1_001)
    state_before_trip: CircuitState = breaker.state
    assert state_before_trip is CircuitState.CLOSED

    breaker.record_failure(now_ms=1_002)
    state_after_trip: CircuitState = breaker.state
    assert state_after_trip is CircuitState.OPEN

    with pytest.raises(CircuitOpenError) as excinfo:
        breaker.before_call(now_ms=1_003)
    assert excinfo.value.endpoint == "anthropic"


def test_cooldown_elapses_into_a_half_open_trial() -> None:
    # Scenario: Cooldown elapses into a half-open trial.
    breaker = CircuitBreaker(endpoint="anthropic", threshold=1, cooldown_ms=5_000)
    breaker.record_failure(now_ms=0)
    state_after_trip: CircuitState = breaker.state
    assert state_after_trip is CircuitState.OPEN

    # Before cooldown: still fast-fails.
    with pytest.raises(CircuitOpenError):
        breaker.before_call(now_ms=4_999)

    # Cooldown elapsed: exactly one trial call is permitted.
    breaker.before_call(now_ms=5_000)
    state_after_cooldown: CircuitState = breaker.state
    assert state_after_cooldown is CircuitState.HALF_OPEN

    breaker.record_success()
    state_after_success: CircuitState = breaker.state
    assert state_after_success is CircuitState.CLOSED


def test_half_open_failure_reopens_the_breaker() -> None:
    # Scenario: A half-open failure re-opens the breaker.
    breaker = CircuitBreaker(endpoint="anthropic", threshold=1, cooldown_ms=1_000)
    breaker.record_failure(now_ms=0)
    breaker.before_call(now_ms=1_000)
    state_half_open: CircuitState = breaker.state
    assert state_half_open is CircuitState.HALF_OPEN

    breaker.record_failure(now_ms=1_000)

    state_reopened: CircuitState = breaker.state
    assert state_reopened is CircuitState.OPEN
    with pytest.raises(CircuitOpenError):
        breaker.before_call(now_ms=1_000)


def test_open_breaker_before_call_raises_without_touching_provider() -> None:
    # Scenario: an OPEN breaker fails `before_call` fast (no provider call site
    # exists at this layer — asserting the pure raise is the unit-level contract
    # the facade's `complete` relies on to skip the transport entirely).
    breaker = CircuitBreaker(endpoint="anthropic", threshold=1, cooldown_ms=1_000)
    breaker.record_failure(now_ms=0)

    with pytest.raises(CircuitOpenError):
        breaker.before_call(now_ms=500)
