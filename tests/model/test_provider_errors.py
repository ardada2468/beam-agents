"""Provider-error taxonomy tests for the `model-client` capability.

Covers: `RateLimitError`/`ServerError` expose their structured attributes,
`ProviderTimeout` is a distinct type, and a single `except ProviderError`
catches every subclass.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from beam_agents.model import ProviderError, ProviderTimeout, RateLimitError, ServerError

# --- Requirement: Typed provider-error taxonomy ------------------------------


def test_rate_limit_error_carries_429_semantics() -> None:
    # Scenario: Rate-limit error carries 429 semantics.
    error = RateLimitError(retry_after_ms=1500)

    assert isinstance(error, ProviderError)
    assert error.retry_after_ms == 1500
    assert not isinstance(error, ServerError)
    assert not isinstance(error, ProviderTimeout)


def test_server_error_carries_its_status() -> None:
    # Scenario: Server error carries its status.
    error = ServerError(status=503)

    assert isinstance(error, ProviderError)
    assert error.status == 503


def test_timeout_is_its_own_type() -> None:
    # Scenario: Timeout is its own type.
    error = ProviderTimeout()

    assert isinstance(error, ProviderError)
    assert not isinstance(error, RateLimitError)
    assert not isinstance(error, ServerError)


@pytest.mark.parametrize(
    "make_error",
    [
        lambda: RateLimitError(retry_after_ms=None),
        lambda: ServerError(status=500),
        ProviderTimeout,
    ],
)
def test_base_type_catches_all_provider_failures(make_error: Callable[[], ProviderError]) -> None:
    # Scenario: Base type catches all provider failures.
    error = make_error()

    try:
        raise error
    except ProviderError as caught:
        assert caught is error
    else:
        pytest.fail("ProviderError did not catch the raised error")
