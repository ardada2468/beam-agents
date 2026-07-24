"""Non-retryable taxonomy tests for the `model-client` capability.

Covers: `ProviderRequestError` carries `status`, is deliberately NOT a
`ProviderError` subclass, and is not caught by an `except ProviderError`
retry handler.
"""

from __future__ import annotations

import pytest

from beam_agents.model import ProviderError, ProviderRequestError

# --- Requirement: Typed provider-error taxonomy (non-retryable) --------------


def test_non_retryable_request_error_carries_status() -> None:
    # Scenario: Non-retryable request error is outside the retryable base.
    error = ProviderRequestError(status=400)

    assert error.status == 400


def test_non_retryable_request_error_is_not_a_provider_error() -> None:
    # Scenario: Non-retryable request error is outside the retryable base.
    error = ProviderRequestError(status=400)

    assert not isinstance(error, ProviderError)


def test_except_provider_error_does_not_catch_it() -> None:
    # Scenario: Non-retryable request error is outside the retryable base.
    with pytest.raises(ProviderRequestError):
        try:
            raise ProviderRequestError(status=401)
        except ProviderError:
            pytest.fail("ProviderError incorrectly caught a ProviderRequestError")
