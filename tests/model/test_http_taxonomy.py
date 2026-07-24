"""Tests for the shared HTTP-outcome -> provider-error-taxonomy mapper
(`beam_agents.model._http`), the `model-providers` capability requirement
"HTTP outcomes map onto the retryable/non-retryable taxonomy".

All offline: no network, built directly from `httpx.Response`/exceptions.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from beam_agents.model._http import raise_for_status_taxonomy, wrap_timeout
from beam_agents.model.client import (
    ProviderRequestError,
    ProviderTimeout,
    RateLimitError,
    ServerError,
)


def _response(status: int, *, headers: dict[str, str] | None = None) -> httpx.Response:
    return httpx.Response(
        status, headers=headers or {}, request=httpx.Request("POST", "https://example.test")
    )


def test_429_with_retry_after_maps_to_rate_limit_error() -> None:
    # Scenario: 429 maps to a retryable rate-limit error with Retry-After.
    response = _response(429, headers={"Retry-After": "2"})

    with pytest.raises(RateLimitError) as exc_info:
        raise_for_status_taxonomy(response)
    assert exc_info.value.retry_after_ms == 2000


def test_429_without_retry_after_leaves_retry_after_ms_none() -> None:
    response = _response(429)

    with pytest.raises(RateLimitError) as exc_info:
        raise_for_status_taxonomy(response)
    assert exc_info.value.retry_after_ms is None


def test_429_with_non_numeric_retry_after_leaves_retry_after_ms_none() -> None:
    response = _response(429, headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"})

    with pytest.raises(RateLimitError) as exc_info:
        raise_for_status_taxonomy(response)
    assert exc_info.value.retry_after_ms is None


@pytest.mark.parametrize("status", [500, 502, 503])
def test_5xx_maps_to_retryable_server_error(status: int) -> None:
    # Scenario: 5xx maps to a retryable server error carrying its status.
    response = _response(status)

    with pytest.raises(ServerError) as exc_info:
        raise_for_status_taxonomy(response)
    assert exc_info.value.status == status


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
def test_non_429_4xx_maps_to_non_retryable_error(status: int) -> None:
    # Scenario: A non-429 4xx maps to the non-retryable error.
    response = _response(status)

    with pytest.raises(ProviderRequestError) as exc_info:
        raise_for_status_taxonomy(response)
    assert exc_info.value.status == status


def test_2xx_does_not_raise() -> None:
    raise_for_status_taxonomy(_response(200))


def test_classification_is_by_status_code_not_message() -> None:
    # Scenario coverage for "never string-matches messages": a response with
    # an empty/absent body still classifies purely off the numeric status.
    response = _response(503, headers={})
    with pytest.raises(ServerError):
        raise_for_status_taxonomy(response)


def test_timeout_exception_maps_to_provider_timeout() -> None:
    # Scenario: A transport timeout maps to ProviderTimeout.
    async def _boom() -> None:
        raise httpx.TimeoutException("timed out")

    async def run() -> None:
        async with wrap_timeout():
            await _boom()

    with pytest.raises(ProviderTimeout):
        asyncio.run(run())
