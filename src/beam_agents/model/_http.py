"""Shared HTTP-outcome -> provider-error-taxonomy mapping (design D2).

Both real providers (`model/anthropic.py`, `model/openai_compat.py`) route
every terminal HTTP outcome through this module so the mapping is defined
once: 429 -> `RateLimitError`, 5xx -> `ServerError`, transport timeout ->
`ProviderTimeout` (all retryable), non-429 4xx -> `ProviderRequestError`
(non-retryable). Classification is always by status code or exception type,
never by parsing response text.

Importing this module has no side effects.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator

import httpx

from beam_agents.model.client import (
    ProviderRequestError,
    ProviderTimeout,
    RateLimitError,
    ServerError,
)

_HTTP_TOO_MANY_REQUESTS = 429
_HTTP_CLIENT_ERROR_START = 400
_HTTP_SERVER_ERROR_START = 500


def raise_for_status_taxonomy(response: httpx.Response) -> None:
    """Raise the taxonomy member matching `response.status_code`; no-op on 2xx.

    Design D5: `Retry-After` parsing is seconds-only; a missing or
    non-numeric header yields `retry_after_ms=None` and the facade falls back
    to its own backoff.
    """
    status = response.status_code
    if status < _HTTP_CLIENT_ERROR_START:
        return
    if status == _HTTP_TOO_MANY_REQUESTS:
        raise RateLimitError(retry_after_ms=_parse_retry_after_ms(response))
    if status >= _HTTP_SERVER_ERROR_START:
        raise ServerError(status=status)
    raise ProviderRequestError(status=status)


def _parse_retry_after_ms(response: httpx.Response) -> int | None:
    raw = response.headers.get("retry-after")
    if raw is None:
        return None
    try:
        return int(float(raw) * 1000)
    except ValueError:
        return None


@contextlib.asynccontextmanager
async def wrap_timeout() -> AsyncIterator[None]:
    """Convert `httpx.TimeoutException` into the retryable `ProviderTimeout`."""
    try:
        yield
    except httpx.TimeoutException as exc:
        raise ProviderTimeout() from exc
