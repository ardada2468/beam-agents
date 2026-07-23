"""Provider-neutral async model-client seam.

See :mod:`beam_agents.model` for the capability overview and the change design
(``openspec/changes/add-fake-llm-provider/design.md``) for the load-bearing
decisions: opaque-bytes ``LlmResponse`` and request material excluding the
activation-scoped ``key``/``seq`` (D1), ``LLMClient`` as an async-only
structural ``Protocol`` (D2), and the typed, by-class-retryable error
taxonomy (D3).

Importing this module has no side effects.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class LlmRequest:
    """Provider-neutral request material: the four components ``compute_cache_key``
    hashes, minus the activation-scoped ``entity_key``/``seq`` that scope the
    cache rather than the call.

    ``messages``/``tools_schema``/``sampling_params`` are typed ``object``
    (provider-shaped Python), exactly as ``compute_cache_key`` already treats
    them.
    """

    model_id: str
    messages: object
    tools_schema: object
    sampling_params: object


@dataclass(frozen=True, slots=True)
class LlmResponse:
    """Opaque provider response bytes plus their digest.

    ``response`` is exactly what :meth:`ReplayCache.put` stores, so a real
    provider's response is cacheable with no re-serialization. ``response_digest``
    is derived, never supplied, so it can never disagree with ``response``.
    """

    response: bytes
    response_digest: bytes = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "response_digest", hashlib.sha256(self.response).digest())


@runtime_checkable
class LLMClient(Protocol):
    """Structural protocol every provider (FakeLLM now; real providers later)
    implements. Async-only: the bridge-thread architecture never calls a
    provider off the async loop.
    """

    async def complete(self, request: LlmRequest) -> LlmResponse: ...


class ProviderError(Exception):
    """Base class for provider failures the loop driver classifies by type,
    never by parsing a message.
    """


class RateLimitError(ProviderError):
    """Provider signalled HTTP 429."""

    def __init__(self, retry_after_ms: int | None = None) -> None:
        super().__init__(f"rate limited (retry_after_ms={retry_after_ms})")
        self.retry_after_ms = retry_after_ms


class ServerError(ProviderError):
    """Provider signalled a 5xx status."""

    def __init__(self, status: int) -> None:
        super().__init__(f"server error (status={status})")
        self.status = status


class ProviderTimeout(ProviderError):
    """The provider did not respond within its deadline."""
