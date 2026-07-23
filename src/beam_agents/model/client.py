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
    """Provider-neutral request material: the components ``compute_cache_key``
    hashes, minus the activation-scoped ``entity_key``/``seq`` that scope the
    cache rather than the call.

    ``messages``/``tools_schema``/``output_schema``/``sampling_params`` are
    typed ``object`` (provider-shaped Python), exactly as
    ``compute_cache_key`` already treats them.
    """

    model_id: str
    messages: object
    tools_schema: object
    output_schema: object | None = None
    sampling_params: object = None


@dataclass(frozen=True, slots=True)
class LlmResponse:
    """Opaque provider response bytes plus their digest.

    ``response`` is exactly what :meth:`ReplayCache.put` stores, so a real
    provider's response is cacheable with no re-serialization. ``response_digest``
    is derived, never supplied, so it can never disagree with ``response``.
    """

    response: bytes
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    from_replay_cache: bool = False
    response_digest: bytes = field(init=False)
    _source_response_digest: bytes | None = field(default=None, repr=False, kw_only=True)

    def __post_init__(self) -> None:
        digest = (
            self._source_response_digest
            if self._source_response_digest is not None
            else hashlib.sha256(self.response).digest()
        )
        object.__setattr__(self, "response_digest", digest)


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


class FacadeError(Exception):
    """Base class for facade-local failures."""


class OutputSchemaError(FacadeError):
    """Response payload does not satisfy the requested output schema."""


class CircuitOpenError(FacadeError):
    """Per-endpoint circuit is open and request was short-circuited."""

    def __init__(self, endpoint_key: str, retry_after_ms: int) -> None:
        super().__init__(
            f"circuit open for endpoint {endpoint_key!r} (retry_after_ms={retry_after_ms})"
        )
        self.endpoint_key = endpoint_key
        self.retry_after_ms = retry_after_ms
