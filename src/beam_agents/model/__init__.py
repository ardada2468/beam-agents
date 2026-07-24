"""LLM replay cache over the keyed ``LlmCacheBlob`` state value, plus the
async model-client seam (:class:`LLMClient`, :class:`LlmRequest`,
:class:`LlmResponse`, the provider-error taxonomy) and its deterministic test
double, :class:`FakeLLM`.

Correctness invariant 3 requires that bundle retries make zero additional
provider calls on the cached path. The model client reaches the per-key replay
cache through :class:`ReplayCache`, never by touching Beam state or the
``LlmCacheBlob`` proto directly. The facade stages lookups and inserts on an
in-memory blob (the stateful DoFn loads it before and commits it after each
activation) and enforces the cache invariants: content-hash keying
(:func:`compute_cache_key`), a 64-entry LRU bound, a 6h TTL, and a 100 KiB blob
cap with a digest-only fallback for oversized responses.

Importing this package has no side effects.
"""

from beam_agents.model.anthropic import AnthropicProvider
from beam_agents.model.anthropic import decode as anthropic_decode
from beam_agents.model.client import (
    LLMClient,
    LlmRequest,
    LlmResponse,
    ProviderError,
    ProviderRequestError,
    ProviderTimeout,
    RateLimitError,
    ServerError,
)
from beam_agents.model.facade import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    DecodedResponse,
    FacadeResult,
    LlmFacade,
    OutputSchemaError,
    RetryPolicy,
    StagingSink,
    TokenUsage,
)
from beam_agents.model.fake import (
    FakeLLM,
    UnmatchedRequestError,
    fail_then_succeed,
    match_any,
    match_contains,
    match_model_id,
    raise_error,
    respond_with,
)
from beam_agents.model.openai_compat import OpenAICompatProvider
from beam_agents.model.openai_compat import decode as openai_compat_decode
from beam_agents.model.replay_cache import (
    BLOB_CAP_BYTES,
    MAX_ENTRIES,
    TTL_MS,
    ReplayCache,
    ReplayEntry,
    compute_cache_key,
)

__all__ = [
    "BLOB_CAP_BYTES",
    "MAX_ENTRIES",
    "TTL_MS",
    "AnthropicProvider",
    "CircuitBreaker",
    "CircuitOpenError",
    "CircuitState",
    "DecodedResponse",
    "FacadeResult",
    "FakeLLM",
    "LLMClient",
    "LlmFacade",
    "LlmRequest",
    "LlmResponse",
    "OpenAICompatProvider",
    "OutputSchemaError",
    "ProviderError",
    "ProviderRequestError",
    "ProviderTimeout",
    "RateLimitError",
    "ReplayCache",
    "ReplayEntry",
    "RetryPolicy",
    "ServerError",
    "StagingSink",
    "TokenUsage",
    "UnmatchedRequestError",
    "anthropic_decode",
    "compute_cache_key",
    "fail_then_succeed",
    "match_any",
    "match_contains",
    "match_model_id",
    "openai_compat_decode",
    "raise_error",
    "respond_with",
]
