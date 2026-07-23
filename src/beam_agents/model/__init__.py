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

from beam_agents.model.client import (
    CircuitOpenError,
    FacadeError,
    LLMClient,
    LlmRequest,
    LlmResponse,
    OutputSchemaError,
    ProviderError,
    ProviderTimeout,
    RateLimitError,
    ServerError,
)
from beam_agents.model.facade import CircuitBreakerPolicy, LlmClientFacade, RetryPolicy, TraceEvent
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
    "CircuitBreakerPolicy",
    "CircuitOpenError",
    "FacadeError",
    "FakeLLM",
    "LLMClient",
    "LlmClientFacade",
    "LlmRequest",
    "LlmResponse",
    "OutputSchemaError",
    "ProviderError",
    "ProviderTimeout",
    "RateLimitError",
    "ReplayCache",
    "ReplayEntry",
    "RetryPolicy",
    "ServerError",
    "TraceEvent",
    "UnmatchedRequestError",
    "compute_cache_key",
    "fail_then_succeed",
    "match_any",
    "match_contains",
    "match_model_id",
    "raise_error",
    "respond_with",
]
