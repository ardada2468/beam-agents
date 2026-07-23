## 1. Contract and Types

- [x] 1.1 Extend `LlmRequest` to include `output_schema` alongside `model_id`, `messages`, `tools_schema`, and `sampling_params`.
- [x] 1.2 Extend `LlmResponse` to carry usage/provenance metadata (`input_tokens`, `output_tokens`, `total_tokens`, `from_replay_cache`) while preserving immutable/cacheable response bytes semantics.
- [x] 1.3 Update `LLMClient` protocol and fake/provider client shims to satisfy the revised request/response contract.

## 2. Facade Completion Flow

- [x] 2.1 Implement async facade `complete()` input normalization for messages, tools, output schema, and sampling params.
- [x] 2.2 Implement read-through replay-cache lookup before provider attempts and write-through insert on successful provider completion.
- [x] 2.3 Ensure completion results distinguish cache-hit vs provider-fresh responses and preserve digest/usage semantics in both paths.

## 3. Resilience: Retry and Circuit Breaker

- [x] 3.1 Implement retry classification over `RateLimitError`, retryable `ServerError`, and `ProviderTimeout` with explicit non-retryable fast-fail behavior.
- [x] 3.2 Implement delay strategy that honors `Retry-After` when present, otherwise uses capped jittered exponential backoff.
- [x] 3.3 Implement per-endpoint circuit-breaker state machine (closed/open/half-open), short-circuit errors, cooldown, and probe-based recovery.

## 4. Observability and Accounting

- [x] 4.1 Add token usage accounting extraction/mapping from provider responses into `LlmResponse`.
- [x] 4.2 Emit trace events for completion start, cache decision, provider attempt outcomes, retry scheduling, circuit-open short-circuit, and completion end/error.
- [x] 4.3 Attach required trace attributes (attempt index, endpoint key, model id, cache status, retry delay, usage when known).

## 5. Validation

- [x] 5.1 Add/extend unit tests for cache key perturbation including `output_schema` and for deterministic normalization behavior.
- [x] 5.2 Add retry/backoff tests covering Retry-After precedence, jitter bounds, max-attempt termination, and non-retryable failures.
- [x] 5.3 Add circuit-breaker tests for open short-circuiting, cooldown transition to half-open, close-on-success, and reopen-on-failure.
- [x] 5.4 Add end-to-end facade tests for cache hit bypassing provider, miss-then-store, usage accounting propagation, and trace emission point ordering.
