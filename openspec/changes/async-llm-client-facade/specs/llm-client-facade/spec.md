## ADDED Requirements

### Requirement: Async facade completion API accepts messages, tools, and optional output schema constraints
The system SHALL provide an async facade entrypoint `complete()` that accepts provider-neutral message input, optional tool definitions, optional constrained JSON output schema, and sampling parameters. The facade MUST normalize these inputs into request material suitable for cache keying, provider invocation, and trace attribution without leaking provider-specific request types.

#### Scenario: Minimal message-only completion call
- **WHEN** `complete()` is called with messages and no tools or output schema
- **THEN** the facade issues a provider completion using normalized request material and returns a completion result

#### Scenario: Tool-enabled constrained output completion call
- **WHEN** `complete()` is called with messages, tools, and an `output_schema`
- **THEN** the facade passes schema/tool intent through the provider-neutral contract and returns a result whose content conforms to the schema or raises a typed error

### Requirement: Retry policy uses jittered exponential backoff and honors Retry-After
For retryable failures (`RateLimitError`, retryable `ServerError`, and `ProviderTimeout`), the facade SHALL retry up to configured attempt limits. Delay selection MUST prefer provider `Retry-After` when present and valid; otherwise it MUST use jittered exponential backoff with a configured cap. Non-retryable errors MUST fail immediately.

#### Scenario: Retry-After overrides computed backoff
- **WHEN** attempt 1 fails with `RateLimitError(retry_after_ms=2500)` and retries remain
- **THEN** attempt 2 is scheduled no earlier than 2500 ms later (plus no additional mandatory backoff component)

#### Scenario: Jittered backoff is used when Retry-After is absent
- **WHEN** attempt 1 fails retryably with no retry-after hint
- **THEN** the next delay is derived from the exponential schedule with jitter and bounded by the configured maximum

#### Scenario: Non-retryable failure stops retry loop
- **WHEN** `complete()` receives a non-retryable provider/facade error
- **THEN** no additional attempts are made and the error is raised

### Requirement: Circuit breaker is maintained per endpoint
The facade SHALL maintain circuit-breaker state independently per endpoint key. After configured consecutive or rate-based retryable failures, the endpoint breaker MUST transition to open and short-circuit new calls until cooldown. After cooldown it MUST enter half-open and allow probe traffic; success closes the breaker and failure reopens it.

#### Scenario: Open breaker rejects call without provider attempt
- **WHEN** an endpoint breaker is open and cooldown has not elapsed
- **THEN** `complete()` fails fast with a typed circuit-open error and no provider call occurs

#### Scenario: Half-open probe closes breaker on success
- **WHEN** cooldown elapses and the next call for that endpoint succeeds
- **THEN** breaker state transitions from half-open to closed

### Requirement: Completion results surface token usage accounting
The facade completion result SHALL include token-usage accounting fields for input tokens, output tokens, and total tokens when provider data is available, and explicit unknown/empty semantics when unavailable. Usage reporting MUST be consistent whether the result is provider-fresh or cache-replayed.

#### Scenario: Provider usage is surfaced on fresh completion
- **WHEN** provider response includes usage metadata
- **THEN** the completion result exposes input, output, and total token counts matching provider-reported values

#### Scenario: Cache replay preserves usage semantics
- **WHEN** a replay-cache hit returns a previously stored completion
- **THEN** the completion result includes usage metadata equivalent to the cached response and marks provenance as cache replay

### Requirement: Replay-cache integration wraps provider calls
The facade SHALL perform read-through lookup before provider invocation and write-through insert after successful provider completion using canonical request material for cache key derivation. Cache misses and write outcomes MUST be observable to traces and completion metadata.

#### Scenario: Cache hit bypasses provider call
- **WHEN** cache lookup for normalized request material returns a live entry
- **THEN** `complete()` returns the replayed result and provider call counters remain unchanged

#### Scenario: Cache miss writes successful provider response
- **WHEN** cache lookup misses and provider completion succeeds
- **THEN** the response is inserted into replay cache under the canonical key before `complete()` returns

### Requirement: Trace events are emitted at defined facade boundaries
The facade SHALL emit trace events at completion start, cache decision (hit/miss), each provider attempt, retry scheduling, circuit-breaker short-circuit decisions, and completion end/error. Events MUST include correlation attributes sufficient to link attempts, endpoint key, model id, cache status, and usage fields when known.

#### Scenario: Retry flow emits ordered trace points
- **WHEN** the first attempt fails retryably and a second attempt succeeds
- **THEN** trace output contains start, cache miss, attempt#1 failure, retry scheduled, attempt#2 success, and completion end in causal order

#### Scenario: Circuit-open flow emits short-circuit trace point
- **WHEN** breaker state is open and `complete()` is called
- **THEN** trace output contains completion start, breaker short-circuit, and completion error without any provider attempt event
