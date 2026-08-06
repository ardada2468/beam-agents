# model-facade Specification

## Purpose
TBD - created by archiving change async-llmclient-facade. Update Purpose after archive.
## Requirements
### Requirement: Resilient async facade over a provider LLMClient

The system SHALL provide an async `LlmFacade` that wraps a single provider `LLMClient` and turns a per-activation `complete` call into a resilient, observable request. The facade SHALL be constructed per activation from: the wrapped `LLMClient`, the activation's `ReplayCache`, an injected `now_ms` clock, an injected `rng`, an injected async `sleep(ms)` sleeper, a per-endpoint `CircuitBreaker`, a `RetryPolicy`, a provider `decode(response_bytes) -> DecodedResponse` callable (yielding token usage and the response's JSON text), and a staging sink for trace/usage effects. The facade MUST NOT read wall-clock time, generate un-seeded randomness, or perform Beam state I/O directly — every non-determinism source is injected so a replayed bundle behaves identically.

`complete` SHALL accept an `LlmRequest` (`model_id`, `messages`, `tools_schema`, `sampling_params`), the activation scope `entity_key` and `seq`, and an optional Pydantic v2 `output_schema`, and SHALL return a `FacadeResult` exposing at least: the `LlmResponse`, the parsed `output_schema` instance (or `None`), the call's `TokenUsage`, a `cache_hit` boolean, and the `attempts` count.

#### Scenario: Facade returns a structured result for a successful call

- **WHEN** `complete` is called with a request that the wrapped provider serves on the first attempt
- **THEN** the returned `FacadeResult` carries the provider's `LlmResponse`, `cache_hit` is `False`, `attempts` is `1`, and the call's `TokenUsage` is populated from the provider `decode`

#### Scenario: Facade never touches wall-clock or unseeded randomness

- **WHEN** `complete` executes any path (hit, miss, retry, open breaker)
- **THEN** all time comparisons use the injected `now_ms`, all jitter draws use the injected `rng`, and all delays await the injected `sleep`, with no call to `time`/`asyncio.sleep`/`random` module state

### Requirement: Replay-cache integration short-circuits provider calls

`complete` SHALL compute the activation cache key via `compute_cache_key(model_id, messages, tools_schema, sampling_params, entity_key, seq)` — using the effective request material including any `output_schema` contribution — and consult the `ReplayCache` before any provider call. On a live cache hit the facade SHALL return the cached response with ZERO provider calls, ZERO retry attempts, and WITHOUT consulting the circuit breaker (correctness invariant 3: bundle retries incur no extra provider calls and replay even while the endpoint is unhealthy). On a miss the facade SHALL call the provider and, on success, stage the response bytes into the `ReplayCache` under the same key.

#### Scenario: A cache hit incurs no provider call

- **WHEN** the `ReplayCache` already holds a live entry for the request's cache key and `complete` is called
- **THEN** the wrapped provider's `complete` is not invoked, `cache_hit` is `True`, `attempts` is `0`, and the returned response equals the cached bytes

#### Scenario: A cache miss calls the provider and stages the response

- **WHEN** `complete` is called for a key absent from the `ReplayCache` and the provider returns a response
- **THEN** the provider is called exactly once and the response bytes are staged into the `ReplayCache` under the request's key

#### Scenario: A cache hit replays even while the breaker is open

- **WHEN** the endpoint's circuit breaker is `OPEN` and `complete` is called for a key with a live cache hit
- **THEN** the cached response is returned without raising and without a provider call

#### Scenario: The output schema perturbs the cache key

- **WHEN** two otherwise-identical requests are completed, one with an `output_schema` and one without
- **THEN** they resolve to different cache keys and do not alias each other's cached responses

### Requirement: Typed retry classification with a bounded attempt cap

The facade SHALL retry only the retryable provider errors — `RateLimitError`, `ServerError`, and `ProviderTimeout` — classified by exception type via the `ProviderError` taxonomy, never by string-matching messages. Non-`ProviderError` exceptions (including `UnmatchedRequestError` and `OutputSchemaError`) SHALL propagate immediately without retry. The `RetryPolicy` SHALL cap the total number of provider attempts; when the cap is reached the facade SHALL re-raise the last provider error. A successful attempt after prior failures SHALL return normally with `attempts` reflecting the number of provider calls made.

#### Scenario: A transient failure is retried to success

- **WHEN** the provider raises `ServerError(status=503)` on its first attempt and returns a response on its second, under a policy allowing at least 2 attempts
- **THEN** `complete` returns the successful response and `attempts` is `2`

#### Scenario: Attempts are capped and the last error is re-raised

- **WHEN** the provider raises `ProviderTimeout` on every attempt under a policy capping attempts at `N`
- **THEN** the provider is called exactly `N` times and `complete` raises the last `ProviderTimeout`

#### Scenario: A non-retryable error is not retried

- **WHEN** the provider raises a non-`ProviderError` exception
- **THEN** the provider is called exactly once and the exception propagates unchanged

### Requirement: Jittered exponential backoff honoring Retry-After

Between retry attempts the facade SHALL wait a delay computed as exponential backoff with jitter: a base delay scaled by `2^(attempt-1)`, clamped to a configured maximum, then randomized with the injected `rng`. When the provider error is a `RateLimitError` carrying `retry_after_ms`, the facade SHALL honor it as a lower bound — the actual delay is at least `retry_after_ms`. All waits SHALL be performed by awaiting the injected `sleep`, never `asyncio.sleep` on a wall-clock. No delay SHALL be awaited after the final (cap-reaching) attempt.

#### Scenario: Backoff grows and stays within the cap

- **WHEN** consecutive retryable failures occur under a policy with base `b` and max `m`
- **THEN** each computed pre-jitter delay is `min(b * 2^(attempt-1), m)` and the injected `sleep` is awaited with the jittered delay before each retry

#### Scenario: Retry-After is a floor on the delay

- **WHEN** the provider raises `RateLimitError(retry_after_ms=1500)` and the computed jittered backoff is below 1500 ms
- **THEN** the awaited delay is at least 1500 ms

#### Scenario: No sleep after the final attempt

- **WHEN** the last permitted attempt fails
- **THEN** the facade raises without awaiting a further `sleep`

### Requirement: Per-endpoint circuit breaker

The facade SHALL guard each endpoint with a worker-local `CircuitBreaker` (a documented worker-local singleton, never keyed Beam state) with three states: `CLOSED` (calls pass through), `OPEN` (calls fail fast without touching the provider), and `HALF_OPEN` (a single trial call is permitted). Consecutive retryable failures reaching a configured threshold SHALL trip the breaker to `OPEN`; while `OPEN`, a `complete` that reaches the provider path SHALL raise `CircuitOpenError` without calling the provider. After a configured cooldown (measured against the injected `now_ms`) the breaker SHALL allow one `HALF_OPEN` trial; a success SHALL reset it to `CLOSED` and a failure SHALL return it to `OPEN`. `CircuitOpenError` SHALL NOT be a `ProviderError` and SHALL NOT be retried by the `RetryPolicy`.

#### Scenario: Consecutive failures trip the breaker open

- **WHEN** retryable provider failures reach the breaker's failure threshold
- **THEN** the breaker transitions to `OPEN` and the next provider-bound `complete` raises `CircuitOpenError` without calling the provider

#### Scenario: Cooldown elapses into a half-open trial

- **WHEN** the breaker is `OPEN` and a `complete` occurs after the cooldown has elapsed per the injected clock
- **THEN** exactly one trial provider call is permitted, and a success resets the breaker to `CLOSED`

#### Scenario: A half-open failure re-opens the breaker

- **WHEN** the single `HALF_OPEN` trial call fails with a retryable error
- **THEN** the breaker returns to `OPEN` and subsequent calls fast-fail with `CircuitOpenError`

### Requirement: Token usage accounting

The facade SHALL derive per-call `TokenUsage` (prompt, completion, and total token counts) from the provider `decode` of the response bytes and SHALL accumulate provider-billed usage — cache misses only — into the staging context for metrics and trace attributes. A cache hit SHALL report the cached response's `TokenUsage` on its `FacadeResult` but SHALL NOT add to the billed accumulator (no provider call was made). Accumulated usage SHALL be staged, so a failed or timed-out activation contributes nothing.

#### Scenario: A provider call accumulates billed usage

- **WHEN** two cache-missing `complete` calls each decode to non-zero usage
- **THEN** the staged billed usage equals the sum of both calls' prompt/completion/total counts

#### Scenario: A cache hit reports but does not bill usage

- **WHEN** a `complete` resolves from a cache hit
- **THEN** its `FacadeResult` carries the cached `TokenUsage` and the billed accumulator is unchanged

### Requirement: Constrained JSON output via output_schema

When `complete` is given a Pydantic v2 `output_schema`, the facade SHALL incorporate the schema into the effective request (so the provider is driven toward structured output and the cache key reflects the schema), SHALL parse the response's JSON text through the model, and SHALL return the validated model instance on the `FacadeResult`. Response text that is not valid JSON, or that fails schema validation, SHALL raise a typed `OutputSchemaError` (not a `ProviderError`); it SHALL NOT be retried by the transport `RetryPolicy`. When no `output_schema` is supplied, the parsed field SHALL be `None` and no JSON parsing SHALL occur.

#### Scenario: Valid structured output is parsed into the model

- **WHEN** `complete` is given an `output_schema` and the provider returns JSON satisfying it
- **THEN** the `FacadeResult.parsed` is an instance of the schema with the decoded field values

#### Scenario: Schema-violating output raises a typed error

- **WHEN** the provider returns text that is not valid JSON or violates the `output_schema`
- **THEN** `complete` raises `OutputSchemaError` and does not retry the transport call

#### Scenario: No schema means no parsing

- **WHEN** `complete` is called without an `output_schema`
- **THEN** `FacadeResult.parsed` is `None` and the raw `LlmResponse` is returned unparsed

### Requirement: Per-call trace emission

The facade SHALL stage exactly one `TraceEvent` of type `LLM_CALL` per `complete` invocation into the staging context, carrying the activation `entity_key`, `seq`, and `step_index`, and OTel-GenAI-shaped string attributes including at least: `gen_ai.operation.name`, `gen_ai.request.model`, the cache outcome (`beam_agents.cache_hit`), whether the call was billed by the provider (`beam_agents.billed`), the number of attempts, and the endpoint circuit-breaker state. Token-usage attributes (`gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`) SHALL be populated from the provider `decode` whenever a response was decoded — including on a cache hit, where the **stored** response is decoded — and SHALL be **omitted entirely** when no response was decoded; the facade MUST NOT emit a placeholder value for usage it does not know. `start_ms`/`end_ms` SHALL be taken from the injected clock, and the facade SHALL NOT read a wall clock for trace timestamps or durations.

The facade SHALL leave `trace_id`, `span_id`, and `parent_span_id` unset and SHALL NOT accept correlation parameters: the staging sink stamps correlation onto the event (see the `trace-events` capability). Trace events SHALL be staged like every other activation effect and applied only on activation success; a `complete` that ultimately raises SHALL still stage a trace event describing the failure, carrying `error.type`.

#### Scenario: A successful call emits one LLM_CALL trace

- **WHEN** `complete` returns successfully
- **THEN** exactly one `LLM_CALL` `TraceEvent` is staged with `gen_ai.request.model` set, usage and attempt attributes populated, the cache outcome recorded, and `beam_agents.billed = true`

#### Scenario: A cache hit is recorded in the trace attributes

- **WHEN** `complete` resolves from a cache hit
- **THEN** the staged `LLM_CALL` trace records the cache-hit outcome, zero attempts, and `beam_agents.billed = false`

#### Scenario: A cache hit reports the stored response's real token counts

- **WHEN** `complete` resolves from a cache hit whose stored response decodes to non-zero usage
- **THEN** the staged trace carries `gen_ai.usage.input_tokens` and `gen_ai.usage.output_tokens` equal to that decoded usage

#### Scenario: Unknown usage is omitted rather than reported as zero

- **WHEN** `complete` fails before any response is decoded (retries exhausted, or the circuit breaker fails the call closed)
- **THEN** the staged trace contains no `gen_ai.usage.input_tokens` key and no `gen_ai.usage.output_tokens` key

#### Scenario: A failed call still emits a trace

- **WHEN** `complete` exhausts retries and raises
- **THEN** an `LLM_CALL` `TraceEvent` describing the failure (`error.type` and attempt count) is staged before the exception propagates

#### Scenario: The facade stages uncorrelated events

- **WHEN** any `complete` path stages its trace event
- **THEN** the event's `trace_id`, `span_id`, and `parent_span_id` are left empty for the staging sink to fill, and `complete`'s signature carries no correlation arguments
