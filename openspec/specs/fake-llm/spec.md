# fake-llm Specification

## Purpose
TBD - created by archiving change add-fake-llm-provider. Update Purpose after archive.

## Requirements

### Requirement: FakeLLM implements the model-client protocol

`FakeLLM` SHALL be a structural `LLMClient`: `await fake.complete(request)` returns an `LlmResponse` or raises a `ProviderError`, exactly as any provider would. It SHALL be constructible with no arguments (empty script) and SHALL live in `beam_agents.model` test infrastructure, never in the public root API.

#### Scenario: FakeLLM is usable wherever an LLMClient is expected

- **WHEN** a `FakeLLM` instance is passed where an `LLMClient` is annotated and `complete` is awaited
- **THEN** it returns an `LlmResponse` (or raises `ProviderError`) with no adapter or wrapper required

### Requirement: Scripted responses via ordered matchers

`FakeLLM` SHALL serve requests from an ordered list of `(matcher, behavior)` rules, where `matcher` is a `Callable[[LlmRequest], bool]`. On each `complete` call the rules are evaluated in registration order and the FIRST rule whose matcher returns true serves the request; a serving behavior either yields configured response bytes (as an `LlmResponse`) or raises a configured `ProviderError`. The system SHALL also provide convenience matcher constructors for common cases (match by `model_id`, match by a substring occurring in the request material, and match-any) so tests rarely write raw predicates.

#### Scenario: First matching rule wins

- **WHEN** two rules both match a request and the first is registered before the second
- **THEN** the first rule's behavior serves the request and the second rule is not consulted

#### Scenario: Convenience matcher by model id

- **WHEN** a rule is registered with the match-by-`model_id` matcher for `"m-1"` and a request with `model_id == "m-1"` arrives
- **THEN** that rule matches and serves its configured response

#### Scenario: Scripted response bytes are returned verbatim

- **WHEN** a matching rule is configured to return specific response bytes
- **THEN** `complete` resolves to an `LlmResponse` whose `response` equals those bytes and whose `response_digest` is their sha256

### Requirement: Unmatched requests fail closed

When no registered rule matches a request, `FakeLLM` SHALL raise a distinct, descriptive error (not return a default response and not raise a generic `ProviderError`). This surfaces missing test scripting loudly rather than letting an unscripted request pass silently.

#### Scenario: No matching rule raises

- **WHEN** `complete` is called with a request that no registered matcher accepts
- **THEN** a descriptive "unmatched request" error is raised that identifies the offending request material, and it is not a `ProviderError`

#### Scenario: Empty FakeLLM raises on first call

- **WHEN** a `FakeLLM` with no rules receives any request
- **THEN** the unmatched-request error is raised

### Requirement: Request recording

`FakeLLM` SHALL append every incoming `LlmRequest` to an ordered, queryable log at the moment `complete` is invoked, BEFORE applying latency or a failure behavior, so that requests which ultimately raise are still recorded. The log SHALL be exposed as an immutable-to-callers ordered view and SHALL preserve call order.

#### Scenario: Requests are recorded in call order

- **WHEN** three distinct requests are completed in sequence
- **THEN** the recorded log contains those three `LlmRequest` values in the same order

#### Scenario: A failing call is still recorded

- **WHEN** a request matches a rule that raises a `ProviderError`
- **THEN** that request appears in the recorded log even though `complete` raised

### Requirement: Injectable latency

A rule's behavior MAY carry a `latency_ms` delay that `FakeLLM` applies before serving, via an injected async delay hook (default awaits `asyncio.sleep`; overridable at construction). `FakeLLM` SHALL NOT read a wall clock or call blocking `sleep`; the delay is realized only through the injected awaitable, so tests can make it instantaneous or observe it deterministically.

#### Scenario: Latency is applied through the injected hook

- **WHEN** a rule with `latency_ms=250` serves a request and a recording delay hook is injected
- **THEN** the hook is awaited exactly once with `250` before the response resolves, and no real wall-clock time is required to pass

#### Scenario: Latency can outlast an activation deadline

- **WHEN** a rule's `latency_ms` exceeds the caller's `activation_timeout` and the real `asyncio.sleep` hook is used
- **THEN** the awaiting caller can cancel the `complete` coroutine and the response never resolves

### Requirement: Injectable failures

A rule's behavior MAY raise a `RateLimitError` (429), `ServerError` (5xx), or `ProviderTimeout` instead of returning a response. `FakeLLM` SHALL additionally support a "fail N times then succeed" behavior: the first N matching calls raise the configured error and subsequent matching calls serve the configured response, enabling retry-path tests.

#### Scenario: Rule raises a configured provider error

- **WHEN** a matching rule is configured to raise `ServerError(status=503)`
- **THEN** awaiting `complete` raises that `ServerError` with `status == 503`

#### Scenario: Fail N times then succeed

- **WHEN** a rule is configured to fail twice with `RateLimitError` then succeed, and three matching requests are completed
- **THEN** the first two awaits raise `RateLimitError`, the third resolves to the configured `LlmResponse`, and all three requests are recorded

### Requirement: Provider-call counting for determinism assertions

`FakeLLM` SHALL expose deterministic counters of provider invocations: a total `call_count` and a per-request-key breakdown, where the request key is derived from the request material the same way `compute_cache_key` derives its request portion (i.e., logically equal requests share a key regardless of dict ordering). Every `complete` invocation — success or raise — increments both the total and that request's per-key count. These counters let the retry-determinism gate assert that the cached path adds zero provider calls.

#### Scenario: Total count increments per invocation

- **WHEN** four `complete` calls are made (including one that raises)
- **THEN** `call_count` equals 4

#### Scenario: Per-key count groups logically equal requests

- **WHEN** two requests whose `messages`/`tools_schema`/`sampling_params` differ only in dict key order (same `model_id`) are completed
- **THEN** they map to the same request key and that key's per-key count equals 2

#### Scenario: Counts support a zero-additional-calls assertion

- **WHEN** a request is completed once and then a cached path replays the same request material without invoking `FakeLLM`
- **THEN** that request key's per-key count remains 1

### Requirement: Deterministic and offline

`FakeLLM` SHALL be fully deterministic and require no network, no docker, and no wall-clock dependence: given the same rules and the same request sequence it produces the same responses, raises the same errors, records the same log, and reports the same counts on every run. It MUST NOT introduce import-time side effects.

#### Scenario: Repeated runs are identical

- **WHEN** the same `FakeLLM` script processes the same request sequence in two separate runs
- **THEN** the responses, raised errors, recorded log, and counters are identical across both runs

#### Scenario: Import has no side effects

- **WHEN** `beam_agents.model` is imported
- **THEN** no network call, no logging, and no global-state mutation occurs
