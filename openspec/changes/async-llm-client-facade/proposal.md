## Why

The runtime has core value types (`LlmRequest`, `LlmResponse`) and replay-cache primitives, but it does not yet define the async facade behavior that production providers must share for retries, resilience, cache integration, token accounting, and tracing. We need a single provider-neutral `complete()` contract now so future provider adapters are consistent and testable.

## What Changes

- Add a provider-neutral async LLM facade capability that defines `complete()` behavior for message-based calls with optional tools and constrained-JSON output schemas.
- Define retry policy semantics with jittered exponential backoff, including explicit precedence for provider `Retry-After` hints.
- Define per-endpoint circuit breaker behavior for repeated upstream failures.
- Define token usage accounting surfaced on responses and trace attributes.
- Define replay-cache integration points for read-through/write-through behavior around provider calls.
- Define trace emission points for request start, retry scheduling, provider invocation result, cache hit/miss, and completion.

## Capabilities

### New Capabilities
- `llm-client-facade`: Async provider-neutral facade behavior for `complete()` including request shape, retry/backoff, per-endpoint circuit breaking, token usage accounting, replay-cache integration, and trace emission points.

### Modified Capabilities
- `model-client`: Extend the protocol and related value/error semantics to support message/tool/output-schema inputs and usage-rich completion outcomes.
- `llm-replay-cache`: Add/clarify integration requirements for facade-driven cache lookup, cache write, and digest-only replay behavior during completion flows.

## Impact

- Affected code: `src/beam_agents/model/*` (client protocol, facade, retry/circuit logic, cache glue), provider adapters, and trace utilities.
- Affected APIs: `LLMClient.complete()` contract and response metadata surface for usage and cache outcome.
- Dependencies/systems: Existing replay cache primitives, error taxonomy, and tracing event schema/attribute conventions.
