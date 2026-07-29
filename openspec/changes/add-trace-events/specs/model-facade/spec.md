## MODIFIED Requirements

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
