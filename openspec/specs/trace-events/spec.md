# trace-events Specification

## Purpose
TBD - created by archiving change add-trace-events. Update Purpose after archive.
## Requirements
### Requirement: Deterministic trace and span identity

The system SHALL derive trace and span identifiers as pure functions of activation scope, reading no clock, counter, or randomness source. `trace_id` SHALL be the 16 bytes of `uuid5(namespace, "<entity_key hex>|<seq>")`. `span_id` SHALL be the first 8 bytes of `uuid5(namespace, "<entity_key hex>|<seq>|<role>|<index>")`, where `role` is `activation`, `timer`, or the `EventType` name of the event, and `index` is the step index (or the timer's step cursor). The namespace SHALL be a fixed constant that MUST NOT change without a `state_schema_version` bump. Identifier widths SHALL be 16 bytes for `trace_id` and 8 bytes for `span_id`/`parent_span_id`, matching the W3C trace-context and OTel wire formats.

#### Scenario: Identifiers are reproducible across processes

- **WHEN** `trace_id` and `span_id` are derived twice for the same `(entity_key, seq, role, index)` in separate processes
- **THEN** both derivations return byte-identical values

#### Scenario: Replayed bundle produces byte-identical trace events

- **WHEN** a bundle is retried and re-runs an activation that walks the same path
- **THEN** every emitted `TraceEvent` is byte-identical to the one the first attempt emitted, including `trace_id`, `span_id`, `parent_span_id`, and deterministically-serialized `attributes`

#### Scenario: Different event kinds at the same step do not share a span id

- **WHEN** an `LLM_CALL` and an `INTENT_EMITTED` event are both produced with the same `step_index` in the same activation
- **THEN** their `span_id`s differ, because the event-type name participates in the derivation

#### Scenario: Identity derivation reads no ambient state

- **WHEN** any trace or span identifier is derived
- **THEN** no wall clock, no un-seeded randomness, and no mutable module state is consulted

### Requirement: One trace per activation scope, spanning suspension

The system SHALL scope one trace to one `(entity_key, seq)` pair. A resumed activation SHALL recompute the same `trace_id` from its `Continuation`'s `seq` rather than reading any correlation identifier from the wire, so a suspend/resume cycle is a single trace. Each activation attempt SHALL own an `activation`-role span whose `index` is the step index the attempt entered at; the initial attempt's span SHALL be the trace root with an empty `parent_span_id`, and every resumed attempt's span SHALL be parented to the root activation span. `ToolResult` and `AgentEnvelope.Approval` SHALL NOT carry a trace identifier.

#### Scenario: A resume shares the suspended activation's trace

- **WHEN** an activation suspends at `seq = 4` and a later `ToolResult` resumes it
- **THEN** the events emitted before and after the suspension carry the same `trace_id`

#### Scenario: A resumed attempt is a child of the initial attempt

- **WHEN** a resumed activation emits its `ACTIVATION_START`
- **THEN** its `span_id` differs from the initial attempt's activation span and its `parent_span_id` equals that initial activation span

#### Scenario: The initial activation span is the trace root

- **WHEN** a fresh activation emits its `ACTIVATION_START`
- **THEN** `parent_span_id` is empty and `span_id` is the `activation`-role span for entry step `0`

#### Scenario: A new seq starts a new trace

- **WHEN** two consecutive activations for one key run at `seq = 7` and `seq = 8`
- **THEN** their `trace_id`s differ

### Requirement: Correlation stamped at the staging boundary

The activation-scoped staging surfaces SHALL stamp correlation onto every `TraceEvent` staged through them: `trace_id`, `span_id`, and `parent_span_id` SHALL be filled in from the activation's trace whenever the incoming event leaves them empty, using the event's own `event_type` and `step_index` for the span derivation. An event that arrives with a non-empty field SHALL keep that value. Producers of trace events — the model facade, the loop driver, the tool path — SHALL NOT be required to compute identifiers themselves or to accept correlation parameters.

#### Scenario: An uncorrelated event is stamped on staging

- **WHEN** the model facade stages an `LLM_CALL` event with empty `trace_id`, `span_id`, and `parent_span_id`
- **THEN** the staged event carries the activation's `trace_id`, its own derived `span_id`, and the activation span as `parent_span_id`

#### Scenario: A producer-supplied parent is preserved

- **WHEN** an event is staged with a non-empty `parent_span_id`
- **THEN** the stamping leaves that `parent_span_id` unchanged

#### Scenario: The facade signature carries no correlation parameters

- **WHEN** `LlmFacade.complete` is invoked
- **THEN** it is called with the request, activation scope, and optional output schema only, and the correlated trace event still reaches the staging sink fully populated

### Requirement: Activation span with start, end, and outcome

Every activation SHALL emit an `ACTIVATION_START` event when it begins and exactly one terminal activation event when it returns. Both SHALL carry the attempt's `activation` span as `span_id`. `ACTIVATION_END` SHALL carry `beam_agents.activation.status` with value `completed` or `suspended`, and `beam_agents.activation.kind` with value `start` or `resume`, so a consumer that does not recognize newer event types can still read the outcome. `start_ms` and `end_ms` SHALL be taken from the injected activation clock.

#### Scenario: A completing activation brackets its work

- **WHEN** an activation runs to completion
- **THEN** an `ACTIVATION_START` and an `ACTIVATION_END` event are emitted with the same `span_id`, and `ACTIVATION_END` carries `beam_agents.activation.status = completed`

#### Scenario: A suspending activation reports suspended status

- **WHEN** an activation returns `Suspend`
- **THEN** its `ACTIVATION_END` carries `beam_agents.activation.status = suspended`

#### Scenario: A resumed activation is labelled as a resume

- **WHEN** an activation is entered from a `ToolResult` or `Approval`
- **THEN** its `ACTIVATION_START` carries `beam_agents.activation.kind = resume`

#### Scenario: Activation events use the injected clock

- **WHEN** any activation event is emitted
- **THEN** `start_ms` and `end_ms` both equal the activation's injected `now_ms` and no wall clock is read

### Requirement: LLM call child events with GenAI attributes

Every model call SHALL emit exactly one `LLM_CALL` child event of the activation span, on both the `AgentContext` (model-facade) path and the `ActivationContext` (loop-driver) path. The event SHALL carry OTel-GenAI-shaped attributes including `gen_ai.operation.name`, `gen_ai.request.model`, and — when known — `gen_ai.usage.input_tokens` and `gen_ai.usage.output_tokens`, plus the runtime attributes `beam_agents.cache_hit` and `beam_agents.billed`. A call that raises SHALL still emit its event, carrying `error.type`.

#### Scenario: A provider call emits one correlated LLM_CALL

- **WHEN** an activation makes one model call that reaches the provider
- **THEN** exactly one `LLM_CALL` event is emitted, parented to the activation span, with `gen_ai.request.model` set and `beam_agents.cache_hit = false`

#### Scenario: Both context surfaces emit the same event shape

- **WHEN** the same model request is served through `AgentContext.model.complete` and through `ActivationContext.call_model`
- **THEN** both emit an `LLM_CALL` event carrying the same attribute keys for model, cache outcome, and usage

#### Scenario: A failing call is traced with its error type

- **WHEN** a model call exhausts retries and raises
- **THEN** an `LLM_CALL` event is emitted carrying `error.type` naming the exception class

### Requirement: Token counts are truthful or absent, and cache hits are unbilled

Token-usage attributes SHALL report decoded counts or SHALL be omitted entirely; the system MUST NOT emit a placeholder value for usage it does not know. A call served from the replay cache SHALL decode the **stored** response and report its real `gen_ai.usage.input_tokens` and `gen_ai.usage.output_tokens`, and SHALL carry `beam_agents.billed = false`; a call that reached the provider SHALL carry `beam_agents.billed = true`. A call that produced no response (transport failure, open circuit) SHALL omit both usage attributes and carry `beam_agents.billed = false`. `ActivationContext` SHALL accept an optional provider `Decode` callable for this purpose and, when none is configured, SHALL omit usage attributes rather than report zeros.

#### Scenario: A cache-hit call reports the stored response's real token counts

- **WHEN** a model call is served from a live replay-cache entry whose stored response decodes to 1200 input and 300 output tokens
- **THEN** the emitted `LLM_CALL` event carries `gen_ai.usage.input_tokens = "1200"` and `gen_ai.usage.output_tokens = "300"`

#### Scenario: A cache hit is marked unbilled

- **WHEN** a model call is served from the replay cache
- **THEN** the emitted event carries `beam_agents.cache_hit = true` and `beam_agents.billed = false`

#### Scenario: A provider call is marked billed

- **WHEN** a model call reaches the provider and returns a response
- **THEN** the emitted event carries `beam_agents.cache_hit = false` and `beam_agents.billed = true`

#### Scenario: Unknown usage is omitted, not zeroed

- **WHEN** a model call fails before any response is decoded
- **THEN** the emitted event contains no `gen_ai.usage.input_tokens` and no `gen_ai.usage.output_tokens` key

#### Scenario: Summing billed usage over a retried bundle counts each provider call once

- **WHEN** a bundle is retried and the retry serves every model call from the replay cache
- **THEN** summing `gen_ai.usage.input_tokens` over events with `beam_agents.billed = true` yields the same total as the first attempt

### Requirement: Intent, tool, and suspension child events

Staging a `ToolIntent` SHALL emit an `INTENT_EMITTED` child event carrying `beam_agents.intent_id`, `beam_agents.tool_name`, `beam_agents.intent_kind`, and `beam_agents.expires_at_ms`. Running a read-only tool inline SHALL emit a `TOOL_CALL` child event carrying the tool name; `TOOL_CALL` events MUST NOT advance the step counter that seeds `intent_id` derivation. An activation that returns `Suspend` SHALL emit a `SUSPENDED` child event carrying `beam_agents.deadline_ms`, `beam_agents.adapter`, and the pending intent ids. Intents minted by the escalation route outside any activation SHALL also emit an `INTENT_EMITTED` event.

#### Scenario: Each staged intent is traced

- **WHEN** an activation stages two intents
- **THEN** two `INTENT_EMITTED` events are emitted, each carrying the corresponding `beam_agents.intent_id`

#### Scenario: A read-only tool call is traced without perturbing intent ids

- **WHEN** an activation runs a read-only tool and then stages an intent
- **THEN** a `TOOL_CALL` event is emitted and the intent's `intent_id` equals the id the same activation would have minted without the tool call

#### Scenario: A suspension records its deadline and adapter

- **WHEN** an activation returns `Suspend` with a deadline and an adapter name
- **THEN** a `SUSPENDED` event is emitted carrying `beam_agents.deadline_ms` and `beam_agents.adapter`

#### Scenario: An escalation intent is traced from the timer callback

- **WHEN** the HITL timeout route escalates and mints an approval intent
- **THEN** an `INTENT_EMITTED` event is emitted on `.traces` carrying that intent's id, in the trace of the suspended activation's `(entity_key, seq)`

### Requirement: trace_id propagates into emitted intents

Every `ToolIntent` the runtime stages SHALL carry the emitting activation's `trace_id` in its `trace_id` field, including intents minted by the escalation route. The value SHALL be the same 16 bytes the activation's trace events carry, and SHALL be deterministic so a replayed bundle produces byte-identical intents. Adding the field SHALL NOT change `intent_id` derivation.

#### Scenario: A committed intent carries the activation's trace id

- **WHEN** an activation stages an intent and commits
- **THEN** the `ToolIntent` emitted on `.intents` carries a `trace_id` equal to the `trace_id` on that activation's `ACTIVATION_START` event

#### Scenario: Replay produces byte-identical intents with the trace id populated

- **WHEN** a bundle is retried and re-stages the same intent
- **THEN** the re-staged `ToolIntent` is byte-identical to the original, `trace_id` included

#### Scenario: An escalation intent carries the suspended activation's trace id

- **WHEN** the escalation route mints an approval intent for a suspended continuation
- **THEN** that intent's `trace_id` equals `trace_id` derived from the continuation's `(entity_key, seq)`

#### Scenario: Intent ids are unchanged by the new field

- **WHEN** an intent is staged for a given `(entity_key, seq, step_index)`
- **THEN** its `intent_id` equals the value `intent_id_for` produced before `trace_id` existed

### Requirement: The traces output is deliverable to a configured sink

`RunAgent` SHALL expose all trace events on its `.traces` tagged output, and a configured `AgentConfig.traces_to` SHALL resolve to a transform that serializes `TraceEvent` messages into the form its scheme accepts: deterministic protobuf bytes keyed by `entity_key` for `kafka://` and `pubsub://`, and a flat row with hex-encoded identifiers and key/value attributes for `bigquery://`. An unset `traces_to` SHALL leave `.traces` exposed to the caller unchanged.

#### Scenario: Traces flow through a pipeline end to end

- **WHEN** a `TestStream` pipeline runs an activation that calls the model and stages an intent
- **THEN** `.traces` contains the activation start/end events and one child event per model call and staged intent, all sharing one `trace_id`

#### Scenario: A message-bus traces sink receives keyed deterministic bytes

- **WHEN** `traces_to` is a `kafka://` or `pubsub://` URI
- **THEN** each element written is the event's deterministically-serialized bytes keyed by its `entity_key`

#### Scenario: A BigQuery traces sink receives rows, not protos

- **WHEN** `traces_to` is a `bigquery://` URI
- **THEN** each element written is a row mapping with hex `trace_id`/`span_id`, the event-type name, `start_ms`/`end_ms`, and the attributes as key/value pairs

#### Scenario: An unset traces sink leaves the output exposed

- **WHEN** `traces_to` is not configured
- **THEN** `RunAgentOutputs.traces` is the raw `TraceEvent` `PCollection` and no write transform is attached

### Requirement: Failure routes emit ERROR trace events with failure position

Each DoFn failure route SHALL emit an `ERROR` `TraceEvent` on `.traces` alongside the `.errors` record it already produces, for `activation_timeout`, `activation_error`, `orphaned_result`, `hitl_timeout`, and `ttl_wiped_suspension`. The event SHALL be synthesized from the key, seq, injected clock, and reason the DoFn already holds, and SHALL carry `beam_agents.reason` and, where an exception is available, `error.type`. The event SHALL NOT carry the failed activation's staged events, intents, outputs, or state blobs; it MAY carry failure-position metadata derived from the failed context — scalars describing where the activation was, never what it staged.

For the `activation_error` route specifically, the runtime SHALL capture failure-position context when the agent raises — the intent-step cursor at failure, the `EventType` name of the last staged trace event, the count of staged intents, and the count of provider-reached model calls — carried out of the activation on a typed wrapper exception raised `from` the original, wrapping `Exception` only (never `CancelledError` or any other `BaseException`, so cancellation semantics are untouched). The `ERROR` event SHALL surface it as `beam_agents.failure.step`, `beam_agents.failure.last_event`, `beam_agents.failure.staged_intents`, and `beam_agents.failure.llm_calls`, and the `.errors` detail SHALL be the original exception's `repr` followed by ` failed_at_step=<step> after=<last_event>`. Every captured field SHALL be a pure function of the activation's deterministic path, so a replayed bundle that fails identically synthesizes a byte-identical enriched event. Routes where no context is reachable — `activation_timeout` (the coroutine may still be running), failures before context construction, and the non-activation routes — SHALL omit the `beam_agents.failure.*` attributes entirely rather than default them.

Emitting these events SHALL NOT mutate any keyed state, and a failed or timed-out activation SHALL still commit none of its staged effects.

#### Scenario: An activation timeout is traced and commits nothing

- **WHEN** an activation exceeds `activation_timeout`
- **THEN** an `ERROR` event with `beam_agents.reason = activation_timeout` and no `beam_agents.failure.*` attributes is emitted on `.traces`, the `.errors` record is emitted as before, and all five state specs are byte-for-byte unchanged

#### Scenario: A raising activation is traced with its error type and failure position

- **WHEN** the agent makes one provider-reached model call, stages one intent, and then raises
- **THEN** the `ERROR` event carries `beam_agents.reason = activation_error`, `error.type` naming the original exception class, `beam_agents.failure.step = "2"`, `beam_agents.failure.last_event = "INTENT_EMITTED"`, `beam_agents.failure.staged_intents = "1"`, and `beam_agents.failure.llm_calls = "1"`

#### Scenario: The dead letter names the failure position

- **WHEN** the agent raises `RuntimeError("boom")` after its second step
- **THEN** the `.errors` record's detail begins with `RuntimeError('boom')` and ends with ` failed_at_step=2 after=<the last staged event's kind>`, so a consumer prefix-matching on the exception `repr` is unaffected

#### Scenario: A replayed failure synthesizes a byte-identical enriched event

- **WHEN** a bundle is retried and the activation fails at the same point on the same path
- **THEN** the enriched `ERROR` event is byte-for-byte identical to the first attempt's, including every `beam_agents.failure.*` attribute

#### Scenario: Cancellation is never wrapped

- **WHEN** the bridge cancels a timed-out activation's coroutine
- **THEN** the resulting `CancelledError` propagates unwrapped, the element is routed as `activation_timeout`, and no failure-position capture is attempted

#### Scenario: An orphaned resume is traced

- **WHEN** a `ToolResult` arrives that matches no live, unexpired continuation
- **THEN** an `ERROR` event with `beam_agents.reason = orphaned_result` and no `beam_agents.failure.*` attributes is emitted and no state is mutated

#### Scenario: A HITL timeout is traced

- **WHEN** the HITL timer fires on a live continuation and the policy denies or drops
- **THEN** an `ERROR` event with `beam_agents.reason = hitl_timeout` is emitted in the suspended activation's trace

#### Scenario: A TTL-wiped suspension is traced

- **WHEN** working-memory GC reaches a key with a live continuation
- **THEN** an `ERROR` event with `beam_agents.reason = ttl_wiped_suspension` is emitted alongside the existing `.errors` record

#### Scenario: Staged traces of a failed activation are discarded

- **WHEN** an activation stages trace events and then fails
- **THEN** none of those staged events are emitted, and the only trace record for the failure is the single synthesized `ERROR` event — position metadata included, contents excluded
