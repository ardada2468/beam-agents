## MODIFIED Requirements

### Requirement: Failure routes emit ERROR trace events

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
