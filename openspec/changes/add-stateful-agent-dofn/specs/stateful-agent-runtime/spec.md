## ADDED Requirements

### Requirement: Durable keyed runtime state
The runtime SHALL declare `MEMORY`, `CONTINUATION`, `LLM_CACHE`, `PENDING`, and `SEQ` as Beam keyed state specs, SHALL encode protobuf state with deterministic protobuf coders, and MUST NOT persist state through pickle.

#### Scenario: State is isolated per entity key
- **WHEN** envelopes for two entity keys are processed by the stateful runtime
- **THEN** each key reads and mutates only its own memory, continuation, replay cache, pending intents, and sequence

#### Scenario: Persisted protobuf state is deterministic
- **WHEN** equivalent runtime state is encoded repeatedly
- **THEN** the encoded bytes are deterministic within the pinned protobuf runtime and no pickle coder is used

### Requirement: Monotonic activation sequence
The runtime SHALL allocate the next per-key `SEQ` value for each accepted external event and SHALL resume a continuation using the continuation's existing sequence.

#### Scenario: New event allocates sequence
- **WHEN** an external event is accepted for a key whose current sequence is `n`
- **THEN** the activation runs with sequence `n + 1` and successful commit persists that value

#### Scenario: Reinjected result preserves sequence
- **WHEN** a correlated tool result or approval resumes a continuation at sequence `n`
- **THEN** the resumed activation uses sequence `n` and does not increment `SEQ`

### Requirement: Envelope routing
The runtime SHALL route an `AgentEnvelope` according to its payload discriminator and SHALL validate that the Beam KV key equals the envelope entity key before invoking the agent.

#### Scenario: External event starts activation
- **WHEN** a valid envelope contains `external_event` and no continuation is pending
- **THEN** the runtime starts a new activation with the external event payload

#### Scenario: Tool result resumes continuation
- **WHEN** a `tool_result` matches an intent in `PENDING` and the persisted continuation
- **THEN** the runtime removes the resolved pending intent and resumes that continuation

#### Scenario: Approval resumes continuation
- **WHEN** an `approval` matches an intent in `PENDING` and the persisted continuation
- **THEN** the runtime removes the resolved pending intent and resumes with the approval decision

#### Scenario: Invalid envelope is rejected
- **WHEN** an envelope has no recognized payload or its entity key differs from the Beam KV key
- **THEN** the runtime emits a typed invalid-envelope error and does not invoke the agent or mutate keyed state

#### Scenario: New event cannot overwrite suspension
- **WHEN** an external event arrives while a continuation is awaiting reinjection for the same key
- **THEN** the runtime emits a typed busy-key error and preserves the existing continuation and pending intents

### Requirement: Correlated suspension and reinjection
An activation that requests an external effect or human decision SHALL persist its continuation and pending intents together, and the runtime SHALL fail closed for uncorrelated or expired reinjections.

#### Scenario: Activation suspends for pending intent
- **WHEN** a successful activation stages a side-effect or approval intent and a continuation
- **THEN** commit persists the continuation, stores the intent in `PENDING`, emits the intent, and schedules the HITL deadline atomically

#### Scenario: Unknown result is orphaned
- **WHEN** a tool result or approval does not match the persisted continuation and pending intents
- **THEN** the runtime emits a typed `orphaned_result` error, does not invoke the agent, and does not mutate keyed state

#### Scenario: Late result is orphaned
- **WHEN** a result arrives after timeout handling has resolved and removed its continuation
- **THEN** the runtime emits a typed `orphaned_result` error and does not recreate or resume the continuation

### Requirement: Async activation bridge
The runtime SHALL execute async activations on one dedicated asyncio event-loop thread per `DoFn` instance and SHALL reuse loop-owned async resources across elements.

#### Scenario: Setup creates one bridge
- **WHEN** a `DoFn` instance is set up and processes multiple elements
- **THEN** all activation coroutines execute on the same instance-owned bridge loop

#### Scenario: Teardown closes the bridge
- **WHEN** Beam tears down the `DoFn` instance
- **THEN** outstanding bridge work is cancelled, loop-owned async resources are closed, and the bridge thread terminates

### Requirement: Activation timeout cancellation
The runtime SHALL bound synchronous waiting by `activation_timeout`, SHALL request coroutine cancellation when that timeout expires, and SHALL discard all effects staged by the timed-out activation.

#### Scenario: Activation exceeds timeout
- **WHEN** an activation does not complete within `activation_timeout`
- **THEN** the runtime cancels its bridge future, emits one typed timeout error, and commits no staged state, timers, intents, outputs, traces, or usage

#### Scenario: Cancellation completes near timeout
- **WHEN** a cancellation races with normal coroutine completion after the timeout has been observed
- **THEN** the activation is treated as timed out and its staged context is not committed

### Requirement: Atomic staged commit
The runtime SHALL keep activation mutations and outputs in an activation-scoped staging context and SHALL apply them only after execution and commit validation succeed. Beam state changes, timer changes, and emitted records SHALL participate in the same bundle commit.

#### Scenario: Successful activation commits all effects
- **WHEN** an activation completes and its staged effects pass validation
- **THEN** the runtime applies staged memory, replay cache, continuation, pending intents, sequence, timers, and tagged outputs as one bundle-atomic result

#### Scenario: Failed activation commits nothing
- **WHEN** agent execution, state-size validation, or commit preparation raises
- **THEN** the runtime discards staged effects, preserves pre-activation keyed state, and emits only a typed runtime error created outside the failed staging context

#### Scenario: Commit preparation precedes output
- **WHEN** a successful activation has tagged outputs
- **THEN** all state and timer commit operations are prepared before the first staged output is yielded

#### Scenario: Pending intents have stable replacement order
- **WHEN** a successful activation changes `PENDING`
- **THEN** the runtime clears the bag and re-adds remaining and new intents in deterministic `intent_id` order

### Requirement: Replay cache durability
The runtime SHALL construct each activation's replay cache from `LLM_CACHE` state and SHALL write the staged cache blob only on successful commit.

#### Scenario: Retry reuses committed response
- **WHEN** a bundle retry repeats an LLM request whose response is present in keyed replay-cache state
- **THEN** the activation makes zero additional provider calls and follows the cached path

#### Scenario: Failed activation does not publish cache insert
- **WHEN** an activation stages an LLM response and subsequently fails or times out
- **THEN** the new replay-cache entry is not written to `LLM_CACHE`

### Requirement: Event-time state TTL
The runtime SHALL use a watermark timer to garbage-collect expired working memory and SHALL derive timer updates from successfully committed state.

#### Scenario: Watermark expires memory
- **WHEN** the watermark passes a key's configured memory-expiry deadline
- **THEN** the TTL callback clears expired working memory without changing that key's sequence

#### Scenario: Failed activation does not extend TTL
- **WHEN** an activation that accessed or changed memory fails or times out
- **THEN** its staged TTL update is discarded and the prior durable deadline remains effective

### Requirement: HITL and result timeout
The runtime SHALL use a real-time timer for the earliest pending continuation deadline and SHALL process timer expiry through the same staged activation and commit protocol as element-driven execution.

#### Scenario: Pending wait schedules earliest deadline
- **WHEN** a successful activation leaves one or more pending intents
- **THEN** the HITL timer is set to the earliest durable pending deadline

#### Scenario: Resolution updates deadline
- **WHEN** a result or approval resolves the earliest pending intent and later pending work remains
- **THEN** successful commit advances the HITL timer to the next deadline

#### Scenario: Timer invokes fallback
- **WHEN** the HITL timer fires for an unresolved continuation
- **THEN** the runtime invokes the configured timeout or fallback path using the continuation's sequence and atomically commits its successful result

#### Scenario: Fallback failure is atomic
- **WHEN** timeout or fallback execution fails
- **THEN** no fallback effects are committed and the runtime emits a typed timeout-handling error
