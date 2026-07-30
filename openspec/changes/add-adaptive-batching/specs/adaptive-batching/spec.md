# adaptive-batching Specification

## ADDED Requirements

### Requirement: BatchPolicy is opt-in configuration and NONE preserves per-event semantics

`AgentConfig` SHALL expose `batch_policy: BatchPolicy` defaulting to `BatchPolicy.NONE`, plus `max_batch_size`, `max_wait_ms`, and `max_buffered_events` knobs meaningful only under `BatchPolicy.ADAPTIVE`. Under `NONE` the runtime SHALL preserve current per-event semantics exactly: every `event` element runs one activation immediately, `ctx.event` remains `bytes`, the `BATCH` state SHALL never be read or written, and `FLUSH_TIMER` SHALL never be armed. Configuration SHALL validate at `AgentConfig` construction: under `ADAPTIVE`, `max_batch_size`, `max_wait_ms`, and `max_buffered_events` MUST be positive with `max_buffered_events >= max_batch_size`; under `NONE`, explicitly setting any batch knob SHALL raise `ValueError` with an actionable message.

#### Scenario: NONE policy preserves existing semantics

- **WHEN** a pipeline runs with the default `BatchPolicy.NONE` over a stream of events, tool results, and approvals
- **THEN** every element behaves exactly as it does today — one activation per event with `ctx.event` as `bytes`, unchanged suspension/resume behavior — and the `BATCH` state stays empty with `FLUSH_TIMER` never armed

#### Scenario: Misconfigured batch knobs fail at the construction site

- **WHEN** an `AgentConfig` is constructed with `batch_policy=ADAPTIVE` and a non-positive `max_batch_size` or `max_wait_ms`, or with `batch_policy=NONE` and an explicit `max_batch_size`
- **THEN** construction raises `ValueError` naming the offending field, before any pipeline exists

#### Scenario: ADAPTIVE opt-in buffers instead of activating

- **WHEN** a pipeline runs with `batch_policy=ADAPTIVE` and a single event arrives for a key
- **THEN** no activation runs for that element: the envelope is appended to the key's `BATCH` state, `SEQ` is unchanged, and nothing is emitted on any output

### Requirement: Reaching the size threshold flushes the buffer as one activation

Under `ADAPTIVE`, when appending an `event` element brings the key's buffer to `max_batch_size` and no `Continuation` is live, the DoFn SHALL flush inline in the same `process()` call: read the buffered envelopes in arrival order, run exactly one activation over them, clear the `BATCH` state atomically with the activation's commit, and clear any pending `FLUSH_TIMER` so the consumed buffer cannot be flushed again.

#### Scenario: Size-threshold flush runs one activation over the whole buffer

- **WHEN** `max_batch_size = 3` and a third event arrives for a key with two buffered events and no live continuation
- **THEN** one activation runs with all three events in arrival order, its effects commit atomically with the buffer clear, and `SEQ` increases by exactly one

#### Scenario: A size flush disarms the pending flush timer

- **WHEN** a size-threshold flush commits while a `FLUSH_TIMER` armed by the buffer's first event is still pending
- **THEN** the timer is cleared, and no later firing flushes an empty buffer or re-processes the consumed events

### Requirement: max_wait is honored via a processing-time FLUSH_TIMER

Under `ADAPTIVE`, when an `event` element transitions the key's buffer from empty to non-empty, the DoFn SHALL arm `FLUSH_TIMER` (REAL_TIME domain) to fire `max_wait_ms` of processing time later, measured from an injected wall clock; subsequent buffered elements SHALL NOT re-arm it, so `max_wait_ms` bounds the wait from the first buffered event. When `FLUSH_TIMER` fires over a non-empty buffer with no live `Continuation`, the DoFn SHALL flush the full buffer as one activation exactly as a size flush does. A firing over an empty buffer SHALL be a no-op that mutates nothing and emits nothing. The wall-clock reading SHALL decide only the timer's firing time — never any staged effect, intent ID, cache key, or output byte — and timer behavior SHALL be tested with `TestStream` scripted processing-time advances, never `sleep()`.

#### Scenario: An undersized buffer flushes when max_wait elapses

- **WHEN** `max_batch_size = 10`, `max_wait_ms = 500`, two events are buffered for a key, and `TestStream` advances processing time past 500 ms from the first event
- **THEN** `FLUSH_TIMER` fires and one activation runs over exactly those two events, committing atomically with the buffer clear

#### Scenario: The wait is measured from the first buffered event

- **WHEN** events arrive for a key at processing times t, t+400 ms, and t+800 ms with `max_wait_ms = 500` and `max_batch_size = 10`
- **THEN** the flush fires at t+500 ms containing the first two events; the third event starts a new buffer and arms a new `FLUSH_TIMER`

#### Scenario: A stale flush firing over an empty buffer is a no-op

- **WHEN** a `FLUSH_TIMER` delivery arrives for a key whose buffer is empty
- **THEN** no activation runs, no state changes, and nothing is emitted on any output

### Requirement: Batch activations are batch-visible with ctx.event as a list

A flush activation SHALL present the batch to the agent as `ctx.event: list[bytes]` containing the buffered events' payloads in arrival order. The list shape SHALL be determined by policy, not by batch size: under `ADAPTIVE`, every flush — including a flush of one — presents a list, and under `NONE`, `ctx.event` is always `bytes`. The context SHALL expose `ctx.is_batch` (true exactly on `ADAPTIVE` flush activations) and a uniform `ctx.events` tuple accessor (the batch under `ADAPTIVE`, a singleton under `NONE`, empty on resume) so agents and adapters detect batch mode without `isinstance` checks. The activation clock `now_ms` SHALL be the maximum `event_time_ms` across the batched envelopes — a pure function of buffer contents.

#### Scenario: The agent receives the batch as a list in arrival order

- **WHEN** a flush activation runs over three buffered events under `ADAPTIVE`
- **THEN** the agent observes `ctx.event` as a three-element `list[bytes]` in arrival order with `ctx.is_batch` true

#### Scenario: A single-event flush is still a list

- **WHEN** `FLUSH_TIMER` fires over a buffer holding one event
- **THEN** the activation presents `ctx.event` as a one-element list, so the agent-visible type does not depend on runtime batch size

#### Scenario: The batch clock is the latest buffered event time

- **WHEN** a flush activation runs over envelopes with `event_time_ms` values 1000, 3000, and 2000
- **THEN** the activation's `now_ms` is 3000, and intent `expires_at_ms` and suspension deadlines derive from it

### Requirement: One activation per flush with unchanged seq, intent, and replay-cache accounting

A committed flush SHALL consume exactly one `SEQ` increment, mint intent IDs as `intent_id_for(key, seq, step_index)` exactly as a per-event activation does, and key its replay-cache entries by the same `(entity_key, seq)` scope. A retried bundle that re-runs a flush SHALL re-read the same buffer contents, produce byte-identical intents, and incur zero additional provider calls on the cached path. The flush activation's trace SHALL carry `beam_agents.batch.size` and `beam_agents.batch.trigger` attributes identifying the batch and which trigger fired.

#### Scenario: A batch of N events consumes one seq

- **WHEN** a flush over five buffered events commits
- **THEN** `SEQ` increases by exactly one and the next activation for the key reads the incremented value

#### Scenario: A retried flush bundle replays deterministically

- **WHEN** a bundle containing a flush activation is retried after its LLM call was cached
- **THEN** the re-run flush reads the same batch, resolves the call from the replay cache with zero extra provider calls, and produces byte-identical `intent_id`s

### Requirement: A suspending batch activation suspends and resumes as a whole batch

When a flush activation returns `Suspend`, the runtime SHALL persist exactly one `Continuation` at the batch's `seq`, covering the entire batch. A matching `tool_result` or `approval` SHALL resume that continuation exactly once, at the batch's `seq`, seeded from its `step_index` and `snapshot`, and the resumed activation's outcome SHALL apply to the batch as a unit. There SHALL be no per-element suspension and no partial resume; `ctx.event`/`ctx.events` SHALL be empty on resume, with the agent's `snapshot` carrying any resume state, exactly as on the per-event path. HITL timeout over a batch suspension SHALL fail closed for the whole batch through the existing fallback route, and a late result SHALL be one `orphaned_result`.

#### Scenario: A batch suspension persists one continuation for the whole batch

- **WHEN** a flush activation over four events stages an approval intent and returns `Suspend`
- **THEN** exactly one `Continuation` is persisted at the batch's `seq`, `HITL_TIMER` is armed once, and no per-element continuations exist

#### Scenario: The batch resumes together

- **WHEN** the approval for a suspended batch activation arrives before its deadline
- **THEN** one resumed activation runs at the batch's `seq` from the persisted `step_index` and `snapshot`, and its completion clears the single continuation

#### Scenario: HITL timeout fails the whole batch closed

- **WHEN** `HITL_TIMER` fires for a suspended batch activation's deadline
- **THEN** the configured fallback route runs once for the whole batch, the continuation is cleared, `SEQ` is unchanged, and a later result for the batch's intent is dead-lettered as `orphaned_result`

### Requirement: Flush triggers defer while a continuation is live

While a `Continuation` is live for a key, the DoFn SHALL NOT flush: an `event` element that reaches `max_batch_size` SHALL buffer without flushing, and a `FLUSH_TIMER` firing SHALL leave the buffer intact rather than run an activation that could overwrite the live continuation. When the suspension resolves — a resume activation commits, or the HITL fallback's `Deny`/`Drop` route ends the wait — the resolving path SHALL re-arm `FLUSH_TIMER` to fire promptly whenever the buffer is non-empty, so the deferred batch flushes in its own timer callback with its own commit. An `Escalate` route SHALL keep the suspension live and keep deferring.

#### Scenario: The size trigger defers during a suspension

- **WHEN** a batch suspension is live and buffered events reach `max_batch_size`
- **THEN** no flush runs, the live continuation is untouched, and the events remain buffered

#### Scenario: A timer firing during a suspension does not overwrite the continuation

- **WHEN** `FLUSH_TIMER` fires while a continuation is live
- **THEN** the buffer and the continuation are both unchanged and no activation runs

#### Scenario: Resolution flushes the deferred buffer promptly

- **WHEN** a suspended batch's resume commits while deferred events sit in the buffer
- **THEN** `FLUSH_TIMER` is re-armed by the resolving path and the deferred events flush as one activation in a subsequent timer callback, with its own `SEQ` increment

### Requirement: Batch failure and overflow fail closed without wedging the key

A flush activation that fails or exceeds `activation_timeout` SHALL commit none of its staged effects, SHALL emit one `ActivationError` per buffered envelope on `.errors` (reason `activation_error` or `activation_timeout`, detail carrying the batch size and trigger), SHALL clear the `BATCH` state and `FLUSH_TIMER` so the same batch is never retried unboundedly, and SHALL leave `SEQ` and all other state specs unchanged. When a key's buffer already holds `max_buffered_events` envelopes, a further `event` element SHALL be dead-lettered on `.errors` with reason `batch_buffer_overflow` instead of appended, so buffer growth during deferral is explicitly bounded.

#### Scenario: A failed flush dead-letters every batched event and consumes the buffer

- **WHEN** the agent raises during a flush activation over three buffered events
- **THEN** three `activation_error` records reach `.errors`, the buffer and flush timer are cleared, and `SEQ`, `MEMORY`, `LLM_CACHE`, `CONTINUATION`, and `PENDING` are unchanged

#### Scenario: Overflow during deferral is explicit

- **WHEN** a suspension defers flushing and events keep arriving until the buffer holds `max_buffered_events`
- **THEN** the next event is emitted on `.errors` as `batch_buffer_overflow` and the buffer does not grow past the cap
