## ADDED Requirements

### Requirement: Keyed state and timer topology
The `_AgentDoFn` SHALL declare exactly five keyed state specs — `MEMORY` (ReadModifyWriteState over `MemoryBlob`), `CONTINUATION` (ReadModifyWriteState over `Continuation`), `LLM_CACHE` (ReadModifyWriteState over `LlmCacheBlob`), `PENDING` (BagState of `ToolIntent`), and `SEQ` (CombiningValueState with sum combiner) — and exactly two timers for this change: `TTL_TIMER` in the WATERMARK domain and `HITL_TIMER` in the REAL_TIME domain. All persisted state SHALL be protobuf; no state spec SHALL store pickled Python objects.

#### Scenario: State specs are protobuf-backed and pickle-free
- **WHEN** the DoFn persists any of `MEMORY`, `CONTINUATION`, `LLM_CACHE`, `PENDING`, or `SEQ`
- **THEN** the stored value is a serialized protobuf (or an integer for `SEQ`) and no coder falls back to pickle

#### Scenario: A fresh key reads versioned-empty facades and zero seq
- **WHEN** the first element for a key is processed and no prior state exists
- **THEN** the memory and replay-cache facades are seeded from versioned-empty blobs and `SEQ` reads as `0`

### Requirement: Element routing by envelope kind
The DoFn SHALL consume a single keyed `AgentEnvelope` input and route each element by its payload variant: an `event` starts (or continues) an activation, and a `tool_result` or `approval` resumes the persisted `Continuation`. A resume element whose `intent_id` matches no live, unexpired `Continuation` SHALL be emitted as `orphaned_result` on `.errors` and SHALL mutate no state.

#### Scenario: Event starts an activation
- **WHEN** an `AgentEnvelope` with the `event` variant arrives for a key with no live continuation
- **THEN** a fresh activation runs and its effects commit on success

#### Scenario: Tool-result resumes the matching continuation
- **WHEN** an `AgentEnvelope` with a `tool_result` variant arrives whose `intent_id` matches a live `Continuation`
- **THEN** the DoFn rehydrates that continuation and resumes the agent from the suspended point

#### Scenario: Orphaned resume mutates nothing
- **WHEN** a `tool_result` or `approval` arrives whose `intent_id` matches no live, unexpired `Continuation`
- **THEN** the element is routed to `.errors` as `orphaned_result` and `MEMORY`, `CONTINUATION`, `LLM_CACHE`, `PENDING`, and `SEQ` are all unchanged

### Requirement: Async-bridge activation bounded by activation_timeout
`setup()` SHALL start one background thread per DoFn instance running a dedicated asyncio event loop with shared httpx pools, and `teardown()` SHALL stop and join it. `process()` SHALL submit the activation coroutine to that loop and block for at most `activation_timeout`. On timeout the DoFn SHALL cancel the coroutine, route the element to `.errors`, and commit no staged effects, leaving all state unchanged.

#### Scenario: One event loop thread spans the DoFn lifetime
- **WHEN** `setup()` runs then multiple elements are processed
- **THEN** all activation coroutines run on the single per-instance loop and httpx pools are reused across elements

#### Scenario: Activation timeout cancels and mutates no state
- **WHEN** an activation coroutine does not complete within `activation_timeout`
- **THEN** the coroutine is cancelled, the element is emitted on `.errors`, and `MEMORY`, `CONTINUATION`, `LLM_CACHE`, `PENDING`, and `SEQ` are byte-for-byte unchanged from before the element

#### Scenario: Cancellation does not leak into the next element
- **WHEN** an element times out and a subsequent element for the same key is processed
- **THEN** the next activation runs on the same healthy loop and observes the pre-timeout committed state

### Requirement: Atomic staged commit with fixed ordering
All activation effects — memory writes, replay-cache inserts, continuation set/clear, pending intents, `SEQ` increment, and emitted outputs/intents/traces — SHALL be staged in the activation context and applied to Beam state only on activation success, in the order: `MEMORY`, `LLM_CACHE`, `CONTINUATION`, `PENDING`, `SEQ`, timers, emits. A failed or timed-out activation SHALL discard the staged context without touching state.

#### Scenario: Successful activation commits all effects together
- **WHEN** an activation completes successfully with memory writes, a cache insert, and an emitted output
- **THEN** all staged effects are visible after the bundle commits and none were applied before activation success

#### Scenario: Failed activation commits nothing
- **WHEN** the agent loop raises before completion
- **THEN** no staged effect is applied and all five state specs retain their pre-activation values

#### Scenario: Replay of a retried bundle incurs zero extra provider calls
- **WHEN** a bundle is retried and re-runs an activation whose LLM request was already cached in `LLM_CACHE`
- **THEN** the request resolves from the replay cache and the provider client is not called again

### Requirement: Monotonic SEQ incremented exactly once per committed activation
`SEQ` SHALL be read at activation start to seed deterministic `intent_id`s and SHALL be incremented by exactly one in the commit tail of a successful activation. It SHALL NOT be incremented on the timeout path, the error path, or on any timer-only firing.

#### Scenario: One increment per committed activation
- **WHEN** an activation commits successfully
- **THEN** `SEQ` increases by exactly one relative to its pre-activation value

#### Scenario: No increment on non-committed paths
- **WHEN** an activation times out, raises, or a `TTL_TIMER`/`HITL_TIMER` fires without an activation
- **THEN** `SEQ` is unchanged

#### Scenario: Replayed activation reads the same seq and reproduces intents
- **WHEN** the same activation is replayed after a bundle retry
- **THEN** it reads the identical `seq` and produces byte-identical `intent_id`s

### Requirement: TTL timer garbage-collects all state and re-arms per element
Every processed element SHALL set `TTL_TIMER` to a fresh `now + ttl` watermark time, superseding any prior mark. When `TTL_TIMER` fires, the DoFn SHALL clear all five state specs so an idle key retains no residue, and a subsequent element for that key SHALL start from versioned-empty facades with `SEQ = 0`.

#### Scenario: TTL fire wipes every state spec
- **WHEN** `TTL_TIMER` fires for a key that has accumulated memory, a continuation, cache entries, pending intents, and a non-zero seq
- **THEN** `MEMORY`, `CONTINUATION`, `LLM_CACHE`, `PENDING`, and `SEQ` are all cleared

#### Scenario: New element re-arms the TTL timer
- **WHEN** an element is processed for a key that already had a pending `TTL_TIMER`
- **THEN** the timer is re-armed to the new `now + ttl` and the earlier mark does not fire

#### Scenario: Key recovers cleanly after TTL wipe
- **WHEN** an element arrives for a key after its `TTL_TIMER` has fired and wiped state
- **THEN** the activation seeds versioned-empty facades and reads `SEQ = 0`

### Requirement: HITL timer fails closed
`HITL_TIMER` SHALL be armed in the REAL_TIME domain when an activation suspends awaiting an approval or tool result. On fire, the DoFn SHALL run the fallback path, clear the dangling `Continuation`, and SHALL NOT increment `SEQ`. A real result arriving after the timeout SHALL be treated as `orphaned_result`.

#### Scenario: HITL timeout triggers fallback and clears continuation
- **WHEN** `HITL_TIMER` fires while a `Continuation` is live
- **THEN** the fallback path runs, `CONTINUATION` is cleared, and `SEQ` is unchanged

#### Scenario: Late result after HITL timeout is orphaned
- **WHEN** a `tool_result`/`approval` arrives after its `HITL_TIMER` has already fired and cleared the continuation
- **THEN** it is emitted as `orphaned_result` on `.errors` and mutates no state

### Requirement: Per-key ordering preserved under interleaving
Because Beam serializes elements per key and the DoFn introduces no cross-key shared mutable state, interleaved `event`, `tool_result`, and `approval` elements for a single key SHALL be processed and committed in arrival order, and no two activations for the same key SHALL run concurrently on the async bridge.

#### Scenario: Interleaved elements commit in arrival order
- **WHEN** a stream interleaves events and results across multiple keys via `TestStream`
- **THEN** for each key the committed state reflects the elements applied in the order they arrived

#### Scenario: No concurrent activations per key
- **WHEN** an activation for a key is in flight on the async bridge
- **THEN** the next element for that key is not processed until the in-flight activation has committed or been discarded
