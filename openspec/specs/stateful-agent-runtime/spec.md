# stateful-agent-runtime Specification

## Purpose
TBD - created by archiving change add-stateful-dofn-runtime. Update Purpose after archive.
## Requirements
### Requirement: Keyed state and timer topology
The `_AgentDoFn` SHALL declare exactly six keyed state specs — `MEMORY` (ReadModifyWriteState over `MemoryBlob`), `CONTINUATION` (ReadModifyWriteState over `Continuation`), `LLM_CACHE` (ReadModifyWriteState over `LlmCacheBlob`), `PENDING` (BagState of `ToolIntent`), `SEQ` (CombiningValueState with sum combiner), and `BATCH` (BagState of `AgentEnvelope`, the adaptive-batching buffer) — and exactly three timers: `TTL_TIMER` in the WATERMARK domain, `HITL_TIMER` in the REAL_TIME domain, and `FLUSH_TIMER` in the REAL_TIME domain (adaptive batching only). All persisted state SHALL be protobuf; no state spec SHALL store pickled Python objects. Under `BatchPolicy.NONE`, `BATCH` SHALL never be read or written and `FLUSH_TIMER` SHALL never be armed, so declaring them changes no existing behavior.

#### Scenario: State specs are protobuf-backed and pickle-free
- **WHEN** the DoFn persists any of `MEMORY`, `CONTINUATION`, `LLM_CACHE`, `PENDING`, `SEQ`, or `BATCH`
- **THEN** the stored value is a serialized protobuf (or an integer for `SEQ`) and no coder falls back to pickle

#### Scenario: A fresh key reads versioned-empty facades and zero seq
- **WHEN** the first element for a key is processed and no prior state exists
- **THEN** the memory and replay-cache facades are seeded from versioned-empty blobs, `SEQ` reads as `0`, and the `BATCH` buffer reads empty

#### Scenario: The batch topology is inert under NONE
- **WHEN** a pipeline runs with `BatchPolicy.NONE`
- **THEN** `BATCH` is never written and `FLUSH_TIMER` never fires, and the five pre-existing specs and two pre-existing timers behave exactly as before this change

### Requirement: Element routing by envelope kind
The DoFn SHALL consume a single keyed `AgentEnvelope` input and route each element by its payload variant. Under `BatchPolicy.NONE`, an `event` starts (or continues) an activation immediately, unchanged. Under `BatchPolicy.ADAPTIVE`, an `event` SHALL be appended to the `BATCH` buffer instead — committing only the buffering effects (bag append, `TTL_TIMER` re-arm, `FLUSH_TIMER` arm on the empty-to-non-empty transition), with no activation, no `SEQ` increment, and no emitted outputs — until a flush trigger runs the buffered batch as one activation. Under both policies, a `tool_result` or `approval` SHALL never buffer: it resumes the persisted `Continuation` immediately, and a resume element whose `intent_id` matches no live, unexpired `Continuation` SHALL be emitted as `orphaned_result` on `.errors` and SHALL mutate no state.

#### Scenario: Event starts an activation
- **WHEN** an `AgentEnvelope` with the `event` variant arrives under `BatchPolicy.NONE` for a key with no live continuation
- **THEN** a fresh activation runs and its effects commit on success

#### Scenario: Event buffers under ADAPTIVE
- **WHEN** an `AgentEnvelope` with the `event` variant arrives under `BatchPolicy.ADAPTIVE` and no flush trigger is met
- **THEN** the envelope is appended to `BATCH`, no activation runs, `SEQ` is unchanged, and nothing is emitted

#### Scenario: Tool-result resumes the matching continuation
- **WHEN** an `AgentEnvelope` with a `tool_result` variant arrives whose `intent_id` matches a live `Continuation`, under either policy
- **THEN** the DoFn rehydrates that continuation and resumes the agent from the suspended point without buffering the element

#### Scenario: Orphaned resume mutates nothing
- **WHEN** a `tool_result` or `approval` arrives whose `intent_id` matches no live, unexpired `Continuation`
- **THEN** the element is routed to `.errors` as `orphaned_result` and `MEMORY`, `CONTINUATION`, `LLM_CACHE`, `PENDING`, `SEQ`, and `BATCH` are all unchanged

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
Every processed element — including a buffered `event` that runs no activation — SHALL set `TTL_TIMER` to a fresh `now + ttl` watermark time, superseding any prior mark. When `TTL_TIMER` fires, the DoFn SHALL clear all six state specs and any pending `FLUSH_TIMER` so an idle key retains no residue, and a subsequent element for that key SHALL start from versioned-empty facades with `SEQ = 0` and an empty buffer. Buffered envelopes wiped by the fire SHALL first be dead-lettered on `.errors` with reason `ttl_wiped_batch`, one record per wiped envelope, so the loss is observable rather than silent.

#### Scenario: TTL fire wipes every state spec
- **WHEN** `TTL_TIMER` fires for a key that has accumulated memory, a continuation, cache entries, pending intents, buffered events, and a non-zero seq
- **THEN** `MEMORY`, `CONTINUATION`, `LLM_CACHE`, `PENDING`, `SEQ`, and `BATCH` are all cleared and no `FLUSH_TIMER` remains armed

#### Scenario: New element re-arms the TTL timer
- **WHEN** an element is processed for a key that already had a pending `TTL_TIMER`, whether it activates or only buffers
- **THEN** the timer is re-armed to the new `now + ttl` and the earlier mark does not fire

#### Scenario: Key recovers cleanly after TTL wipe
- **WHEN** an element arrives for a key after its `TTL_TIMER` has fired and wiped state
- **THEN** the activation (or buffering) seeds versioned-empty facades, reads `SEQ = 0`, and starts from an empty buffer

#### Scenario: Wiped buffered events are reported
- **WHEN** `TTL_TIMER` fires for a key whose `BATCH` buffer holds two un-flushed envelopes
- **THEN** two `ttl_wiped_batch` records are emitted on `.errors` before the wipe, and the buffer is cleared

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
