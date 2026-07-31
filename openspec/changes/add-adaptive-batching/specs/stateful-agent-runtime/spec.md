# stateful-agent-runtime Delta Specification

## MODIFIED Requirements

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
