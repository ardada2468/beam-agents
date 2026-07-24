## 1. Scaffolding — state/timer topology and transform wiring

- [x] 1.1 Create `core/dofn.py` with `_AgentDoFn` declaring the five state specs (`MEMORY`, `CONTINUATION`, `LLM_CACHE` as ReadModifyWriteState; `PENDING` as BagState of `ToolIntent`; `SEQ` as CombiningValueState/sum) using the deterministic proto coders — no pickle fallback.
- [x] 1.2 Declare `TTL_TIMER` (WATERMARK) and `HITL_TIMER` (REAL_TIME) timer specs; leave `FLUSH_TIMER` out of scope.
- [x] 1.3 Wire `_AgentDoFn` into `RunAgent` in `core/transform.py`, attaching the deterministic `AgentEnvelope` key/element coder.
- [x] 1.4 Add `core/context.py` skeleton for the staged activation context (memory facade, replay-cache facade, pending list, staged emits, `next_seq`) — no commit logic yet.
- [x] 1.5 Test: a `TestPipeline` round-trips an `AgentEnvelope` through `RunAgent` with no pickle fallback; fresh key seeds versioned-empty facades and `SEQ = 0`.

## 2. Lifecycle step 1 — Ingest & route by kind

- [x] 2.1 Implement `process()` decode + read of all five state specs into the activation context.
- [x] 2.2 Route by `AgentEnvelope` payload variant: `event` → start/continue; `tool_result`/`approval` → resume persisted `Continuation`.
- [x] 2.3 Handle unmatched/expired resumes: emit `orphaned_result` on `.errors` and mutate no state.
- [x] 2.4 Test (`TestPipeline`): event starts an activation; matching `tool_result` rehydrates the continuation; orphaned resume routes to `.errors` and leaves all five specs unchanged.

## 3. Lifecycle step 2 — Async-bridge activation with timeout

- [x] 3.1 Implement `setup()`/`teardown()`: one daemon thread per instance running a dedicated asyncio loop with shared httpx pools; stop-and-join on teardown.
- [x] 3.2 Submit the activation coroutine via `run_coroutine_threadsafe`; block up to `activation_timeout`.
- [x] 3.3 On timeout: cancel the coroutine, drain cancellation, route the element to `.errors`, and skip staging/commit entirely.
- [x] 3.4 Resolve `activation_timeout` default and per-agent override (Open Question in design).
- [x] 3.5 Test (`TestPipeline`): a coroutine exceeding `activation_timeout` is cancelled, routed to `.errors`, mutates no state, and a following element for the same key runs on the same healthy loop.

## 4. Lifecycle step 3 — Drive the agent loop

- [x] 4.1 Implement `core/loop.py` activation driver: run the async agent, resolve LLM calls through the seeded `LLM_CACHE` facade, run read-only tools inline.
- [x] 4.2 On a side-effect tool or approval, stage a `ToolIntent` with deterministic `intent_id = uuid5(NS, key + seq + step_index)` and stage a `Continuation` for resume.
- [x] 4.3 On completion, stage the terminal output and mark the `Continuation` for clearing.
- [x] 4.4 Test (`TestPipeline`): a cached LLM request incurs zero additional provider calls on a retried bundle; a replayed activation reads the same `seq` and produces byte-identical `intent_id`s.

## 5. Lifecycle step 4 — Stage effects

- [x] 5.1 Route all loop mutations (memory writes, cache inserts, pending intents, outputs, traces) exclusively through the activation context — nothing touches Beam state during the loop.
- [x] 5.2 Surface facade cap violations (`MemoryOverflow`, oversized cache blob) to `.errors` without partial commit.
- [x] 5.3 Test: a mid-loop failure leaves the staged context discardable and all five specs at pre-activation values (unit + `TestPipeline`).

## 6. Lifecycle step 5 — Atomic commit ordering

- [x] 6.1 Implement the commit tail applying staged effects in fixed order: `MEMORY` → `LLM_CACHE` → `CONTINUATION` (set/clear) → `PENDING` → `SEQ += 1` → arm/re-arm timers → emit `.output`/`.intents`/`.traces`.
- [x] 6.2 Gate the entire commit behind activation success; discard the context on failure/timeout.
- [x] 6.3 Increment `SEQ` by exactly one here and nowhere else (not in the loop, timeout, error, or timer paths).
- [x] 6.4 Test (`TestPipeline`): successful activation commits all effects together and `SEQ` increases by exactly one; failed/timed-out activation commits nothing and `SEQ` is unchanged.

## 7. Timers — TTL wipe/re-arm and HITL fail-closed

- [x] 7.1 Re-arm `TTL_TIMER` to `now + ttl` on every processed element, superseding any prior mark.
- [x] 7.2 On `TTL_TIMER` fire, clear all five state specs (no emit, no `SEQ` change).
- [x] 7.3 Arm `HITL_TIMER` on suspend; on fire run the fallback path, clear the dangling `Continuation`, and leave `SEQ` unchanged; treat a late real result as `orphaned_result`.
- [x] 7.4 Test (`TestStream`): TTL fire wipes memory/continuation/cache/pending/seq; a new element re-arms and the old mark does not fire; the key recovers with `SEQ = 0`.
- [x] 7.5 Test (`TestStream`): HITL fire triggers fallback, clears the continuation without touching `SEQ`, and a post-timeout result is orphaned.

## 8. Ordering and end-to-end verification

- [x] 8.1 Test (`TestStream`): interleaved `event`/`tool_result`/`approval` across multiple keys commit per-key in arrival order with no cross-key state bleed.
- [x] 8.2 Test: assert no two activations for one key run concurrently on the async bridge.
- [x] 8.3 Run the full suite under `pytest` (no docker) plus `ruff` and `mypy --strict`; confirm the four invariant verifications (TTL wipe/re-arm, timeout cancels with no mutation, exactly-once `SEQ`, per-key ordering) all pass.
