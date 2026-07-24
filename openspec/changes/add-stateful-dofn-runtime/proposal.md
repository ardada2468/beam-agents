## Why

The wire schemas (`ToolIntent`, `Continuation`, `MemoryBlob`, `AgentEnvelope`), the deterministic proto coder, and the in-memory `memory-facade` / `llm-replay-cache` facades all exist, but nothing binds them into a running Beam transform. `RunAgent(agent)` has no execution engine: no keyed state, no timers, no async bridge to the LLM client, and no atomic commit discipline. This change delivers `core/dofn.py` — the keyed, stateful `_AgentDoFn` that turns those staged facades into a durable, replayable agent runtime. It is the largest and riskiest change in the project because it is where every correctness invariant (atomic commit, deterministic intent IDs, replay cache, per-key serialization, fail-closed timeouts) is actually enforced.

## What Changes

- Introduce `_AgentDoFn` in `core/dofn.py`, a keyed stateful `DoFn` consuming a single `AgentEnvelope` input type, wired into `RunAgent` via the deterministic proto coder.
- Declare five state specs: `MEMORY` (ReadModifyWriteState → MemoryBlob), `CONTINUATION` (ReadModifyWriteState → Continuation), `LLM_CACHE` (ReadModifyWriteState → LlmCacheBlob), `PENDING` (BagState of ToolIntent), `SEQ` (CombiningValueState, sum).
- Declare two timers: `TTL_TIMER` (WATERMARK — working-memory GC) and `HITL_TIMER` (REAL_TIME — approval/result timeout). `FLUSH_TIMER` is out of scope for this change.
- **Element routing by kind:** dispatch each `AgentEnvelope` by its payload variant — event → new/continued activation; tool-result & approval → resume a persisted `Continuation`; unmatched/late resumes → `orphaned_result` on `.errors`.
- **Async bridge:** `setup()` starts one background thread per DoFn instance running a dedicated asyncio loop with shared httpx pools; `process()` submits the activation coroutine and blocks up to `activation_timeout`; on timeout it cancels the coroutine, routes the element to `.errors`, and mutates no state.
- **Atomic commit ordering:** all effects (memory writes, cache inserts, pending intents, `SEQ` increment, emitted outputs/intents/traces) are STAGED in the activation context and applied to Beam state in a fixed order only on activation success; `SEQ` increments exactly once per committed activation.
- **TTL lifecycle:** every processed element (re)arms `TTL_TIMER`; on fire it clears all five state specs so an idle key leaves no residue, and future elements start fresh.
- **Timeout fail-closed:** HITL timeout fires the fallback path; the async `activation_timeout` cancels in-flight work — both leave state consistent with the atomic-commit invariant.

## Capabilities

### New Capabilities
- `stateful-agent-runtime`: The `_AgentDoFn` keyed state/timer topology, element-by-kind routing, async-bridge activation with `activation_timeout` cancellation, TTL and HITL timer behavior, monotonic `SEQ`, and the atomic staged-commit ordering that ties the memory, replay-cache, and continuation facades to Beam-managed state.

### Modified Capabilities
<!-- The memory-facade, llm-replay-cache, wire-schemas, proto-coders, and model-client specs are consumed unchanged; their requirements are not modified. -->
(none — dependency specs are consumed as-is)

## Impact

- **New code:** `core/dofn.py` (state/timer specs, routing, async bridge, commit), `core/context.py` (staged activation context), `core/loop.py` (activation driver invoking the agent + model client), and `RunAgent` wiring in `core/transform.py` to attach the proto coder.
- **Consumes (unchanged):** `wire-schemas` (`AgentEnvelope`, `ToolIntent`, `Continuation`, `MemoryBlob`, `LlmCacheBlob`), `proto-coders` (deterministic key/element coder), `memory-facade`, `llm-replay-cache`, `model-client`.
- **Dependencies:** `apache-beam` stateful DoFn API (state, timers, watermark & real-time domains); `httpx`/asyncio for the bridge thread. No new third-party dependencies.
- **Verification surface:** `TestPipeline` / `TestStream` coverage for TTL wipe-and-rearm, activation-timeout cancellation with no state mutation, exactly-once `SEQ` increment, and per-key ordering under interleaving.
- **Risk:** highest in the repo — touches every correctness invariant. Tasks are split per lifecycle step (§5.2 steps 1–5) in `design.md`/`tasks.md` to bound blast radius.
