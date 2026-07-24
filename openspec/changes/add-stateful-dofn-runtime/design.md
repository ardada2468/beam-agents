## Context

The project already ships the pieces the runtime consumes: the `AgentEnvelope` keyed input and `ToolIntent`/`Continuation`/`MemoryBlob`/`LlmCacheBlob` protos (`wire-schemas`), the deterministic proto coder (`proto-coders`), and the in-memory staging facades (`memory-facade`, `llm-replay-cache`). None of them touch Beam state. This change adds `core/dofn.py`, the keyed stateful `_AgentDoFn` that reads those facades from Beam-managed state, drives one agent activation per element, and commits every effect atomically with the bundle.

This is the load-bearing correctness layer. Every invariant in `openspec/project.md` — atomic commit, deterministic intent IDs, replay cache with zero extra provider calls on retry, per-key serialization, fail-closed timeouts, protobuf-only state — is either enforced here or trivially violable here. Because the blast radius is the whole runtime, the work is decomposed into a fixed five-step activation lifecycle (§5.2) and tasks are cut per step.

**Constraints (load-bearing):**
- Python Beam SDK has **no `MapState`**: bounded maps live inside single-value proto blobs with explicit LRU eviction. All five state specs are `ReadModifyWriteState`/`BagState`/`CombiningValueState`.
- State is **protobuf, never pickle**. `--update` compatibility requires additive proto changes only.
- Beam stateful DoFns process **one element at a time per key**; cross-key parallelism is the runner's job. No cross-key shared mutable state.
- Beam is synchronous; the LLM client is async. The bridge must not leak event-loop lifetime across `process()` calls or across bundles.

## Goals / Non-Goals

**Goals:**
- Define the `_AgentDoFn` state/timer topology and wire it into `RunAgent` through the deterministic coder.
- Route each `AgentEnvelope` by payload variant (kind) to the correct activation path.
- Run one activation on a background asyncio loop, bounded by `activation_timeout`, cancelling cleanly on timeout with **zero** state mutation.
- Stage all effects in an activation context and commit them to Beam state in a fixed, atomic order; increment `SEQ` exactly once per committed activation.
- Implement `TTL_TIMER` (watermark) memory GC that clears all state and re-arms per element, and `HITL_TIMER` (real-time) fail-closed fallback.
- Preserve per-key ordering under interleaved event/result/approval streams.

**Non-Goals:**
- `FLUSH_TIMER` / adaptive batching (deferred).
- Effector service, outbox sinks, long-term MemoryStore upserts (separate changes).
- Framework adapters (LangGraph/ADK/pydantic_ai) beyond the plain async-agent protocol needed to drive the loop.
- Provider client internals, retry/backoff policy, and the replay-cache/memory facade internals (consumed as-is).
- Changing any dependency spec's requirements.

## Decisions

### D1. Single keyed input type: `AgentEnvelope`, routed by variant
`RunAgent` upstream normalizes events, tool-results, and approvals onto one keyed `AgentEnvelope` with exactly one payload variant set (`wire-schemas` guarantees this). `_AgentDoFn.process()` dispatches on the variant — **element routing by kind**:
- `event` → start a fresh activation, or continue if a `Continuation` for a matching correlation is live.
- `tool_result` / `approval` → rehydrate the persisted `Continuation` and resume; if none matches (`intent_id` unknown or expired), emit `orphaned_result` on `.errors` and mutate nothing.

*Alternative considered:* three separate inputs / a tagged `DoFn` per kind. Rejected — multiplies coder wiring and splits per-key state ordering across DoFns, breaking the single-serialization guarantee.

### D2. One async bridge thread per DoFn instance, owned by `setup()`/`teardown()`
`setup()` starts one daemon thread running a dedicated `asyncio` event loop with shared `httpx` pools; `teardown()` stops the loop and joins. `process()` submits the activation coroutine via `run_coroutine_threadsafe` and blocks on the returned future up to `activation_timeout`. On timeout, cancel the future/coroutine, drain the cancellation, route the element to `.errors`, and return **without staging or committing**.

*Alternative considered:* a fresh loop per `process()` (`asyncio.run`). Rejected — destroys httpx connection pools every element and serializes TLS handshakes; the workload is system-triggered, not sub-second, but per-element loop churn is still wasteful and brittle under bundle retries.

### D3. Effects are staged, never applied inline; commit is a fixed-order tail step
The activation context holds a `MemoryFacade`, an `LlmReplayCache` facade (both seeded from the current state blobs), a pending-intent list, staged outputs/traces, and a `next_seq`. The agent loop only mutates the context. **Commit ordering** (only on success): (1) write `MEMORY`, (2) write `LLM_CACHE`, (3) write `CONTINUATION` (set on suspend, clear on completion), (4) append `PENDING` intents, (5) increment `SEQ` by 1, (6) arm/re-arm `TTL_TIMER` and (if suspending) `HITL_TIMER`, (7) emit `.output`/`.intents`/`.traces`. A failed or timed-out activation discards the context untouched — the atomic-commit invariant.

*Alternative considered:* apply-as-you-go with a rollback log. Rejected — Beam has no transactional rollback for already-written state within a `process()`; staging is the only way to get all-or-nothing.

### D4. `SEQ` is `CombiningValueState(sum)`, incremented exactly once at commit
`SEQ` is the monotonic activation counter feeding deterministic `intent_id = uuid5(NS, key + seq + step_index)`. It is read at activation start (to seed `intent_id`s) and incremented by exactly `1` in the commit tail — never inside the loop, never on the timeout/error path. A replayed bundle that re-runs the same activation reads the same `seq` and produces byte-identical intents; the effector dedups. Timer-only firings (TTL/HITL) do **not** increment `SEQ`.

### D5. `TTL_TIMER` in the WATERMARK domain; total wipe on fire; re-arm every element
Working memory is event-time garbage. Every processed element sets `TTL_TIMER` to `now + ttl` (watermark), superseding any prior mark. On fire, clear **all five** state specs (`MEMORY`, `CONTINUATION`, `LLM_CACHE`, `PENDING`, `SEQ`) so an idle key leaves zero residue; the next element for that key starts from a versioned-empty blob and `seq = 0`. `HITL_TIMER` lives in the REAL_TIME domain because approval SLAs are wall-clock; on fire it runs the fallback path and clears the dangling `Continuation`.

### D6. Per-key ordering is inherited from Beam, protected by not sharing state across keys
Beam serializes elements per key; the DoFn adds no cross-key state and no background mutation of committed state outside the async bridge (which is joined per element before commit). Interleaved event/result/approval elements for one key therefore commit in arrival order; the async bridge never observes two activations for the same key concurrently.

## Risks / Trade-offs

- **[Async cancellation leaks state or connections]** → Commit is gated strictly behind future success; the timeout path stages nothing and only routes to `.errors`. `teardown()` joins the loop thread; tests assert no state mutation after a forced timeout (`TestPipeline`).
- **[Double `SEQ` increment or increment on a non-committed path]** → `SEQ += 1` lives only in the commit tail, unit- and pipeline-tested for exactly-once per committed activation and zero on timeout/timer paths.
- **[TTL fires mid-flight / races a new element]** → Per-key serialization means a timer callback and an element cannot run concurrently for the same key; the TTL callback is a pure state-clear with no emit. Re-arm on every element keeps live keys from being GC'd.
- **[Bundle retry causes duplicate provider calls]** → The `LLM_CACHE` facade is seeded from state at activation start and committed at the tail; a retried bundle hits the cache. Verified by asserting zero additional provider calls on replay.
- **[Blob growth blows the 100 KiB/1 MiB caps]** → Delegated to the memory/replay-cache facades (already cap-enforcing); the DoFn only persists their serialized blobs and surfaces `MemoryOverflow` to `.errors`.
- **[`--update` incompatibility]** → State is proto only; no pickled Python in any spec. Additive-only changes; golden-blob compat is a `wire-schemas` concern reused here.

## Migration Plan

New capability, no existing runtime to migrate. Rollout is additive: `RunAgent` gains the coder-wired `_AgentDoFn`. Rollback = revert the change; no state schema exists in production yet, so no state migration. Future pipeline `--update` is governed by the additive-proto rule (D3/§project.md invariant 7).

## Open Questions

- ~~Default value for `activation_timeout` and whether it is per-agent configurable.~~ **Resolved:** transform-level constant (`30.0s`) with a per-construction override on `RunAgent`/`_AgentDoFn` (`activation_timeout_s=`). TTL default is `3_600_000ms`, also overridable.
- ~~Whether `HITL_TIMER` fallback emits a synthetic `tool_result` or a distinct record.~~ **Resolved:** the fallback emits the fixed `HITL_TIMEOUT_OUTPUT` marker on `.output`, clears the `Continuation` (and `PENDING`), and leaves `SEQ` unchanged; a late real result is then `orphaned_result` on `.errors`.
- Correlation between an `event` arriving while a `Continuation` is live (concurrent trigger vs. queued). **Current behavior:** an `event` always starts a fresh activation at the current `SEQ`; the live continuation stays until its result/approval arrives or `HITL_TIMER` fires. Revisit if adapters need queuing semantics.
- Model calls currently go through a thin cache-first seam on the `ReplayCache` facade (miss → provider → cache insert); layering the richer `LlmFacade` (breaker/retry/decode/trace) onto this path is deferred to a follow-up, as it is an independent capability.
