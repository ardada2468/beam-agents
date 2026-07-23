## Context

The repository already provides deterministic protobuf coders, staged working-memory and replay-cache facades, and wire messages for envelopes, continuations, intents, results, and approvals. The missing runtime boundary is the Beam stateful `DoFn` that loads those values per key, executes one agent activation, and turns the activation's staged changes into Beam state writes and tagged outputs.

Beam Python stateful `DoFn`s are synchronous and require KV input. Agent and model APIs are async, while Beam offers neither a portable async `DoFn` nor `MapState`. The implementation must therefore bridge synchronous `process()` calls to one worker-local event loop, persist bounded maps as protobuf blobs, preserve per-key serialization, and rely on Beam's bundle transaction for atomic state/output commit.

## Goals / Non-Goals

**Goals:**

- Define the five keyed state specs and two timers required for durable activation, suspension, replay, and cleanup.
- Route external events, tool results, approvals, and timer callbacks to the correct activation path.
- Run async activations with bounded blocking time and cancellation while keeping all activation effects staged.
- Commit successful activations deterministically and leave state untouched after activation failure or timeout.
- Preserve a continuation's sequence across reinjected results while allocating monotonically increasing sequences to new external activations.
- Emit routing and execution failures as a typed, deterministically coded dead-letter record.
- Make timer, retry, timeout, and orphan behavior testable without wall-clock sleeps.

**Non-Goals:**

- Public `RunAgent` transform construction or configuration API.
- Agent-authoring, prompt, or orchestration abstractions.
- Side-effect execution inside the Beam worker.
- Adaptive batching and its optional `FLUSH_TIMER`.
- Wire changes beyond the additive `RuntimeError` message required by the dead-letter output.
- Cross-key coordination or mutable process-global keyed state.

## Decisions

### D1: Keep all durable data in five explicitly coded Beam state specs

`_AgentDoFn` declares:

- `MEMORY`: `ReadModifyWriteStateSpec` containing `MemoryBlob`.
- `CONTINUATION`: `ReadModifyWriteStateSpec` containing `Continuation`.
- `LLM_CACHE`: `ReadModifyWriteStateSpec` containing `LlmCacheBlob`.
- `PENDING`: `BagStateSpec` containing `ToolIntent`.
- `SEQ`: `CombiningValueStateSpec` using integer sum.

Every protobuf state spec uses `DeterministicProtoCoder`; `SEQ` uses Beam's deterministic integer coder and sum combine function. No state value uses pickle. `PENDING` is rebuilt by `clear()` followed by deterministic `intent_id` ordering when a successful activation changes it, because BagState does not support indexed removal.

This layout matches the runtime's distinct durability and access needs. A single aggregate state blob was rejected because every small update would rewrite unrelated data and would make independent bounds and compatibility harder to enforce. Simulated map state was rejected because Beam Python does not expose portable `MapState`.

### D2: Load state into an activation-scoped staging context

For each routed activation, the `DoFn` reads state once and constructs an activation context containing `Memory`, `ReplayCache`, the current continuation and pending intents, plus staged tagged outputs, timer changes, and usage/trace effects. The async driver can mutate only this context; it receives no Beam state handles.

The context freezes activation time and sequence. A new external event increments the loaded `SEQ` value in staging and uses the new value. A matching result or approval resumes the persisted continuation with its existing `seq`; it does not allocate another sequence. A missing, mismatched, or expired correlation is routed as a typed orphan error without invoking the agent.

Direct Beam state mutation from async code was rejected because cancellation could race with state writes and break the no-mutation-on-timeout invariant.

### D3: Route by the protobuf payload discriminator before execution

`process()` validates that the KV key equals `AgentEnvelope.entity_key`, then uses `AgentEnvelope.WhichOneof("payload")`:

- `external_event` starts a new activation only when no continuation is awaiting reinjection.
- `tool_result` must match both a pending `intent_id` and the persisted continuation.
- `approval` follows the same correlation rule and supplies the decision to the resume path.
- An absent or unknown discriminator produces a typed invalid-envelope error.

An external event arriving while a continuation is pending is failed closed to the errors output rather than overwriting suspended work. Buffering arbitrary events in keyed state was rejected because it introduces an unbounded queue not present in the state model.

### D4: Use one asyncio bridge thread per `DoFn` instance

`setup()` starts a daemon thread with a dedicated asyncio event loop and initializes async worker-local resources on that loop. `process()` submits the activation coroutine with `asyncio.run_coroutine_threadsafe()` and waits on the returned future for `activation_timeout`.

On timeout, `process()` cancels the future, observes cancellation completion up to a small bounded grace interval, discards the entire activation context, and emits one typed timeout error. `teardown()` cancels remaining futures, closes loop-owned async resources, stops the loop, and joins the thread.

Calling `asyncio.run()` per element was rejected because it destroys connection pooling and repeatedly creates loops. A shared module-global loop was rejected because its lifecycle and mutable resources would cross `DoFn` instances.

### D5: Commit only a completed activation context

The coroutine returns either a completed activation context or raises. `process()` performs no durable mutation before that return. On success it validates state-size limits and output invariants, then applies staged changes in this stable order:

1. write or clear `MEMORY`;
2. write or clear `LLM_CACHE`;
3. write or clear `CONTINUATION`;
4. clear and repopulate `PENDING` in deterministic order;
5. add the staged delta to `SEQ`;
6. set or clear `TTL_TIMER` and `HITL_TIMER`;
7. yield tagged intents, outputs, traces, and runtime errors in their staged order.

The order is for deterministic behavior and reviewability; Beam still commits state mutations, timer changes, and emitted records atomically with the bundle. If validation, execution, cancellation, or commit preparation fails, staged state and staged outputs are discarded. The `DoFn` emits only a typed runtime error constructed outside the failed activation context.

Incremental state writes during execution were rejected because an exception after the first write would expose a partial activation. Emitting staged outputs before commit preparation was rejected because a generator exception could make behavior dependent on runner buffering details.

### D6: Derive timers from durable state

`TTL_TIMER` uses watermark time. A successful activation that leaves working memory schedules it at the activation event time plus the configured memory TTL. Its callback clears expired memory and reschedules to the next live deadline when needed; tests advance a `TestStream` watermark rather than sleeping.

`HITL_TIMER` uses real time and is set to the earliest deadline represented by the current continuation and pending intents. A result or approval that resolves the wait clears or advances the timer. When it fires, the runtime resumes the continuation through the configured timeout/fallback path. That timer activation uses the same staging and commit protocol as element-driven activation; fallback failure preserves no partial mutations and emits a typed error. Results arriving after timeout resolution are orphaned.

Per-intent timers were rejected because the Python state/timer API exposes one logical timer per spec and key. Polling deadlines from incoming traffic was rejected because idle keys would never time out.

### D7: Make injected seams explicit for deterministic tests

The `DoFn` receives an activation driver, bridge/thread factory where needed, activation timeout, cancellation grace, memory TTL, and clock conversion helpers through internal typed interfaces. Unit tests exercise routing and commit preparation with fakes; Beam `TestPipeline`/`TestStream` tests cover actual state and timer semantics. Retry tests use `FakeLLM` and forced bundle replay to assert zero extra provider calls on cache hits and byte-identical staged intents.

Mocking Beam internals as the only test strategy was rejected because it cannot prove runner-managed state/timer behavior.

### D8: Represent dead-letter output with an additive protobuf message

Add `RuntimeError` to `beam_agents.proto` with an `ErrorType` enum covering invalid envelope, busy key, orphaned result, activation timeout, activation failure, and timeout-handling failure. The message carries `entity_key`, `seq`, `intent_id` when applicable, a stable human-readable message, and `observed_at_ms`. It is exported from `_protos`, registered with `DeterministicProtoCoder`, and included in golden compatibility coverage.

Reusing `TraceEvent.ERROR` was rejected because traces are observability records rather than an actionable dead-letter contract. A Python exception or dataclass output was rejected because Beam could silently select pickle and the record would not be language-neutral.

## Risks / Trade-offs

- [A timed-out coroutine ignores cancellation and occupies the bridge loop] -> Keep all built-in awaits cancellation-safe, bound cancellation observation, track the future, and close outstanding tasks during teardown; never let it retain Beam state handles.
- [Python generator output timing obscures atomicity] -> Complete execution and commit preparation before the first yield, then rely on Beam's documented bundle transaction for state/output durability.
- [BagState iteration order differs by runner] -> Correlate by ID and canonicalize pending intents by `intent_id` whenever rebuilding state.
- [Watermark stalls delay TTL cleanup] -> Treat TTL as eventual event-time garbage collection; enforce hard memory bounds independently so stalled cleanup cannot create unbounded state.
- [Real-time timer behavior varies across local runners] -> Keep deadline decisions pure and separately unit tested, with portable runner integration tests where supported.
- [Cancellation races with provider I/O] -> Cancellation discards staging even if transport shutdown is delayed; provider calls remain replay-protected, and no external side-effect tool executes inline.
- [The new error schema becomes a public wire commitment] -> Keep the message minimal, additive, deterministically coded, and golden-blob guarded; do not hand-edit generated protobuf code.

## Migration Plan

This is a new internal runtime surface with no existing persisted `_AgentDoFn` state to migrate. Land tests and internal implementation first, then wire it into `RunAgent` in a later OpenSpec change. Rollback removes the unused internal `DoFn`; once a public transform persists these state IDs, their names and coder schemas become compatibility commitments.

## Open Questions

None for this change. Public configuration defaults and the exact adapter driver contract belong to the subsequent `RunAgent` integration change.
