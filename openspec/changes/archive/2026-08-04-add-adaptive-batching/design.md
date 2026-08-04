## Context

The stateful runtime activates the agent once per `AgentEnvelope`. The topology in [dofn.py](../../../src/beam_agents/core/dofn.py:199) is five state specs (`MEMORY`, `CONTINUATION`, `LLM_CACHE`, `PENDING`, `SEQ`) and two timers (`TTL_TIMER` WATERMARK, `HITL_TIMER` REAL_TIME); [project.md:53](../../project.md) reserves a third timer, `FLUSH_TIMER` (REAL_TIME — adaptive batching only), which `add-stateful-dofn-runtime` deliberately deferred. This change is that deferred slot: per-key adaptive batching that turns an event burst into one activation over a list of events.

Everything here must ride the existing correctness machinery, not fork it. A flush is *one activation*: it runs through `run_activation` ([loop.py:149](../../../src/beam_agents/core/loop.py:149)), stages effects in the `ActivationContext`, commits through the fixed-order tail ([dofn.py:514](../../../src/beam_agents/core/dofn.py:514)), consumes one `SEQ`, and obeys atomic commit, deterministic intent IDs, and the replay cache exactly as a per-event activation does. The genuinely new problems are (a) when to flush, (b) what the agent sees, and (c) what suspension means when the suspended activation was fed twenty events instead of one. The roadmap directs (c): whole-batch suspension.

**Constraints (load-bearing):**

- Beam serializes elements and timer callbacks per key; the buffer needs no locking, but a flush and a live suspension share the single-value `CONTINUATION` spec — a flush that suspends while another suspension is live would overwrite it.
- Timer/watermark behavior is tested with `TestStream` scripted advances, never `sleep()`.
- State is protobuf, never pickle; blobs ≤ 100 KiB, working state soft-capped at 1 MiB. The buffer must be bounded.
- The latency budget (runtime overhead p50 < 15 ms) applies to the flush activation itself; buffering deliberately trades *event-to-decision* latency for cost, bounded by `max_wait_ms`.

## Goals / Non-Goals

**Goals:**
- An opt-in `BatchPolicy.ADAPTIVE` on `AgentConfig`, with `BatchPolicy.NONE` the default and a byte-for-byte no-op relative to today.
- Per-key buffering in a `BATCH` bag state; flush on size threshold (`max_batch_size`) or elapsed processing time (`max_wait_ms`) via `FLUSH_TIMER`.
- Batch-visible activations: `ctx.event` is a `list[bytes]` under `ADAPTIVE`, with explicit detection (`ctx.is_batch`) and stable typing per policy.
- Defined suspension semantics: the whole batch suspends and resumes as one continuation, and flushes defer while a continuation is live.
- Fail-closed failure, overflow, and TTL-GC paths for buffered events — nothing lost silently, no unbounded state, no wedged keys.
- Replay determinism: a retried flush bundle reproduces byte-identical intents and zero extra provider calls.

**Non-Goals:**
- Cross-key batching or global micro-batching (violates per-key serialization; the runner owns cross-key parallelism).
- Batching of `tool_result`/`approval` elements (they answer a specific suspension; delaying them only burns the HITL deadline).
- Event-time/windowing-based batching semantics (`max_wait` is a processing-time freshness bound, not a windowing feature).
- Partial-batch suspension or per-element continuations (explicitly rejected in D5).
- Splitting an oversized flush into multiple activations (the whole buffer is one batch; see D4/D7).
- Any change to agent authoring surfaces beyond the `ctx.event`/`ctx.events`/`ctx.is_batch` typing (no DSL, no batching combinators).

## Decisions

### D1. `BatchPolicy` is an enum on `AgentConfig`, and `NONE` means zero behavioral diff

`BatchPolicy` (`NONE` | `ADAPTIVE`) lives in a new `core/batching.py` with the pure flush-decision helpers, and `AgentConfig` ([transform.py:395](../../../src/beam_agents/core/transform.py:395)) gains `batch_policy: BatchPolicy = BatchPolicy.NONE`, `max_batch_size: int`, `max_wait_ms: int`, and `max_buffered_events: int` (default `4 * max_batch_size`). `__post_init__` validates: under `ADAPTIVE` all three bounds must be positive and `max_buffered_events >= max_batch_size`; under `NONE`, explicitly setting a batch knob raises `ValueError` at the construction site — a knob that silently does nothing is a misconfiguration trap. Under `NONE` the DoFn never reads `BATCH`, never arms `FLUSH_TIMER`, and the element path is the existing code path unchanged; the conformance matrix and every existing test run under `NONE` and must not move.

*Alternative considered:* a separate `RunAgentBatched` transform. Rejected — it forks the DoFn (two copies of every invariant) and breaks the "one transform, config-driven" public surface (`RunAgent`, `AgentConfig` are the API).

### D2. Only `external_event` elements buffer; resumes never do

Routing by envelope kind gains one branch: under `ADAPTIVE`, an `event` element appends to `BATCH` and commits only the buffering effects (bag add, `TTL_TIMER` re-arm, `FLUSH_TIMER` arm on the empty→non-empty transition). No activation runs, so no `SEQ` increment, no `activations` count, no trace. `tool_result`/`approval` elements keep their existing path untouched: they resume a specific suspended continuation, and buffering them would spend the suspension's own `deadline_ms` doing nothing — a self-inflicted HITL timeout.

### D3. `BATCH` is a `BagStateSpec` of `AgentEnvelope`; `FLUSH_TIMER` arms on first-in, from an injected wall clock

The buffer is `BagStateSpec("batch", DeterministicProtoCoder(AgentEnvelope))` — append-only per element, read in arrival order at flush, cleared atomically with the flush commit. Storing the whole envelope (not just payload bytes) keeps each event's `event_time_ms` for the batch clock (D4) and costs nothing new in schema: no proto change, no `state_schema_version` bump.

`FLUSH_TIMER` is armed only when the bag transitions empty→non-empty, at `wall_now + max_wait_ms`, so `max_wait` measures from the *first* buffered event — re-arming per element would let a steady trickle starve the flush forever. The arming site reads a wall clock (`time_fn`, an injected seam defaulting to `time.time`, like `monotonic_ns` in [context.py:74](../../../src/beam_agents/core/context.py:74)): this is the first wall-clock read on the element path, and it is safe for the same reason the monotonic clock is — the reading decides only *when the timer fires*, never any staged effect, intent ID, cache key, or output byte. Replay determinism is scoped to bundle retry: the batch composition a retried bundle sees is fixed by committed bag state plus the bundle's own replayed elements (bag reads are read-your-writes within a bundle), so the same flush re-runs over the same batch. A size flush clears the pending `FLUSH_TIMER`; a timer that fires over an empty buffer (cleared mark delivered anyway) is a guarded no-op, mirroring the stale-handle guard in `on_hitl`.

*Alternative considered:* arming from the element's `event_time_ms` (fully deterministic). Rejected — during backlog replay event time lags wall time by hours, the mark lands in the past, and every element flushes a batch of one: the roadmap's requirement is `max_wait` honored in *processing* time, which is precisely what makes batching keep working during catch-up.

### D4. A flush is one activation over the whole buffer; `ctx.event` is a `list[bytes]`; the batch clock is `max(event_time_ms)`

Flush (inline on size, or in the `FLUSH_TIMER` callback) reads the full bag in order, runs one activation with `events=[e.external_event for e in batch]`, and stages a bag `clear()` with the commit. `ActivationContext.event` widens to `bytes | list[bytes]`: a `list[bytes]` on every `ADAPTIVE` flush — including a batch of one, so the agent-visible type is a function of the configured policy, not of runtime batch size, and an `ADAPTIVE` agent is written against exactly one shape. `ctx.is_batch` and a uniform `ctx.events` tuple accessor (singleton under `NONE`, empty on resume) make detection explicit for adapters. Under `NONE`, `ctx.event` remains `bytes`.

The activation clock `now_ms` is `max(event_time_ms)` over the batch — a pure function of buffer contents (deterministic under retry, unlike the timer's firing time on first arming), and the latest event is the freshest ground truth the batch contains. Intent `expires_at_ms`, suspension deadlines, and trace timestamps all derive from it exactly as on the per-event path.

The whole buffer flushes as one batch even when deferral (D6) has grown it past `max_batch_size`: `max_batch_size` is a *trigger threshold*, not a hard batch cap. Splitting would mint multiple activations from one trigger, multiplying `SEQ` consumption and forcing an ordering story between the fragments for no benefit the cap in D7 doesn't already provide.

### D5. Whole-batch suspension: one continuation, one resume, for the entire batch

**The roadmap-directed decision.** When a batch activation returns `Suspend`, the runtime persists exactly one `Continuation` at the batch's `seq` — the batch suspended *as a unit*. The resume (`tool_result`/`approval` matching a pending intent) runs once, at that `seq`, seeded from the continuation's `step_index` and `snapshot`, and its outcome (complete or re-suspend) applies to the batch as a unit. `ctx.event`/`ctx.events` are empty on resume, exactly as on the per-event path today: `Continuation` does not persist the batched events, and the agent's `snapshot` carries whatever the resume needs.

*Alternatives considered:*
- **Per-element suspension fan-out** — split the batch at suspend time into N continuations, resume each independently. Rejected: `CONTINUATION` is a single-value spec by design (Beam Python has no MapState); N continuations per key means a repeated-field blob with its own admission, deadline, escalation, and TTL story per entry — a second HITL implementation. It also breaks the intent model: the batch activation's intents were minted at one `(key, seq)`; there is no per-element attribution to split them by.
- **Partial resume** — resume the batch but re-buffer "unprocessed" elements. Rejected: the runtime cannot know which elements the agent "consumed"; only the agent's snapshot can, and inventing a runtime-level consumed-set protocol is an orchestration DSL (a framework feature, constitutionally rejected).
- **Persisting the batch events in the `Continuation`** so resume re-presents the list. Rejected: it is a proto change the per-event path never needed, it double-stores payloads against the 100 KiB blob cap, and it would make resume semantics diverge between policies. The snapshot-owns-resume-state rule already covers it uniformly.

Failure of the whole-batch resume is governed by the existing rules unchanged: HITL timeout fails closed for the entire batch (one fallback route, one continuation cleared), and a late result is one `orphaned_result`.

### D6. Flushes defer while a continuation is live; resolution re-arms `FLUSH_TIMER` to fire promptly

If a flush could run while a prior suspension is live, a flush that itself suspends would overwrite the live `Continuation` and orphan its pending intents. So the interaction rule is: **a live continuation suppresses both flush triggers.** In `process()`, reaching `max_batch_size` with a live continuation buffers without flushing; in the `FLUSH_TIMER` callback, a live continuation defers rather than flushing (the callback leaves the buffer intact). When the suspension resolves — a resume activation commits, or `on_hitl`'s `Deny`/`Drop` route ends the wait ([dofn.py:666](../../../src/beam_agents/core/dofn.py:666)); `Escalate` keeps the suspension live and keeps deferring — the resolving path re-arms `FLUSH_TIMER` to fire promptly (at the resolution's wall clock) whenever `BATCH` is non-empty. Routing the deferred flush through the timer keeps the invariant of one activation per `process()`/timer call: the resume element's call runs the resume activation only, and the deferred batch flushes in its own callback with its own commit, its own `SEQ`, and its own timeout budget.

*Alternative considered:* run the resume activation and then the deferred flush inside the same `process()` call. Rejected — two activations under one `activation_timeout` budget, two `SEQ` increments in one commit tail, and a partial-failure matrix (resume committed, flush failed?) that atomic commit is designed to make unrepresentable.

### D7. Batch failure dead-letters every buffered event and clears the buffer; overflow dead-letters explicitly

A failed or timed-out flush activation commits nothing (atomic commit), but unlike the per-event path the inputs live in state, so "commit nothing" alone would retry the same poison batch on the next trigger forever. The failure route therefore mirrors the per-event contract at batch granularity: emit one `ActivationError` per buffered envelope (existing reasons `activation_error`/`activation_timeout`, detail carrying `batch_size` and the flush trigger), clear `BATCH`, clear `FLUSH_TIMER`, leave `SEQ` and every other spec untouched. This is a deliberate, explicit state mutation on a failure path — the same class of fail-closed cleanup `on_hitl` already performs when it clears a dangling continuation — not a violation of the staged-commit rule, which governs the activation's own effects.

Deferral (D6) makes the buffer grow while a suspension is live. The wait is time-bounded (fail-closed HITL guarantees every suspension ends by its `deadline_ms`), but a hot key can still buffer a lot in that window, so `max_buffered_events` is a hard cap: once reached, further `event` elements dead-letter to `.errors` as `batch_buffer_overflow` instead of appending. Dropping is explicit, counted, and triage-able on the errors sink; growing keyed state silently toward the 1 MiB cap is none of those.

### D8. TTL GC wipes the buffer with everything else, and reports what it wiped

`TTL_TIMER`'s wipe extends to all six specs. Buffered elements re-arm `TTL_TIMER` like every processed element, and `max_wait_ms` (milliseconds–seconds) is orders of magnitude inside `ttl_ms` (default 1 h), so a TTL fire over a non-empty buffer means a stalled pipeline or a backlog watermark jump — the same clock-skew corner as `ttl_wiped_suspension`. The callback dead-letters one record per wiped buffered envelope (reason `ttl_wiped_batch`) before the unconditional wipe, so the loss is observable, then clears everything including `FLUSH_TIMER`.

### D9. Batches are first-class in traces and metrics, invisible when `NONE`

One flush = one `ActivationTrace` at `(key, seq)`, exactly like any activation; the flush stamps `beam_agents.batch.size` and `beam_agents.batch.trigger` (`size` | `timer`) attributes so a trace consumer can tell a batch decision from a per-event one. Buffered (non-flush) elements emit no trace — they are not activations. Metrics extend the `beam_agents.runtime` surface by `events_buffered` (counter, one per buffered envelope), `batch_flushes_size` / `batch_flushes_timer` (counters, one per *committed* flush by trigger), and `batch_size` (distribution, sampled per committed flush). All recording stays on the Beam thread via the existing staged-tally path; overflow and wipe dead-letters flow through the `_dead_letter` chokepoint, so `agent_errors + orphaned_results == len(.errors)` keeps holding by construction. Under `NONE` all four read zero and no dashboard moves.

## Risks / Trade-offs

- **[Watermark vs. buffered elements: timer-flush outputs can be late]** The watermark advances past a buffered element's timestamp while it sits in the bag, so outputs emitted from the `FLUSH_TIMER` callback may be droppably late downstream of windowed consumers. Beam Python timers do not expose an output-timestamp/watermark-hold parameter the way Java's `TimerSpec.withOutputTimestamp` does. Mitigation: `max_wait_ms` is small by design; outputs carry the batch clock (`max(event_time_ms)`); the risk is documented in `docs/` and the DirectRunner/Flink conformance legs assert flush outputs are observed. Revisit if the SDK grows timer output timestamps (Open Question).
- **[Buffer state growth on hot keys]** Bounded three ways: `max_batch_size` flushes eagerly when not suspended, `max_buffered_events` hard-caps deferral growth with explicit overflow dead-letters (D7), and TTL GC is the backstop (D8). The spec pins each bound with a scenario.
- **[Poison batch wedges a key]** Ruled out by D7: the failure route consumes the buffer. The cost is that one bad event dead-letters its whole batch — acceptable because `.errors` records are per-envelope, so triage and replay remain element-granular.
- **[Suspension overwrite through a mis-ordered flush]** The entire hazard class is closed structurally by D6 (a live continuation suppresses flushing); the spec makes it a scenario, and the mutation gate covers the guard.
- **[A second wall-clock read erodes replay determinism]** The `time_fn` reading arms a timer and nothing else; the retry-determinism semantics gate is extended with a chaos-retried flush bundle asserting byte-identical intents and zero extra provider calls, pinning "arming time is not data".
- **[`ctx.event` union type leaks complexity to `NONE` users]** It does not: under `NONE` the runtime always passes `bytes`, existing agents see no change, and `ADAPTIVE` agents see only `list[bytes]`. The union exists in the annotation, not in any one pipeline's behavior; `ctx.events` gives adapters a single shape when they want one.

## Migration Plan

Purely additive and opt-in. `BatchPolicy.NONE` is the default and is specified (with a scenario) to preserve current semantics byte-for-byte; no existing pipeline, test, or conformance cell changes behavior. The new `BATCH` spec and `FLUSH_TIMER` are declared but untouched under `NONE`, and declaring them is `--update`-compatible (new state IDs and timer families start empty; no existing blob changes shape — no `state_schema_version` bump). Enabling `ADAPTIVE` on an existing pipeline is a config change reviewed like any semantic change. Rollback = revert; under `NONE` there is no batch state to migrate, and an `ADAPTIVE` pipeline being rolled back drains naturally (flush-on-size/timer) or dead-letters via TTL GC in the worst case.

## Open Questions

- Should the resolved-suspension re-arm (D6) fire literally immediately (`wall_now`) or at `wall_now + small fixed delay` to coalesce a resume burst? Proposal: immediately; revisit only with benchmark evidence.
- Beam Python timers currently lack an output-timestamp (watermark-hold) knob; if the SDK gains one, `FLUSH_TIMER` should hold the watermark at the earliest buffered `event_time_ms`. Tracked as a follow-up, not blocking.
- Whether `batch_flushes_*` should also count *failed* flush attempts (currently: committed only, so the counters reconcile with `SEQ`/`activations`; failures are visible via `agent_errors`). Decide in review; the spec states committed-only.
