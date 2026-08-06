## Why

A permanently failing activation is the one element an on-call engineer most wants a trace for, and it is the one element `add-trace-events` cannot show. Transient failures self-heal — Beam retries the bundle, the retry walks the same path through the replay cache, and its committed trace reveals everything the failed attempt did. But a poison pill (an agent bug triggered by specific input, a malformed provider response at step 3) never commits, so its trace is forever one synthesized `ERROR` event carrying `beam_agents.reason` and `error.type`, and its `.errors` dead letter carries `repr(exc)`. Triage has to reconstruct "it made two model calls and died staging the third intent" from nothing.

Recovering the failed attempt's *staged events* is deliberately off the table: `project.md`'s correctness invariant 1 lists traces among the staged effects applied only on success, and the timeout path's context lives on a cancelled coroutine where nothing can be recovered at all. But there is a cheap middle ground that `add-trace-events`'s D5 left unexplored: the failure can carry **metadata about where it happened** — the step cursor, the last event kind staged, the counts — without emitting a single staged effect. All of it is a pure function of the deterministic activation path, so it costs no invariant and no replay guarantee.

## What Changes

- **`run_activation` wraps agent failures with failure-position context.** A new `ActivationFailed` exception (raised `from` the original, which stays attached as the cause) carries a small frozen `FailureContext`: the intent-step cursor at failure, the kind of the last staged trace event, the count of staged intents, and the count of provider-reached model calls. Only `Exception` is wrapped — `CancelledError` and other `BaseException`s pass through untouched, so the bridge's cancellation semantics are unchanged.
- **The DoFn's `activation_error` route surfaces that context in both records.** The synthesized `ERROR` trace event gains `beam_agents.failure.step`, `beam_agents.failure.last_event`, `beam_agents.failure.staged_intents`, and `beam_agents.failure.llm_calls`; the `.errors` dead-letter detail becomes `repr(cause)` plus a compact ` failed_at_step=N after=<EVENT>` suffix, so triage reads the failure position without opening a trace viewer.
- **The timeout route is explicitly unchanged.** `ActivationTimeout` is raised on the Beam thread while the coroutine may still be running; its context is unreachable by construction, and per the trace vocabulary's standing rule, unknown attributes are **absent, not defaulted** — a timeout's `ERROR` event simply has no `beam_agents.failure.*` keys.
- **Staged effects stay discarded.** No staged trace, intent, blob, or output of a failed activation is emitted — this change exports *scalars about* the rolled-back context, never its contents. The determinism property extends to the new fields: a replayed bundle that fails the same way synthesizes a byte-identical enriched `ERROR` event.

## Capabilities

### New Capabilities
<!-- None. -->

### Modified Capabilities
- `trace-events`: the "Failure routes emit ERROR trace events" requirement is amended — the `activation_error` route's synthesized event MAY carry failure-position metadata derived from the failed context (cursor, counts, last event kind) while still never emitting the staged events themselves; the dead-letter detail format for `activation_error` gains the failure-position suffix. **Sequencing:** `trace-events` is itself an unarchived change (`add-trace-events`); this change's delta applies on top of it and archives after it.

## Impact

- **Modified code:** `core/loop.py` (`ActivationFailed`, `FailureContext`, the wrap site around the agent invocation), `core/dofn.py` (`_start`/`_resume` catch `ActivationFailed` ahead of the generic `Exception` fallback, thread the context into `_error_trace` and the dead-letter detail), `observability/traces.py` (the four `beam_agents.failure.*` attribute names, `ActivationTrace.error` accepting them).
- **No wire/state change.** `TraceEvent.attributes` is already a string map and `ActivationError` is a Python dataclass; no proto edit, no `state_schema_version` implication, no new configuration knob.
- **Behavior change:** the `activation_error` dead-letter `detail` string gains a suffix (consumers parsing `detail == repr(exc)` exactly will see the new tail); the `ERROR` trace event for agent raises gains four attributes. Timeout, orphaned-result, HITL-timeout, and TTL-wipe routes are byte-for-byte unchanged.
- **Invariant statement update, not weakening:** D5's "synthesized from the DoFn's own knowledge, never from the rolled-back context" becomes "never from the rolled-back context's *staged effects*" — reading scalar metadata off the failed context is not applying its effects, and the tests pin that the staged traces themselves still never escape.
- **Tests:** loop-level tests for the wrap (cause preserved, `CancelledError` untouched, context fields exact); fake-handle DoFn tests (inside the mutation selection) for both records on the raise route, absence on the timeout route, byte-identical synthesis under replay, and untouched state; mutation/coverage baselines re-checked (`loop.py` and `dofn.py` are under the gate).
