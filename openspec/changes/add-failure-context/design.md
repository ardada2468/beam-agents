## Context

`add-trace-events` D5 settled how failures are traced: each DoFn failure route synthesizes a self-contained `ERROR` event from the key, seq, clock, and reason it already holds, and the failed activation's staged effects are discarded whole. That was the right trade against building partial-commit machinery, and it left one documented loss: an activation that calls the provider twice and dies staging its third intent leaves no record of any of it — *if it never commits*. For transient failures the loss self-heals (the Beam retry re-walks the same path through the replay cache and commits a full trace). For poison pills it never does, and the poison pill is precisely the element an operator is staring at.

The current raise path, end to end:

- `run_activation` documents "Raises whatever the agent raises (or a provider error); the caller commits nothing on failure" ([loop.py:101](src/beam_agents/core/loop.py)).
- `_AgentDoFn._start`/`_resume` catch `ActivationTimeout` then generic `Exception`, and yield `_dead_letter(key, REASON_ERROR, repr(exc))` plus `_error_trace(key, seq, now_ms, REASON_ERROR, error_type=type(exc).__name__)`.
- The context object — holding the step cursor, the staged traces, the tally — is unreachable from the DoFn on this path: it is local to the coroutine that raised.

## Goals / Non-Goals

**Goals:**
- The `activation_error` route's two records (dead letter, `ERROR` trace) name *where* the activation failed: step cursor, last staged event kind, staged-intent count, provider-reached call count.
- The enrichment is deterministic: a replayed bundle that fails identically produces a byte-identical enriched `ERROR` event, preserving trace dedup on content.
- Invariant 1 stays intact and *visibly* so: staged traces/intents/blobs/outputs of a failed activation still never escape, pinned by test.

**Non-Goals:**
- **Enriching the timeout route.** `ActivationTimeout` is raised by the bridge on the Beam thread when `future.result(timeout)` expires; the coroutine may still be running and its context is racy to read and unreachable to hand off. Unknown stays absent.
- **Emitting the failed attempt's staged events** (the D5 line, unmoved): that is partial commit, and it also cannot work on timeout.
- **A logging side channel.** Worker logs are runner-specific; the two records the runtime already owns are where triage looks first. A debug-log dump of discarded events can be its own later change if the enriched records prove insufficient.
- **Enriching the orphaned/HITL/TTL routes.** No activation ran; there is no position to report.

## Decisions

### D1. Carry the context out on a typed wrapper exception, raised `from` the cause

`run_activation` wraps the agent invocation (and everything after context construction) in a `try/except Exception` that raises `ActivationFailed(context=FailureContext(...))` **`from exc`**. The DoFn catches `ActivationFailed` ahead of its generic `Exception` fallback, reads `.context` and `.__cause__`, and builds both records from them. The generic fallback stays, catching anything raised outside the wrap window (context construction, bridge internals), with today's un-enriched shape.

Alternatives rejected:
- **Stashing attributes on the original exception** (`exc._beam_agents_ctx = ...`): invisible in signatures, breaks on exceptions with `__slots__`, and survives no re-raise boundary honestly.
- **An out-parameter or contextvar**: threads mutable state across the bridge for no gain; the exception already travels the exact path the data needs to travel.
- **Wrapping `BaseException`**: must not happen. `CancelledError` is how the bridge's timeout cancellation completes; swallowing or wrapping it would corrupt cancellation semantics. The wrap catches `Exception` only.

`ActivationFailed` is runtime-internal (`core/loop.py`), not exported from any package `__init__`: agents never see it (they are *inside* the wrap), and the DoFn consumes it immediately.

### D2. `FailureContext` is four scalars, all pure functions of the deterministic path

```
step_index      the intent-step cursor at failure (ctx.step_index)
last_event      EventType name of the last staged trace event, "" if none
staged_intents  len(ctx.staged_intents)
llm_calls       provider-reached calls so far (tally.llm_calls)
```

Every field derives from the activation's deterministic walk — cursor advances, staged lists, the tally's call count — never from a clock or the discarded payloads. That is what makes the enriched `ERROR` event byte-identical under replay, and it is the line between this change and partial commit: **counts and kinds about the rolled-back context, never its contents**. `last_event` is in practice at least `ACTIVATION_START` (staged before the agent runs); the empty-string case exists only for a failure inside context construction, which the generic fallback handles anyway.

Deliberately excluded: the last request's bytes, the staged intents' ids, memory keys touched. Each is contents, not position, and each grows the record unboundedly.

### D3. Surface the same four fields in both records, in each record's native shape

- `ERROR` trace attributes, in the established dotted-group style (`beam_agents.activation.*` precedent): `beam_agents.failure.step`, `beam_agents.failure.last_event`, `beam_agents.failure.staged_intents`, `beam_agents.failure.llm_calls`. Present only when known — the timeout route emits none of them, per the trace vocabulary's absent-means-unknown rule.
- Dead-letter `detail`: `f"{cause!r} failed_at_step={step} after={last_event}"`. The detail keeps leading with `repr(cause)` — the original exception, not the wrapper — so existing triage habits (and log greps for exception types) keep working; the suffix is additive text.

One source builds both (`FailureContext` methods or a small helper), so the two records cannot disagree.

### D4. The spec's D5 clause is reworded, not repealed

The `trace-events` requirement currently reads "synthesized from the key, seq, injected clock, and reason the DoFn already holds — never from the failed activation's discarded staged effects." The delta narrows the prohibition to what invariant 1 actually protects: the event **SHALL NOT carry the staged events, intents, outputs, or blobs themselves** and MAY carry failure-position metadata. Invariant 1 governs the *application of effects*; four scalars describing an execution position apply nothing. The pinning test stays: a failed activation's staged traces are not emitted, only the one synthesized `ERROR` event.

## Risks / Trade-offs

- **[Detail-string consumers]** Anything parsing `detail == repr(exc)` exactly on `activation_error` breaks on the new suffix. → The format is documented in the delta spec; the suffix is space-separated `key=value` tail after the unchanged `repr`, so prefix-matching consumers survive.
- **[The wrap window is not the whole `_activate` path]** Failures outside `run_activation`'s wrap (bridge submit, context construction) arrive un-enriched via the generic fallback. → Correct behavior, not a gap: there is no position to report before a context exists; the fallback keeps today's exact shape.
- **[`except ActivationFailed` ordering in the DoFn]** A future edit that reorders the generic `Exception` catch above it would silently drop the enrichment. → The fake-handle tests assert the enriched attributes on the raise route, so the reorder fails the suite (and the mutation gate covers the branch).
- **[mutation/coverage baselines]** `loop.py` and `dofn.py` are under the gate; the new branches are reachable from the selection (fake-handle suites), so they must be killed, not ceilinged. Baselines re-checked at the end as usual.

## Migration Plan

Single small change, no wire or state migration. Revert restores today's un-enriched records; nothing downstream can have depended on the new attributes' *absence*.

## Open Questions

- Should `beam_agents.failure.llm_calls` count cache hits too (calls the *agent* made) rather than provider-reached calls only? Provider-reached matches the metrics vocabulary (`llm_calls`) and the cost question; the agent-step story is already carried by `failure.step`. Current answer: provider-reached, for vocabulary consistency.
- Should the orphaned-result route carry the *continuation's* position (its `step_index`, pending count) the same way? It is available and deterministic, but it describes a suspension, not a failure position; deferred until triage demand shows up.
