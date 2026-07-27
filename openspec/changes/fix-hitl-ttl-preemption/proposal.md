## Why

`add-human-in-the-loop` built the fail-closed HITL path, but it left one timer able to preempt the other. `TTL_TIMER` (working-memory GC, WATERMARK) is armed at `now_ms + ttl_ms` on every committed activation, including a *suspending* one. `HITL_TIMER` (REAL_TIME) is armed at the suspension's `deadline_ms`. Nothing relates the two marks, so the memory GC can fire first, and `on_ttl` clears `CONTINUATION`/`PENDING` and emits nothing at all. The later HITL fire then reads `cont is None`, treats it as a stale handle, and returns silently.

The result is a suspension that ends with **no `.output` and no `.errors` record** — the timeout signal correctness invariant 6 exists to produce simply disappears.

This is live at stock defaults, not just under exotic configuration. `AgentConfig.ttl_ms` and `HitlPolicy.intent_ttl_ms` both default to `3_600_000`, and the default suspension deadline is `min(now + 86_400_000, now + 3_600_000) = now + 3_600_000` — the *same millisecond* the TTL mark lands on, in a different time domain. Which timer wins is runner- and ordering-dependent. It becomes deterministic as soon as the approval window exceeds `ttl_ms`: a 2-hour approval window on the 1-hour default TTL loses its timeout every time.

No test catches it because every pipeline test pins `ttl_ms` to `_BIG_TTL_MS = 1_000_000_000`, so `TTL_TIMER` and `HITL_TIMER` are never in contention.

## What Changes

- **A suspension pushes its own memory-GC mark past its deadline.** `_commit` arms `TTL_TIMER` at `max(now_ms, deadline_ms) + ttl_ms` when the activation suspends, instead of `now_ms + ttl_ms`. The memory a resume will read must outlive the wait, and the GC must not be able to beat the timer that reports the wait ending. A completing activation is unchanged: `now_ms + ttl_ms`.
- **An escalation extends the mark with the deadline.** `_escalate` already rewrites `deadline_ms` and re-arms `HITL_TIMER`; it now re-arms `TTL_TIMER` past the new deadline too, so a bounded escalation chain cannot outlive working memory. `on_hitl` takes a `TimerParam(TTL_TIMER)` to do this.
- **A TTL fire over a live suspension is reported, not silent.** The two timers live in different time domains, so re-arming cannot close the gap completely: during a backlog replay the watermark can pass the re-armed event-time mark long before real time reaches the deadline. When `on_ttl` finds a live `Continuation`, it now emits an `ActivationError(reason="ttl_wiped_suspension")` on `.errors` alongside the wipe it already performs. Whatever happens to the suspension, it stops being unobservable.

## Capabilities

### Modified Capabilities
- `human-in-the-loop`: the working-memory TTL can no longer garbage-collect a suspension that is still awaiting its answer, and a TTL fire that does reach a live continuation is reported on `.errors` instead of silently discarding it.

## Impact

- **Modified code:** `core/dofn.py` only — `_commit` (TTL mark derived from the suspension deadline), `on_hitl`/`_escalate` (re-arm TTL with the extended deadline), `on_ttl` (report a live continuation), and a new `REASON_TTL_WIPED_SUSPENSION` constant.
- **No wire/state change.** No proto edit, no `state_schema_version` implication, no new configuration knob.
- **Behavior change:** a suspended key now retains working memory until `deadline + ttl_ms` rather than `suspension + ttl_ms`. That is the point — the previous number was short enough to eat the continuation — but it does mean a long HITL window holds keyed state for that window plus the TTL. Callers who sized `ttl_ms` against memory cost should re-check it against their `HitlPolicy.timeout_ms`.
- **Rejected alternative: validate `ttl_ms > min(timeout_ms, intent_ttl_ms)` in `AgentConfig.__post_init__`.** It rejects the stock default config (`3_600_000 > 3_600_000` is false), and it cannot see the values that actually decide the deadline — `Suspend(timeout_ms=...)` and `act(ttl_ms=...)` are supplied per activation, and escalation moves the deadline after construction. A construction-time check would be both noisy and unsound; the runtime derives the correct mark instead.
- **Tests:** new `TestStream` scenarios with `ttl_ms` deliberately smaller than the HITL window (the configuration no existing test exercises), plus fake-handle unit tests for `on_ttl` that keep the new branch inside the mutation-gate test selection.
- **Mutation gate:** `core/dofn.py` is under the gate; `_commit`'s new lines are reachable only from the deselected pipeline suite, so `mutation-baseline.toml`'s `dofn.py` ceiling needs re-checking after implementation.
