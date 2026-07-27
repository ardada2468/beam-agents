## Context

Two timers guard a suspended key, and until now nothing related their marks:

| Timer | Domain | Armed at | On fire |
|---|---|---|---|
| `TTL_TIMER` | WATERMARK (event time) | `now_ms + ttl_ms`, every committed activation | wipe all five state specs, emit nothing |
| `HITL_TIMER` | REAL_TIME | the suspension's `deadline_ms` | run the `HitlPolicy` route, emit its result |

A suspending activation commits both. If the TTL mark is reached first, `on_ttl` clears `CONTINUATION` and `PENDING`; the subsequent HITL fire reads `cont is None`, correctly classifies it as a stale handle (`dofn.py:443`), and returns. Nothing is emitted on any output. The suspension is gone and no downstream consumer can tell it ever existed.

Measured at stock defaults (`AgentConfig(provider_factory=...)`, `HitlPolicy()`), both marks land on the same millisecond:

```
ttl mark  = now + AgentConfig.ttl_ms            = now + 3_600_000
deadline  = min(now + HitlPolicy.timeout_ms,      now + 86_400_000
                now + HitlPolicy.intent_ttl_ms) = now +  3_600_000
```

so ordering alone decides whether the caller sees `b"__hitl_timeout__"` or nothing. Widening the approval window past `ttl_ms` makes the loss deterministic.

## Goals / Non-Goals

**Goals:**
- Working-memory GC can never garbage-collect a suspension that is still awaiting its answer.
- A TTL fire that nonetheless reaches a live continuation is observable on `.errors`.
- No new configuration knob, no wire change, no change to the completing-activation path.

**Non-Goals:**
- Unifying the two time domains. `TTL_TIMER` is event-time by design (working memory is event-time garbage) and `HITL_TIMER` is real-time by design (approvals are wall-clock bound). This change makes them agree on ordering, not on a clock.
- Bounding how long a suspension may hold keyed state. `HitlPolicy.timeout_ms` already bounds the wait; retaining memory for that wait is the intended cost.
- Revisiting `on_ttl`'s wipe semantics. It still clears every spec, including `SEQ`.

## Decisions

### D1. Derive the TTL mark from the deadline, rather than validating the configuration

`_commit` arms `TTL_TIMER` at `max(now_ms, result.hitl_deadline_ms) + self._ttl_ms` when the activation suspends, and at `now_ms + self._ttl_ms` otherwise.

The `max(now_ms, ...)` guard is not decorative: `Suspend.timeout_ms` and `act(ttl_ms=...)` are agent-supplied and unvalidated, so a non-positive value can produce a deadline at or before the activation clock. Taking the max keeps the mark strictly in the future in every case, and reduces to the old expression exactly when the deadline is not in the future.

The obvious alternative — reject a config whose `ttl_ms` is not comfortably larger than its HITL window, in `AgentConfig.__post_init__` — was rejected on two counts. It rejects the shipped defaults, since `ttl_ms` and the effective default deadline horizon are both `3_600_000` and equality is precisely the coin-flip case. More fundamentally it checks the wrong values: the deadline is `min(now + Suspend.timeout_ms, earliest staged intent expiry)`, and both terms are chosen per activation, after any config validation has run. Escalation then moves the deadline again. A construction-time predicate over `HitlPolicy` cannot be sound about a quantity the agent computes at runtime; the commit path can, because it holds the actual `deadline_ms` it is about to persist.

### D2. Escalation re-arms the TTL mark, so `on_hitl` needs `TimerParam(TTL_TIMER)`

`_escalate` writes a continuation whose `deadline_ms` is `fired_at_ms + route.timeout_ms` — later than the deadline the original `_commit` sized the TTL mark against. Without re-arming, a bounded escalation chain walks the deadline forward past a fixed memory-GC mark and reintroduces exactly the bug this change removes. The escalate path therefore sets `TTL_TIMER` to `deadline_ms + ttl_ms` alongside the `HITL_TIMER` re-arm it already performs.

`Deny` and `Drop` do not re-arm: they end the suspension, so the existing mark is correct and firing it later is the desired GC.

### D3. `on_ttl` reports a live continuation instead of discarding it silently

D1 and D2 order the two marks in the *same* quantity — but they are consumed in different time domains. The TTL mark is compared against the watermark and the deadline against the wall clock, so a pipeline replaying a backlog can advance event time past `deadline + ttl_ms` while real time has not yet reached `deadline`. `add-human-in-the-loop`'s design accepted the event-time/real-time skew for resume admission ("layer 2 is exactly the backstop invariant 6 asks for"); the same skew survives here, and no arrangement of marks in one domain can close it.

So `on_ttl` reads `CONTINUATION` before wiping. If one is live, it emits `ActivationError(entity_key, "ttl_wiped_suspension", "seq=…,deadline_ms=…")` on `.errors` and then wipes as before. This does not rescue the suspension — the memory it would resume against is genuinely event-time garbage — but it converts a silent loss into a dead-letter record naming the key and the deadline that was dropped.

`on_ttl` consequently gains `beam.DoFn.KeyParam` and becomes a generator. It still touches no other state and emits nothing when there is no live continuation, so the existing TTL scenarios are unaffected.

### D4. The regression tests configure `ttl_ms` *below* the HITL window

Every existing pipeline test pins `ttl_ms = _BIG_TTL_MS` (1e9 ms), which is why the bug shipped: with that value the two timers are never in contention and the interaction is untested by construction. The new scenarios deliberately invert it — a HITL window larger than `ttl_ms` — and assert on the presence of the route's output rather than on timer internals, so they fail against the current implementation for the right reason (nothing is emitted) rather than by asserting on a mark.

Both timer orderings are exercised: watermark-before-processing-time (the ordering that loses the output today) and processing-time-before-watermark (the ordering that happens to work today), so the fix is shown to make the outcome ordering-independent.

## Risks / Trade-offs

- **A suspended key holds working memory longer** — up to `deadline + ttl_ms` instead of `suspension + ttl_ms`. For a 24h `HitlPolicy.timeout_ms` that is a day of retained keyed state per suspended key. This is the correct cost (the state is what a resume reads), but it is a real storage-footprint change for anyone who sized `ttl_ms` purely against memory cost. Called out in the proposal's Impact.
- **`on_ttl` gains an output** — a callable that previously emitted nothing can now produce an `.errors` element. A consumer treating `.errors` as fatal will start seeing records for backlog replays over suspended keys. That is the intended signal, and the alternative is the silence this change exists to remove.
- **`_commit`'s new branch is reachable only from the pipeline suite**, which mutmut deselects, so its mutants land in `dofn.py`'s "no tests" bucket and the gate ceiling moves. The `on_ttl` branch is covered by fake-handle unit tests inside the selection to keep as much of the change mutation-covered as possible.

## Open Questions

- Should `HitlPolicy.timeout_ms`'s 24h default be reconciled with `AgentConfig.ttl_ms`'s 1h default as a separate follow-up? After this change they no longer conflict operationally, but a reader comparing the two constants still finds a 24× mismatch with no comment explaining why it is safe.
- Should `Suspend.timeout_ms` and `act(ttl_ms=...)` be validated positive at the call site, the way `HitlPolicy` validates its own fields? D1's `max(now_ms, ...)` guard makes a non-positive value harmless here, but it silently produces an already-expired suspension rather than an error.
