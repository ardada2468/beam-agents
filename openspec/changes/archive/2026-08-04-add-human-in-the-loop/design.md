## Context

`add-stateful-dofn-runtime` shipped the skeleton of the HITL path: `Suspend(timeout_ms=...)` persists a `Continuation` with a `deadline_ms`, `_commit` arms `HITL_TIMER` (REAL_TIME) at that deadline, and `on_hitl` clears the continuation and yields the module constant `HITL_TIMEOUT_OUTPUT`. `AgentEnvelope.Approval` exists on the wire, and `_resume` matches an inbound `approval`/`tool_result` against `Continuation.pending_intent_ids`.

What is missing is everything that makes it *fail closed*:

1. **Nothing mints an approval request.** `ctx.act(...)` stages a side-effect `ToolIntent`; there is no way to say "a human must approve this". An `Approval` element can only ever resume a continuation whose `pending_intent_ids` came from a regular tool intent, and the effector has no wire-level way to tell "execute this" from "ask a human".
2. **The fallback is a constant, not a policy.** `on_hitl` yields fixed bytes on the main output. A caller cannot route a timeout to `.errors`, cannot escalate, and cannot supply its own deterministic degraded output.
3. **`expires_at_ms` is write-only.** `ActivationContext.act` stamps it; nothing anywhere reads it. `AgentContext.act` does not even stamp it (leaves `0`).
4. **`_resume` never checks a deadline.** The stateful-dofn spec says a resume matching "no live, **unexpired** `Continuation`" is orphaned, but the implementation only tests `intent_id in cont.pending_intent_ids`. A REAL_TIME timer is not a hard fence — it can fire late, be delivered late, or (in a drained/updated pipeline) be re-delivered — so an approval arriving well past its deadline currently resumes the agent and lets the blocked side effect through. That is a fail-open hole in correctness invariant 6.
5. **`on_hitl` cannot tell a live deadline from a superseded one.** It reads the continuation and fires unconditionally. A timer delivery that belongs to an older, superseded suspension would kill a live continuation.

Constraints that shape every decision below: timer callbacks re-execute on bundle retry (so anything they do must be deterministic and idempotent); `SEQ` increments exactly once per *committed activation* and a timer firing is not one; the async bridge is for activations only; and proto evolution is additive-only under `state_schema_version = 1`.

## Goals / Non-Goals

**Goals:**
- A first-class approval request: `request_approval(...)` on both context surfaces, minting a `ToolIntent` with `kind = APPROVAL` and a mandatory expiry, deterministic under replay.
- A configurable, *pure* timeout fallback (`HitlPolicy`) with three routes — deny, drop, escalate — dispatched from `HITL_TIMER` with `kind="timer"` and the expired continuation handle.
- Stale-timer-handle rejection: a fire that does not cover the live continuation's deadline mutates nothing.
- Fail-closed expiry in both layers: the DoFn refuses expired resumes; a pure guard lets the effector refuse expired intents without external state.
- One deadline, agreed by both layers: `min(suspend timeout, earliest staged intent expiry)`.

**Non-Goals:**
- The effector service itself (dedup, execution, result publication). This change ships the guard it will call, not the service.
- An approval UI, queue, notification channel, or identity/authorization model for approvers. `tool_name` names the channel; delivery is the effector's problem.
- Multi-approver quorum, partial approval, or approval of a *subset* of a suspension's pending intents. One suspension, one deadline, all-or-nothing.
- Cross-key or global rate limits on escalation (per-key bound only).
- Changing `SEQ` semantics, the atomic-commit order, or the replay cache.

## Decisions

### D1. Approval is a `ToolIntent` with a `kind` discriminator, not a new message or a magic `tool_name`

An approval request travels the same outbox → effector → results path as any other intent, gets the same deterministic `intent_id`, the same dedup, and the same `expires_at_ms`. Reusing `ToolIntent` keeps one outbox, one dedup table, and one expiry rule.

The alternative — a reserved `tool_name` sentinel such as `"__approval__"` — was rejected: it is stringly typed, it collides with a legitimately registered tool name, it forces `AgentContext.act`'s registry lookup to special-case a name that has no registered tool behind it, and a non-Python effector would have to hardcode the sentinel. A second alternative — a new top-level `ApprovalRequest` message with its own topic — was rejected because it duplicates the entire dedup/expiry/re-injection apparatus for one field's worth of difference.

So: additive `enum ToolIntent.Kind { TOOL_KIND_UNSPECIFIED = 0; TOOL = 1; APPROVAL = 2; }`, field 10. Proto3 additive, old readers ignore it, no `state_schema_version` bump. Because `0` is the unspecified default, readers treat `UNSPECIFIED` and `TOOL` identically (an intent written before this change is a tool call), and only an explicit `APPROVAL` routes to a human. `request_approval` always sets `APPROVAL` explicitly; `act` always sets `TOOL` explicitly.

`request_approval` bypasses the tool registry (`AgentContext.act`'s `side_effect=True` check has nothing to check — the approval channel is not a registered tool) but shares `intent_id_for` and the canonical-JSON argument encoding, so both surfaces mint identical intents for identical inputs.

### D2. `HitlPolicy` is data + a pure routing function; the timer never runs an activation

```python
@dataclass(frozen=True, slots=True)
class HitlPolicy:
    timeout_ms: int = DEFAULT_HITL_TIMEOUT_MS
    intent_ttl_ms: int = DEFAULT_INTENT_TTL_MS
    approval_channel: str = "approval"
    max_escalations: int = 0
    on_timeout: Callable[[FallbackContext], Route] = deny  # module-level default
```

`Route` is `Deny(output: bytes) | Drop(reason: str) | Escalate(tool_name, args_json, timeout_ms)`. The default is `Deny(HITL_TIMEOUT_OUTPUT)`, so a caller who configures nothing gets exactly today's behavior — this change is behavior-preserving by default.

The routing function is **pure and synchronous**: no `await`, no model call, no bridge submission, no clock read (`FallbackContext` carries the times). The reason is structural, not stylistic — a timer callback re-executes when its bundle is retried, and the atomic-commit invariant only covers what the runner replays deterministically. Running an activation from a timer would need a `SEQ` increment (which the stateful-dofn spec forbids for a timer fire), a bridge submission with its own timeout semantics, and a second commit path. Rejected alternative: "re-invoke the agent with a `resume_timeout` flag". It is more expressive but it makes the fallback non-deterministic (an LLM call in a retried timer bundle) and gives the timeout path the same failure modes as the thing that just timed out.

`Escalate` is the one route that mutates state: it stages a fresh approval intent and re-arms the timer with a new deadline. The intent **consumes the continuation's next free step index** — `intent_id_for(entity_key, cont.seq, cont.step_index)`, with the rewritten continuation recording `step_index + 1`. Both operands are persisted, so a retried timer bundle (whose state rolls back) re-mints the identical intent and the effector dedups it. Deriving the step from `cont.escalations` instead was the original plan and is wrong: it would hand escalation intents the same steps a resumed activation seeds from (D7), reintroducing exactly the collision D7 exists to fix. `escalations` is only the bound counter.

The escalated continuation keeps its earlier `pending_intent_ids` and adds the new one: escalating adds a channel, it does not withdraw the original request, so an answer to either resumes the activation — subject, per D5, to that intent's own `expires_at_ms`, which is where the per-intent backstop earns its place. It is bounded by `HitlPolicy.max_escalations` (default `0`, i.e. escalation off unless asked for); when the bound is reached the policy result is coerced to `Deny`, never to another `Escalate`. The count is persisted as the additive `Continuation.escalations` (uint32, field 9) because an unbounded escalate loop would be a fail-*open* hole — the whole point of the timer is that the wait ends.

### D3. `on_hitl` takes the fire timestamp and rejects stale handles

```python
@on_timer(HITL_TIMER)
def on_hitl(self, key=beam.DoFn.KeyParam, timestamp=beam.DoFn.TimestampParam, ...)
```

Verified against the pinned Beam 2.72.0: `MethodWrapper.invoke_timer_callback` substitutes `TimestampParam` with the timer's fire timestamp and supports `KeyParam`, `StateParam`, and `TimerParam` in `@on_timer` callbacks, so the callback can both read the fire time and re-arm the timer for `Escalate`.

The guard is: fire the fallback **only if** a continuation exists *and* `fired_at_ms >= cont.deadline_ms`. Anything else is a stale handle — a delivery belonging to a suspension that has since been resolved (continuation cleared → no-op, already the behavior today) or superseded by a later suspension with a later deadline (`fired_at_ms < deadline_ms` → no-op, new). Beam's own `set()` replaces a timer with the same tag, so on a well-behaved runner this is belt-and-braces; it is written anyway because a fail-closed mechanism that can be tricked into killing a *live* continuation by a duplicate delivery is not fail-closed, it is just a different failure. The comparison is `>=`, not `>`, so a timer that fires exactly at its deadline is live, not stale.

Rejected alternative: store a monotonic "suspension epoch" in the continuation and compare epochs. Equivalent power, an extra proto field, and the deadline is already the thing that matters.

### D4. One deadline: `min(suspend timeout, earliest staged intent expiry)`

`run_activation` currently computes `deadline_ms = now_ms + timeout`. It now takes the minimum of that and every staged intent's `expires_at_ms`. If an agent suspends for 24h awaiting an intent whose TTL is 60s, the effector will refuse that intent after 60s and the result will *never* arrive — waiting 24h to discover this is a fail-open stall. Taking the minimum makes the two layers agree on a single moment after which nothing can be resumed.

Consequence: `Continuation.deadline_ms` is the single authority for layer-1 admission, and it is always `<=` every pending intent's expiry, so the per-intent check in D5 is a redundant backstop rather than a second source of truth. It is still written, because a `PENDING` bag entry can outlive a rewritten continuation.

### D5. Layer 1 admission: live continuation **and** unexpired, else `orphaned_result`

`_resume` admits a `tool_result`/`approval` only when all of:
- a `Continuation` exists, and
- `intent_id ∈ cont.pending_intent_ids`, and
- `now_ms < cont.deadline_ms`, and
- the matching `PENDING` intent (if the bag still holds one for this ID) has `now_ms < expires_at_ms`.

Otherwise: `ActivationError(REASON_ORPHANED, detail=...)` on `.errors`, zero state mutation. The reason stays `orphaned_result` rather than a new `expired_result` because the stateful-dofn spec already defines "matches no live, **unexpired** `Continuation`" as orphaned — an expired continuation *is* that case. The `detail` field distinguishes the sub-cases for triage (`no_continuation`, `unknown_intent`, `deadline_passed`, `intent_expired`).

`deadline_ms <= 0` is treated as **expired**, not as "unbounded". Every `Continuation` this runtime writes has a positive deadline (`run_activation` always sets one), so a zero can only come from a corrupt or hand-built blob — and under invariant 6 the safe reading of "no deadline recorded" is "do not resume", not "resume forever". Same rule for `expires_at_ms <= 0` on a pending intent, which is why `AgentContext.act` must start stamping it (see D6).

`now_ms` here is `envelope.event_time_ms`, consistent with the rest of the DoFn: the runtime never reads a wall clock inside `process`, so a replayed bundle makes the same admission decision. The trade-off is that a producer that stamps a stale `event_time_ms` can admit a resume the wall clock would refuse; the effector guard (layer 2, which runs against real time outside the pipeline) is the backstop, which is precisely why invariant 6 wants two layers.

### D6. Layer 2 is a pure guard shipped now, effector shipped later

`hitl.py` exports:

```python
def intent_expired(intent: ToolIntent, now_ms: int) -> bool
def refuse_expired(intent: ToolIntent, now_ms: int) -> ToolResult | None   # ToolResult(status=EXPIRED) or None
```

No I/O, no clock, no Beam import — an effector (in any process) calls `refuse_expired` before doing anything else and publishes the returned `ToolResult(status=EXPIRED)` back to the results topic instead of executing. Re-injected, that `EXPIRED` result resumes a still-live continuation and the agent gets to take its own degraded path — which is the *correct* fail-closed handoff, and is why layer 1 admits `EXPIRED` results like any other (the continuation deadline, being `<=` the intent expiry per D4, is the thing that decides).

`AgentContext.act` stops leaving `expires_at_ms` at `0` and stamps `now_ms + policy.intent_ttl_ms`, matching `ActivationContext.act`'s existing `ttl_ms` behavior. Without this, D5's "0 means expired" rule would instantly expire every intent minted through the authoring surface.

### D7. Resumed activations continue their step index (latent-collision fix)

`_resume` builds the resumed `ActivationContext` with `step_index` starting at `0` — inside the *same* `seq` as the suspended activation. So a resumed activation's first `act(...)` mints `intent_id_for(key, seq, 0)`, which the suspended activation already used, and the effector — correctly doing its job — dedups the new side effect away as a duplicate and never executes it.

This is a pre-existing latent defect, but it is load-bearing here: "request an approval, and on approval request a *second* approval (or escalate)" is exactly the shape that trips it. Fix: seed the resumed context from `Continuation.step_index`, so step indices are monotonic within a `seq` across suspend/resume boundaries. `Continuation.step_index` becomes a single "next free step" cursor for the whole `seq`: an activation leaves it at its post-activation count, and an escalation consumes one and advances it. All three producers — suspended activation, escalation, resumed activation — draw from that one cursor, so none can overlap.

The fix changes `intent_id`s produced by resumed activations. It is not a wire-compat break (IDs remain deterministic functions of persisted state), but any test asserting a literal resumed-activation intent ID must be updated, and an in-flight pipeline mid-suspension at upgrade time may re-mint one differing ID. Noted in Migration.

### D8. Configuration lives on `AgentConfig`, validated at construction

`AgentConfig(hitl_policy: HitlPolicy = HitlPolicy())`, validated in `__post_init__` alongside the existing knobs: `timeout_ms`/`intent_ttl_ms` positive, `max_escalations >= 0`, `approval_channel` non-empty. Misconfiguration raises `ValueError` at the construction site, before any pipeline exists — the project's standing rule. The policy is passed to `_AgentDoFn` and reaches `run_activation` as the default timeout, `on_hitl` as the routing function, and both contexts as the intent TTL. Because `HitlPolicy` holds a callable, it must pickle for the DirectRunner: `on_timeout` must be a module-level function or another picklable callable, and the tests use module-level policies for that reason (same convention as the agents in `tests/core/_dofn_helpers.py`).

## Risks / Trade-offs

- **A user-supplied `on_timeout` that is non-deterministic or raises** → it runs inside a timer callback whose bundle can be retried, so a raise fails the bundle and a clock/RNG read breaks replay. Mitigated by: documenting the purity contract on `HitlPolicy`, passing every time value in `FallbackContext` so the function has no reason to read a clock, and wrapping the dispatch so an exception routes to `.errors` as `hitl_timeout` (a `Drop`) rather than wedging the key forever.
- **`min(timeout, intent expiry)` shortens deadlines that today are long** → an agent that suspends for 24h with a 60s intent TTL now gets a 60s fallback instead of a 24h wait. This is the intended correction (the 24h wait was for a result that could never arrive), but it is a visible behavior change for anyone relying on the long deadline. Mitigated by making `intent_ttl_ms` a policy knob and calling it out in Migration.
- **`deadline_ms <= 0` treated as expired** → a hand-built or corrupt `Continuation` becomes unresumable rather than resumable-forever. Chosen deliberately (fail closed); the runtime never writes a zero, and the `orphaned_result` detail names `deadline_passed` so triage is unambiguous.
- **Event-time admission vs. real-time expiry** → a producer stamping stale `event_time_ms` can slip a resume past layer 1. Accepted: reading a wall clock in `process` would break replay determinism, and layer 2 (real-time, outside the pipeline) is exactly the backstop invariant 6 asks for.
- **`Escalate` mutates state from a timer** → a retried timer bundle must not double-escalate. Mitigated by deterministic `intent_id` derivation from persisted values (the effector dedups an identical re-mint) and by the persisted `escalations` bound; `Escalate` is off by default (`max_escalations = 0`).
- **Step-index fix changes resumed intent IDs** → see D7 and Migration.
- **Two more mutable knobs in `core/`** → `core/dofn.py` and `core/context.py` are under the mutation gate; new branches (expiry checks, stale-handle guard, route dispatch) will move the surviving-mutant ceiling. Mitigated by writing scenario-derived tests for each branch first, then re-checking `mutation-baseline.toml` rather than raising it reflexively.

## Migration Plan

1. Add the two proto fields, regenerate (`make proto`), confirm the `_pb2` diff is exactly the additive change and CI's regen check is clean. Extend the golden-blob compat test: an old blob (no `kind`, no `escalations`) parses with `TOOL_KIND_UNSPECIFIED`/`0`, and a new blob round-trips.
2. Ship `hitl.py` and the context/loop changes behind default values that reproduce current behavior (`Deny(HITL_TIMEOUT_OUTPUT)`, `max_escalations = 0`).
3. Existing pipelines: `--update`-compatible (additive proto, no `state_schema_version` bump). A pipeline updated while a key is mid-suspension keeps its persisted `deadline_ms`; the new admission check applies to it immediately, which is the intended fail-closed tightening. Per D7, such a key's next resumed activation may mint one `intent_id` differing from what the old code would have produced — harmless (the effector dedups on ID; a differing ID means one extra execution of an effect that had not been executed).
4. Rollback: revert the code; the two proto fields can stay (unread additive fields are inert) so a rollback needs no state migration.

## Open Questions

- Should `Drop` be able to name a custom error reason, or is a single `hitl_timeout` reason enough for triage? Currently `Drop(reason: str)` defaults to `"hitl_timeout"`; if the errors sink schema wants a closed set, this becomes a constant.
- Should an escalation reset the *approval* intent's expiry only, or also extend the working-memory `TTL_TIMER`? Currently only the HITL deadline moves; a long escalation chain could outlive the memory TTL and resume against wiped memory. Leaning toward documenting the interaction and letting `ttl_ms` be sized accordingly, rather than coupling the two timers.
