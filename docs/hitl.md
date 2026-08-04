# Human-in-the-loop

An agent on this runtime never blocks a worker waiting for a person. It stages
an approval request as a `kind = APPROVAL` [`ToolIntent`](https://github.com/ardada2468/beam-agents/blob/main/protos/beam_agents.proto),
**suspends**, and frees the key; the answer re-enters the pipeline as an
ordinary element on the same key, minutes or days later, and resumes the
activation where it left off. When no answer ever arrives, the wait fails
**closed** — at two independent layers, so a lost message and a slow effector
are both survivable without a side effect escaping.

This page is the operator's map of that machinery. The authoring surface is
one method (`ctx.request_approval`) and one config object
([`HitlPolicy`](api.md#human-in-the-loop-beam_agentshitl)); the
[fraud triage example](examples/fraud-triage.md) shows the whole loop running
offline, and the [Slack approval example](examples/slack-approval.md) shows a
real approval surface closing it.

## The suspension lifecycle

1. **Stage.** The activation calls `ctx.request_approval(args, channel=...,
   ttl_ms=...)` (or, for a side-effect tool, `ctx.act(...)`). Nothing
   executes: an intent is staged with a deterministic
   `intent_id = intent_id_for(entity_key, seq, step_index)`, a
   `created_at_ms` from the activation clock, and an
   `expires_at_ms = now_ms + ttl_ms`. The approval channel names where the
   [effector](effector.md) routes the request — a queue, a pager, a Slack
   handler — it is *not* a registered tool, and the effector never executes an
   approval intent.
2. **Suspend.** The agent returns `Suspend(snapshot=..., timeout_ms=...)`. On
   commit the runtime persists a `Continuation` — the snapshot, the pending
   intent ids, and a real-time deadline (`timeout_ms`, or the policy's
   `timeout_ms` when unset) — emits the staged intents on `.intents`, and arms
   the `HITL_TIMER` at the deadline.
3. **Resume.** The approval (or tool result) is re-injected keyed by the same
   `entity_key`. The DoFn admits it against the live continuation and
   re-invokes the agent with `ctx.resume` set and `ctx.snapshot` carrying the
   suspension's bytes. Results accumulate: an activation waiting on several
   intents re-suspends until every pending id is answered.

Because `intent_id` is a pure function of `(entity_key, seq, step_index)`, a
retried bundle stages byte-identical intents and the effector's dedup makes
the request effectively once — the same [invariant](index.md#guarantees-and-the-gates-that-enforce-them)
the rest of the side-effect path rides.

## Configuring it: `HitlPolicy`

```python
from beam_agents import AgentConfig, Deny, Drop, Escalate, FallbackContext, HitlPolicy

def route(fallback: FallbackContext) -> Deny | Drop | Escalate:
    return Deny()  # module-level and pure — see below

config = AgentConfig(
    provider_factory=make_client,
    hitl_policy=HitlPolicy(
        timeout_ms=900_000,          # suspension deadline (default: 24 h)
        intent_ttl_ms=600_000,       # staged-intent lifetime (default: 1 h)
        approval_channel="approval", # where the effector routes requests
        max_escalations=0,           # bound on Escalate re-arms
        on_timeout=route,            # default: deny
    ),
)
```

`on_timeout` must be **pure, synchronous, and picklable** — a module-level
function, never a lambda or closure. That is a correctness requirement, not
style: the route runs inside a timer callback whose bundle can be retried, so
a fallback that read a clock or called a model would make the retry diverge.
Every time value the route could need is already on the
[`FallbackContext`](https://github.com/ardada2468/beam-agents/blob/main/src/beam_agents/core/agent.py)
it is handed: the suspended activation's `seq` and `snapshot`, the
`deadline_ms` that elapsed, the timer's `fired_at_ms`, and the
`pending_intent_ids` nothing answered. Validation runs at construction —
non-positive timeouts, a negative escalation bound, or an empty channel raise
`ValueError` before any pipeline exists.

## The three timeout routes

| route | what happens |
| --- | --- |
| `Deny(output=...)` | Deterministic bytes (default `b"__hitl_timeout__"`) are emitted on the **main output** and the suspension ends. The agent's downstream sees an answer; it is just the configured "no". |
| `Drop(reason=...)` | Nothing on the main output; one record on `.errors` with reason `hitl_timeout`. The suspension ends. |
| `Escalate(tool_name, args_json, timeout_ms)` | Ask again, louder: a **fresh approval intent** is staged on the named escalation channel and the deadline extends by `timeout_ms` from the fire time. Bounded by `max_escalations` — an unbounded escalate loop would be a fail-*open* hole — after which the wait falls through to the deny path. |

`Deny` and `Drop` clear the continuation and its pending intents; a late
answer arriving afterwards finds no continuation and dead-letters (see below).
A route function that *raises* is itself failed closed: the raise becomes a
`Drop` to `.errors` rather than a wedged key retrying a broken policy forever.

## Fail-closed, at both layers

Correctness invariant 6: a timed-out approval must not execute, no matter
which side of the pipeline boundary learns about the timeout first.

**Layer 1 — the pipeline.** `HITL_TIMER` is a real-time timer armed at the
suspension's deadline. When it fires over a live continuation, the policy
routes it (`Deny`/`Drop`/`Escalate`, above). Stale firings — the suspension
was already resolved, or superseded by a later one with a farther deadline —
mutate nothing: a fail-closed mechanism that could be tricked into killing a
*live* continuation would not be fail-closed.

**Layer 2 — the effector.** Every staged intent carries `expires_at_ms`, and
the [effector](effector.md) calls
[`refuse_expired`](api.md#human-in-the-loop-beam_agentshitl) before executing
*anything*: an expired intent is never executed; a `ToolResult` with status
`EXPIRED` is published instead, and re-injected on the key it lets a
still-live continuation take its own degraded path. The boundary is
deliberately strict — `intent_expired` reads a non-positive `expires_at_ms`
as **already expired**, never as unbounded, so an intent that somehow lost
its expiry cannot execute.

**Late answers are dropped, visibly.** A result or approval whose suspension
is gone — the timer already routed it, the continuation was superseded, or
the intent's own TTL passed — fails resume admission and lands on `.errors`
as one `orphaned_result` record. The record's detail names which of the four
admission checks failed (`no_continuation`, `unknown_intent`,
`deadline_passed`, `intent_expired`), so triage never has to re-derive it.

Both layers are release gates, not intentions:
[`tests/semantics/test_hitl_fail_closed.py`](https://github.com/ardada2468/beam-agents/blob/main/tests/semantics/test_hitl_fail_closed.py)
drives the timeout at both layers, and the
[adapter conformance matrix](adapters.md#the-conformance-matrix) pins the
suspend/approve/timeout lifecycle to identical behavior across every adapter
on DirectRunner and Flink.

## Observing it

Suspensions and their outcomes are first-class records: `suspensions` and
`orphaned_results` counters in the [runtime metrics](metrics.md), `INTENT`
and timeout events in the [trace stream](traces.md) (each with the intent's
`expires_at_ms` and the suspension's `deadline_ms` as attributes), and a
live approval queue in the [console](console.md). The
[console demo](examples/console-demo.md) generates approvals, denials, and
timeouts on purpose so those views have something honest to show.
