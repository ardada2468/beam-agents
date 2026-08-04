# human-in-the-loop Specification

## Purpose
TBD - created by archiving change add-human-in-the-loop. Update Purpose after archive.
## Requirements
### Requirement: An activation can request a human approval as a first-class intent
Both activation surfaces (`AgentContext` and `ActivationContext`) SHALL expose `request_approval(...)`, which stages a `ToolIntent` with `kind = APPROVAL`, `tool_name` set to the configured approval channel, a canonical-JSON payload describing what is being approved, `created_at_ms` set to the activation clock, and a positive `expires_at_ms`. `request_approval` SHALL NOT require the approval channel to be a registered tool and SHALL NOT execute anything. Its `intent_id` SHALL be `intent_id_for(entity_key, seq, step_index)` — the same deterministic derivation `act(...)` uses — so a replayed activation re-mints a byte-identical approval request, and both surfaces mint the same ID for the same `(entity_key, seq, step_index)`.

#### Scenario: Requesting an approval stages an APPROVAL intent
- **WHEN** an activation calls `request_approval(...)` and returns `Suspend(...)`
- **THEN** exactly one staged `ToolIntent` carries `kind = APPROVAL`, the configured approval channel as `tool_name`, canonical-JSON arguments, and a positive `expires_at_ms`, and it appears in the persisted `Continuation`'s `pending_intent_ids`

#### Scenario: Approval requests are deterministic under replay
- **WHEN** the same activation is replayed and issues the same sequence of calls
- **THEN** the re-minted approval intent is byte-identical to the original, including its `intent_id`

#### Scenario: Requesting an approval executes nothing
- **WHEN** an activation calls `request_approval(...)`
- **THEN** no tool is looked up or invoked, and the only effect is the staged intent

### Requirement: Every staged intent carries a positive expiry
Every `ToolIntent` staged by either activation surface SHALL carry a positive `expires_at_ms`, derived from the activation clock plus the intent TTL (`HitlPolicy.intent_ttl_ms` by default, or the TTL supplied at the call site). An intent whose `expires_at_ms` is zero or negative SHALL be treated as already expired by every consumer, never as unbounded.

#### Scenario: Intents staged through the authoring surface carry an expiry
- **WHEN** an activation stages a side-effect intent through `AgentContext.act(...)`
- **THEN** the staged intent's `expires_at_ms` equals the activation clock plus the configured intent TTL, and is positive

#### Scenario: A zero expiry is treated as expired
- **WHEN** a consumer evaluates an intent whose `expires_at_ms` is zero
- **THEN** the intent is treated as expired and is refused, not treated as never expiring

### Requirement: A suspension's deadline is the earliest of its timeout and its intents' expiries
When an activation suspends, the runtime SHALL set the `Continuation`'s `deadline_ms` to the minimum of (a) the activation clock plus the suspension timeout (the `Suspend`'s `timeout_ms`, else `HitlPolicy.timeout_ms`) and (b) the smallest `expires_at_ms` among the intents staged by that activation. A suspension that stages no intents SHALL use (a).

#### Scenario: A short intent TTL shortens the suspension deadline
- **WHEN** an activation stages an intent expiring in 60s and suspends with a 24h timeout
- **THEN** the persisted `deadline_ms` is the 60s expiry, and `HITL_TIMER` is armed at that moment rather than 24h out

#### Scenario: A suspension with no intents uses its timeout
- **WHEN** an activation suspends with a timeout and stages no intents
- **THEN** the persisted `deadline_ms` is the activation clock plus that timeout

### Requirement: The HITL timer dispatches a pure policy fallback with the expired continuation handle
When `HITL_TIMER` fires over a live `Continuation` whose deadline it covers, the DoFn SHALL invoke the configured `HitlPolicy`'s timeout function with a `FallbackContext` carrying `kind = "timer"`, the entity key, and the expired continuation's handle — its `seq`, `snapshot`, `deadline_ms`, `pending_intent_ids`, and the timer's fire timestamp. The fallback SHALL be a pure, synchronous function: it SHALL NOT call the model, SHALL NOT run on the async bridge, and SHALL NOT increment `SEQ`. If the fallback function raises, the DoFn SHALL route a `hitl_timeout` record to `.errors` and clear the dangling continuation rather than failing the bundle or leaving the key suspended.

#### Scenario: Timer fire invokes the policy with kind=timer and the expired handle
- **WHEN** `HITL_TIMER` fires while a `Continuation` is live and its deadline has passed
- **THEN** the policy's timeout function receives a `FallbackContext` with `kind == "timer"`, the continuation's `seq`, `snapshot`, `deadline_ms`, and `pending_intent_ids`, and the timer's fire timestamp

#### Scenario: A timer fire does not increment SEQ
- **WHEN** the HITL fallback runs to completion
- **THEN** `SEQ` is unchanged, and the next real activation for that key uses the same `seq` it would have used had the timer never fired

#### Scenario: A raising policy fails closed to the errors output
- **WHEN** the configured timeout function raises
- **THEN** a `hitl_timeout` record is emitted on `.errors`, the `Continuation` and `PENDING` state are cleared, and the bundle does not fail

### Requirement: A stale HITL timer handle mutates nothing
The DoFn SHALL run the fallback only when a `Continuation` exists and the timer's fire timestamp is at or after that continuation's `deadline_ms`. A fire with no live continuation, or whose fire timestamp precedes the live continuation's `deadline_ms`, SHALL be treated as a stale handle: no fallback runs, no output is emitted, and no state is mutated.

#### Scenario: A superseded timer handle does not kill a live continuation
- **WHEN** a `HITL_TIMER` fire is delivered whose timestamp is earlier than the live `Continuation`'s `deadline_ms` (the suspension it belonged to was superseded by a later one)
- **THEN** no fallback runs, nothing is emitted, and the live `Continuation` and `PENDING` state are left intact

#### Scenario: A timer fire with no continuation is a no-op
- **WHEN** `HITL_TIMER` fires and no `Continuation` is stored for the key
- **THEN** nothing is emitted and no state is mutated

#### Scenario: A fire exactly at the deadline is live, not stale
- **WHEN** `HITL_TIMER` fires with a timestamp exactly equal to the live `Continuation`'s `deadline_ms`
- **THEN** the fallback runs

### Requirement: HitlPolicy routes a timeout to deny, drop, or escalate
`HitlPolicy`'s timeout function SHALL return exactly one route, and the DoFn SHALL apply it as follows:
- **Deny** — emit the route's deterministic bytes on the main output, clear the `Continuation` and `PENDING` state, leave `SEQ` unchanged.
- **Drop** — emit nothing on the main output, emit a typed timeout record on `.errors`, clear the `Continuation` and `PENDING` state, leave `SEQ` unchanged.
- **Escalate** — stage a new `APPROVAL` intent on `.intents`, rewrite the `Continuation` with a later `deadline_ms`, an incremented `escalations` count, and the escalation intent added to its `pending_intent_ids`; re-arm `HITL_TIMER` at the new deadline; leave `SEQ` unchanged. The suspension's earlier pending intents SHALL remain pending — an escalation adds a channel, it does not invalidate the original request — so an answer to either may still resume the activation, subject to that intent's own expiry.

The default policy SHALL be `Deny` with the runtime's existing HITL timeout output bytes, so a caller that configures nothing observes the current behavior. An escalation intent SHALL consume the continuation's next free step index: its `intent_id` is `intent_id_for(entity_key, continuation.seq, continuation.step_index)` and the rewritten continuation records `step_index + 1`. Both operands are persisted values that a retried timer bundle reads back unchanged, so the retry re-mints an identical intent; and because the step is consumed, no escalation can collide with another escalation or with a later resumed activation seeded from the same counter. `Escalate` SHALL be bounded by `HitlPolicy.max_escalations` (default `0`): once `escalations` has reached the bound, an `Escalate` route SHALL be applied as `Deny` instead.

#### Scenario: Deny emits deterministic bytes and clears the continuation
- **WHEN** the policy returns `Deny` for a timed-out suspension
- **THEN** the route's bytes appear on the main output, `CONTINUATION` and `PENDING` are cleared, and `SEQ` is unchanged

#### Scenario: Drop routes the timeout to the errors output
- **WHEN** the policy returns `Drop` for a timed-out suspension
- **THEN** nothing is emitted on the main output, a timeout record appears on `.errors`, and `CONTINUATION` and `PENDING` are cleared

#### Scenario: Escalate re-arms the deadline with a deterministic intent
- **WHEN** the policy returns `Escalate` and the escalation bound has not been reached
- **THEN** a new `APPROVAL` intent is emitted on `.intents` with a deterministic `intent_id`, the `Continuation` is rewritten with the later deadline, `escalations` incremented, its `step_index` advanced past the escalation, and the escalation added to `pending_intent_ids`; `HITL_TIMER` is re-armed at that deadline and `SEQ` is unchanged

#### Scenario: An answer to the escalated suspension resumes the agent
- **WHEN** an approval for the escalation intent arrives before the extended deadline
- **THEN** the agent resumes, and the resumed activation's own intents collide with neither the original nor the escalation intent

#### Scenario: Escalation is bounded
- **WHEN** the policy returns `Escalate` for a continuation whose `escalations` has reached `max_escalations`
- **THEN** the timeout is applied as `Deny` instead, no further escalation intent is emitted, and the continuation is cleared

#### Scenario: The default policy preserves existing behavior
- **WHEN** a pipeline is configured with no explicit `HitlPolicy` and a suspension times out
- **THEN** the runtime's existing HITL timeout output bytes are emitted on the main output and the continuation is cleared

### Requirement: A resume is admitted only against a live, unexpired continuation
The DoFn SHALL admit a `tool_result` or `approval` element and resume the activation only when all of the following hold: a `Continuation` exists for the key; the element's `intent_id` is among its `pending_intent_ids`; the element's activation clock is strictly before the continuation's `deadline_ms`; and, when the `PENDING` bag still holds the matching intent, the activation clock is strictly before that intent's `expires_at_ms`. A `deadline_ms` or `expires_at_ms` that is zero or negative SHALL be treated as already expired. Any element failing these conditions SHALL be emitted as `orphaned_result` on `.errors`, carrying a detail identifying which condition failed, and SHALL mutate no state.

#### Scenario: Timer first, then a late approval is orphaned
- **WHEN** `HITL_TIMER` fires and takes the fallback, and an `approval` for the same `intent_id` arrives afterwards
- **THEN** the fallback's output is produced exactly once, the late approval is emitted as `orphaned_result` on `.errors`, no activation runs for it, and no state is mutated

#### Scenario: An approval arriving after the deadline is refused even before the timer fires
- **WHEN** an `approval` matching a live `Continuation`'s `pending_intent_ids` arrives with an activation clock at or after that continuation's `deadline_ms`
- **THEN** the approval is emitted as `orphaned_result` on `.errors`, the agent is not resumed, and no state is mutated

#### Scenario: A result whose intent has expired is refused
- **WHEN** a `tool_result` arrives whose matching `PENDING` intent has `expires_at_ms` at or before the activation clock
- **THEN** the result is emitted as `orphaned_result` on `.errors` and the agent is not resumed

#### Scenario: An in-time approval resumes the agent and clears the timer
- **WHEN** an `approval` matching a live, unexpired `Continuation` arrives before the deadline and the resumed activation completes
- **THEN** the agent resumes, the activation commits, `CONTINUATION` and `PENDING` are cleared, `HITL_TIMER` is cleared, and advancing real time past the original deadline afterwards produces no fallback output

### Requirement: The effector guard refuses expired intents without external state
The runtime SHALL export a pure, import-light guard usable outside a Beam pipeline that decides, from a `ToolIntent` and a current-time value alone, whether the intent has expired, and produces the refusal `ToolResult` for an expired one. The guard SHALL NOT perform I/O, read a clock, or import Beam. A refused intent SHALL yield a `ToolResult` with status `EXPIRED` correlated by `intent_id`, `entity_key`, and `seq`, and the effector SHALL publish that result instead of executing the intent's effect.

#### Scenario: An expired intent is refused, not executed
- **WHEN** the guard evaluates an intent whose `expires_at_ms` is at or before the supplied current time
- **THEN** it reports the intent as expired and returns a `ToolResult` with status `EXPIRED` correlated to that intent, and the effect is not executed

#### Scenario: An unexpired intent passes the guard
- **WHEN** the guard evaluates an intent whose `expires_at_ms` is strictly after the supplied current time
- **THEN** it reports the intent as live and returns no refusal result

#### Scenario: An EXPIRED result re-injected into a live suspension resumes the agent
- **WHEN** a `ToolResult` with status `EXPIRED` is re-injected for an intent belonging to a still-live, unexpired `Continuation`
- **THEN** the agent is resumed with that result so it can take its own degraded path, rather than the element being orphaned

### Requirement: A resumed activation continues its suspended activation's step index
A resumed activation SHALL begin its step index at the persisted `Continuation.step_index` rather than at zero, so intent IDs remain unique within a `seq` across a suspend/resume boundary. Escalation intents SHALL be minted from that same monotonic sequence, so no intent minted by a suspended activation, a resumed activation, or an escalation for the same `seq` SHALL share an `intent_id`.

#### Scenario: An intent staged on resume does not collide with the suspended activation's intent
- **WHEN** an activation stages an intent and suspends, then resumes and stages another intent within the same `seq`
- **THEN** the two intents carry different `intent_id`s

#### Scenario: Escalation intents do not collide with resumed-activation intents
- **WHEN** a suspension escalates and is later resumed, and the resumed activation stages an intent
- **THEN** the escalation intent and the resumed activation's intent carry different `intent_id`s

### Requirement: HitlPolicy is validated at pipeline-construction time
`AgentConfig` SHALL accept a `HitlPolicy` and validate it in its constructor, raising `ValueError` with an actionable message — before any pipeline exists — when the timeout or intent TTL is not positive, when `max_escalations` is negative, or when the approval channel is empty. The policy SHALL be picklable so the DoFn holding it serializes for the runner.

#### Scenario: A non-positive timeout is rejected at construction
- **WHEN** an `AgentConfig` is built with a `HitlPolicy` whose timeout or intent TTL is zero or negative
- **THEN** construction raises `ValueError` naming the offending field, and no pipeline is built

#### Scenario: An empty approval channel is rejected at construction
- **WHEN** an `AgentConfig` is built with a `HitlPolicy` whose approval channel is empty
- **THEN** construction raises `ValueError` naming the offending field

### Requirement: Working-memory GC never preempts a live suspension's deadline

When an activation suspends, the DoFn SHALL arm `TTL_TIMER` at `max(activation clock, the persisted Continuation's deadline_ms) + ttl_ms`, so the working-memory garbage collector cannot fire while the suspension is still awaiting its answer. A completing activation SHALL continue to arm `TTL_TIMER` at the activation clock plus `ttl_ms`, unchanged.

When the HITL fallback escalates and rewrites the continuation with a later `deadline_ms`, the DoFn SHALL re-arm `TTL_TIMER` past the new deadline as well, so a bounded escalation chain cannot outlive working memory. The `Deny` and `Drop` routes end the suspension and SHALL NOT re-arm it.

#### Scenario: A HITL window longer than the memory TTL still reports its timeout

- **WHEN** an activation suspends with a HITL window longer than the configured `ttl_ms`, and both the watermark and processing time then advance past the deadline
- **THEN** the configured timeout route runs and emits its result, rather than the suspension being garbage-collected with nothing emitted on any output

#### Scenario: The timeout is reported regardless of which clock advances first

- **WHEN** a suspension whose HITL window exceeds `ttl_ms` times out, with the watermark advanced before processing time in one run and after it in another
- **THEN** both runs emit the same timeout output, so the outcome does not depend on the order the two clocks advance

#### Scenario: An escalation carries the memory TTL forward with the deadline

- **WHEN** the fallback escalates, extending `deadline_ms` beyond the mark the original suspension armed, and the watermark then advances past that original mark
- **THEN** the continuation and its pending intents survive, and an answer to the escalation still resumes the activation

#### Scenario: A completing activation's TTL mark is unchanged

- **WHEN** an activation completes without suspending
- **THEN** `TTL_TIMER` is armed at the activation clock plus `ttl_ms`, and the existing wipe-and-re-arm behavior is unaffected

### Requirement: A TTL fire over a live suspension is reported, not silent

`TTL_TIMER` is watermark-domain and `HITL_TIMER` is real-time, so ordering their marks in one quantity cannot prevent event time from passing the mark before real time reaches the deadline. When `TTL_TIMER` fires while a `Continuation` is still live, the DoFn SHALL emit an `ActivationError` on `.errors` with reason `ttl_wiped_suspension`, carrying the entity key and a detail naming the dropped suspension's `seq` and `deadline_ms`, in addition to the state wipe it already performs. A TTL fire with no live continuation SHALL continue to emit nothing.

#### Scenario: A TTL fire over a live suspension emits a dead-letter record

- **WHEN** `TTL_TIMER` fires while a `Continuation` is live
- **THEN** an `ActivationError` with reason `ttl_wiped_suspension` is emitted on `.errors` naming the key, `seq`, and `deadline_ms`, and every state spec is still cleared

#### Scenario: A TTL fire with no live suspension stays silent

- **WHEN** `TTL_TIMER` fires for a key with no `Continuation` in state
- **THEN** every state spec is cleared and nothing is emitted on any output
