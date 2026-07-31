# Adaptive batching

By default `RunAgent` activates the agent once per event. For bursty keys — a
sensor emitting twenty readings in a second, a card fired at twenty times in a
minute — that buys twenty LLM round-trips where one activation over the whole
burst reaches the same decision at a twentieth of the cost.

`BatchPolicy.ADAPTIVE` turns that burst into one activation:

```python
from beam_agents import AgentConfig, RunAgent
from beam_agents.core.batching import BatchPolicy

out = keyed_envelopes | RunAgent(
    my_agent,
    config=AgentConfig(
        provider_factory=make_client,
        batch_policy=BatchPolicy.ADAPTIVE,
        max_batch_size=20,   # flush as soon as the buffer reaches this
        max_wait_ms=500,     # ...or this much processing time after the first event
    ),
)
```

`BatchPolicy.NONE` is the default and changes nothing: one activation per
event, `ctx.event` as `bytes`, no buffer, no flush timer.

## The knobs

| Knob | Meaning | Default under `ADAPTIVE` |
|---|---|---|
| `batch_policy` | `NONE` (per-event) or `ADAPTIVE` (buffered) | `NONE` |
| `max_batch_size` | Buffer length that triggers an immediate flush | `10` |
| `max_wait_ms` | Processing-time bound, measured from the buffer's **first** event | `500` |
| `max_buffered_events` | Hard cap on buffered events; beyond it, events dead-letter | `4 × max_batch_size` |

All four are validated at `AgentConfig` construction. Setting a batch knob
while `batch_policy` is `NONE` raises `ValueError` — a knob that silently does
nothing is a misconfiguration, not a default.

## What the agent sees

Under `ADAPTIVE` a flush activation presents the batch as a list:

```python
async def react(ctx: ActivationContext) -> Complete:
    assert ctx.is_batch                    # true on every ADAPTIVE flush
    readings = ctx.events                  # tuple[bytes, ...], arrival order
    return Complete(output=summarize(readings))
```

- `ctx.event` is a `list[bytes]` on **every** flush, including a flush of one.
  The shape follows the configured policy, not the runtime batch size, so an
  `ADAPTIVE` agent is written against exactly one shape.
- `ctx.events` is the uniform accessor: the batch under `ADAPTIVE`, a
  one-element tuple under `NONE`, and empty on a resume under either policy.
- `ctx.single_event` is the narrowing accessor for agents (and the framework
  adapters) written against exactly one event. It raises under `ADAPTIVE`.

The activation clock `ctx.now_ms` is `max(event_time_ms)` over the batch — the
freshest ground truth the batch contains, and a pure function of the buffer, so
a retried bundle reproduces it. Intent expiries and suspension deadlines derive
from it exactly as on the per-event path.

## One flush is one activation

A committed flush consumes exactly one `SEQ`, mints intent IDs from
`(entity_key, seq, step_index)`, and keys its replay-cache entries by the same
`(entity_key, seq)` scope — all identical to a per-event activation. Five
buffered events that flush together produce **one** `activations` count, one
commit, and one trace.

The flush's trace carries `beam_agents.batch.size` and
`beam_agents.batch.trigger` (`size` or `timer`) on its `ACTIVATION_START`
event, so a trace consumer can tell a batch decision from a per-event one.

## Suspension is whole-batch

If a flush activation returns `Suspend`, the **entire batch** suspends: one
`Continuation` at the batch's `seq`, one `HITL_TIMER`, one resume. There is no
per-element suspension and no partial resume.

- The resume runs once, at the batch's `seq`, from the persisted `step_index`
  and `snapshot`. `ctx.events` is empty on a resume — the `Continuation` does
  not persist the batched events, so whatever the resume needs must be in the
  agent's own `snapshot`.
- A HITL timeout fails the whole batch closed through the usual fallback route,
  once, and a late result is one `orphaned_result`.

**While a continuation is live, flushing defers.** Both triggers are suppressed
(a flush that suspended would overwrite the live continuation and orphan its
pending intents), so the buffer keeps absorbing events past `max_batch_size`.
When the suspension resolves — the resume commits, or the HITL policy's
`Deny`/`Drop` route ends the wait — `FLUSH_TIMER` is re-armed to fire promptly
and the deferred batch flushes in its own callback, with its own `SEQ`. An
`Escalate` keeps the suspension live and keeps deferring.

## When things go wrong

| Situation | Behavior |
|---|---|
| The flush activation raises or times out | Nothing commits; **one `.errors` record per buffered envelope** (`activation_error`/`activation_timeout`, with `batch_size=<n>,trigger=<size\|timer>` in `detail`), and the buffer plus its timer are cleared. A poison batch cannot wedge the key in a retry loop; triage stays element-granular. |
| The buffer reaches `max_buffered_events` | The next event is dead-lettered as `batch_buffer_overflow` instead of appended. Buffer growth during deferral is bounded and visible. |
| `TTL_TIMER` fires over a non-empty buffer | Every buffered envelope is dead-lettered as `ttl_wiped_batch`, then all six state specs and the flush timer are wiped. Reaching this means a stalled pipeline or a watermark jump — `max_wait_ms` is orders of magnitude inside `ttl_ms`. |

## Caveats

- **Lateness.** `max_wait_ms` is a *processing-time* bound, not a windowing
  feature — which is what makes batching keep working during a backlog replay,
  when event time lags wall time by hours. The cost is that the watermark can
  advance past a buffered element while it waits, so outputs emitted from the
  `FLUSH_TIMER` callback may be droppably late for a windowed consumer
  downstream. Outputs carry the batch clock (`max(event_time_ms)`), and
  `max_wait_ms` is small by design. Beam's Python SDK does not expose a timer
  output-timestamp (watermark hold); if it gains one, `FLUSH_TIMER` should hold
  the watermark at the earliest buffered event time.
- **Resumes never buffer.** A `tool_result` or `approval` answers one specific
  suspension; delaying it would spend that suspension's own deadline doing
  nothing.
- **Per key only.** Batching is per entity key, like everything else in the
  runtime. Cross-key or global micro-batching would break per-key
  serialization; cross-key parallelism belongs to the runner.
- **Framework adapters are single-event.** The LangGraph/ADK/Pydantic AI
  adapters consume one event per activation and raise on `ctx.single_event`
  under `ADAPTIVE`. Batching those authoring surfaces is a separate change.
