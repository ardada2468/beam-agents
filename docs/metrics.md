# Runtime metrics

`RunAgent` publishes Beam user metrics under the namespace **`beam_agents.runtime`**.
They surface wherever your runner surfaces user metrics: the Dataflow job page,
the Flink metrics reporter, or `PipelineResult.metrics()` on the DirectRunner.

```python
from apache_beam.metrics.metric import MetricResults, MetricsFilter

result = pipeline.run()
result.wait_until_finish()
query = result.metrics().query(MetricsFilter().with_namespace("beam_agents.runtime"))
{m.key.metric.name: m.result for m in query[MetricResults.COUNTERS]}
```

Nothing needs enabling: metrics are published unconditionally, with no
configuration knob. (`AgentConfig.tool_registry` supplies the tools
`ctx.run_tool` executes — it configures tool execution, not metrics.) Working
memory's own `beam_agents.memory/soft_cap_warnings` counter is unchanged and
keeps its separate namespace.

## Counters

| Counter | Incremented |
|---|---|
| `activations` | Once per activation that reached the commit path — a start or a resume. Same event as the `SEQ` increment. |
| `llm_calls` | Once per model call that reached the provider. A replay-cache hit is not a call. |
| `tool_calls` | Once per read-only tool executed inline via `ctx.run_tool(...)`, on either activation surface. The tools come from `AgentConfig.tool_registry`. |
| `intents_emitted` | Once per `ToolIntent` put on `.intents`, including one minted by a HITL escalation. |
| `agent_errors` | Once per `.errors` record that is not an orphaned result (`activation_timeout`, `activation_error`, `hitl_timeout`, `ttl_wiped_suspension`, `ttl_wiped_batch`, `batch_buffer_overflow`). |
| `suspensions` | Once per committed activation whose outcome was `Suspend`. |
| `orphaned_results` | Once per `.errors` record with reason `orphaned_result`. |
| `longterm_upserts` | Once per long-term memory row flushed through the `MemoryStore` in a committed activation's commit tail (`docs/memory.md`). A failed activation flushes nothing and a failed flush fails the activation, so this only counts durable writes on the committed path. |
| `events_buffered` | Once per event appended to a key's adaptive-batching buffer (`docs/batching.md`). Zero under the default `BatchPolicy.NONE`. |
| `batch_flushes_size` | Once per **committed** flush that the `max_batch_size` threshold triggered. |
| `batch_flushes_timer` | Once per **committed** flush that the `max_wait_ms` `FLUSH_TIMER` triggered. |

Three identities hold by construction, and are worth alerting on if they break:

- `intents_emitted` equals the element count on `.intents`.
- `agent_errors + orphaned_results` equals the element count on `.errors`.
- `batch_flushes_size + batch_flushes_timer` equals the `batch_size` sample
  count, and each committed flush counts as exactly one `activation`.

## Distributions

Beam distributions are integer-only and report **sum, count, min, max** — there
are no percentiles. (Beam's histogram cell is marked internal-use-only, so a
portable percentile is not available; percentile work belongs to the trace sink
or the benchmark suite.)

| Distribution | One sample per |
|---|---|
| `activation_ms` | Agent run, **including failures and timeouts**. Sample count is therefore `activations` + failed activations. A resume refused at admission never runs the agent and is not sampled. |
| `overhead_ms` | Committed activation: its wall time minus its model-call and inline-tool time, clamped at zero. **This is the release-gate figure** (the budget excludes LLM/tool time). Sample count equals `activations`; a failed activation's tally does not escape, so failures contribute `activation_ms` only. |
| `llm_ms` | Provider-reached model call. Sample count equals `llm_calls`. |
| `tokens` | Committed activation whose provider usage was actually decoded. Activations that decoded no usage contribute no sample, so the count means "activations with known usage". |
| `memory_bytes` | Committed activation: the working-memory size that was committed. |
| `iterations` | Committed activation: the agent steps it consumed. A resume reports only its own steps. |
| `batch_size` | Committed batch flush: how many events it activated over. The mean is the batching ratio — how many events one activation (and one set of model calls) covered. No samples under `BatchPolicy.NONE`. |

### `activation_ms` vs. `overhead_ms`

`activation_ms` is total wall time and **includes provider and tool latency** —
useful for end-to-end latency questions, wrong for the release gate. The
release-gating budget (p50 < 15 ms, p99 < 60 ms per activation) excludes LLM and
tool time; `overhead_ms` publishes exactly that subtraction, so it is the
distribution to alert on for the budget. It is clamped at zero: an agent that
awaits calls concurrently can make summed call time exceed wall time. Beam
distributions carry no percentiles (see above), so the p99 check itself is
rendered by the benchmark suite ([`docs/benchmarks.md`](benchmarks.md) — its
`overhead_*ms` tiers record the *same* subtraction per activation and its gate
enforces the p50/p99 budget); `overhead_ms`'s sum/count/max give the
dashboard-level early warning.

## What these numbers are, and are not

**Attempted, not committed.** Most runners report *attempted* metric values, so
a bundle that fails and is retried re-applies its increments even though its
state and outputs roll back. These counters are telemetry, not an accounting
ledger: nothing in the runtime reads one back, and the authoritative record of
what happened is `.traces`, `.intents`, and `.errors`. Effectively-once
execution is the effector's `intent_id` dedup, not a counter.

**No labels.** Beam user metrics carry no dimensions, so there is no per-tool,
per-model, or per-error-reason breakdown here. Those dimensions are on every
`TraceEvent` (`gen_ai.request.model`, `beam_agents.cache_hit`, …) and on every
`ActivationError` (`reason`), which is where dimensioned analysis belongs.

**Cache-hit ratio is a trace question.** `llm_calls` deliberately counts only
provider-reached calls, which is what makes "a replayed bundle adds zero
provider calls" visible on a dashboard. The hit/miss split lives on the
`LLM_CALL` traces' `beam_agents.cache_hit` attribute.

## A DirectRunner caveat for local runs

`_AgentDoFn` declares a REAL_TIME timer (the HITL deadline), which rules out
Beam's `FnApiRunner`, so every `RunAgent` pipeline runs on the *classic*
DirectRunner. Its metrics implementation reports one bundle's updates and drops
the rest, so a local multi-bundle run (several `TestStream` groups, or a
timer-fired bundle) under-reports. This is a runner artifact — a plain
`beam.ParDo` counter shows the same thing — not a property of the metrics
themselves. Dataflow and Flink aggregate normally.
