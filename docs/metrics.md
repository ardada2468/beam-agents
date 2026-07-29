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

Nothing needs enabling: there is no configuration knob, and no `AgentConfig`
field. Working memory's own `beam_agents.memory/soft_cap_warnings` counter is
unchanged and keeps its separate namespace.

## Counters

| Counter | Incremented |
|---|---|
| `activations` | Once per activation that reached the commit path — a start or a resume. Same event as the `SEQ` increment. |
| `llm_calls` | Once per model call that reached the provider. A replay-cache hit is not a call. |
| `tool_calls` | Once per read-only tool executed inline. |
| `intents_emitted` | Once per `ToolIntent` put on `.intents`, including one minted by a HITL escalation. |
| `agent_errors` | Once per `.errors` record that is not an orphaned result (`activation_timeout`, `activation_error`, `hitl_timeout`, `ttl_wiped_suspension`). |
| `suspensions` | Once per committed activation whose outcome was `Suspend`. |
| `orphaned_results` | Once per `.errors` record with reason `orphaned_result`. |

Two identities hold by construction, and are worth alerting on if they break:

- `intents_emitted` equals the element count on `.intents`.
- `agent_errors + orphaned_results` equals the element count on `.errors`.

## Distributions

Beam distributions are integer-only and report **sum, count, min, max** — there
are no percentiles. (Beam's histogram cell is marked internal-use-only, so a
portable percentile is not available; percentile work belongs to the trace sink
or the benchmark suite.)

| Distribution | One sample per |
|---|---|
| `activation_ms` | Agent run, **including failures and timeouts**. Sample count is therefore `activations` + failed activations. A resume refused at admission never runs the agent and is not sampled. |
| `llm_ms` | Provider-reached model call. Sample count equals `llm_calls`. |
| `tokens` | Committed activation whose provider usage was actually decoded. Activations that decoded no usage contribute no sample, so the count means "activations with known usage". |
| `memory_bytes` | Committed activation: the working-memory size that was committed. |
| `iterations` | Committed activation: the agent steps it consumed. A resume reports only its own steps. |

### `activation_ms` is not the runtime-overhead budget

`activation_ms` is total wall time and **includes provider latency**. The
release-gating budget (p50 < 15 ms, p99 < 60 ms per activation) excludes LLM and
tool time. The comparable figure is `activation_ms - Σ llm_ms` for an
activation; it is not published as its own metric today.

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

**`tool_calls` reads zero today.** Inline tools execute through `AgentContext`,
and the stateful DoFn currently drives `ActivationContext`, which has no inline
tool surface. The counter is wired at the execution site so it starts reporting
when the adapter path lands.

## A DirectRunner caveat for local runs

`_AgentDoFn` declares a REAL_TIME timer (the HITL deadline), which rules out
Beam's `FnApiRunner`, so every `RunAgent` pipeline runs on the *classic*
DirectRunner. Its metrics implementation reports one bundle's updates and drops
the rest, so a local multi-bundle run (several `TestStream` groups, or a
timer-fired bundle) under-reports. This is a runner artifact — a plain
`beam.ParDo` counter shows the same thing — not a property of the metrics
themselves. Dataflow and Flink aggregate normally.
