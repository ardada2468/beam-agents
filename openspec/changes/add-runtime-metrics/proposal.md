## Why

The runtime produces one per-activation `TraceEvent` stream on `.traces` and exactly one Beam metric in the whole codebase: `beam_agents.memory/soft_cap_warnings` ([facade.py:254](../../../src/beam_agents/memory/facade.py:254)). Everything an operator wants from a runner dashboard — activation rate, how many activations suspend, how much of the stream is being dead-lettered, how long an activation actually takes — currently requires standing up a trace sink and querying it. Beam metrics surface natively on every supported runner (the Dataflow job page, the Flink metrics reporter, `PipelineResult.metrics()` on DirectRunner); the runtime just does not populate them.

It is also the missing instrument for a release-blocking constraint. "Runtime overhead p50 < 15 ms / p99 < 60 ms per activation (excluding LLM/tool time)" is a stated release gate, and nothing in the pipeline measures activation wall time at all today.

There is a second, non-obvious reason to do this deliberately rather than by sprinkling `Metrics.counter(...).inc()` through the code: **a metric update made from the async bridge thread is silently discarded.** `MetricUpdater.__call__` resolves its cell through `statesampler.get_current_tracker()`, and `_STATE_SAMPLERS` is a `threading.local()` ([statesampler.py:43](../../../.venv/lib/python3.11/site-packages/apache_beam/runners/worker/statesampler.py)) — off the Beam worker thread the tracker is `None` and the update is dropped with no error. Since every LLM call, tool call, and memory write happens on the bridge thread, the naive placement of these counters produces a dashboard of permanent zeros. The measurement has to be staged with the rest of the activation's effects and recorded on the Beam thread at commit.

## What Changes

- **New `observability/` package** (the module map's designated home for "traces, metrics, exporters"), starting with `observability/metrics.py`: the `beam_agents.runtime` namespace, the twelve metric handles, a `MetricsSink` protocol, the Beam-backed `RuntimeMetrics` recorder, and a `NullMetrics` no-op for tests.

- **Seven counters** under `beam_agents.runtime`:

  | Counter | Incremented |
  |---|---|
  | `activations` | once per activation that reaches the commit path (start or resume) |
  | `llm_calls` | once per model call that reached the provider (a replay-cache hit is not a call) |
  | `tool_calls` | once per read-only tool executed inline (side-effecting tools are `intents_emitted`) |
  | `intents_emitted` | once per `ToolIntent` put on `.intents`, including a HITL escalation's |
  | `agent_errors` | once per `.errors` record that is not an orphaned result |
  | `suspensions` | once per committed activation whose outcome is `Suspend` |
  | `orphaned_results` | once per `.errors` record with reason `orphaned_result` |

  The counters are defined so that they close over the outputs exactly: `agent_errors + orphaned_results` equals the number of elements on `.errors`, and `intents_emitted` equals the number of elements on `.intents`. Both are asserted end-to-end rather than left as prose.

- **Six distributions** under the same namespace: `activation_ms` (one sample per activation that ran the agent, success *or* failure — a timeout's duration is exactly the interesting one), `overhead_ms` (the activation's wall time minus its model-call and inline-tool time, clamped at zero — the direct instrument for the release-gating latency budget, which excludes LLM/tool time; one sample per committed activation), `llm_ms` (one per provider-reached model call), `tokens` (total tokens per activation, sampled only when usage was actually decoded), `memory_bytes` (committed working-memory size per activation), `iterations` (agent steps consumed per activation — the same step cursor that mints intent IDs).

- **A per-activation tally staged like every other effect.** `ActivationContext` accumulates counts and durations in-process; `ActivationResult` carries them out; `_AgentDoFn._commit` records them on the Beam thread, after the state writes and before the emits, extending the documented fixed commit order to `MEMORY, LLM_CACHE, CONTINUATION, PENDING, SEQ, timers, metrics, emits`. A failed or timed-out activation contributes no counts except `agent_errors` and its `activation_ms`, exactly as it contributes no state.

- **The runtime surface gains inline read-only tool execution, making `tool_calls` live.** `ActivationContext.run_tool(...)` executes a `side_effect=False` tool against a `ToolRegistry` configured on the new `AgentConfig.tool_registry` field (default: empty) — the fast-path behavior the architecture documents, previously present only on the authoring surface, which is why `tool_calls` could only ever read zero in a pipeline. Side-effecting tools are still refused before executing (invariant 5), and `run_tool` does not advance the step cursor, so intent IDs and `iterations` are unperturbed.

- **`ActivationContext.accumulate_usage` stops discarding token usage.** It is currently a documented no-op stub ("the runtime keeps no separate usage tally in this change", [context.py:442](../../../src/beam_agents/core/context.py:442)); it becomes the `tokens` source, fed by the model facade's existing `StagingSink` seam.

- **Durations come from injected monotonic clocks, never from event time.** The DoFn times `_activate` on the Beam thread; `ActivationContext` times the provider await with an injected `monotonic_ns` callable (defaulted to `time.monotonic_ns`, overridable in tests). No staged effect, cache key, intent ID, or branch depends on the reading — measurement is not decision — so replay determinism is untouched.

## Capabilities

### New Capabilities
- `runtime-metrics`: the runner-visible counter/distribution surface `RunAgent` publishes — which metrics exist, what each one counts, where in the activation lifecycle each is recorded, and the determinism and thread-affinity rules that recording must obey.

### Modified Capabilities

None. No existing spec's requirements change: the commit-order extension, the usage-tally wiring, and the new module are all stated as requirements of `runtime-metrics` itself, and `beam_agents.memory/soft_cap_warnings` keeps its current namespace and behavior.

## Impact

- **New code:** `src/beam_agents/observability/__init__.py`, `src/beam_agents/observability/metrics.py`, `tests/observability/test_metrics.py`, plus the runner-free DoFn tests the mutation gate requires once the element path becomes reachable: `tests/core/_dofn_fakes.py` (shared fake state/timer handles), `tests/core/test_dofn_metrics.py`, and `tests/core/test_dofn_activation.py`.
- **Modified code:** `core/context.py` (`ActivationContext` gains the tally, real `accumulate_usage`, and provider-call timing), `core/loop.py` (`ActivationResult` carries the tally), `core/dofn.py` (times the activation, records at commit, counts at the three `.errors` sites and in the two timer callbacks). `AgentContext`/`AgentResult` gain the same tally so an inline `run_tool` is counted at its execution site.
- **No wire or state change.** The tally is worker-local and never persisted: no proto edit, no `state_schema_version` implication, no golden-blob movement. It is deliberately *not* stored in `MemoryBlob` or `Continuation`, so it cannot perturb the coder round-trip or the retry-determinism gate.
- **One public API addition: `AgentConfig.tool_registry`.** It supplies the tools `run_tool` executes; it is not a metrics knob, and metrics remain unconditional with no configuration. Everything else stays internal: `observability` is not re-exported from `beam_agents/__init__.py`, and the recorder is injectable only through a private DoFn constructor parameter for tests.
- **Metrics are best-effort telemetry, not a ledger.** Beam reports *attempted* metric values on most runners, so a retried bundle double-counts its increments while its state and outputs roll back. This is inherent to Beam metrics, is called out in the spec, and is why nothing in the runtime may ever read a metric back to make a decision.
- **`tool_calls` reports from day one on both surfaces.** The first cut wired it only at `AgentContext.run_tool` and shipped it reading zero; review judged a permanently-zero metric a defect, and the fix was the missing runtime capability, not the counter.
- **Gates:** `core/dofn.py` and `core/context.py` are both under the mutation gate. Recording at commit meant driving the DoFn's element path without a runner, which pulled ~264 previously unreachable mutants into the mutation selection and required runner-free assertions for all of them — `mutation-baseline.toml`'s `dofn.py` ceiling moves 267 → 3, and two exclusion entries are renumbered where the edits shifted mutant indices. Coverage ratchet moves 0.9349 → 0.9369.
