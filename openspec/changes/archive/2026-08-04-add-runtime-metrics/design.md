## Context

The runtime today has one Beam metric (`beam_agents.memory/soft_cap_warnings`) and a rich per-element `TraceEvent` stream. The gap is aggregate, runner-visible telemetry: nothing on a Dataflow job page or a Flink metrics reporter says how many activations ran, how many suspended, how much is being dead-lettered, or how long an activation took.

Three properties of the existing architecture constrain how these metrics can be collected.

**Thread affinity.** `Metrics.counter(...).inc()` routes through `MetricUpdater.__call__`, which resolves its cell with `statesampler.get_current_tracker()`; `_STATE_SAMPLERS` is a `threading.local()`. Off the Beam worker thread the tracker is `None` and the update is dropped silently — no exception, no log. Every LLM call, tool call, and memory mutation in this runtime happens on the async bridge thread (`core/bridge.py`), so metric calls placed at those sites would be permanently zero and nothing would tell us.

**Atomic commit (invariant 1).** All activation effects are staged and applied only on success. A metric incremented mid-activation by an activation that then raises would report work whose state and outputs were rolled back — a weaker version of the same problem the staging design exists to prevent.

**Two context surfaces.** `ActivationContext` is what the DoFn drives; `AgentContext` is the richer authoring surface (inline `run_tool`, `LlmFacade` with decoded token usage) that adapters will drive. The metrics have to be counted at the real execution sites in both, without the DoFn needing to know which surface produced them.

Beam's user-facing metric types are counters, distributions, gauges, string sets, and bounded tries. `Metrics.histogram` exists but its cell is documented "For internal use only; no backwards-compatibility guarantees" (`apache_beam/metrics/cells.py`), so a portable percentile is not on the table; distributions report sum/count/min/max.

## Goals / Non-Goals

**Goals:**
- Twelve named metrics under one namespace, with definitions precise enough that a test can assert they close over `.intents` and `.errors` exactly.
- Collection that survives the bridge-thread boundary and obeys the all-or-nothing commit rule.
- Zero effect on replay determinism, keyed state, wire schemas, and the public API.
- A seam that unit tests can assert against without a pipeline.

**Non-Goals:**
- OTel metric export, exporters, or a metrics pipeline. `observability/` will grow those; this change fills in the Beam-native surface only.
- Per-tool, per-model, or per-reason breakdowns. Beam user metrics have no labels, and a counter per dimension is cardinality by copy-paste; dimensioned analysis stays on `.traces`.
- Replacing the trace stream. Traces remain the authoritative per-activation record; metrics are aggregates.
- Unifying with the effector's `MetricsSink` (`effector/service.py`). That is a separate process with its own lifecycle and no Beam context.
- Any configuration knob to disable metrics.

## Decisions

### D1. Stage the tally, record it at commit — never update a metric mid-activation

An activation accumulates its counts and durations into a plain in-process tally hanging off the context (`llm_calls`, `tool_calls`, `iterations`, `tokens`, and the per-call `llm_ms` durations). The tally rides out on `ActivationResult`, and `_AgentDoFn._commit` records everything on the Beam thread.

This is forced twice over. The bridge thread has no metrics container, so recording at the call sites would produce silent zeros. And even if it worked, recording during the activation would count work that a subsequent failure rolls back, contradicting invariant 1. Staging solves both with the mechanism the runtime already uses for every other effect.

The alternative — a `beam.utils.shared.Shared`-style worker-local accumulator flushed from the Beam thread later — was rejected: it reintroduces cross-key shared mutable state (invariant 4) to solve a problem that per-activation staging already solves, and it decouples the count from the commit that justifies it.

The commit order becomes `MEMORY, LLM_CACHE, CONTINUATION, PENDING, SEQ, timers, metrics, emits`. Metrics go after the state writes (so they describe a commit that happened) and before the emits (so the recording is not contingent on how the downstream consumes the generator).

### D2. Counters are defined to partition the outputs, and one chokepoint enforces it

`intents_emitted` equals the element count on `.intents`; `agent_errors + orphaned_results` equals the element count on `.errors`. These are chosen definitions, not observations — they make the metrics checkable against the transform's own outputs in a `TestPipeline`, which is the only way to catch a missed increment at a new emission site.

To keep it true, every dead-letter emission inside the DoFn goes through one method that increments the right counter and returns the tagged output, rather than calling the module-level `_error` builder directly. `_error` stays a pure builder (the chaos helper and tests use it); the DoFn's own path always goes through the counting wrapper, including inside `on_ttl` and `on_hitl`. A new `.errors` site that forgets to count is then a visibly different call, not an invisible omission.

`activations` is recorded with the rest of the metrics rather than inline next to `seq.add(1)`, so the commit keeps exactly one metrics step in its documented order; a comment at the `SEQ` increment names the other half, since the two are definitionally the same event ("a committed activation").

### D3. `llm_calls` and `llm_ms` count provider-reached calls; a cache hit is not a call

A replay-cache hit costs nothing and takes microseconds. Counting it as a call inflates the volume signal that people use for cost and rate-limit reasoning, and folding its near-zero duration into `llm_ms` deflates the latency distribution toward the cache-hit ratio rather than toward provider behavior.

Scoping both to provider-reached calls also makes correctness invariant 3 observable: replaying an activation whose calls are all cached adds zero to `llm_calls`. That is exactly the property the retry-determinism gate asserts, now visible on a dashboard.

The cost is that cache-hit ratio is not directly derivable from these two metrics. It is already on every LLM_CALL trace as `beam_agents.cache_hit`, and a dedicated `llm_cache_hits` counter is a one-line follow-up if the dashboards want it — deliberately not added here, since it is outside the requested set.

### D4. `tokens` flows through the existing `StagingSink` seam, not through new decode plumbing

`ActivationContext.accumulate_usage` is currently a no-op with a comment saying the runtime keeps no usage tally "in this change". This is that change: it accumulates into the tally.

That seam is the right one because the model facade already calls it with decoded `TokenUsage`, and already calls it only on a provider-reached call. The runtime's own `ActivationContext.call_model` bypasses the facade and awaits the provider directly with no decode, so it reports no usage — which is why the spec samples `tokens` only when usage was actually decoded rather than recording a zero. A distribution whose count means "activations with known usage" is honest; one padded with zeros from a path that never decodes is not.

The alternative was threading a provider-specific `Decode` callable into `ActivationContext` (and therefore into `AgentConfig`, and therefore into every construction site) so the raw path could decode usage too. That is a real feature — provider-neutral usage accounting on the fast path — but it is a model-layer change with its own config surface, and bolting it onto a metrics change would hide it. Left as an open question.

### D5. `iterations` is the step-cursor delta

There is no loop counter to read: the driver invokes the agent once per activation and the agent's internal control flow is opaque. The observable measure of "how much work did this activation do" is the advance of the step cursor — the same monotonic index that mints `intent_id`s and orders replay-cache entries — which advances once per model call and once per staged intent.

For a resumed activation the cursor is seeded from the continuation, so the delta is that activation's own steps, not the cumulative total. Sampling the cumulative value instead would make every resume look larger than it was and would double-count the suspended activation's steps.

### D6. `activation_ms` covers every agent run, including failures, and is measured on the Beam thread

The DoFn brackets its `self._activate(...)` call — the bounded bridge submission — with an injected monotonic clock, and records the sample on all three exits: success, `ActivationTimeout`, and any other exception. A timeout's duration is the single most interesting one on the distribution, and dropping failures would silently exclude the tail.

A resume refused at admission never runs the agent and is not sampled; `activation_ms`'s sample count is therefore `activations + failed activations`, which is `>= activations`.

Measuring here rather than inside `run_activation` keeps the clock read on the Beam thread and out of the pure driver, and captures exactly what the caller waits for, including bridge submission overhead.

Note for whoever wires the release gate: `activation_ms` is *total* wall time and includes provider latency, while the stated budget (p50 < 15 ms, p99 < 60 ms) excludes LLM and tool time. The runtime-overhead figure is `activation_ms - Σ llm_ms` for an activation, which is computable in-process at record time but is not one of the twelve requested metrics. See Open Questions.

### D7. Durations come from an injected monotonic clock; measurement is not decision

`ActivationContext` takes a `monotonic_ns` callable (default `time.monotonic_ns`) and brackets the provider await with it; the DoFn takes the same injection for `activation_ms`. Injection matches the module's existing treatment of every non-determinism source (`now_ms`, `rng`, `sleep` are all injected) and lets tests script exact durations instead of sleeping — the project forbids `sleep()`-based timing tests.

Reading a monotonic clock inside the activation does not weaken determinism, because nothing downstream of the reading is a decision: no cache key, `intent_id`, deadline, timer mark, or branch consumes it. The value lands only in a worker-local tally that is never persisted (D8) and never read back. The retry-determinism gate is unaffected, and the spec states this as a requirement so a future change cannot quietly start branching on a duration.

Event time (`now_ms`) is deliberately not used: it is frozen per activation by design, so every duration derived from it would be zero.

### D8. A small sink protocol, a Beam-backed recorder, a null recorder — and nothing persisted

`observability/metrics.py` holds the namespace constant, the metric names, a `MetricsSink` protocol (`incr(name, n)` / `observe(name, value)`), the Beam-backed `RuntimeMetrics` holding pre-built counter/distribution handles, and `NullMetrics`. `_AgentDoFn` takes a private constructor parameter defaulting to `RuntimeMetrics()`, so unit tests can drive fake state handles and assert recorded values with no pipeline — the same pattern `effector/service.py` already uses with its own `MetricsSink`/`CountingMetrics` pair, kept separate here because the effector is a different process with no Beam context.

Handles are built once per recorder rather than per call (`Metrics.counter()` allocates a `MetricName` and a delegating object each time). Construction touches no global state, so `observability/metrics.py` stays import-side-effect-free like every other module.

The tally is a frozen-at-read dataclass on `ActivationResult` and `AgentResult`. It is never written to `MemoryBlob` or `Continuation`: keeping it out of keyed state is what makes this change proto-free, golden-blob-neutral, and unable to perturb the coder round-trip.

### D9. The runtime surface gains `run_tool`, so `tool_calls` is live in the shipped pipeline

*(Revised in-change: the first cut wired `tool_calls` only at `AgentContext.run_tool` and shipped it reading zero, since the DoFn drives `ActivationContext`, which had no inline-tool surface. Review judged a permanently-zero metric a defect, and the fix is the missing capability, not the counter.)*

`ActivationContext` gains `run_tool(tool_name, arguments)` — the fast-path behavior the architecture has documented from the start ("pure/read-only tools execute inline") — backed by a `ToolRegistry` configured on `AgentConfig.tool_registry` (default: empty) and threaded `RunAgent → _AgentDoFn → run_activation → ActivationContext`. `ToolRunner.run`'s existing guard refuses `side_effect=True` tools before execution, preserving invariant 5; a refused or failing tool is not counted. `run_tool` does not advance the step cursor: the cursor mints `intent_id`s, and an inline read-only call must not perturb replay identity — pinned by a test asserting the same intent IDs with and without an interleaved tool call.

Both surfaces now count at their own execution site. Each runtime execution is also timed with the injected clock into `tally.tool_ms`, which D10 subtracts. The registry (holding `Tool`s with dynamically-created pydantic argument models) pickles into the DoFn through the DirectRunner — verified by the end-to-end pipeline test rather than assumed.

Side-effecting tools remain covered by `intents_emitted`: they never execute in the pipeline.

### D10. `overhead_ms` publishes the release-gate figure

*(Revised in-change: was an open question; review promoted it.)*

The latency budget (p50 < 15 ms / p99 < 60 ms per activation) excludes LLM and tool time, but `activation_ms` includes both — so the requested twelve metrics contained no instrument for the one number that gates releases. `overhead_ms` closes that: `max(0, activation_ms − Σ llm_ms − Σ tool_ms)`, recorded in `_record_commit`, one sample per committed activation.

Placement follows from what is knowable where. The subtraction needs both the activation's wall time (measured by the DoFn around the bridge submission) and the tally (inside the `ActivationResult`), so `_activate` now returns `(result, elapsed_ms)` and `_commit` carries `activation_ms` down to the recorder. A failed activation's tally never escapes — atomic staging discards it with everything else — so failures contribute `activation_ms` but no overhead sample; a wrong number would be worse than none. The clamp is not decorative: an agent that `asyncio.gather`s calls concurrently can make summed call time exceed wall time, and a negative duration sample is nonsense.

This is why inline tool executions are timed at all (`tally.tool_ms`): without the subtrahend, `overhead_ms` would silently re-absorb tool time the moment anyone used `run_tool`, and the gate figure would drift exactly when the feature got used.

## Risks / Trade-offs

- **Attempted, not committed, metric values** → Beam reports attempted values on most runners, so a retried bundle re-applies its increments while its state and outputs roll back. Mitigation: stated as a spec requirement, not a footnote; the runtime never reads a metric back, and `intent_id` dedup remains the effectively-once mechanism. Anyone treating these counters as an effect ledger is misreading them.
- **No percentiles** → distributions give sum/count/min/max, so the p99 activation budget cannot be read off the dashboard. Mitigation: none available portably (Beam's histogram cell is internal-only); percentile work stays with the benchmark suite and the trace sink.
- **No labels** → one `agent_errors` counter cannot say *which* reason dominates. Mitigation: the reason is on every `.errors` record and every trace; per-reason counters remain available as a cheap follow-up if operational experience demands them.
- **`tool_calls` reads zero until the adapter path lands** → looks like a broken metric. Mitigation: documented in the proposal and the spec's rationale; the wiring is at the execution site so it starts reporting with no metrics work later.
- **Timer callbacks are retried too** → an `on_ttl` or `on_hitl` bundle retry re-counts its `agent_errors`. Mitigation: same attempted-metric semantics as everything else, already covered by the spec requirement.
- **Per-element cost** → a dozen counter updates per activation against a 15 ms p50 overhead budget. Each update is a dict lookup and an integer add on a pre-built handle; the tally is a handful of integer adds. Mitigation: handles built once per recorder, no per-call allocation, no string formatting on the hot path.
- **Mutation-gate movement** → the recording call in `_commit` is reachable only from the TestPipeline suites that mutmut deselects, so its mutants land in `dofn.py`'s "no tests" bucket and the ceiling moves. Mitigation: the counting wrapper and the tally arithmetic are pure and unit-tested inside the selection, keeping as much of the change mutation-covered as possible; the baseline move is documented in `mutation-baseline.toml` as prior changes did. *(Implementation note: driving `process` and `_record_commit` directly with fake state handles in `test_dofn_metrics.py` put most of the new code inside the selection after all.)*

- **The DirectRunner under-reports metrics, which limits what a local test can assert** → `_AgentDoFn` declares a REAL_TIME timer, which rules out the `FnApiRunner` (`_FnApiRunnerSupportVisitor` in `direct_runner.py`), so every `RunAgent` pipeline runs on the classic DirectRunner, whose metrics implementation reports one bundle's updates and drops the rest. Verified independently of this code: a plain `beam.ParDo` counter reports 1 for three elements split across three `TestStream` groups. Mitigation: pipeline-level metric assertions are confined to single-bundle runs (which do report exact totals, including from the async bridge thread — the point of those tests); everything multi-bundle, including the timer callbacks' counting, is asserted with fake handles where no runner is in the way. Dataflow and Flink aggregate normally, so this is a local-testing constraint, not a production one.

## Migration Plan

No migration. There is no wire, state, or API change: an existing pipeline picks the metrics up by upgrading, and a pipeline `--update` is unaffected because no state schema moved. Dashboards and alerts are built after the fact against the `beam_agents.runtime` namespace. Rollback is a code revert with no data implications.

## Open Questions

- Should an `overhead_ms` distribution (`activation_ms - Σ llm_ms`) be added so the release-gate constraint has a direct instrument? It is computable at record time for free, but it is outside the requested set, and the same number is derivable from the benchmark suite.
- Should the raw `ActivationContext.call_model` path decode token usage, so `tokens` is populated without the `LlmFacade`? That means threading a provider-specific `Decode` into the runtime path and its configuration — a model-layer change worth its own proposal.
- Should `llm_cache_hits` be added alongside `llm_calls`, making the cache-hit ratio a dashboard quantity instead of a trace query?
- Should the effector's `MetricsSink` and this one converge when `observability/exporters` lands, so both processes emit the same names to the same backend?
