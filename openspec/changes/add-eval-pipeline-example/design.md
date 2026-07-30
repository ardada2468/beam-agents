## Context

`add-trace-exporters` finished the observability delivery story: `.traces` reaches Kafka/Pub/Sub as keyed deterministic proto bytes ([exporters.py:57](../../../src/beam_agents/observability/exporters.py:57)), BigQuery as schema'd rows partitioned on `event_time` and clustered on `trace_id` ([exporters.py:32](../../../src/beam_agents/observability/exporters.py:32)), and OTLP collectors best-effort ([otlp.py:395](../../../src/beam_agents/observability/otlp.py:395)). Trace identity is a pure function of activation scope — `trace_id_for(entity_key, seq)` ([traces.py:83](../../../src/beam_agents/observability/traces.py:83)) — and `ToolIntent` carries `trace_id` on the wire ([beam_agents.proto:70](../../../protos/beam_agents.proto:70)), so an agent's external effect can carry activation identity into the business system whose later outcome judges it.

This change is an *example*, and that constrains everything. The repo's constitution says beam-agents is a runtime, not a framework: an evaluation pipeline shipped in `src/` would be exactly the kind of user-land orchestration the project refuses to own. What the project does own is the claim that its outputs are consumable by ordinary downstream pipelines — a claim currently proved only for `.errors` by the doc-contract pair [docs/errors.md](../../../docs/errors.md) ↔ [test_failure_streak_alarm.py](../../../tests/examples/test_failure_streak_alarm.py). So the deliverable is a documented pipeline whose code is copied verbatim into an offline test, not a package module.

Facts the design must respect:

- **Outcomes lag activations**, by minutes to days, with skewed and effectively unbounded tails (a chargeback window is 120 days). The join cannot assume any fixed lag bound without either delaying every result to the bound or silently dropping the tail.
- **Trace delivery is at-least-once.** A retried bundle re-emits byte-identical events; downstream dedup is by `(trace_id, span_id, event_type)` — stated in [docs/traces.md](../../../docs/traces.md). Any aggregation that does not dedup double-counts.
- **One activation is many events.** A joined record needs a per-activation summary (status, token usage from the [usage attributes](../../../src/beam_agents/observability/traces.py:110), error reason), folded from `ACTIVATION_START`…`ACTIVATION_END`/`ERROR` plus children.
- **The Python SDK has no MapState** and stateful DoFns require KV input — the same realities `core/dofn.py` designs around.
- **The doc-contract test must run offline**: no docker, no network, `FakeLLM` for the judge, trace bytes produced by the runtime's own encoder.

## Goals / Non-Goals

**Goals:**

- A complete, honest, documented continuous-evaluation pipeline: exported traces in, per-scenario quality metrics out.
- A join design that is correct under outcome lag, at-least-once trace delivery, and bounded state — with the failure surfaces (`no_outcome`, `orphaned_outcomes`) explicit outputs, not silent drops.
- An LLM-as-judge stage that goes through the `LLMClient` seam, versions its prompt explicitly, and fails closed on unparseable verdicts.
- A doc-contract test that keeps the documented code true, and in passing proves the trace stream's public-consumability claim.

**Non-Goals:**

- Shipping evaluation machinery in `beam_agents` (`src/`). No new public API, no `EvalPipeline` transform. If recurring demand later argues for promotion, that is its own change.
- Judge quality itself: rubric design, judge calibration against human labels, multi-judge ensembles. The example shows *where* the judge sits and how its prompt is versioned, not how to write a good rubric.
- Online feedback: routing scores back into agent behavior (prompt selection, model routing). Out of scope for the runtime and the example.
- An outcome-ingestion standard. The example defines a minimal outcome record for itself; real deployments map their own business events onto it.
- Exactly-once metric rows. Outputs are at-least-once like every sink in the system; the verdict row carries deterministic identity (`trace_id`, prompt version) so downstream dedup is exact.

## Decisions

### D1. Trace source: the Kafka bytes topic, not BigQuery

The example reads the traces topic written by a `kafka://` `traces_to` sink and decodes values with nothing but the public proto bindings — `TraceEvent.ParseFromString` over the deterministic bytes `serialize_trace_event` produced. Reasons:

- **It is the continuous option.** "Continuous evaluation" means scores minutes after outcomes arrive, which needs a streaming source; BigQuery as a source makes the example a scheduled batch job and buries the join problem (SQL can join lagged tables trivially — and un-illuminatingly).
- **The delivery contract fits.** The Kafka sink is documented lossless at-least-once and keyed by `entity_key`, so one key's events keep relative order through a partition — the property the per-activation fold relies on. OTLP is explicitly best-effort and would make the eval pipeline's input lossy.
- **It is offline-testable.** The doc-contract test feeds the *same parse function* bytes produced by the runtime's own encoder, exactly as the errors example feeds `serialize_error_envelope` output to `parse_error_record`. A BigQuery source has no offline analogue that tests the real wire format.
- **It proves the standing claim.** Decoding with public bindings is precisely the promise [docs/traces.md](../../../docs/traces.md) makes and no test holds.

The BigQuery variant is not ignored — teams already landing traces in the published table ([exporters.py:32](../../../src/beam_agents/observability/exporters.py:32)) get a documented SQL section showing the same join as a scheduled query, clearly labeled as the batch alternative. Code and contract test cover the streaming path only.

### D2. Join: stateful DoFn keyed by `(entity_key, seq)` with an event-time deadline, not windowed CoGroupByKey

The two streams are flattened into one KV stream keyed by `entity_key.hex() + "|" + str(seq)` — the same composite `trace_id_for` hashes, so the key *is* activation identity and `trace_id` is recomputable from it. A stateful DoFn holds, per activation:

- `EVENTS`: a `BagState` of encoded `TraceEvent` bytes — blind append per element, no read-modify-write on the hot path;
- `OUTCOME`: a `ReadModifyWriteState` holding the outcome record;
- `DONE`: a `ReadModifyWriteState` flag marking an already-emitted activation;
- `DEADLINE`: a WATERMARK-domain timer set to the activation's event time plus the configured evaluation deadline (e.g. 7 days), doing double duty as emission fallback and state GC — the same shape as the runtime's `TTL_TIMER`.

Emission is outcome-triggered: when the outcome arrives, the DoFn folds the bagged events — deduping by `(span_id, event_type)` first, which makes at-least-once trace delivery harmless — into an activation summary and emits the joined record immediately. When the deadline fires first, it emits a `no_outcome` record from the summary alone (an activation nobody ever judged is itself a signal — silent absence would bias every rate the pipeline reports) and clears state. An outcome arriving for a `DONE` or already-GC'd activation goes to `orphaned_outcomes`, the downstream mirror of the runtime's `orphaned_result` route.

Why not CoGroupByKey over windows: a windowed CGBK emits only at window close, so the window must be at least the worst credible outcome lag — which delays *every* result by that worst case — and outcomes beyond `allowed_lateness` are dropped inside the GBK where no application code can observe or count them. The stateful join emits at outcome arrival (latency = outcome lag, irreducible), bounds state by an explicit, per-activation deadline rather than a global window, and turns both failure modes into named outputs. Session windows fare no better: a trace event and an outcome days apart never merge without gap durations as long as the lag bound, which is the same delay in different clothes. The cost is real — the example must manage state and a timer explicitly, in the global window — but that cost is the honest content of the example: it is the same set of moves (`BagState` blind appends, single-value blobs, watermark GC, orphan routing) the runtime itself makes in `core/dofn.py`, taught in user-land form.

Outcome identity: the example requires outcome records to carry `(entity_key, seq)`. This is not an imposition invented here — the runtime already exports activation identity into the world via `ToolIntent.trace_id`, so the system acting on the agent's intent can thread it through to the eventual outcome event. The doc says exactly that, and shows `trace_id_for(entity_key, seq)` recomputing the trace ID for drill-down links into the BigQuery trace table.

### D3. Judge: a plain DoFn over the `LLMClient` seam, not `RunAgent`

The judge stage is an ordinary `beam.DoFn` that builds an `LlmRequest` ([client.py:21](../../../src/beam_agents/model/client.py:21)) from the joined record and calls `provider.complete(...)` on a client built once in `setup()` from an injected `provider_factory` — the same factory shape `AgentConfig` takes. Scoring one joined record is a pure function of that record: it needs no keyed memory, no continuation/resume, no replay cache in keyed state, no HITL, no intents — every piece of machinery `RunAgent` exists to provide. Running the judge under `RunAgent` would cost a shuffle onto keys that mean nothing (there is no per-key serialization requirement), spend keyed state on a stage with no state, and misleadingly present evaluation as agentic. The seam, not the transform, is the reuse boundary: `FakeLLM` substitutes structurally in the test, and a real deployment passes `AnthropicProvider`/`OpenAICompatProvider` exactly as it would to `AgentConfig`. The async protocol is bridged with a per-element `asyncio.run(...)` in the example — deliberately the simplest correct thing; the doc notes that throughput-sensitive deployments batch elements per call, and that the runtime's bridge-thread machinery is not part of the example's claims.

Retries and cost control stay out: the example does not rebuild `LlmFacade`'s breaker/retry/cache stack. A judge call that raises `ProviderError` routes the record to `judge_errors` with the error type — visible, re-drivable from the lossless input, and honest about the example's scope.

### D4. Judge prompts are versioned artifacts; verdicts are constrained and fail closed

The judge prompt is a module-level constant paired with an explicit `JUDGE_PROMPT_VERSION` string (e.g. `"triage-judge/v2"`). Every request includes it; every verdict row records it alongside `gen_ai.request.model`. Rationale: a judge-score time series is only meaningful within one (prompt, model) pair — an unversioned prompt edit shifts every score and reads as an agent regression. Stamping the version on each row makes the discontinuity queryable (`GROUP BY judge_prompt_version`) instead of invisible; the documented aggregate SQL groups by it.

The verdict is parsed as constrained JSON via a small pydantic model (`score: int` in a fixed range, `rationale: str`) — pydantic v2 is already a core dependency for exactly this purpose. A verdict that fails to parse or leaves the range routes the record to `judge_errors`; the example never coerces, defaults, or averages-in a fabricated score, because a silently defaulted score corrupts the aggregate it feeds.

### D5. Outputs: per-record verdict rows plus fixed-window per-scenario aggregates

Two outputs, both documented as BigQuery-shaped rows beside their layout table, mirroring how [docs/traces.md](../../../docs/traces.md) documents the trace table:

- **Verdict rows** (one per judged record): `trace_id` (hex — the join key back into the trace table, which is clustered on it), `entity_key` (hex), `seq`, `scenario` (from the outcome record), `outcome_label`, `score`, `judge_prompt_version`, `judge_model`, `activation_status`, `input_tokens`/`output_tokens`/`billed` (folded from the [usage attributes](../../../src/beam_agents/observability/traces.py:110), summed only over deduped, `billed=true` events so replay-cache hits are not double-billed), `event_time`. Deterministic identity (`trace_id`, prompt version) makes the at-least-once rows dedupable exactly.
- **Aggregate rows**: fixed windows (1 hour in the example) combined per `(scenario, judge_prompt_version)` — judged count, mean score, `no_outcome` count, `judge_error` count. Deeper slicing is documented as SQL over the verdict rows rather than piled into the pipeline.

Per-agent attribution is a pipeline-level constant (`AGENT_ID`) stamped on every row: one `RunAgent` deployment produces one traces topic, so the eval pipeline knows which agent it is evaluating by configuration, not by inspecting events.

### D6. Delivery as a doc-contract pair; the doc is the artifact

The example ships as `docs/examples/continuous_eval.md` with its pipeline stages copied verbatim into `tests/examples/test_continuous_eval.py` between the same `begin/end (keep in sync)` markers the errors example uses. The test drives the copied stages with `TestPipeline`: trace bytes from `serialize_trace_event` over events built with `ActivationTrace` ([traces.py:129](../../../src/beam_agents/observability/traces.py:129)) — the runtime's own producer, so the test consumes the real wire format — outcome records timed to lag them, `FakeLLM` scripted with verdict payloads (including one unparseable), and assertions per spec scenario. Timer/lateness behavior is exercised with scripted watermark advances, never `sleep()`, per the testing conventions. No `examples/` script directory is added: a runnable-but-untested script would be a second copy that drifts, and the doc-contract pattern exists precisely to prevent that. If `add-docs-site` (C24) lands a different examples layout, only the page's mount point moves; the contract pairing is path-agnostic.

## Risks / Trade-offs

- **[Example complexity: a stateful join with timers is a lot of doc code]** → That complexity is the subject matter — a trivial windowed join would document a design that drops the outcome tail. Mitigated by structure: the doc builds the pipeline in stages (parse → join → judge → emit), each independently copied and tested, and reuses the runtime's own state-management idioms so a reader of `core/dofn.py` recognizes every move.
- **[Doc drift]** → The failure mode the doc-contract pattern exists for; the verbatim-copy markers plus the test make drift a test failure, not a stale page.
- **[`asyncio.run` per judge call is slow at scale]** → Accepted for an example; stated in the doc with the batching direction. The example's claims are about correctness of the join and the scoring contract, not judge throughput.
- **[Unbounded per-activation state if the deadline is misconfigured]** → The deadline timer is mandatory in the example (no infinite-wait mode), and the doc calls out state cost per in-flight activation. `BagState` of events is bounded by the activation's own event count.
- **[Two-stream watermark skew: the outcome stream's watermark can hold back the deadline timer]** → Watermark-domain timers fire on the flattened stream's combined watermark, so a stalled outcome source delays `no_outcome` emission but never causes wrong emission; noted in the doc. The alternative (processing-time deadline) would make replay produce different join results — rejected for the same reason the runtime keeps GC on the watermark.
- **[Judge cost: every joined record spends provider tokens]** → The example samples nothing, but the doc names sampling (a `beam.Filter` on a hash of `trace_id` — deterministic, replay-stable) as the first knob a real deployment turns.
- **[C24 lands a different examples layout or slips]** → Only the page path is coupled; the pair falls back to the established flat `docs/` layout (`docs/continuous_eval.md`) without touching the spec, which names no paths.

## Migration Plan

1. Write the doc-contract test skeleton first (spec scenarios → failing tests) with the fixtures: encoder-produced trace bytes, lagged outcomes, scripted `FakeLLM`.
2. Write the example pipeline *in the doc*, stage by stage, copying each stage verbatim into the test as it lands: parse/summarize → stateful join (deadline, orphans, dedup) → judge (versioned prompt, fail-closed parse) → verdict/aggregate rows.
3. Cross-link from `docs/traces.md` ("consuming `.traces` downstream") to the example page; align the mount point with `add-docs-site` if it has landed, flat `docs/` otherwise.
4. Gates. Rollback is deleting the pair — no runtime surface is touched, nothing depends on the example.

## Open Questions

- Should the outcome record's minimal shape (entity_key, seq, scenario, label, event time) eventually be a proto in `wire-schemas` so effector-adjacent systems can emit it natively? Deferred until a second consumer exists; a premature standard here is exactly the framework-creep the constitution warns against.
- Once `add-docs-site` (C24) lands, should the aggregate stage move from the pipeline into a documented materialized view over verdict rows? The pipeline-side Combine is kept for now because it demonstrates windowed aggregation over the joined stream, which is part of what the example teaches.
