## Context

See `proposal.md` — Why. Constraints that shape the approach:

- The runtime seam is already uniform: every adapter ultimately presents an `Agent` (async activation over `ActivationContext` returning `Complete | Suspend`), so scenarios can assert exclusively on `RunAgent`'s four outputs plus committed keyed state, never on framework internals.
- Existing machinery to reuse, not rebuild: `FakeLLM` (scripted matchers, records requests), `beam_agents.testing.chaos` (targeted commit failure + Beam's own bundle retry), `tests/core/_dofn_fakes.py` (fake state/timer handles driving `_AgentDoFn` without a runner), `tests/adapters/_helpers.py` (context builders), and the e2e harness's Flink stack provisioning (`tests/semantics/_e2e/stack.py`: per-run topic/consumer-group isolation, `freshen_flink`, health checks, `InfraFailure` vs. invariant separation).
- Testing-tier rules bind hard: DirectRunner cells must be offline and use TestStream scripted advances (never `sleep()` for watermark/TTL behavior); Flink cells carry `integration` and run under docker compose; `scripts/check_semantics_partition.py` requires every semantics test to land in exactly one selection.
- The beam-sdk-harness container executes pipeline code by module reference (the e2e gate already ships `tests.semantics._e2e.agent` this way), so conformance pipeline code must be module-level and importable inside the container — no closures, no fixtures captured in the DoFn graph.
- Only LangGraph exists as a framework adapter today; ADK / Pydantic AI are named in the module map but unimplemented. The reference protocol agent is the semantics baseline.

## Goals / Non-Goals

**Goals:**

- One scenario body per lifecycle behavior; adapters differ only in the factory that builds the agent.
- A registry that makes "added an adapter, forgot conformance" a collection error, and "removed a cell" a meta-test failure.
- Flink coverage that reuses the compose stack and harness-freshness machinery without dragging in the effectively-once gate's effector/ledger/spool apparatus.
- Honest per-leg scenario declarations: every (scenario, runner) cell is either run or an explicit, reasoned skip counted by the meta-test.

**Non-Goals:**

- Not a replacement for the effectively-once e2e gate (crash-window accounting, SIGKILLed effectors, 10k-event populations stay there) nor for the per-adapter unit suites (`tests/adapters/` keeps testing LangGraph-specific translation details).
- No new runtime hooks or production-code changes; if a scenario proves inexpressible without one, that's a finding to surface in review, not something this change engineers around.
- No ADK / Pydantic AI factories yet — the registry is built for them, their cells arrive with their adapter changes.
- No Dataflow leg (nightly `-m dataflow` remains a separate concern), no real-provider calls anywhere in the matrix.

## Decisions

### D1 — Adapter seam: a declarative `ConformanceAdapter` entry, not a subclassed harness

Each adapter registers a module-level entry: `name`, `requires` (importable package that gates skipping, e.g. `"langgraph"`; `None` for the reference agent), and `build(scenario: ScenarioSpec) -> AgentBundle` where `AgentBundle` carries the `Agent` callable (or `RunAgent`-ready config), the `ToolRegistry`, and the `FakeLLM` script. The scenario suite owns a canonical `ScenarioSpec` per scenario — the scripted conversation (matcher/response pairs), tool definitions, expected terminal output, expected intent coordinates `(seq, step_index)`, deadlines/TTLs — and each factory translates that one spec into its framework's shape (the LangGraph factory builds a `StateGraph` + `BeamToolNode` + transport-routed model, mirroring `tests/adapters/test_e2e_pipeline.py`).

*Why:* equivalence must be checkable by reading one file per adapter; a shared spec object makes "same conversation, same tools, same behavior" structural rather than aspirational. *Alternative rejected:* per-adapter scenario subclasses (N×M test bodies) — that is exactly the divergence this change exists to prevent.

*Picklability:* factories and every function they emit are module-level in `tests/conformance/_adapters/<name>.py`; the Flink leg references agents by `(adapter, scenario)` string pair resolved worker-side at first activation (lazy build, same shape as the LangGraph e2e test's worker-side construction).

### D2 — Registry enforcement via package introspection

`tests/conformance/_registry.py` lists registrations explicitly. A collection-time check walks `beam_agents.adapters.__path__` for subpackages, subtracts known non-adapter modules, and raises `CollectError` naming any importable adapter subpackage without a registration. The meta-test computes `expected = Σ over (adapter × scenario × leg)` from the registry plus each scenario's per-leg declaration (`legs: {"direct": RUN, "flink": RUN | SKIP(reason)}`) and compares against collected-cell counts gathered by a session-scoped hook.

*Why explicit list + introspection guard, not entry-point autodiscovery:* autodiscovery hides wiring; the guard converts forgetting into a loud failure while keeping registration greppable.

### D3 — Restart-mid-suspension differs by leg, deliberately

- **DirectRunner:** the cell drives `_AgentDoFn` directly over `_dofn_fakes` handles: activation 1 suspends and commits into fake state; the DoFn instance (bridge thread, caches, adapter object) is discarded; a *fresh* `_AgentDoFn` is constructed over the same committed state contents and the matching result element is delivered. Asserts terminal output, trace shape, and intent set equal the uninterrupted run.
- **Flink:** the cell publishes the event, waits for the intent on the intents topic (proof the suspend committed), restarts the TaskManager via the stack helpers, then publishes the result and asserts the terminal output on the output topic.

*Why:* DirectRunner cannot restart a running streaming pipeline, and faking it with a second pipeline run would silently reuse in-memory state. The DoFn-level drive is the strongest honest statement of "resume uses committed protobuf state only," and the Flink leg supplies the real-recovery version of the same claim. *Alternative rejected:* marking the scenario Flink-only — loses the fast, offline, per-PR signal for every adapter.

### D4 — Per-leg scenario declarations

| Scenario | DirectRunner | Flink |
|---|---|---|
| single-shot | TestPipeline/TestStream | Kafka in/out |
| multi-tool inline | TestPipeline/TestStream | Kafka in/out |
| suspension resume | TestPipeline/TestStream | Kafka in/out (harness responder answers intents) |
| approval timeout fallback | TestStream processing-time advance | real-time HITL timer + late-decision feeder (e2e-gate pattern: late answer gated on observing the timeout terminal) |
| restart mid-suspension | fresh-DoFn drive over committed fake state (D3) | TaskManager restart (D3) |
| bundle retry cache | chaos commit failure + Beam bundle retry | **declared skip** — the chaos monkeypatch is in-process and cannot reach the sdk-harness container; the TaskManager-restart cell already exercises real replay on Flink |
| TTL expiry | TestStream watermark advance past TTL | **declared skip** — advancing an unbounded Kafka source's watermark deterministically past a TTL requires idle-partition watermark control the harness doesn't have; TTL is a runtime (not adapter-visible) behavior, so DirectRunner × all adapters retains full coverage |

*Why declared skips over quiet omission:* the spec requires every cell accounted for; the meta-test counts declared skips as cells.

### D5 — Flink leg: minimal responder loop, extracted stack helpers

Split `tests/semantics/_e2e/stack.py`'s reusable parts (compose control, `freshen_flink`, health checks, `InfraFailure`, run-id topic naming) into a shared module (e.g. `tests/semantics/_flink_stack.py`) that both the e2e gate and the conformance leg import; the e2e-specific pieces (spool, ledger, effector supervision) stay where they are. The conformance leg runs one pipeline per (adapter) with all Flink-runnable scenarios multiplexed as distinct keys in one job — one Flink submission per adapter, not per cell — because per-submission cost (jobserver artifact staging, classloader churn) dominates; scenario isolation comes from per-scenario key prefixes, exactly the e2e gate's key-population pattern. The harness-side responder consumes the intents topic and answers tool intents/approval requests deterministically per key prefix (no effector process, no dedup store — dedup correctness is the e2e gate's job).

*Why one-job-per-adapter:* N jobs × M scenarios would put integration wall-clock over budget and hit the known jobserver degradation the stack's freshness machinery exists to contain. *Trade-off accepted:* a scenario failure on Flink is diagnosed from per-key outputs rather than per-test pipelines; assertions therefore name the scenario key prefix in every failure message.

### D6 — Marker and CI wiring

DirectRunner cells: `pytest.mark.semantics` only → selected by the existing required offline `ci` step; no new workflow. Flink cells: `semantics + integration` → new `make test-conformance-flink` target (`-m "semantics and integration" tests/conformance`) added to `integration.yml` alongside `make test-semantics` (kept separate so an e2e-gate timeout and a conformance failure are distinguishable in CI). `check_semantics_partition.py` needs no change — the two markers already partition correctly — but its test list assertions are re-run in CI as today.

## Risks / Trade-offs

- [LangGraph cells double runtime of the offline semantics step] → the offline leg uses small scripted conversations (≤3 model turns); measured budget: the whole DirectRunner matrix must stay under ~2 minutes or scenarios get batched per adapter like the Flink leg.
- [Behavioral-equivalence drift: an adapter factory scripts a subtly different conversation and the matrix "passes" while testing different things] → the shared `ScenarioSpec` is the single source for scripts/expectations, and a fixture-level assertion checks each built bundle against the spec (tool names, matcher count, deadline values) before the pipeline runs.
- [Flink leg flakiness under CI load (real-time HITL timer, TaskManager restart)] → reuse the e2e gate's proven patterns wholesale: terminal-observation gating for late decisions, generous TTLs stamped from event time, `InfraFailure` separation so stack problems never read as adapter failures, per-run topic/consumer-group isolation.
- [Chaos-scenario coupling to `_AgentDoFn._commit`'s signature (monkeypatch)] → already an accepted coupling in the retry-determinism gate; the conformance suite imports the same helper rather than adding a second patch site.
- [DoFn-level restart cell drifts from real runner behavior] → it asserts against the *uninterrupted* run's outputs produced by the same DoFn-level drive (self-consistent baseline), and the Flink restart cell keeps a real-recovery instance of the same scenario.
- [Stack-helper extraction destabilizes the effectively-once gate] → extraction is move-only (same functions, new module, e2e imports updated); the e2e gate runs unchanged in the same integration workflow and gates the refactor itself.

## Findings (surfaced during implementation)

- **Inline tool executions are invisible in `.traces` for the LangGraph adapter.** `TOOL_CALL` trace events are staged only by `ActivationContext.run_tool`; `BeamToolNode` executes read-only tools directly (`registered(**validated)`) and has no context seam, so a LangGraph activation's inline tools produce no trace events. Per Non-Goals (no production changes from this change), the multi-tool scenario proves inline execution via the terminal output (which embeds every tool result, uniformly for all adapters) and asserts model turns via `LLM_CALL` traces; the delta spec was reworded accordingly. If `TOOL_CALL` observability should be adapter-uniform, that is a future runtime/adapter change: give `BeamToolNode` (or the adapter) a path to stage `TOOL_CALL` events — e.g. via the transport hook's activation contextvar.

## Open Questions

- ~~Whether the Flink leg's one-job-per-adapter pipeline can share a single compose stack bring-up across adapters in one pytest session (likely yes via a session fixture + per-adapter `freshen_flink`), or needs a full stack restart per adapter — decided by measured integration wall-clock during implementation; either answer fits the specs and tasks.~~ **Resolved by measurement:** one compose bring-up per session with `freshen_flink` per adapter leg. Each adapter's full leg (freshen + submit + five scenarios including the TaskManager restart and the 30s real-time HITL deadline) measured ~70s locally; both adapters run well under budget, so nothing heavier than the per-adapter Flink-service restart is needed.
