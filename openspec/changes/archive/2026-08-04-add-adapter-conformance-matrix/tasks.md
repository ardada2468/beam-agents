## 1. Conformance seam and registry

- [x] 1.1 Create `tests/conformance/` package with `ScenarioSpec` (scripted FakeLLM conversation, tool definitions, expected terminal output, expected `(seq, step_index)` intent coordinates, deadlines/TTLs, per-leg declarations `{"direct": RUN, "flink": RUN | SKIP(reason)}`) and the seven canonical scenario specs (single-shot, multi-tool-inline, suspension-resume, approval-timeout-fallback, restart-mid-suspension, bundle-retry-cache, ttl-expiry)
- [x] 1.2 Add `tests/conformance/_registry.py`: `ConformanceAdapter` entry (`name`, `requires`, module-level `build(spec) -> AgentBundle`), explicit registration list, and the collection-time guard that walks `beam_agents.adapters` subpackages and raises a `CollectError` naming any importable adapter without a registration
- [x] 1.3 Add the bundle-equivalence fixture check: every built `AgentBundle` is validated against its `ScenarioSpec` (tool names, matcher count, deadline/TTL values) before any pipeline runs
- [x] 1.4 Unit-test the registry guard and equivalence check themselves (unregistered fake adapter package → collection error; mismatched bundle → assertion naming the divergence)

## 2. Reference protocol adapter + DirectRunner scenarios (offline leg)

- [x] 2.1 Implement the reference adapter factory (`tests/conformance/_adapters/reference.py`): module-level `Agent` functions realizing each `ScenarioSpec` directly against `ActivationContext`
- [x] 2.2 Write the parameterized DirectRunner suite for the pipeline-expressible scenarios — single-shot, multi-tool inline, suspension resume — on a streaming `TestPipeline`/`TestStream`, asserting on `.output`/`.intents`/`.traces`/`.errors` and the deterministic `intent_id_for` expectation; mark `semantics`, verify it runs green with only the reference adapter registered
- [x] 2.3 Write the approval-timeout-fallback DirectRunner scenario: `TestStream` processing-time advance past the HITL deadline → fallback terminal output + cleared continuation; late decision after the timer → `orphaned_result` on `.errors`, no second terminal
- [x] 2.4 Write the restart-mid-suspension DirectRunner scenario via the fresh-DoFn drive over `_dofn_fakes` state (suspend with instance 1, rebuild `_AgentDoFn` over the committed state contents, deliver the result), asserting output/trace/intent parity with the uninterrupted baseline run
- [x] 2.5 Write the bundle-retry-cache DirectRunner scenario reusing `beam_agents.testing.chaos.fail_first_matching_commit` on the resume commit: zero additional `cache_hit == "false"` provider calls and byte-identical committed intent versus the no-retry expectation
- [x] 2.6 Write the TTL-expiry DirectRunner scenario: memory written, second event before TTL sees it, `TestStream` watermark advance past TTL, third event sees empty memory

## 3. LangGraph adapter factory

- [x] 3.1 Implement `tests/conformance/_adapters/langgraph.py`: module-level factory translating each `ScenarioSpec` into a `StateGraph` + `BeamToolNode` + transport-routed model (worker-side lazy build, `pytest.importorskip` gating), following the `tests/adapters/test_e2e_pipeline.py` shape
- [x] 3.2 Register LangGraph in `_registry.py` and bring all seven DirectRunner cells green for it; confirm the unit lane (no `langgraph` installed) reports its cells as clean skips

## 4. Matrix accounting

- [x] 4.1 Implement the meta-test: expected cell count computed from registry × scenario × per-leg declarations (declared skips counted), compared against actually collected cells via a session-scoped collection hook
- [x] 4.2 Verify `scripts/check_semantics_partition.py` passes with the new tests and that the offline semantics selection picks up every DirectRunner cell; measure the offline leg's wall-clock and batch scenarios per adapter if it exceeds the ~2-minute budget

## 5. Flink leg

- [x] 5.1 Extract the reusable stack machinery (compose control, `freshen_flink`, health checks, `InfraFailure`, run-id topic naming) from `tests/semantics/_e2e/stack.py` into a shared module; move-only refactor, e2e gate imports updated, `make test-semantics` still green
- [x] 5.2 Build the conformance Flink pipeline builder: one job per adapter multiplexing the Flink-runnable scenarios as per-scenario key prefixes over Kafka in/out, plus the harness-side responder that answers tool intents and approval requests deterministically by key prefix (late-decision answers gated on observing the timeout terminal, per the e2e-gate pattern)
- [x] 5.3 Write the Flink cells (marked `semantics` + `integration`): single-shot, multi-tool inline, suspension resume, approval timeout fallback, and restart-mid-suspension via TaskManager restart between the observed intent and the result publish; bundle-retry-cache and ttl-expiry report as declared skips with their recorded reasons
- [x] 5.4 Run the full Flink leg for both adapters against the compose stack; resolve the open question (shared stack bring-up per session vs. per-adapter restart) from measured wall-clock

## 6. CI wiring and docs

- [x] 6.1 Add `make test-conformance-flink` (`-m "semantics and integration"` over `tests/conformance`) and wire it into `integration.yml` as a step distinct from `make test-semantics`
- [x] 6.2 Update `openspec/project.md` testing-tier notes to record the adapter conformance matrix (offline leg in the required `ci` semantics selection, Flink leg in `integration`)
- [x] 6.3 Full verification pass: `ruff`, `mypy --strict` on touched `src/` (none expected), offline suite (`pytest -m "semantics and not integration"`), partition check, and the docker-backed legs (`make test-semantics`, `make test-conformance-flink`)
