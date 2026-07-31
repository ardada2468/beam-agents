# Tasks: add-pydantic-ai-adapter

## 1. Tests (written first, must fail for the right reason)

- [x] 1.1 Import-isolation tests (spec: "Core import works without the extra"):
      `import beam_agents` succeeds with Pydantic AI absent (module-import blocking,
      same harness as the LangGraph isolation test), and accessing
      `beam_agents.PydanticAIAgent` raises `ImportError` naming
      `beam-agents[pydantic-ai]`; guard all other adapter tests with
      `pytest.importorskip("pydantic_ai")`
      — `tests/adapters/pydantic_ai/test_import_isolation.py` (3 tests)
- [x] 1.2 History-persistence tests (specs: "Failed activation leaves no history
      mutation", "Conversation continues across activations on the same key",
      "History expires with the memory TTL"): serialize/deserialize round-trip under
      `__pydantic_ai__/messages`, committed-`MemoryBlob` byte equality after a failed
      run, cross-activation history continuity over a fresh context on the committed
      blob, TTL-cleared memory yielding a fresh conversation, and `MemoryOverflow`
      failing closed on an oversized history
      — `tests/adapters/pydantic_ai/test_history.py` (5 tests)
- [x] 1.3 Fast-path test (spec: "Fast-path run completes in one activation"):
      FakeLLM-backed agent with a read-only tool returns `Complete` with the encoded
      output and commits history under the reserved namespace
      — `tests/adapters/pydantic_ai/test_agent_fast_path.py`
- [x] 1.4 Suspension/resume tests (specs: "Deferred side-effect call suspends instead
      of executing", "Re-injected result resumes the run with the deferred result",
      "Parallel deferred calls resume after all results arrive", "Approval-requiring
      call maps to an approval intent", "Bundle replay stages byte-identical
      intents"): deferred call → exactly one staged `ToolIntent` with the
      deterministic `intent_id` and zero tool-body executions; result re-injection
      resumes with the original tool call ID; two-call accumulation re-suspends
      staging nothing new; approval flow through the approval channel; identical
      re-execution from identical committed state stages byte-identical intents
      — `tests/adapters/pydantic_ai/test_agent_deferred.py` (7 tests, incl. the
      denial path and the unknown-intent fail-closed guard)
- [x] 1.5 Inline-tool test (spec: "Read-only tool runs inline with a trace event"):
      `side_effect=False` call executes via `ctx.run_tool`, returns its value to the
      conversation, stages a `TOOL_CALL` trace event, stages no intent
      — `test_agent_fast_path.py::test_read_only_tool_runs_inline_with_a_trace_event`
- [x] 1.6 Transport tests (specs: "Recognized model is replay-cached across retries",
      "Unrecognized model warns once and falls back"): a layout double shaped like a
      Pydantic AI provider model (SDK client exposing an `httpx.AsyncClient`) rerun
      from identical state makes zero provider HTTP requests on the second run; an
      unrecognized model warns exactly once per agent instance, increments the
      fallback counter, and still completes; the hoisted shared transport keeps the
      existing LangGraph transport tests green unchanged
      — `tests/adapters/pydantic_ai/test_transport_hook.py` (3 tests);
      `tests/adapters/test_transport_hook.py` unchanged and green
- [x] 1.7 Usage test (spec: "Completed run reports usage in the tally"): a completed
      run folds the framework's reported usage into the tally
      (`total_tokens` > 0, `usage_observed`)
      — `test_agent_fast_path.py::test_completed_run_reports_usage_in_the_tally`
- [x] 1.8 Conformance expectations (specs: "All seven scenarios pass on both legs",
      "Missing extra skips cells without shrinking the matrix silently"): with the
      registration in place the meta-test's expected cell count includes the
      `pydantic_ai` axis entry; before the factories exist these cells must fail for
      the right reason (missing factory/registration), not skip
      — `test_matrix.py` audits 3 adapters × 7 scenarios × 2 legs = 42 cells;
      `test_harness_unit.py::test_pydantic_ai_cells_skip_cleanly_when_the_framework_is_absent`

## 2. Packaging and scaffolding

- [x] 2.1 Add the `pydantic-ai` optional extra to `pyproject.toml`
      (`pydantic-ai-slim`, range pinned against the current release and the lockfile;
      confirm co-resolution with the core `pydantic>=2` constraint), mirror it into
      the test dependency group, and run `uv sync`
      — extra `pydantic-ai = ["pydantic-ai-slim>=2.21,<3"]` plus the same pin
      mirrored into the `test` group; `uv pip install pydantic-ai-slim` resolved
      2.21.0 against the existing `pydantic>=2` with no conflict.
      **`uv.lock` deliberately NOT regenerated** (out of scope for this execution) —
      a follow-up `uv lock` is required before CI's `uv sync --locked` passes. See
      the Revision note below.
- [x] 2.2 Create `src/beam_agents/adapters/pydantic_ai/__init__.py`; add the lazy
      `__getattr__` branch re-exporting `PydanticAIAgent` from
      `beam_agents/__init__.py` with the actionable `ImportError` for the missing
      extra
- [x] 2.3 Hoist the framework-neutral transport core (`_ReplayTransport`, the
      activation contextvar, `warn_fallback` + the `transport_fallback` counter) from
      `adapters/langgraph/transport.py` into `adapters/_transport.py`; move-only, with
      the LangGraph module re-exporting the moved names so existing imports and tests
      are untouched
      — `src/beam_agents/adapters/_transport.py`; the LangGraph module keeps
      `_ReplayTransport` / `_current_activation` / `_METRIC_NAMESPACE` /
      `_FALLBACK_COUNTER` / `_RESERVED_BODY_KEYS` re-exports and its own
      `find_async_client` / `install_transport` / `warn_fallback` signatures
      unchanged; `tests/adapters/*` untouched and green

## 3. History persistence

- [x] 3.1 Implement `history.py`: message-list serialization via the framework's
      documented message `TypeAdapter` round-trip, stored latest-only as the
      `__pydantic_ai__/messages` scalar through the activation's `Memory` facade;
      document the cap/trimming guidance and the compactor no-evict rule in the
      module docstring
- [x] 3.2 Resolve the serialization-stability open question: decide whether the
      scalar carries a version tag, and add the golden-blob compat test if it does
      — **no version tag**: `ModelMessagesTypeAdapter` validates on read, so a
      schema break surfaces as a loud validation error that fails the activation
      closed rather than as silent corruption. Rationale recorded in the module
      docstring; no golden-blob test needed.

## 4. PydanticAIAgent core (fast path)

- [x] 4.1 Implement `PydanticAIAgent` as a runtime `Agent`: decode the event into the
      run input, load committed history, run the wrapped agent on the bridge loop
      with the activation exposed to the shared transport contextvar, persist
      `all_messages()` back through the facade, encode `Complete` from the run
      output; resolve the output-type composition open question (adapter-declared
      union vs. user-declared) and document the chosen contract
      — **adapter-declared, per-run**: every run passes
      `output_type=[wrapped.output_type, DeferredToolRequests]` as a per-run
      override, so a user agent is wrapped as-is and its own output type keeps
      validating terminals. Documented in `agent.py`'s module docstring.

## 5. BeamToolset: inline execution and deferred suspension

- [x] 5.1 Implement `toolset.py` over runtime `Tool` objects: `side_effect=False`
      executors routed through `ctx.run_tool`; `side_effect=True` tools declared
      deferred/externally-executed with their pydantic argument schemas visible to
      the model; approval-requiring declaration wired for approval-gated tools
      — one `AbstractToolset` mapping runtime tools onto the framework's
      `function` / `external` / `unapproved` tool kinds
- [x] 5.2 Implement the suspension path: deferred-requests run end → one
      `ctx.act`/`ctx.request_approval` per pending call, the JSON correlation
      snapshot (`intent_id → {kind, tool_call_id}` + collected results), and
      `Suspend(adapter="pydantic_ai")` with the HITL timeout plumbed
- [x] 5.3 Implement resume: decode the snapshot, fold the incoming
      `ToolResult`/`Approval` in (unknown intent fails closed, as in the LangGraph
      adapter), re-suspend while calls remain unanswered, and when complete re-run
      the agent with committed history plus the built deferred-results value

## 6. Transport recognition and usage

- [x] 6.1 Implement `transport.py`: the Pydantic AI recognition table (probe the
      pinned range's model-object layouts for the SDK client's
      `httpx.AsyncClient`; settle the exact attribute chain by inspection),
      idempotent installation via the shared `_ReplayTransport`, and the
      once-per-instance fallback warning
      — settled by reading the installed package: `models/openai.py:922` and
      `models/anthropic.py:548` both expose the SDK client as the `client`
      property, whose `_client` is the httpx client → probe table `("client",)`
- [x] 6.2 Implement usage accumulation: map the run's reported usage onto
      `TokenUsage` (settle the field-name open question) and call
      `ctx.accumulate_usage` once per run segment
      — settled by inspection: `AgentRunResult.usage` is a **property** (not a
      method) returning `RunUsage(input_tokens, output_tokens, …, total_tokens)`
      → `TokenUsage(prompt_tokens=input_tokens, completion_tokens=output_tokens,
      total_tokens=total_tokens)`

## 7. Conformance registration and matrix runs

- [x] 7.1 Implement `tests/conformance/_adapters/pydantic_ai.py`: module-level
      factories translating each `ScenarioSpec` into a Pydantic AI agent
      (`BeamToolset` tools, approval path, transport-instrumented model,
      lazy framework imports, worker-side rebuild by name) and a provider factory
      producing exactly one FakeLLM rule per spec turn wrapping the shared
      `turn_response` directive bytes; resolve design D8's model-object choice
      (provider-flavored model vs. custom `Model` double) against measured
      dependency weight
      — **custom `Model` double**: a provider-flavored model would pull the
      `openai`/`anthropic` SDK into the test group for zero added scenario
      coverage; SDK-layout recognition is covered by the doubles in
      `tests/adapters/pydantic_ai/`
- [x] 7.2 Register the `pydantic_ai` `ConformanceAdapter`
      (`requires="pydantic_ai"`, `adapters_subpackage="pydantic_ai"`) in
      `tests/conformance/_registry.py`; confirm the registry guard passes with the
      extra installed and the cells skip cleanly without it
      — also registered in `tests/conformance/_flink/pipeline.py`'s
      `_RULE_BUILDERS` so the Flink leg's multiplexed provider covers the new axis
- [x] 7.3 Bring all seven DirectRunner cells green; measure the offline semantics
      selection against the ~2-minute budget and batch per adapter if exceeded;
      verify `scripts/check_semantics_partition.py` still passes
      — `pytest tests/conformance/test_direct.py -m "not integration"`: 21 passed
      (7 scenarios × 3 adapters); offline semantics selection 43 passed in **22s**
      (well under the ~2-minute budget, no batching needed);
      `check_semantics_partition.py`: 43 offline + 22 docker = 65 total, OK
- [x] 7.4 Bring the Flink leg green for the `pydantic_ai` axis entry (five runnable <!-- discharged by verify-live-infrastructure phase 2 (2026-07-31): `make test-conformance-flink` on a cold compose stack -> 20 passed, 8 skipped, 0 failed (6 m 54 s); all four adapters green, the declared skips reported as skips. The `pydantic_ai` axis required fixing the SDK-harness image, which was missing `pydantic-ai-slim` (report defect D-4). `make test-semantics` also re-run green: 1 passed, 482.77 s at default volume. See verification-report.md. -->
      scenarios including the TaskManager-restart cell; bundle_retry_cache and
      ttl_expiry report as the matrix's declared skips) via
      `make test-conformance-flink` against the compose stack
      **(blocked: needs docker)** — the registration and the `_RULE_BUILDERS` entry
      the leg needs are in place; the leg itself cannot run in this environment.
- [x] 7.5 Docs: adapter section in README/docs (adoption steps: re-tag side-effect
      tools, hand tools to the adapter, wrap the agent; history size guidance;
      unrecognized-model fallback); update the CI unit matrix to install the
      `pydantic-ai` extra
      — README "Running a Pydantic AI agent" section; the CI unit matrix needs no
      workflow edit (it runs `uv sync --group test`, and the extra is mirrored into
      the `test` group, the same pattern as `langgraph`)

## 8. Gates

- [x] 8.1 `make lint` — ruff check + format --check clean (214 files)
- [x] 8.2 `make type` (mypy --strict clean on the new package and the transport
      hoist) — `Success: no issues found in 212 source files`
- [x] 8.3 `make test-unit` (offline; adapter cells skip cleanly where the extra is
      absent) — 968 passed, 1 skipped (aiokafka absent), 106 deselected
- [x] 8.4 `make test-conformance-flink` (with `make test-semantics` re-run to prove <!-- discharged by verify-live-infrastructure phase 2 (2026-07-31): `make test-conformance-flink` on a cold compose stack -> 20 passed, 8 skipped, 0 failed (6 m 54 s); all four adapters green, the declared skips reported as skips. The `pydantic_ai` axis required fixing the SDK-harness image, which was missing `pydantic-ai-slim` (report defect D-4). `make test-semantics` also re-run green: 1 passed, 482.77 s at default volume. See verification-report.md. -->
      the transport hoist did not disturb the e2e gate)
      **(blocked: needs docker)** — the offline half is green:
      `make test-semantics-offline` 43 passed, 1 skipped
- [x] 8.5 Coverage ratchet passes (`make coverage-ratchet`; coverage may never <!-- discharged by verify-live-infrastructure gates (2026-07-31): `make coverage-ratchet` -> `branch coverage 91.64% is at baseline`. See verification-report.md. -->
      decrease) **(deferred: runs against CI's coverage.xml artifact)**
- [x] 8.6 `uv run pre-commit run --all-files` <!-- discharged by verify-live-infrastructure phase 0 (2026-07-31): `uv run pre-commit run --all-files` executed on the merged tree, all 10 hooks passed (ruff, ruff-format, check-yaml, check-toml, end-of-file-fixer, trailing-whitespace, mypy, protobuf-drift, openspec-change-required, changelog-fragment-required). See verification-report.md. -->
      **(blocked: `uv sync --locked` fails until `uv lock` is regenerated for the
      new extra; ruff / ruff-format / mypy — the hooks' substance — are green above)**
- [x] 8.7 `openspec validate add-pydantic-ai-adapter --strict`

## Revision (implementation findings vs. the proposal/design)

Settled by reading the installed `pydantic-ai-slim` 2.21.0 source rather than by
guessing. Nothing contradicted the specs; four details are now pinned:

1. **Deferred-tool API spellings** (design Open Question 1). The names are
   `pydantic_ai.DeferredToolRequests` (fields `calls` / `approvals`, both
   `list[ToolCallPart]`), `pydantic_ai.DeferredToolResults` (field `calls` keyed
   by `tool_call_id`, field `approvals` likewise), and `ToolApproved` /
   `ToolDenied` for the decision values. A tool is declared deferred by its
   `ToolDefinition.kind`: `"external"` for externally-executed, `"unapproved"`
   for approval-gated. `DeferredToolRequests` must be a member of the run's
   `output_type` union for a run to end at a deferred call. Version floor
   `>=2.21`: both kinds and `Agent.run(deferred_tool_results=...)` are available
   there.
2. **Usage is a property, not a method** (Open Question 4).
   `AgentRunResult.usage` — `result.usage()` raises `TypeError`. Fields are
   `input_tokens` / `output_tokens` with a derived `total_tokens`.
3. **Recognition probing** (Open Question 2) is `model.client` → SDK client →
   `._client`: one probe entry, not the LangChain-style three.
4. **Pydantic AI always closes a run through the model.** After deferred results
   are applied the framework issues one more model request, and it rejects an
   empty text output. This is invisible to the adapter (it is the framework's own
   control flow) but it shapes the conformance factory: a scenario whose script
   ends at a deferred call (`bundle_retry_cache`) needs a terminal turn that adds
   **zero** provider calls, so the factory's scripted model answers locally —
   without posting — once the script's turns are exhausted, with a sentinel
   `encode_output` drops. The rule count still equals `len(spec.turns)`,
   preserving `validate_bundle`'s equivalence check.

Two collateral edits the change made necessary, both minimal:

- `tests/conformance/_registry.py::unregistered_adapters` now skips
  underscore-prefixed names. `adapters/_transport.py` is a private shared module,
  not an adapter subpackage, and the guard would otherwise fail collection
  demanding a conformance registration for it.
- `tests/test_import.py` and
  `tests/conformance/test_harness_unit.py::test_langgraph_cells_skip_cleanly_…`
  hard-coded the pre-change surface (`__all__` without `PydanticAIAgent`;
  `"1 passed"` for a two-adapter axis). Both now derive from the registry
  (`len(ADAPTERS) - 1`) instead of a literal, and the Pydantic AI clean-skip
  scenario got its own mirror test.
