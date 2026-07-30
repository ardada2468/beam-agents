# Tasks: add-pydantic-ai-adapter

## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 Import-isolation tests (spec: "Core import works without the extra"):
      `import beam_agents` succeeds with Pydantic AI absent (module-import blocking,
      same harness as the LangGraph isolation test), and accessing
      `beam_agents.PydanticAIAgent` raises `ImportError` naming
      `beam-agents[pydantic-ai]`; guard all other adapter tests with
      `pytest.importorskip("pydantic_ai")`
- [ ] 1.2 History-persistence tests (specs: "Failed activation leaves no history
      mutation", "Conversation continues across activations on the same key",
      "History expires with the memory TTL"): serialize/deserialize round-trip under
      `__pydantic_ai__/messages`, committed-`MemoryBlob` byte equality after a failed
      run, cross-activation history continuity over a fresh context on the committed
      blob, TTL-cleared memory yielding a fresh conversation, and `MemoryOverflow`
      failing closed on an oversized history
- [ ] 1.3 Fast-path test (spec: "Fast-path run completes in one activation"):
      FakeLLM-backed agent with a read-only tool returns `Complete` with the encoded
      output and commits history under the reserved namespace
- [ ] 1.4 Suspension/resume tests (specs: "Deferred side-effect call suspends instead
      of executing", "Re-injected result resumes the run with the deferred result",
      "Parallel deferred calls resume after all results arrive", "Approval-requiring
      call maps to an approval intent", "Bundle replay stages byte-identical
      intents"): deferred call → exactly one staged `ToolIntent` with the
      deterministic `intent_id` and zero tool-body executions; result re-injection
      resumes with the original tool call ID; two-call accumulation re-suspends
      staging nothing new; approval flow through the approval channel; identical
      re-execution from identical committed state stages byte-identical intents
- [ ] 1.5 Inline-tool test (spec: "Read-only tool runs inline with a trace event"):
      `side_effect=False` call executes via `ctx.run_tool`, returns its value to the
      conversation, stages a `TOOL_CALL` trace event, stages no intent
- [ ] 1.6 Transport tests (specs: "Recognized model is replay-cached across retries",
      "Unrecognized model warns once and falls back"): a layout double shaped like a
      Pydantic AI provider model (SDK client exposing an `httpx.AsyncClient`) rerun
      from identical state makes zero provider HTTP requests on the second run; an
      unrecognized model warns exactly once per agent instance, increments the
      fallback counter, and still completes; the hoisted shared transport keeps the
      existing LangGraph transport tests green unchanged
- [ ] 1.7 Usage test (spec: "Completed run reports usage in the tally"): a completed
      run folds the framework's reported usage into the tally
      (`total_tokens` > 0, `usage_observed`)
- [ ] 1.8 Conformance expectations (specs: "All seven scenarios pass on both legs",
      "Missing extra skips cells without shrinking the matrix silently"): with the
      registration in place the meta-test's expected cell count includes the
      `pydantic_ai` axis entry; before the factories exist these cells must fail for
      the right reason (missing factory/registration), not skip

## 2. Packaging and scaffolding

- [ ] 2.1 Add the `pydantic-ai` optional extra to `pyproject.toml`
      (`pydantic-ai-slim`, range pinned against the current release and the lockfile;
      confirm co-resolution with the core `pydantic>=2` constraint), mirror it into
      the test dependency group, and run `uv sync`
- [ ] 2.2 Create `src/beam_agents/adapters/pydantic_ai/__init__.py`; add the lazy
      `__getattr__` branch re-exporting `PydanticAIAgent` from
      `beam_agents/__init__.py` with the actionable `ImportError` for the missing
      extra
- [ ] 2.3 Hoist the framework-neutral transport core (`_ReplayTransport`, the
      activation contextvar, `warn_fallback` + the `transport_fallback` counter) from
      `adapters/langgraph/transport.py` into `adapters/_transport.py`; move-only, with
      the LangGraph module re-exporting the moved names so existing imports and tests
      are untouched

## 3. History persistence

- [ ] 3.1 Implement `history.py`: message-list serialization via the framework's
      documented message `TypeAdapter` round-trip, stored latest-only as the
      `__pydantic_ai__/messages` scalar through the activation's `Memory` facade;
      document the cap/trimming guidance and the compactor no-evict rule in the
      module docstring
- [ ] 3.2 Resolve the serialization-stability open question: decide whether the
      scalar carries a version tag, and add the golden-blob compat test if it does

## 4. PydanticAIAgent core (fast path)

- [ ] 4.1 Implement `PydanticAIAgent` as a runtime `Agent`: decode the event into the
      run input, load committed history, run the wrapped agent on the bridge loop
      with the activation exposed to the shared transport contextvar, persist
      `all_messages()` back through the facade, encode `Complete` from the run
      output; resolve the output-type composition open question (adapter-declared
      union vs. user-declared) and document the chosen contract

## 5. BeamToolset: inline execution and deferred suspension

- [ ] 5.1 Implement `toolset.py` over runtime `Tool` objects: `side_effect=False`
      executors routed through `ctx.run_tool`; `side_effect=True` tools declared
      deferred/externally-executed with their pydantic argument schemas visible to
      the model; approval-requiring declaration wired for approval-gated tools
- [ ] 5.2 Implement the suspension path: deferred-requests run end → one
      `ctx.act`/`ctx.request_approval` per pending call, the JSON correlation
      snapshot (`intent_id → {kind, tool_call_id}` + collected results), and
      `Suspend(adapter="pydantic_ai")` with the HITL timeout plumbed
- [ ] 5.3 Implement resume: decode the snapshot, fold the incoming
      `ToolResult`/`Approval` in (unknown intent fails closed, as in the LangGraph
      adapter), re-suspend while calls remain unanswered, and when complete re-run
      the agent with committed history plus the built deferred-results value

## 6. Transport recognition and usage

- [ ] 6.1 Implement `transport.py`: the Pydantic AI recognition table (probe the
      pinned range's model-object layouts for the SDK client's
      `httpx.AsyncClient`; settle the exact attribute chain by inspection),
      idempotent installation via the shared `_ReplayTransport`, and the
      once-per-instance fallback warning
- [ ] 6.2 Implement usage accumulation: map the run's reported usage onto
      `TokenUsage` (settle the field-name open question) and call
      `ctx.accumulate_usage` once per run segment

## 7. Conformance registration and matrix runs

- [ ] 7.1 Implement `tests/conformance/_adapters/pydantic_ai.py`: module-level
      factories translating each `ScenarioSpec` into a Pydantic AI agent
      (`BeamToolset` tools, approval path, transport-instrumented model,
      lazy framework imports, worker-side rebuild by name) and a provider factory
      producing exactly one FakeLLM rule per spec turn wrapping the shared
      `turn_response` directive bytes; resolve design D8's model-object choice
      (provider-flavored model vs. custom `Model` double) against measured
      dependency weight
- [ ] 7.2 Register the `pydantic_ai` `ConformanceAdapter`
      (`requires="pydantic_ai"`, `adapters_subpackage="pydantic_ai"`) in
      `tests/conformance/_registry.py`; confirm the registry guard passes with the
      extra installed and the cells skip cleanly without it
- [ ] 7.3 Bring all seven DirectRunner cells green; measure the offline semantics
      selection against the ~2-minute budget and batch per adapter if exceeded;
      verify `scripts/check_semantics_partition.py` still passes
- [ ] 7.4 Bring the Flink leg green for the `pydantic_ai` axis entry (five runnable
      scenarios including the TaskManager-restart cell; bundle_retry_cache and
      ttl_expiry report as the matrix's declared skips) via
      `make test-conformance-flink` against the compose stack
- [ ] 7.5 Docs: adapter section in README/docs (adoption steps: re-tag side-effect
      tools, hand tools to the adapter, wrap the agent; history size guidance;
      unrecognized-model fallback); update the CI unit matrix to install the
      `pydantic-ai` extra

## 8. Gates

- [ ] 8.1 `make lint`
- [ ] 8.2 `make type` (mypy --strict clean on the new package and the transport hoist)
- [ ] 8.3 `make test-unit` (offline; adapter cells skip cleanly where the extra is
      absent)
- [ ] 8.4 `make test-conformance-flink` (with `make test-semantics` re-run to prove
      the transport hoist did not disturb the e2e gate)
- [ ] 8.5 Coverage ratchet passes (`make coverage-ratchet`; coverage may never
      decrease)
- [ ] 8.6 `uv run pre-commit run --all-files`
- [ ] 8.7 `openspec validate add-pydantic-ai-adapter --strict`
