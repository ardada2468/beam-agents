# Tasks: add-adk-adapter

## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 Import-isolation tests (spec: "Core import works without the extra"):
      `import beam_agents` succeeds with ADK absent (module-import blocking, covering
      the `google.adk` namespace-package wrinkle from design D8), and accessing
      `beam_agents.AdkAgent` raises `ImportError` naming `beam-agents[adk]`; guard all
      other adapter tests with `pytest.importorskip("google.adk")`
- [ ] 1.2 Session tests (spec: "Failed activation leaves no partial session", "Worker
      failover resumes from the committed session", "One session per key"): session
      round-trip over a `Memory` facade under the reserved `__adk__/` namespace with
      `state_delta` application, committed-`MemoryBlob` byte equality after a failing
      run, resume over a rebuilt service on a fresh instance, single-session listing,
      and `MemoryOverflow` fail-closed on an oversized session
- [ ] 1.3 Fast-path tests (spec: "Fast-path run completes in one activation"):
      FakeLLM-backed ADK agent with model + read-only shim tools returns `Complete`
      with the final output and commits the session under the reserved namespace
- [ ] 1.4 Shim suspension tests (spec: "Side-effect tool call suspends instead of
      executing", "Read-only tool executes inline", "ToolResult resumes the run as a
      function response", "Parallel side-effect calls resume after all results
      arrive", "Bundle replay stages byte-identical intents"): side-effect callable
      never runs in-pipeline, deterministic `intent_id`s, accumulate-then-resume over
      the JSON resume-map snapshot, function-response identity matching, and
      byte-identical intents across a re-run from identical committed state
- [ ] 1.5 Approval tests (spec: "Approval request suspends with a staged approval
      intent", "Approval decision resumes the run"): APPROVAL-kind intent on the
      approval channel with stamped TTL, `Suspend(adapter="adk")` arming the HITL
      timer, and decision delivery as the approval call's function response
- [ ] 1.6 Trace-tee tests (spec: "Inline tool executions appear as TOOL_CALL trace
      events", "Trace bytes are replay-deterministic"): `TOOL_CALL` events with
      `beam_agents.adapter == "adk"` correlated to the activation's trace/span
      identity, and byte-identical staged traces across two executions from identical
      committed state (asserting no ADK event ids/timestamps leak into trace bytes)
- [ ] 1.7 Transport tests (spec: "Recognized client is replay-cached across retries",
      "Unrecognized client warns once and falls back"): recognized layout double served
      through the replay transport with zero provider HTTP requests on the second run;
      unrecognized model warns exactly once per agent instance and increments the
      fallback counter; plus move-only regression: the existing LangGraph transport
      tests pass unchanged against the hoisted `adapters/_transport` module
- [ ] 1.8 Conformance registration test (spec: "Unregistered ADK package fails
      collection", "All seven scenarios pass on both legs"): with the `adk` subpackage
      importable and no registry entry, `enforce_registry()` raises naming `adk`; the
      meta-test's expected-cell accounting includes the third adapter

## 2. Packaging and scaffolding

- [ ] 2.1 Add the `adk` optional extra to `pyproject.toml` (`google-adk>=1.0,<2`; exact
      floor set against the current release), include it in the test dependency group,
      and run `uv sync` to update the lockfile
- [ ] 2.2 Create `src/beam_agents/adapters/adk/__init__.py` re-exporting `AdkAgent`,
      `BeamSessionService`, and the shim; add the lazy `__getattr__` branch for
      `AdkAgent` in `beam_agents/__init__.py` with dotted-prefix `ModuleNotFoundError`
      detection (`google.adk`, `google.genai`) per design D8
- [ ] 2.3 Hoist the httpx transport hook to `src/beam_agents/adapters/_transport.py`
      (move-only: `_ReplayTransport`, `_current_activation`, `install_transport`,
      `warn_fallback`, fallback metric); turn `adapters/langgraph/transport.py` into a
      re-exporting shell; add the `google-genai` client layout to the recognition table
      (resolving the design Open Question against the pinned SDK)

## 3. BeamSessionService

- [ ] 3.1 Implement `BeamSessionService` over the activation's `Memory` facade:
      canonical-JSON session envelope at `__adk__/session`, `append_event` applying
      `state_delta` and appending to the event history, idempotent `create_session`
      for the fixed per-key identity, single-session `get_session`/`list_sessions`,
      async-first per ADK's ABC (all I/O staged in-memory; nothing blocks the bridge
      loop), and the off-by-default `max_events` trimming knob; resolve the exact
      abstract-method set against the pinned version (design Open Question)

## 4. AdkAgent core (fast path)

- [ ] 4.1 Implement `AdkAgent` as a runtime `Agent`: per-activation `Runner`
      construction over `BeamSessionService` (in-memory artifact/memory services if the
      pinned `Runner` requires them), session ensure, event decode → `new_message`,
      `run_async` drained on the bridge loop inside the shared activation contextvar,
      `Complete` from the final response; instrument `chat_models` once per instance
      (`install_transport` / `warn_fallback`)

## 5. Tool tagging shim and suspension/resume

- [ ] 5.1 Implement the shim (`tools.py`): `beam_tools([...])` mapping runtime `Tool`
      objects to ADK tools — read-only inline with `argument_model` validation,
      `side_effect=True` as long-running declarations feeding the per-activation
      collector — plus `BeamApprovalTool` on the runtime approval channel
- [ ] 5.2 Implement the suspension path in `AdkAgent`: pending collected calls →
      `ctx.act` per tool call / `ctx.request_approval` for approvals, JSON resume-map
      snapshot (`intent_id → {kind, function_call_id, tool_name}` + collected
      results), `Suspend(adapter="adk")` with the HITL timeout plumbed
- [ ] 5.3 Implement resume: decode snapshot, fold the incoming `ToolResult`/`Approval`
      (unknown intent fails closed), re-suspend while intents remain unanswered, and
      when complete build the function-response message (original function-call
      identity per result) and re-run the `Runner` over the committed session

## 6. Event-stream tee

- [ ] 6.1 Implement the tee (`events.py`): inline shim executions staged as `TOOL_CALL`
      via `ActivationTrace.tool_call` (own `tool_index` counter, never the intent step
      cursor) enriched with `beam_agents.adapter="adk"` and author attributes, staged
      through `ctx.stage_trace_event`; enforce the determinism rules (activation clock
      only; no ADK ids/timestamps in trace bytes); document that non-tool, non-model
      ADK events (transfers, escalations) are deferred to the `ADAPTER_EVENT`
      follow-up (design D7)

## 7. Conformance registration

- [ ] 7.1 Implement `tests/conformance/_adapters/adk.py`: `build_adk_agent(spec)` /
      `build_adk_provider(spec)` translating each `ScenarioSpec` via the shared
      directive vocabulary (`turn_response`), the shim-wrapped scenario tools, the
      approval shim, and a model seam posting provider-shaped JSON through an
      httpx client the transport hook instruments (ADK imports lazy inside the build
      functions; worker-side construction)
- [ ] 7.2 Register the `adk` `ConformanceAdapter` (requires `google.adk`,
      `adapters_subpackage="adk"`) in `tests/conformance/_registry.py`; bring all
      seven DirectRunner cells green; confirm clean skips in the no-extra unit lane
- [ ] 7.3 Extend the Flink leg's one-job-per-adapter builder with the ADK job (same
      key-prefix multiplexing and responder); bring the Flink-runnable cells green
      including the TaskManager-restart cell; verify the meta-test cell accounting and
      `scripts/check_semantics_partition.py` pass with the third adapter
- [ ] 7.4 Docs: adapter section (adoption steps: re-tag, wrap with `beam_tools`, pass
      `AdkAgent`; session size guidance; approval shim usage); CI lanes install the
      `adk` extra alongside `langgraph`

## 8. Gates

- [ ] 8.1 `make lint`
- [ ] 8.2 `make type` (mypy --strict on the new package and the hoisted transport)
- [ ] 8.3 `make test-unit` (offline; ADK and LangGraph suites green, clean skips
      without extras)
- [ ] 8.4 `make test-conformance-flink` (full matrix including the ADK leg, against
      the compose stack)
- [ ] 8.5 Coverage ratchet (`make coverage-ratchet`) — coverage may not decrease
- [ ] 8.6 `uv run pre-commit run --all-files`
- [ ] 8.7 `openspec validate add-adk-adapter --strict`
