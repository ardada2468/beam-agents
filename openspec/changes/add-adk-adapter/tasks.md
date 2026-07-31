# Tasks: add-adk-adapter

## Revision (implementation findings against `google-adk` 2.6.0)

Resolved against the installed package source, not guessed. Four items where
reality diverged from the proposal; artifacts updated to match.

1. **Version pin.** The proposal assumed `google-adk>=1.0,<2`; the current
   release is **2.6.0**, so the extra pins `google-adk>=2.6,<3`. Everything
   below was verified against that version.
2. **`BaseSessionService` surface** (Open Question, resolved). The ABC's
   abstract set is `create_session` / `get_session` / `list_sessions` /
   `delete_session`, all async and keyword-only; `append_event` is concrete
   (it applies `state_delta` and skips partials) and is overridden for
   persistence. No deprecated sync variants are abstract, so D3's conditional
   never applies. `GetSessionConfig` (`num_recent_events`, `after_timestamp`)
   and `ListSessionsResponse` are honored.
3. **Long-running call surfacing and resume shape** (Open Question, resolved).
   Both detection paths exist and are used together: the shim's collector
   records the call (`ToolContext.function_call_id` carries ADK's
   `adk-<uuid>` id) and `Event.long_running_tool_ids` + `get_function_calls()`
   give the model's own **ordering**, which is what the adapter stages intents
   in — ADK executes parallel calls concurrently, so completion order is not
   replay-stable but the event stream's order is. Resume is a `types.Content`
   with `role="user"` whose parts are `types.FunctionResponse(id=..., name=...,
   response={...})`; `Runner` is constructed with `auto_create_session=True`
   and needs no artifact/memory services (fourth Open Question: they are
   optional, left `None`).
4. **Model seam** (Open Question, resolved). The transport hook is the v1
   decision, as proposed. The recognized layout is
   `model.api_client._api_client._async_httpx_client` (ADK `Gemini` →
   `google.genai.Client` → `BaseApiClient` → `httpx.AsyncClient`), verified by
   inspecting a real `genai.Client`. `Gemini.api_client` is a
   `cached_property` that *constructs* the client and raises without
   credentials, so the probe is exception-guarded: an unconstructable client
   reads as unrecognized (warning fallback), never as an activation failure.

Two behavioral divergences required artifact edits:

- **The user's agent object is not left byte-identical.** The spec said the
  agent "SHALL NOT be mutated". Verified against **stock ADK with no adapter
  involved**: `Runner.run_async` sets an unset root `LlmAgent.mode` to
  `"chat"`. That is ADK normalizing its own field, idempotent, and the adapter
  neither performs nor can suppress it. The requirement now states what is
  actually guaranteed and testable — no restructuring (sub-agents, tools,
  model) and no adapter state on the agent — and names the ADK normalization
  explicitly.
- **`bundle_retry_cache` is not expressible for ADK.** That scenario's premise
  is a resume that issues *no novel model request*, so the chaos-retried
  resume is served entirely from the suspend-committed replay cache. ADK's
  resume always drives one summarization turn after a function response, which
  is a novel request; a discarded attempt therefore legitimately repeats it.
  Forcing the cell green would have meant weakening the assertion, so instead
  the conformance vocabulary gained a **per-adapter skip declaration**
  (mirroring the existing per-leg `Skip`): the cell is still collected,
  counted by the meta-test, and reported with its reason. The adapter's
  replay-cache guarantee is covered by its own
  `recognized_client_is_replay_cached_across_retries` test and by the
  `restart_mid_suspension` cell; intent-byte determinism by
  `test_bundle_replay_stages_byte_identical_intents`. Both the `adk-adapter`
  and `adapter-conformance-matrix` deltas were updated.

One implementation choice worth recording: sessions are persisted with
`model_dump(mode="json", exclude_none=True)`. ADK's `Event`/`Content` models
carry dozens of optional fields whose nulls cost ~4x the blob for zero
information (measured 7035 → 1865 bytes on a two-turn session) — real headroom
against the 1 MiB cap. It is lossless: pydantic's `exclude_none` drops only
model fields whose value is `None`, never entries inside `state`, so a
session-state key explicitly set to `None` survives the round-trip (asserted).

## 1. Tests (written first, must fail for the right reason)

- [x] 1.1 Import-isolation tests (spec: "Core import works without the extra"):
      `import beam_agents` succeeds with ADK absent (module-import blocking, covering
      the `google.adk` namespace-package wrinkle from design D8), and accessing
      `beam_agents.AdkAgent` raises `ImportError` naming `beam-agents[adk]`; guard all
      other adapter tests with `pytest.importorskip("google.adk")`
- [x] 1.2 Session tests (spec: "Failed activation leaves no partial session", "Worker
      failover resumes from the committed session", "One session per key"): session
      round-trip over a `Memory` facade under the reserved `__adk__/` namespace with
      `state_delta` application, committed-`MemoryBlob` byte equality after a failing
      run, resume over a rebuilt service on a fresh instance, single-session listing,
      and `MemoryOverflow` fail-closed on an oversized session
- [x] 1.3 Fast-path tests (spec: "Fast-path run completes in one activation"):
      FakeLLM-backed ADK agent with model + read-only shim tools returns `Complete`
      with the final output and commits the session under the reserved namespace
- [x] 1.4 Shim suspension tests (spec: "Side-effect tool call suspends instead of
      executing", "Read-only tool executes inline", "ToolResult resumes the run as a
      function response", "Parallel side-effect calls resume after all results
      arrive", "Bundle replay stages byte-identical intents"): side-effect callable
      never runs in-pipeline, deterministic `intent_id`s, accumulate-then-resume over
      the JSON resume-map snapshot, function-response identity matching, and
      byte-identical intents across a re-run from identical committed state
- [x] 1.5 Approval tests (spec: "Approval request suspends with a staged approval
      intent", "Approval decision resumes the run"): APPROVAL-kind intent on the
      approval channel with stamped TTL, `Suspend(adapter="adk")` arming the HITL
      timer, and decision delivery as the approval call's function response
- [x] 1.6 Trace-tee tests (spec: "Inline tool executions appear as TOOL_CALL trace
      events", "Trace bytes are replay-deterministic"): `TOOL_CALL` events with
      `beam_agents.adapter == "adk"` correlated to the activation's trace/span
      identity, and byte-identical staged traces across two executions from identical
      committed state (asserting no ADK event ids/timestamps leak into trace bytes)
- [x] 1.7 Transport tests (spec: "Recognized client is replay-cached across retries",
      "Unrecognized client warns once and falls back"): recognized layout double served
      through the replay transport with zero provider HTTP requests on the second run;
      unrecognized model warns exactly once per agent instance and increments the
      fallback counter. The move-only regression against a hoisted
      `adapters/_transport` is **not part of this change** — see 2.3: the hoist is
      owned by a parallel change, so this adapter ships its own local
      `adapters/adk/transport.py` seam and the LangGraph transport module is
      untouched (its existing tests pass unchanged, verified in the unit run)
- [x] 1.8 Conformance registration test (spec: "Unregistered ADK package fails
      collection", "All seven scenarios pass on both legs"): with the `adk` subpackage
      importable and no registry entry, `enforce_registry()` raises naming `adk`; the
      meta-test's expected-cell accounting includes the third adapter

## 2. Packaging and scaffolding

- [x] 2.1 Added the `adk` optional extra to `pyproject.toml` (`google-adk>=2.6,<3` —
      floor set against the current release, see Revision 1) and mirrored it into the
      `test` dependency group, so every CI lane that syncs `test` installs it (the same
      mechanism the `langgraph` extra uses; no workflow edits needed).
      **`uv.lock` was deliberately NOT regenerated** (parallel-work constraint: the
      lockfile is shared and must not move in this change). The coordinator MUST run
      `uv lock` before merge — `uv sync --locked` in `ci.yml` will otherwise fail on the
      pyproject/lock mismatch. Everything here was verified against `google-adk 2.6.0`
      installed in the worktree venv
- [x] 2.2 Create `src/beam_agents/adapters/adk/__init__.py` re-exporting `AdkAgent`,
      `BeamSessionService`, and the shim; add the lazy `__getattr__` branch for
      `AdkAgent` in `beam_agents/__init__.py` with dotted-prefix `ModuleNotFoundError`
      detection (`google.adk`, `google.genai`) per design D8
- [ ] 2.3 Hoist the httpx transport hook to `src/beam_agents/adapters/_transport.py`
      **(deliberately NOT done here: a parallel change owns this refactor)**. This
      adapter ships an equivalent local seam,
      `src/beam_agents/adapters/adk/transport.py`, carrying the google-genai
      recognition table (`api_client._api_client._async_httpx_client`, resolved against
      the pinned SDK — Revision 4) and sharing the LangGraph adapter's
      `beam_agents.adapters/transport_fallback` counter namespace. The module docstring
      flags the follow-up: once `adapters/_transport` lands, this module should delegate
      to it, keeping only the recognition table. `adapters/langgraph/transport.py` is
      untouched by this change

## 3. BeamSessionService

- [x] 3.1 Implement `BeamSessionService` over the activation's `Memory` facade:
      canonical-JSON session envelope at `__adk__/session`, `append_event` applying
      `state_delta` and appending to the event history, idempotent `create_session`
      for the fixed per-key identity, single-session `get_session`/`list_sessions`,
      async-first per ADK's ABC (all I/O staged in-memory; nothing blocks the bridge
      loop), and the off-by-default `max_events` trimming knob; resolve the exact
      abstract-method set against the pinned version (design Open Question)

## 4. AdkAgent core (fast path)

- [x] 4.1 Implement `AdkAgent` as a runtime `Agent`: per-activation `Runner`
      construction over `BeamSessionService` (in-memory artifact/memory services if the
      pinned `Runner` requires them), session ensure, event decode → `new_message`,
      `run_async` drained on the bridge loop inside the shared activation contextvar,
      `Complete` from the final response; instrument `chat_models` once per instance
      (`install_transport` / `warn_fallback`)

## 5. Tool tagging shim and suspension/resume

- [x] 5.1 Implement the shim (`tools.py`): `beam_tools([...])` mapping runtime `Tool`
      objects to ADK tools — read-only inline with `argument_model` validation,
      `side_effect=True` as long-running declarations feeding the per-activation
      collector — plus `BeamApprovalTool` on the runtime approval channel
- [x] 5.2 Implement the suspension path in `AdkAgent`: pending collected calls →
      `ctx.act` per tool call / `ctx.request_approval` for approvals, JSON resume-map
      snapshot (`intent_id → {kind, function_call_id, tool_name}` + collected
      results), `Suspend(adapter="adk")` with the HITL timeout plumbed
- [x] 5.3 Implement resume: decode snapshot, fold the incoming `ToolResult`/`Approval`
      (unknown intent fails closed), re-suspend while intents remain unanswered, and
      when complete build the function-response message (original function-call
      identity per result) and re-run the `Runner` over the committed session

## 6. Event-stream tee

- [x] 6.1 Implement the tee (`events.py`): inline shim executions staged as `TOOL_CALL`
      via `ActivationTrace.tool_call` (own `tool_index` counter, never the intent step
      cursor) enriched with `beam_agents.adapter="adk"` and author attributes, staged
      through `ctx.stage_trace_event`; enforce the determinism rules (activation clock
      only; no ADK ids/timestamps in trace bytes); document that non-tool, non-model
      ADK events (transfers, escalations) are deferred to the `ADAPTER_EVENT`
      follow-up (design D7)

## 7. Conformance registration

- [x] 7.1 Implement `tests/conformance/_adapters/adk.py`: `build_adk_agent(spec)` /
      `build_adk_provider(spec)` translating each `ScenarioSpec` via the shared
      directive vocabulary (`turn_response`), the shim-wrapped scenario tools, the
      approval shim, and a model seam posting provider-shaped JSON through an
      httpx client the transport hook instruments (ADK imports lazy inside the build
      functions; worker-side construction)
- [x] 7.2 Register the `adk` `ConformanceAdapter` (requires `google.adk`,
      `adapters_subpackage="adk"`) in `tests/conformance/_registry.py`; bring all
      seven DirectRunner cells green; confirm clean skips in the no-extra unit lane
- [x] 7.3 Extended the Flink leg's one-job-per-adapter builder with the ADK job
      (`_RULE_BUILDERS["adk"] = adk_rules`, same key-prefix multiplexing and responder)
      and added `google-adk` to `docker/sdk-harness.Dockerfile` so the framework is
      importable worker-side. Meta-test cell accounting and
      `scripts/check_semantics_partition.py` both verified green with the third adapter
      (43 offline + 22 docker = 65). **Running the Flink cells is blocked: needs docker**
      — the leg itself is wired, not executed (task 8.4)
- [x] 7.4 Docs: adoption steps (re-tag with `@tool(side_effect=True)`, wrap with
      `beam_tools`, pass `AdkAgent`), session-size guidance (`max_events`, the 1 MiB
      cap, `MemoryOverflow` fail-closed), and approval-shim usage are documented in the
      package/module docstrings (`adapters/adk/__init__.py`, `tools.py`, `session.py`,
      `agent.py`) — the same place the LangGraph adapter documents its adoption story;
      `docs/` has no adapter page for either adapter, so none was invented. CI lanes get
      the extra via the `test` dependency group (task 2.1)

## 8. Gates

- [x] 8.1 `make lint`
- [x] 8.2 `make type` (mypy --strict on the new package and the hoisted transport)
- [x] 8.3 `make test-unit` (offline; ADK and LangGraph suites green, clean skips
      without extras)
- [x] 8.4 `make test-conformance-flink` (full matrix including the ADK leg, against <!-- discharged by verify-live-infrastructure phase 2 (2026-07-31): `make test-conformance-flink` on a cold compose stack -> 20 passed, 8 skipped, 0 failed (6 m 54 s); all four adapters green, the declared skips reported as skips. The `pydantic_ai` axis required fixing the SDK-harness image, which was missing `pydantic-ai-slim` (report defect D-4). `make test-semantics` also re-run green: 1 passed, 482.77 s at default volume. See verification-report.md. -->
      the compose stack) — **(blocked: needs docker)**. The DirectRunner leg was run
      instead and is green: 20 passed + 1 declared adapter skip
- [x] 8.5 Coverage ratchet (`make coverage-ratchet`) — **(not run: the shared <!-- discharged by verify-live-infrastructure gates (2026-07-31): `make coverage-ratchet` -> `branch coverage 91.64% is at baseline`. See verification-report.md. -->
      checkout's `pyproject.toml` was mid-merge-conflict from a parallel worker for part
      of this session, breaking `uv` workspace discovery, so the coverage-instrumented
      `make` target could not run; the unit tier itself was run directly and is green)**
- [x] 8.6 `uv run pre-commit run --all-files` — **(not run: same `uv` workspace-discovery <!-- discharged by verify-live-infrastructure phase 0 (2026-07-31): `uv run pre-commit run --all-files` executed on the merged tree, all 10 hooks passed (ruff, ruff-format, check-yaml, check-toml, end-of-file-fixer, trailing-whitespace, mypy, protobuf-drift, openspec-change-required, changelog-fragment-required). See verification-report.md. -->
      breakage as 8.5; `ruff check`, `ruff format --check`, and `mypy --strict` — the
      hooks that cover this change's files — were all run directly and are clean)**
- [x] 8.7 `openspec validate add-adk-adapter --strict` (green after the artifact edits
      recorded in the Revision section)

## 9. Revision: registry-derived counts after a second adapter landed (integration)

- [x] 9.1 `tests/conformance/test_adk_registration.py` asserted `len(ADAPTERS) == 3` and literal
  `"2 passed"`, which were true when this change was written against a two-adapter matrix. The
  Pydantic AI adapter (C39) registered a fourth entry in the same merge window, so both assertions
  failed on the integrated branch. Rewritten to derive from the registry — `any(adapter.name ==
  "adk" ...)` and `f"{len(ADAPTERS) - 1} passed"` — which is what the change's own spec means by
  "adding an adapter must not silently invalidate the matrix accounting", and matches the fix C39
  applied to `tests/test_import.py` and `test_harness_unit.py`. Verified: `pytest
  tests/conformance -m "not integration"` 42 passed, 1 declared skip.
- [x] 9.2 `_run_single_shot_without` in `test_harness_unit.py` matched blocked frameworks on the
  top-level module name, which cannot express ADK (it imports under the `google` namespace package
  that core installs already populate — blocking `"google"` would take out google-cloud too).
  Generalized to an exact-or-dotted-prefix match, backward compatible with the existing
  top-level-name callers. This change's own clean-skip test keeps its dedicated subprocess.
