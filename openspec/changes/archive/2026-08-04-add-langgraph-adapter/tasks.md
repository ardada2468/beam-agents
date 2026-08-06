# Tasks: add-langgraph-adapter

## 1. Packaging and scaffolding

- [x] 1.1 Add the `langgraph` optional extra to `pyproject.toml` (pin `langgraph`/
      `langgraph-checkpoint`/`langchain-core` ranges against the current release),
      include it in the test dependency group, and run `uv sync` to update the lockfile
- [x] 1.2 Create `src/beam_agents/adapters/__init__.py` and
      `src/beam_agents/adapters/langgraph/__init__.py`; add the lazy `__getattr__`
      re-export of `LangGraphAgent` in `beam_agents/__init__.py` with the actionable
      `ImportError` for missing extras
- [x] 1.3 Write the import-isolation tests first (spec: "Core import works without the
      extra"): `import beam_agents` succeeds with LangGraph absent (simulate via
      module-import blocking), and the attribute access error names
      `beam-agents[langgraph]`; guard all other adapter tests with
      `pytest.importorskip("langgraph")`

## 2. BeamCheckpointSaver

- [x] 2.1 Write failing tests from the checkpoint scenarios: latest-only retention,
      `aget_tuple`/`aput`/`aput_writes`/`alist` contract over a `Memory` facade,
      reserved `__langgraph__/` namespace, failed-activation-leaves-no-mutation
      (assert committed `MemoryBlob` byte equality), and mid-graph failover resume
      (rebuild saver over the committed blob on a fresh instance; superstep-N nodes do
      not re-execute)
- [x] 2.2 Implement `BeamCheckpointSaver` over the activation's `Memory` facade:
      `JsonPlusSerializer` serde, `__langgraph__/ckpt` + `__langgraph__/writes`
      scalars, latest-only overwrite semantics, async methods as the implementation
      with sync delegation (no bridge-loop blocking)
- [x] 2.3 Verify cap behavior: an oversized checkpoint raises `MemoryOverflow` and the
      activation fails closed to errors with no partial state; document the trimming
      guidance in the module docstring

## 3. LangGraphAgent core (fast path)

- [x] 3.1 Write failing tests for the fast path: FakeLLM-backed graph with model +
      read-only tool nodes returns `Complete` with the final output and commits the
      checkpoint under the reserved namespace
- [x] 3.2 Implement `LangGraphAgent` as a runtime `Agent`: per-activation config
      (`thread_id` from entity key), checkpointer installation, `ainvoke` on the
      bridge loop, `Complete` construction from the final state

## 4. Interrupt → approval mapping with Command resume

- [x] 4.1 Write failing tests from the interrupt scenarios: `interrupt(payload)`
      yields `Suspend` + exactly one staged approval intent with the deterministic
      `intent_id`; re-running the identical activation stages byte-identical intents;
      approval re-injection resumes via `Command(resume=...)` to completion
- [x] 4.2 Implement interrupt detection after `ainvoke` (pending `__interrupt__`
      state), `request_approval()` staging, the JSON resume-map snapshot
      (`intent_id → {kind, tool_call_id, interrupt_id}` + collected results), and
      `Suspend(adapter="langgraph")` with the HITL timeout plumbed
- [x] 4.3 Implement resume: decode snapshot, record the incoming `Approval`/
      `ToolResult`, re-suspend while intents remain unanswered, invoke
      `Command(resume=...)` when complete

## 5. ToolNode shim

- [x] 5.1 Write failing tests from the shim scenarios: side-effect call stages a
      `ToolIntent` and never executes the callable; read-only call runs inline with a
      `ToolMessage` result; `ToolResult` resumes as a `ToolMessage` with the original
      `tool_call_id`; two parallel side-effect calls re-suspend on the first result
      and resume on the second; the re-tagging-only adoption scenario (take a plain
      graph, re-tag + swap node, run through intents)
- [x] 5.2 Implement `BeamToolNode` over runtime `Tool` objects: inline execution for
      `side_effect=False` (validated args), `GraphInterrupt` carrying side-effect tool
      calls, and answered-call replay to `ToolMessage`s on node re-execution
- [x] 5.3 Wire the shim's interrupt payload into `LangGraphAgent`'s suspension path
      (`ctx.act` per tool call) and its results into the resume `Command` payload

## 6. httpx transport hook

- [x] 6.1 Write failing tests from the transport scenarios: recognized httpx-backed
      client (test double shaped like langchain-anthropic/openai) is retried from
      identical state with zero provider HTTP requests on the second run; unrecognized
      model warns exactly once per DoFn instance, increments the fallback counter, and
      still completes
- [x] 6.2 Implement `_ReplayTransport` (provider request body → `LlmRequest` →
      `ctx.call_model` → synthesized `httpx.Response`) and client recognition/
      installation with the worker-local once-per-instance warning guard and the
      `transport_fallback` Beam metric

## 7. Verification and finish

- [x] 7.1 End-to-end adapter test on TestPipeline: an existing-style graph adopted by
      re-tagging runs suspend → re-inject → resume across activations with
      byte-identical intents under a forced bundle retry (chaos wrapper), zero extra
      FakeLLM calls, and mid-graph failover resume on a fresh DoFn instance
- [x] 7.2 `make lint`, `make type` (mypy --strict on the new package), `make test-unit`
      offline; confirm no LangGraph import leaks into core modules (import-time check)
- [x] 7.3 Docs: adapter section in README/docs (adoption steps: re-tag, swap node,
      pass adapter; node re-execution semantics; checkpoint size guidance); update
      CI unit matrix to install the `langgraph` extra
