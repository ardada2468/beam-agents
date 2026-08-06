## 1. Scaffolding

- [x] 1.1 Create `src/beam_agents/core/context.py` and `src/beam_agents/core/agent.py` module stubs with module docstrings referencing this change's design.
- [x] 1.2 Add a module-level `INTENT_NAMESPACE` UUID constant (fixed `uuid5` namespace) shared for deterministic `intent_id` derivation.
- [x] 1.3 Add `tests/core/test_context.py` and `tests/core/test_agent.py` test modules (pytest-asyncio auto mode).

## 2. AgentResult (test-first)

- [x] 2.1 Write failing tests for the "AgentResult is the immutable drained bundle" scenarios: result carries every effect category; a clean activation yields an empty result; the value is frozen (mutation attempts fail / do not affect the context).
- [x] 2.2 Implement `AgentResult` as `@dataclass(frozen=True, slots=True)` exposing `outputs: tuple[object, ...]`, `intents: tuple[ToolIntent, ...]`, `traces: tuple[TraceEvent, ...]`, `usage: TokenUsage`, `memory_blob: MemoryBlob | None`, and the replay-cache material; no behavior that mutates state.
- [x] 2.3 Make the AgentResult tests pass.

## 3. AgentContext construction & scope (test-first)

- [x] 3.1 Write failing tests for the "Activation-scoped AgentContext surface" scenarios: context exposes injected `entity_key`/`seq`/`now_ms`; constructing a context produces no effects and an empty drain.
- [x] 3.2 Implement `AgentContext.__init__` with injected scope (`entity_key`, `seq`, `now_ms`), `Memory`, model dependencies, `ReplayCache`, `ToolRunner`, and `ToolRegistry`; build the activation `LlmFacade` with `staging=self` (design D1). No wall-clock/unseeded-randomness/Beam-state access.
- [x] 3.3 Make the construction/scope tests pass.

## 4. Staged-effects accumulator & drain-once (test-first)

- [x] 4.1 Write failing tests for the "Staged-effects accumulator applied only on success" scenarios: effects withheld until drain; drain returns the full bundle; a failed activation (raises before drain) applies nothing; draining twice raises.
- [x] 4.2 Implement the internal accumulator (intents list, traces list, running `TokenUsage`, outputs list) plus the live `Memory`/`ReplayCache` as staged state; add `emit(output)` staging in order.
- [x] 4.3 Implement `drain() -> AgentResult`: snapshot staged state into a frozen `AgentResult` (memory delta from `Memory.dirty`/`to_blob()`), set a `_drained` guard, and raise on a second call.
- [x] 4.4 Make the accumulator/drain tests pass.

## 5. StagingSink conformance (test-first)

- [x] 5.1 Write failing tests for the "Context is the model facade's staging sink" scenarios: an `LlmFacade` built against the context lands its `LLM_CALL` trace in the context bundle; two cache-missing calls sum billed usage into the drained result. Use `FakeLLM` per project convention.
- [x] 5.2 Implement `stage_trace_event(event)` and `accumulate_usage(usage)` so `AgentContext` structurally satisfies `StagingSink`; ensure facade cache inserts stage into the same drain.
- [x] 5.3 Make the staging-sink tests pass; assert `isinstance(ctx, StagingSink)` via the runtime-checkable protocol.

## 6. ctx.act deterministic intents (test-first)

- [x] 6.1 Write failing tests for the "Side effects flow only through ctx.act as deterministic intents" scenarios: `act` stages an intent without executing the tool; `intent_id == uuid5(NAMESPACE, entity_key + seq + step_index)`; `step_index` advances 0,1,2 across three calls; a `side_effect=True` tool cannot execute inline.
- [x] 6.2 Implement `act(tool_name, arguments)`: assert the tool is `side_effect=True` (else `ValueError`), canonical-JSON-serialize arguments (matching `replay_cache` canonicalization), compute deterministic `intent_id`, append a `ToolIntent` carrying `entity_key`/`seq`/`step_index`/`tool_name`/`args_json`, and increment the per-activation `step_index`.
- [x] 6.3 Add a replay-determinism test: the same path issued twice under identical scope produces byte-identical `ToolIntent`s.
- [x] 6.4 Make the `act` tests pass.

## 7. Read-only tool execution (test-first)

- [x] 7.1 Write failing tests for the "Read-only tool execution through the context" scenario: a `side_effect=False` tool runs inline via `ToolRunner`, returns its validated value, and stages no intent; a `side_effect=True` tool run through the read-only path raises.
- [x] 7.2 Implement async `run_tool(tool_name, arguments)` delegating to the injected `ToolRunner.run` (reusing its side-effect guard and async-await handling); optionally stage a `TOOL_CALL` trace.
- [x] 7.3 Make the read-only tool tests pass.

## 8. Output staging (test-first)

- [x] 8.1 Write failing tests for the "Output staging" scenario: outputs A then B are withheld until drain and appear ordered `[A, B]` on the `AgentResult`.
- [x] 8.2 Confirm `emit`/output ordering from task 4.2 satisfies the scenario; add any missing behavior.
- [x] 8.3 Make the output-staging tests pass.

## 9. StreamAgent protocol & FunctionAgent (test-first)

- [x] 9.1 Write failing tests for the "StreamAgent authoring protocol" and "FunctionAgent wraps a plain async function" scenarios: a class with `async def activate(self, ctx)` satisfies `StreamAgent` without a base class; `FunctionAgent(fn)` satisfies `StreamAgent` and `activate(ctx)` awaits `fn(ctx)`, staging its effects.
- [x] 9.2 Implement `StreamAgent` as a `@runtime_checkable` `Protocol` with `async def activate(self, ctx: AgentContext) -> None`.
- [x] 9.3 Implement `FunctionAgent` adapting an `async` callable `fn(ctx)`; `activate` awaits `fn(ctx)`.
- [x] 9.4 Make the protocol/wrapper tests pass.

## 10. Public API & quality gates

- [x] 10.1 Export `StreamAgent`, `AgentContext`, `AgentResult`, `FunctionAgent` from `src/beam_agents/core/__init__.py` (`__all__`), updating its docstring; leave root `src/beam_agents/__init__.py` untouched (`tests/test_import.py::test_public_surface_is_empty` must keep passing).
- [x] 10.2 Run `ruff` (incl. ASYNC rules) and `mypy --strict` clean on the new modules; no `Any` in public signatures.
- [x] 10.3 Run the full unit suite offline (no docker) and confirm every new scenario has a passing test; confirm coverage does not decrease.
- [x] 10.4 Update the change's design references in module docstrings and verify `openspec validate --change add-agent-context` passes.
