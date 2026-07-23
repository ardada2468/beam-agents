## Why

The runtime has facades for the pieces an activation uses — `Memory`, `LlmFacade`, `ToolRunner`, `ReplayCache` — but nothing that ties them into a single activation surface an agent programs against, and nothing that guarantees correctness invariant #1 (atomic commit). Today each facade stages its own effects independently, and `LlmFacade` already depends on a `StagingSink` protocol (its D6) that no component yet provides. We need the activation-scoped object that owns that staging, the agent-facing protocol that receives it, and the result type the DoFn commits from — so that a failed or timed-out activation provably mutates nothing.

## What Changes

- Introduce **`AgentContext`**: the single activation-scoped object handed to an agent for one element. It exposes the injected clock and activation scope (`entity_key`, `seq`, `step_index`), the working `Memory`, the `LlmFacade`, read-only tool execution, and `act(...)` to declare a side effect as a `ToolIntent`. It is the concrete `StagingSink` the model facade already expects.
- Introduce a **staged-effects accumulator** inside `AgentContext`: memory mutations, replay-cache inserts, `ToolIntent`s, `TraceEvent`s, and outputs are collected in the context and never applied to keyed state or emitted mid-activation. The context exposes a way to *drain* the staged effects exactly once, which only the DoFn (on activation success) calls.
- Introduce **`AgentResult`**: the immutable value an activation produces — the drained bundle of outputs, intents, traces, and the next memory/cache blobs — that the DoFn commits atomically with the Beam bundle.
- Introduce the **`StreamAgent` protocol**: the async agent-authoring contract, `async def activate(ctx: AgentContext) -> None` (or returning an output), that any framework adapter or hand-written agent implements. This is a runtime protocol, not an authoring DSL.
- Introduce **`FunctionAgent`**: a thin wrapper adapting a plain `async` function `(ctx) -> ...` into a `StreamAgent`, so the simplest agents need no class.
- Enforce that side effects only flow through `ctx.act(...)` (correctness invariant #5): a `side_effect=True` tool is never executed inline; `act` records a deterministic-id `ToolIntent` into the staged bundle instead.
- Export `StreamAgent`, `AgentContext`, `AgentResult`, and `FunctionAgent` from `beam_agents.core`'s package `__init__.py`, following the established per-capability convention (`model`, `memory`, `tools` each export from their own package root). Root `beam_agents/__init__.py` stays empty per repo convention until `RunAgent`/`AgentConfig` land (enforced by `tests/test_import.py::test_public_surface_is_empty`).

## Capabilities

### New Capabilities
- `agent-context`: the activation-scoped `AgentContext`, its staged-effects accumulator, the `AgentResult` it drains into, the `StreamAgent` authoring protocol, and the `FunctionAgent` wrapper.

### Modified Capabilities
<!-- None. model-facade's StagingSink is a structural protocol AgentContext satisfies; no model-facade requirement changes. -->

## Impact

- **New code**: `core/context.py` (`AgentContext`, `AgentResult`, staged-effects accumulator), `core/agent.py` (`StreamAgent` protocol, `FunctionAgent`). New `agent-context` spec.
- **Public API**: `beam_agents/core/__init__.py` gains `StreamAgent`, `AgentContext`, `AgentResult`, `FunctionAgent` (capability-level surface); root `beam_agents/__init__.py` is untouched.
- **Consumes** existing facades: `memory.Memory`, `model.LlmFacade` (satisfies its `StagingSink`), `model.ReplayCache`, `tools.ToolRunner`/`ToolRegistry`, and the `ToolIntent`/`TraceEvent` protos.
- **Unblocks**: the stateful `RunAgent` DoFn (`core/dofn.py`), which will construct an `AgentContext` per `process()`, invoke the agent, and commit the drained `AgentResult` atomically. Deterministic `intent_id` derivation and the loop driver build on this surface.
- **No breaking changes**; no dependency changes.
