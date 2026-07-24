## Why

Agents need a portable way to declare the tools they can call, and the runtime needs machine-readable schemas for those tools to pass to LLM providers and to validate arguments. Correctness invariant 5 also mandates that side-effecting tools NEVER execute inside the pipeline — they must flow through intents — so the tool layer must draw a hard, enforced line between read-only tools that run inline and side-effecting tools whose direct invocation is a programming error. Nothing in `beam-agents` provides this seam yet; it is the foundation the loop driver, adapters, and effector all build on.

## What Changes

- Add a `@tool` decorator that wraps a Python callable into a registered `Tool`, deriving a JSON tool schema from the function's signature and type hints via a Pydantic v2 argument model.
- Each `@tool` carries a `side_effect: bool` flag (default `False`) declaring whether the tool performs external writes.
- Add a `ToolRegistry` that collects registered tools by name, exposes their provider-facing schemas (`tools_schema`), and resolves a name to its `Tool`.
- Provide an inline `ToolRunner` that executes `side_effect=False` tools directly (validating arguments against the Pydantic model) for the fast path.
- **BREAKING** (of intent, enforced at runtime): directly invoking a `side_effect=True` tool — inline or via the `ToolRunner` — raises `SideEffectToolError`. Side-effecting tools may only be requested through `ctx.act(...)`/intents (implemented by a later change); this change establishes and enforces the guard.
- Export `tool` (and `Tool`, `ToolRegistry`, `ToolRunner`, error types) from `beam_agents.tools` as the capability's public surface, matching `beam_agents.memory`/`beam_agents.model` (the root `beam_agents/__init__.py` stays empty per repo convention).

## Capabilities

### New Capabilities
- `tool-registry`: The `@tool` decorator, Pydantic-derived tool schema generation, the `side_effect` flag, the `ToolRegistry` collection/lookup, the inline `ToolRunner` for read-only tools, and the runtime guard that makes direct invocation of a side-effecting tool raise.

### Modified Capabilities
<!-- None. No existing spec's requirements change. -->

## Impact

- New module `src/beam_agents/tools/` (`registry.py`, `runner.py`, `errors.py`, `__init__.py`), following the existing `memory`/`model` subpackage-as-public-surface pattern.
- Depends on `pydantic` v2 (already a project dependency) for argument-model and schema generation.
- Consumers (future): the loop driver (fast-path inline execution), the model facade (`tools_schema` passed to providers), adapters, and the intents/actions layer (side-effect path). No existing modules change behavior.
- New tests under `tests/tools/`; no protobuf or wire-schema changes.
