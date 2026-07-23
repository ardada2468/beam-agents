## Context

`beam-agents` is an agent runtime, not a framework. Agents (or adapters) declare tools; the runtime needs (a) machine-readable schemas to hand to LLM providers alongside each request, (b) inline execution for read-only tools on the fast path, and (c) a hard enforcement of correctness invariant 5 — side-effecting tools never run inside the pipeline; they flow through intents. This change owns the `tools/` seam that the loop driver, model facade, adapters, and actions/intents layer all depend on. `pydantic` v2 is already a project dependency and is the sanctioned mechanism for tool schemas and constrained JSON. No wire/state schema (protobuf) is involved — tools are in-process constructs, and the *intent* that carries a side-effect request across the bus is defined by a later change.

## Goals / Non-Goals

**Goals:**

- A `@tool` decorator that turns an annotated callable into a `Tool`, usable bare or parameterized.
- Deterministic JSON schema generation from the signature via a generated Pydantic v2 model.
- A `side_effect: bool` flag on every tool, defaulting to `False`.
- A `ToolRegistry` for name→tool resolution and aggregate `tools_schema`.
- An inline `ToolRunner` that validates arguments and executes `side_effect=False` tools.
- A runtime guard: any direct execution of a `side_effect=True` tool raises `SideEffectToolError`.
- `tool` exported from `beam_agents/__init__.py`.

**Non-Goals:**

- The intents/actions path (`ctx.act(...)`, `ToolIntent`, deterministic `intent_id`, outbox, effector) — a later change. This change only enforces that the side-effect path is the *only* legal one by making the direct path raise.
- MCP tool integration (read-only MCP is a separate `tools/` concern in the module map).
- Async tool execution semantics, retries, timeouts, and replay-cache interaction — those belong to the loop driver / model facade.
- Prompt templating or any agent-authoring abstraction (explicitly forbidden by the governing principle).

## Decisions

**1. Pydantic v2 model generated per tool via `pydantic.create_model` from the signature.**
For each parameter we emit a field with `(annotation, default)`, where a parameter without a default becomes required (`...`). The tool's JSON `parameters` object is `Model.model_json_schema()` (optionally massaged to the provider-neutral `{type, properties, required}` shape). Rationale: reuses the project's sanctioned schema mechanism, gives free coercion/validation for the `ToolRunner`, and keeps schema and validator as one source of truth. *Alternative considered:* hand-rolled `inspect.signature` → JSON Schema. Rejected — duplicates Pydantic's type handling and drifts from the validator used at call time.

**2. Missing type annotations fail at decoration time (`ToolDefinitionError`), not call time.**
A parameter with no annotation cannot produce a sound schema or validator. Failing eagerly matches the project's "raise `ValueError`-class errors at construction time with actionable messages" convention and keeps provider schemas total. *Alternative:* default un-annotated params to `str`/`Any`. Rejected — silently produces wrong schemas and hides author mistakes.

**3. `side_effect` guard lives on the `Tool.__call__` path, and `ToolRunner` re-checks.**
Two layers enforce invariant 5: calling a decorated `side_effect=True` tool directly raises, and the `ToolRunner` refuses one before validation. Belt-and-suspenders because both are plausible mistaken entry points and the invariant is a blocking-defect boundary. The decorated tool for a read-only function stays transparently callable so existing call sites and tests behave naturally. *Alternative:* only guard in `ToolRunner`. Rejected — a direct call would bypass it, and adapters may hold the `Tool` object directly.

**4. `ToolRegistry` is a plain in-process object keyed by tool name; duplicates rejected; `tools_schema` is a cached list built from members.**
No global mutable registry — a registry instance is owned by the agent config / loop driver, honoring the "no global mutable state" convention and per-key isolation. *Alternative:* module-level global registry populated by import side effects. Rejected — hidden global state, ordering hazards, and cross-agent leakage.

**5. Typed error taxonomy in `tools/errors.py`:** `ToolError` base, with `ToolDefinitionError`, `ToolNotFoundError`, `ToolArgumentError`, and `SideEffectToolError`. Consistent with routing typed errors and never swallowing exceptions.

**Module layout:** `src/beam_agents/tools/__init__.py` (exports `tool`, `Tool`, `ToolRegistry`, `ToolRunner`, error types), `registry.py` (`tool`, `Tool`, `ToolRegistry`), `runner.py` (`ToolRunner`), `errors.py`. Per repo convention (enforced by `tests/test_import.py::test_public_surface_is_empty`), `beam_agents/__init__.py` re-exports nothing — every capability's public surface lives at its own subpackage `__init__.py`, mirroring `beam_agents.memory` and `beam_agents.model`.

## Risks / Trade-offs

- **Pydantic JSON Schema shape may not match every provider's tool-schema dialect** (Anthropic vs OpenAI function-calling differ in envelope). → Keep `Tool.schema` provider-neutral (`name`/`description`/`parameters`); provider-specific envelope wrapping is the model client's job, not this layer's.
- **Complex/unsupported annotations** (bare `Callable`, forward refs, non-Pydantic-encodable types) could fail model generation. → Surface as `ToolDefinitionError` at decoration with the parameter name; document supported types; callers narrow annotations.
- **Two-layer side-effect guard is mild duplication.** → Accepted deliberately; the invariant it protects is a release-gating correctness boundary (`-m semantics`), so redundancy is warranted.
- **Read-only tools that secretly do I/O** can't be detected by the flag alone. → Out of scope for enforcement; `side_effect` is an author declaration. Documented; the honest contract is the author's responsibility, mirroring how the runtime trusts idempotency declarations elsewhere.

## Migration Plan

Purely additive: a new module and one new public export. No existing behavior, specs, protobuf, or state schema changes, so no `--update` compatibility concern and no rollback migration. Ships behind the normal change→spec→test→code flow; revertable by removing the module and the re-export.

## Open Questions

- Should `tools_schema` normalize Pydantic's `$defs`/`$ref` (nested models) into an inlined form now, or defer inlining to the model client? Leaning: keep raw Pydantic schema here, inline at the provider boundary.
- Do we allow async callables as read-only tools in this change, or restrict to sync and defer async execution to the loop-driver change? Leaning: accept the annotation but defer execution semantics; `ToolRunner` in this change targets sync read-only tools.
