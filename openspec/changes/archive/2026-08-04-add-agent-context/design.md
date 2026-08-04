## Context

The runtime already has the facades an activation composes: `Memory` (staged working memory over a `MemoryBlob`), `LlmFacade` (resilient async model calls that stage traces/usage and cache inserts), `ReplayCache`, and the `@tool`/`ToolRunner`/`ToolRegistry` layer. Two seams are deliberately left open for this change:

1. `LlmFacade` is constructed with a `StagingSink` (its D6) — `stage_trace_event` + `accumulate_usage` — but nothing in the tree provides one.
2. Correctness invariant #1 (atomic commit) requires *all* effects — memory writes, cache inserts, intents, traces, outputs — to be staged in "the activation context" and applied only on success. That context does not exist yet.

This change builds that context (`AgentContext`), the immutable bundle it drains into (`AgentResult`), and the agent-facing surface (`StreamAgent`, `FunctionAgent`). It is the join point between the facades and the not-yet-built stateful `RunAgent` DoFn (`core/dofn.py`), which will construct one context per `process()` call, invoke the agent, and — only if the activation completes — commit the drained `AgentResult` atomically with the Beam bundle.

Load-bearing constraints from `project.md`: agent authoring belongs to adapters/frameworks (no DSL, no prompt templating); side effects only via `ctx.act(...)` (invariant #5); deterministic `intent_id = uuid5(NAMESPACE, key + seq + step_index)` (invariant #2); no wall-clock/unseeded randomness inside an activation (replay determinism); async-first internals; `mypy --strict`, no `Any` in public signatures; public surface is only what `__init__.py` re-exports.

## Goals / Non-Goals

**Goals:**
- One activation-scoped `AgentContext` that owns *all* effect staging and exposes memory, model, read-only tools, and `act(...)`.
- Provide the concrete `StagingSink` the `LlmFacade` already expects, so traces/usage/cache inserts route into the same bundle.
- Deterministic, replay-stable `ToolIntent` production via a per-activation `step_index` counter and `uuid5` IDs.
- A drain-once path yielding an immutable `AgentResult` the DoFn commits atomically; a non-drained activation mutates nothing.
- Minimal authoring contract: `StreamAgent` protocol + `FunctionAgent` wrapper, re-exported publicly.

**Non-Goals:**
- The DoFn itself, keyed state I/O, timers, the async bridge thread — those live in `core/dofn.py` (a later change). This change defines what the DoFn drains, not the draining site.
- Suspension/`Continuation`, re-injection, HITL, the loop driver that iterates model↔tool turns. `act` here only *stages* an intent; the suspend/resume machinery is separate.
- Any agent-authoring convenience beyond wrapping a plain async function. No DSL, no templating.
- Multi-turn intent execution or effector semantics (dedup, expiry) — those are the effector's and DoFn's concern.

## Decisions

### D1 — `AgentContext` is a plain object constructed per activation with everything injected

Constructor takes `entity_key: bytes`, `seq: int`, `now_ms: int`, the activation `Memory`, an `LlmFacade` factory or an already-wired facade, a `ToolRunner` + `ToolRegistry`, and the `ReplayCache`. No global state, no ambient clock. Rationale: replay determinism (invariant #3/#2) requires that two runs of the same bundle see identical inputs; injection is how every other facade in this repo achieves it (`Memory` D1 frozen clock, `LlmFacade` injected `now_ms`/`rng`/`sleep`). Alternative considered: a context that lazily reads Beam state handles — rejected, it would couple this pure surface to the DoFn and break offline unit testing.

Because `LlmFacade` needs a `StagingSink` at *its* construction, the cleanest wiring is: DoFn builds the `AgentContext` first, then builds the `LlmFacade` with `staging=ctx` and hands it in (or the context lazily builds the facade from an injected provider + the context as sink). We choose **context-owns-facade-construction from an injected provider**: `AgentContext` receives the provider `LLMClient`, `ReplayCache`, breaker, retry policy, decode, `rng`, `sleep`, and builds the `LlmFacade` with `staging=self`. This keeps the "context is the single staging sink" invariant structurally impossible to violate and gives the agent `ctx.model.complete(...)` directly.

### D2 — The staged-effects accumulator is internal mutable lists/refs, drained once into an immutable `AgentResult`

The context holds: the live `Memory` (already a staging facade — its `to_blob()` + `dirty` are the memory delta), a list of `ToolIntent`, a list of `TraceEvent`, a `TokenUsage` running sum, a list of outputs, and the `ReplayCache` (whose inserts are themselves staged in cache state until the DoFn commits). `drain()` snapshots these into a frozen `AgentResult` and flips a `_drained` flag; a second `drain()` raises. Rationale: invariant #1 wants a single, auditable "apply set." Making `AgentResult` frozen (`@dataclass(frozen=True, slots=True)`, tuples not lists) prevents the DoFn from accidentally mutating half-applied state. Alternative: have each facade expose its own drain — rejected; five independent drains is exactly the fragmentation invariant #1 warns against.

Draining is a pure read of already-staged state; it performs no I/O. Only the DoFn calls it, and only after `await agent.activate(ctx)` returns normally. If `activate` raises or the bridge times out and cancels, the DoFn simply never calls `drain()` and discards the context — nothing was ever applied, so "a failed activation mutates nothing" holds by construction, not by cleanup.

### D3 — `act()` stages a deterministic `ToolIntent`; `step_index` is a per-activation counter

`ctx.act(tool_name, arguments)` looks up the tool, asserts `side_effect=True` (calling `act` on a read-only tool is a misuse → `ValueError`), serializes `arguments` to canonical JSON (sorted keys, matching the cache-key canonicalization already used in `replay_cache`), computes `intent_id = uuid5(NAMESPACE, entity_key + str(seq) + str(step_index))`, appends the `ToolIntent`, and increments `step_index`. `step_index` starts at 0 per activation. Rationale: invariant #2 makes this the "entire effectively-once argument" — the effector dedups on `intent_id`, so the ID must be a pure function of path position. Using a monotonic per-activation counter (not a hash of arguments) means a replay that walks the same path emits byte-identical intents even if the agent recomputes arguments slightly differently in a non-semantic way — but see risk R2. `NAMESPACE` is a fixed module-level `uuid5` namespace constant shared with the DoFn/effector.

`act` does NOT execute the tool and does NOT await anything — it only records. The tool result arrives later via re-injection (out of scope here).

### D4 — Read-only tools run through the existing async `ToolRunner`

`ctx.run_tool(tool_name, arguments)` (async) fetches the tool from the registry and delegates to `ToolRunner.run`, which already refuses `side_effect=True` tools and awaits awaitable results. The context optionally stages a `TOOL_CALL` trace. Rationale: reuse the two-layer side-effect guard already implemented (`Tool.__call__` + `ToolRunner.run`); do not reimplement validation. This keeps the fast path (read-only tools inline) distinct from the re-injection path (`act`).

### D5 — `StreamAgent` is a runtime-checkable structural `Protocol`; `FunctionAgent` is a thin adapter

`StreamAgent` = `Protocol` with `async def activate(self, ctx: AgentContext) -> None`. `@runtime_checkable` so `isinstance` works for adapter wiring and tests. `FunctionAgent` stores an `async` callable and implements `activate` by awaiting it. Rationale: adapters (langgraph, adk, pydantic_ai) and hand-written agents must satisfy one contract without a shared base class — structural typing is the Python-idiomatic, inheritance-free way, and matches how `Compactor`/`StagingSink`/`Decode` are already defined in this repo. The single-method protocol keeps authoring out of the runtime (project principle). Alternative: an ABC — rejected, forces inheritance and fights adapter classes that already extend framework bases.

### D6 — Module and export layout

New `core/context.py` holds `AgentContext` and `AgentResult`. New `core/agent.py` holds `StreamAgent` and `FunctionAgent`. `beam_agents/core/__init__.py` exports `StreamAgent`, `AgentContext`, `AgentResult`, `FunctionAgent` — this is the capability's public surface, matching how `model/__init__.py`, `memory/__init__.py`, and `tools/__init__.py` each already export their own capability's names. Root `beam_agents/__init__.py` is deliberately left untouched: repo convention (established by `add-tool-registry` and `async-llmclient-facade`, enforced by `tests/test_import.py::test_public_surface_is_empty`) is that the root package stays empty until `RunAgent`/`AgentConfig` exist to anchor it. `core/__init__.py`'s current docstring ("Nothing here is part of the public API") predates this change and gets updated, since `core` now holds a genuinely agent-facing surface alongside the still-internal `coders.py`. Everything else stays private. `AgentContext`/`AgentResult` are exported because adapters and tests construct/inspect them, even though end users rarely build them by hand.

## Risks / Trade-offs

- **[R1] `LlmFacade` construction coupling.** The context building the facade means the context must accept the facade's full dependency set (provider, breaker, retry policy, decode, rng, sleep). → Mitigation: accept a small injected `ModelDeps`/factory bundle rather than a long positional list; keep the facade wiring in one private helper so the DoFn passes one object. Revisit if a later change needs multiple models per activation.
- **[R2] Determinism depends on argument canonicalization.** If an agent passes semantically-equal but byte-different arguments to `act` across a replay (e.g. float formatting, dict order), canonical-JSON reduces but does not eliminate drift; `step_index` anchors the ID to *path position*, not arguments, so the `intent_id` stays stable, but the intent *payload* could differ. → Mitigation: `intent_id` (the dedup key) is derived from position only, so effectively-once holds regardless; document that intent payloads must be deterministic functions of staged state, and cover with a replay/chaos semantics test.
- **[R3] Drain-once is enforced at runtime, not by types.** A buggy DoFn could forget to drain (silently dropping effects) or drain a failed activation. → Mitigation: `drain()` raises on second call; the DoFn contract (later change) is "drain iff activate returned"; semantics tests assert failed activations mutate nothing. A `_drained` guard and an assertion that `activate` completed are cheap.
- **[R4] Outputs are untyped (`object`).** The context can't know the agent's output type. → Mitigation: keep outputs `object` on the staging surface; downstream typing/coders are the DoFn/transform's job. Avoids leaking `Any` into a generic parameter this early.
- **[R5] Trade-off: context owns facade vs. DoFn owns facade.** Context-owns simplifies the staging-sink guarantee but makes the context heavier. Accepted because the alternative (two-phase: build context, build facade with `staging=ctx`, inject back) has a mutable-back-reference window that is easy to wire wrong.

## Migration Plan

Additive only — no existing capability changes, no proto changes, no dependency changes. New modules and four new public exports. No `--update` / state-schema implications (this change stages into existing `MemoryBlob`/`LlmCacheBlob` shapes; it does not define new keyed state). Rollback is removing the new modules and exports. The dependent `core/dofn.py` change consumes this surface next.

## Open Questions

- Should `ctx.model` expose the `LlmFacade` directly or a narrowed `complete(...)` method? Leaning direct facade for now; narrow later if the surface needs guarding.
- Exact shape of the injected model-deps bundle (R1) — resolve during implementation against the real `LlmFacade` constructor signature.
- Whether read-only `TOOL_CALL` trace emission is in-scope for this change or deferred to the observability change. Spec marks it MAY; implementation can defer without violating the spec.
