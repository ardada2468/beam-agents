# Design: add-pydantic-ai-adapter

## Context

The runtime driver contract is settled and already carries one framework: an `Agent` is
an async callable over an `ActivationContext` returning `Complete | Suspend`
(`core/agent.py`), every effect is staged on the context (memory via the `Memory`
facade, model calls via cache-first `call_model`, intents via `act`/`request_approval`
with deterministic step-indexed IDs), and the DoFn commits atomically with the bundle.
The LangGraph adapter established the adapter idioms this change reuses: a reserved
working-memory namespace for framework state (`__langgraph__/`), one suspension covering
all pending work with a correlation-only snapshot, an httpx transport hook feeding the
framework's model calls into `ctx.call_model`, and lazy public export behind an extra.

Pydantic AI's own seams differ from LangGraph's in ways that mostly make the adapter
*simpler*:

- Its durable unit is the **message history** (`agent.run(..., message_history=...)`,
  with a documented serialization path for model messages), not a checkpointer ABC —
  there is no superstep persistence protocol to implement.
- Its HITL/external-execution primitive is **deferred tool calls**: a tool can be
  declared as externally executed (and/or approval-requiring), and a run that reaches
  such a call **ends cleanly**, returning the pending tool calls as a typed output
  instead of raising through the graph. Resumption is a *new* `run` carrying the prior
  message history plus the supplied results/decisions. There is no LangGraph-style
  node re-execution on resume.
- Its provider models wrap the official Anthropic/OpenAI SDKs, whose async clients are
  **httpx-based** — the same stack as the runtime's own provider clients
  (`model/anthropic.py`, `model/openai_compat.py`) and as the LangChain clients the
  existing transport hook already intercepts. The interception pattern transfers; only
  the attribute probing that *finds* the `httpx.AsyncClient` on a model object is
  framework-specific.

Constraints unchanged from the LangGraph change: protobuf-backed state behind the
`Memory` facade (100 KiB blob guidance, 1 MiB hard cap), byte-identical replayed
bundles, side effects only via intents, zero framework imports in core, offline tests
with FakeLLM. New constraint from C22: the conformance registry guard makes shipping
`beam_agents.adapters.pydantic_ai` without a conformance registration a collection
error, so this change's definition of done is the full matrix, not a bespoke test file.

## Goals / Non-Goals

**Goals:**

- Run an existing `pydantic_ai.Agent` as a beam-agents activation with the full
  correctness envelope: bundle-atomic history commit, effectively-once side effects via
  deferred-tool→intent mapping, replay-cached model calls, fail-closed HITL timeouts.
- Adoption cost for an existing Pydantic AI agent = re-tag its side-effectful tools
  with the runtime's `@tool(side_effect=True)` and hand the tool set to the adapter's
  toolset; no restructuring of instructions, output types, or control flow.
- Pass all seven conformance scenarios on both legs (honoring the matrix's per-leg
  declared skips) as the primary acceptance gate.
- Keep Pydantic AI an optional extra; `pydantic>=2` stays a core dependency and must
  not conflict with the pinned framework range.

**Non-Goals:**

- No streaming (`run_stream`) support in v1 — activations are single-shot runs; the
  runtime's latency envelope is per-activation, not per-token.
- No support for Pydantic AI's graph layer (`pydantic_graph`) or its own persistence
  interfaces; the runtime owns durability.
- No interception of non-httpx model backends (e.g. gRPC-only providers) — those take
  the existing warn-once fallback path.
- No multi-agent orchestration (agent delegation trees) beyond what a single
  `Agent.run` does internally; delegated sub-agents share the activation's transport
  instrumentation but get no separate runtime identity.

## Decisions

### D1. `PydanticAIAgent` targets the runtime `Agent` protocol; one activation = one `Agent.run` segment

Mirrors LangGraph design D1: the adapter needs `Suspend` with a snapshot and re-entry
via `ctx.resume_result`/`ctx.resume_approval`/`ctx.snapshot`, which is exactly the
runtime driver contract. `PydanticAIAgent(agent, tools=..., decode_event=...,
encode_output=..., hitl_timeout_ms=...)` wraps a user-constructed `pydantic_ai.Agent`.
Per activation it: loads the persisted history (D2), decodes the event into the run's
user prompt, invokes `agent.run(prompt, message_history=history, ...)` on the bridge
loop with the activation exposed to the transport hook (D5), persists the run's new
messages back through the facade, and returns `Complete(encode_output(result.output))`
— or, when the run ends at deferred tool calls, stages intents and returns `Suspend`
(D3). Per-key serialization makes one live conversation per key safe by construction;
no `thread_id` equivalent is needed because history keying *is* the entity key.

*Alternative considered:* implementing `StreamAgent.activate()` — rejected for the same
reason as in the LangGraph change: it would re-encode suspend/resume through a second
seam.

### D2. History lives in a reserved `__pydantic_ai__/` memory namespace, latest-only

The adapter serializes the full model-message list (Pydantic AI documents a
`TypeAdapter`-based JSON round-trip for exactly this purpose) into a single scalar,
`__pydantic_ai__/messages`, via `Memory.set` — the same facade seam
`BeamCheckpointSaver` uses. Because the facade stages in memory and the DoFn commits
the `MemoryBlob` with the bundle, history durability *is* bundle atomicity: a failed or
timed-out activation leaves no history mutation, and a worker failover reloads the
committed history. Latest-only retention: each successful run overwrites the scalar
with `all_messages()`; there is no history-of-histories. Cross-activation conversation
continuity and TTL GC then fall out of ordinary working-memory behavior — which is
precisely what the `ttl_expiry` conformance scenario asserts.

Cap interaction is identical to LangGraph D2: an oversized history raises
`MemoryOverflow` and fails the activation closed to `.errors`; docs prescribe
history-processor–style trimming on the framework side. Compactors must not evict
`__pydantic_ai__/` keys (documented, same trust model as `__langgraph__/`).

*Alternative considered:* carrying the history in the `Suspend` snapshot — rejected:
the snapshot only exists while suspended, so cross-activation memory would be lost on
every `Complete`, and the continuation is not the place for a conversation-sized blob.

### D3. Suspension = the run ends at deferred tool calls; one `Suspend` covers all of them; resume is a fresh run seeded with history + results

`BeamToolset` (D4) declares `side_effect=True` tools as externally-executed (deferred)
and approval-gated tools as approval-requiring. When the model calls any of them, the
run ends with a typed "deferred requests" output carrying the pending tool calls. The
adapter stages one intent per pending item — `ctx.act(name, canonical_args_json)` for
an execution request, `ctx.request_approval(...)` for an approval request — and returns
a single `Suspend(adapter="pydantic_ai")`. The snapshot is a small JSON correlation
map, exactly the LangGraph shape: `intent_id → {kind: tool|approval, tool_call_id}`
plus results already collected. History is NOT in the snapshot — it was committed via
D2 when the suspension committed; the snapshot carries only what the runtime doesn't
track.

Resume: each re-injected `ToolResult`/`Approval` re-invokes the adapter with the
snapshot. It records the result into the map; while intents remain unanswered it
re-suspends staging nothing new (the runtime seeds the resumed context's step index
from the continuation, so no intent re-mint). When all are answered, the adapter builds
the framework's deferred-results value — tool results keyed by `tool_call_id`,
approvals as approved/denied decisions — and calls `agent.run` again with the
committed history and those results. Model calls in the resumed run are replay-cached,
so a chaos-retried resume adds zero provider calls (the `bundle_retry_cache` scenario).
Intent bytes are deterministic because IDs come from the context's step counter
(invariant 2); the snapshot bytes need not be byte-stable across replays — same
implementation finding as LangGraph D4, the effectively-once argument rests on intent
bytes alone.

A key simplification vs. LangGraph: there is no interrupted-node re-execution. The
deferred boundary is a clean run end, so no "code before the interrupt runs again"
caveat exists for this adapter.

*Alternative considered:* one suspension per tool call — rejected (serializes parallel
calls, multiplies re-injection round-trips; same reasoning as LangGraph D4).

### D4. `BeamToolset` maps runtime `@tool` objects; read-only tools execute via `ctx.run_tool`

`BeamToolset(tools)` accepts runtime `Tool` objects (the registry kind) and presents
them to Pydantic AI with their pydantic argument models as schemas:

- `side_effect=False` tools are registered as ordinary framework tools whose executor
  calls `await ctx.run_tool(name, args)` — the runtime path, which validates
  arguments, refuses `side_effect=True` tools with `SideEffectToolError` before
  execution, counts the call in the tally, and stages a `TOOL_CALL` trace event. This
  deliberately improves on `BeamToolNode` (which executes read-only tools directly and
  stages no trace — the surfaced finding in the conformance change's design): Pydantic
  AI tool executors run inside the activation with the context reachable, so routing
  through the runtime costs nothing.
- `side_effect=True` tools are declared deferred/external: their schema is visible to
  the model, their callables are **never** invoked in-pipeline, and a call ends the run
  per D3. Even a mis-wired toolset cannot execute an effect: calling a side-effect
  `Tool` directly raises `SideEffectToolError` (registry design D3).
- Tools the adapter should gate on human approval are passed as approval-requiring;
  their calls surface as approval requests and map to `ctx.request_approval` per D3.

The activation reaches the executor via the shared activation contextvar (D5) — the
same mechanism the transport uses, race-free under per-key serialization and the single
bridge loop. Whether Pydantic AI's own dependency-injection (`deps`) channel is a
cleaner carrier is an open question; the contextvar works regardless of the user's
`deps_type`.

### D5. Transport interception transfers; the framework-neutral core is hoisted, recognition stays per-adapter

Feasibility reasoning: `_ReplayTransport` (LangGraph adapter) contains nothing
LangGraph-specific — it parses a provider-shaped JSON request body into `LlmRequest`,
awaits `ctx.call_model`, and materializes the `LlmResponse` bytes as an
`httpx.Response`; its module imports only httpx, Beam metrics, and
`beam_agents.model.client`. Pydantic AI's Anthropic and OpenAI model classes call the
providers through the official `anthropic`/`openai` SDKs, whose async clients are
`httpx.AsyncClient`s (the identical wire stack the runtime's own
`model/anthropic.py`/`model/openai_compat.py` clients use, and the same SDK layout the
LangGraph hook already probes underneath LangChain wrappers). So the interception
pattern transfers wholesale; what differs is only *where* the `httpx.AsyncClient`
hangs off a Pydantic AI model object.

Mechanics: hoist `_ReplayTransport`, the `_current_activation` contextvar, and
`warn_fallback` (with its `beam_agents.adapters/transport_fallback` counter) into
`beam_agents.adapters._transport` — a move-only refactor; the LangGraph module keeps
re-exports so its tests and any user imports are unaffected. Each adapter keeps its own
`find_async_client` probing table; the Pydantic AI one probes the model-object layouts
of the pinned framework range (expected: the model exposes its SDK client, whose
`_client` is the httpx client — exact attribute names are an open question settled by
inspection at implementation). `install_transport` stays idempotent. Unrecognized
models: warn once per agent instance naming the model class, increment the fallback
counter, run untouched — a degradation, never an error.

Belt-and-braces option documented for users: Pydantic AI providers accept a
caller-supplied `http_client` at construction, so a user can also hand the adapter's
transport an explicitly-built client; the adapter does not require it.

*Alternative considered:* a custom Pydantic AI `Model` subclass users must adopt —
rejected as an authoring-surface change (same reasoning that rejected the
`BaseChatModel` wrapper in LangGraph D6); it remains possible later for non-httpx
backends.

### D6. Usage accounting: run usage folds into the tally via `accumulate_usage`

After each run segment, the adapter maps the framework's reported run usage
(`result.usage()`) into a `TokenUsage` and calls `ctx.accumulate_usage(...)`, so
Pydantic AI activations report `total_tokens` with `usage_observed=True` — closing the
gap where transport-served `call_model` alone never decodes usage. Per-call
billed/unbilled trace attribution stays where it is today: the context's configured
`decode` stamps usage attributes on each `LLM_CALL` trace event, cache hits marked
unbilled. Nuance accepted and documented: on a resumed activation whose model turns
were served from the replay cache, the framework still parses those response bytes and
reports their usage, so the tally reflects tokens *processed* this activation, not
tokens *billed* — the billed signal lives in the traces. The tally is worker-local,
emitted with the committing bundle, and never persisted, so discarded bundle attempts
do not double-report.

### D7. Packaging and import isolation

New extra `pydantic-ai = ["pydantic-ai-slim>=1,<2"]` (the slim distribution — the
adapter needs the framework core, not the batteries-included meta-package; exact floor
pinned at implementation against the lockfile, and the test dependency group mirrors it
plus the one provider flavor the conformance factory needs). `pydantic>=2` is already a
core runtime dependency, so the extra introduces no new pydantic constraint — CI's
`uv sync` proves the ranges co-resolve. `beam_agents.adapters.pydantic_ai` imports the
framework at module scope (it is the extra's own module);
`beam_agents/__init__.py.__getattr__` grows a `PydanticAIAgent` branch raising the
actionable `ImportError` naming `beam-agents[pydantic-ai]`, exactly parallel to the
`LangGraphAgent` branch. Unit tests `pytest.importorskip("pydantic_ai")`; the
conformance registration sets `requires="pydantic_ai"` and
`adapters_subpackage="pydantic_ai"` so cells skip cleanly where the extra is absent and
the registry guard is satisfied where it is present.

### D8. Conformance factory: a provider-flavored model over the transport hook, scripted by the shared directive vocabulary

`tests/conformance/_adapters/pydantic_ai.py` translates each `ScenarioSpec` into a
Pydantic AI agent: `BeamToolset` over the spec's tools (`charge` deferred, `lookup_*`
inline), an approval-requiring path for `request_approval` turns, and a model object
whose httpx client the transport hook instruments so every model call rides
`ctx.call_model` into the cell's FakeLLM. The factory's provider builds exactly one
FakeLLM rule per spec turn (preserving `validate_bundle`'s rule-count equivalence
check), matching on the scenario's unique `model_id` plus conversation position and
responding with the shared `turn_response` directive bytes wrapped in the wire shape
the model class parses. Terminal outputs are re-encoded to the canonical pipe-joined
shape (`seen=N | tool results | resumed:<payload> | answer`) so all three adapters
produce byte-identical terminals for the same conversation. Everything is module-level
and rebuilt worker-side by `(adapter, scenario)` name (the `LazyCellAgent` shape), and
the framework is imported lazily inside build functions so the module imports cleanly
without the extra.

Whether the model object is a real provider-flavored Pydantic AI model (highest
fidelity for the recognition probing; pulls one provider SDK into the test group) or a
minimal custom `Model` subclass posting provider-shaped JSON through an instrumented
httpx client (the LangGraph factory's `_ChatModel` double pattern; zero extra
dependencies, recognition then covered by layout doubles in `tests/adapters/`) is
resolved at implementation by measuring what the pinned range makes cheap — both
satisfy the specs, and the choice is invisible to the scenario bodies.

## Risks / Trade-offs

- **[Pydantic AI API drift]** — the framework iterates quickly (deferred-tool and
  toolset surfaces are newer than the core run API) → pin a tested `>=1,<2` range in
  the extra; adapter tests exercise the public seams (message-history round-trip,
  deferred-call end state, resume-with-results) so a bump fails loudly in CI, not in
  users' pipelines.
- **[History size vs. the 1 MiB cap]** — long conversations overflow working memory →
  `MemoryOverflow` fails the activation closed to `.errors`; docs prescribe
  framework-side history trimming/summarization; the facade's soft-cap warning gives
  early signal. Same posture as LangGraph checkpoints.
- **[Recognition fragility across SDK/framework versions]** — the attribute path to
  the httpx client is private layout → probing covers the pinned range only, is
  tested against layout doubles shaped like the real SDKs, and the failure mode is the
  warn-once fallback (graph still runs), never a crash.
- **[Deferred-results replay determinism]** — the resumed run re-sends the prior
  conversation to the model; if the framework's request serialization is not stable
  across identical inputs, replay-cache keys would miss → covered directly by the
  `bundle_retry_cache` conformance cell (zero extra provider calls is the assertion);
  any instability found is a blocking finding, not something to paper over.
- **[Offline matrix wall-clock growth]** — a third adapter adds seven DirectRunner
  cells to the required `ci` semantics selection → the C22 design already set a
  ~2-minute budget with per-adapter batching as the escape hatch; measure and batch if
  exceeded.
- **[Transport-hoist regression risk to the LangGraph adapter]** — mitigated by
  move-only extraction with unchanged re-exports; the existing LangGraph unit and
  conformance suites gate the refactor itself.

## Migration Plan

Purely additive: new subpackage, new extra, one lazy-export branch, one conformance
registration; the transport hoist preserves all public names. No state schema change —
the reserved `__pydantic_ai__/` namespace only exists for keys that ran a Pydantic AI
agent, inside the existing `MemoryBlob`. Rollback = don't install/use the adapter.
No `--update` concerns.

## Open Questions

- **Exact deferred-tool API spellings and floor**: the names/import paths for the
  deferred-requests output type, the results container, and the approved/denied
  decision values in the pinned range (and therefore the exact version floor where
  external execution + approval-requiring tools are both available) — settled against
  the framework's API at implementation time.
- **Recognition probing paths**: the precise attribute chain from an
  Anthropic/OpenAI-flavored Pydantic AI model object to its SDK client's
  `httpx.AsyncClient` in the pinned range (expected `model.client` → SDK client →
  `._client`, by analogy with the LangChain layouts) — settled by inspection; the
  probing table is data, not architecture.
- **Output-type composition**: how the adapter's deferred-requests output type
  composes with a user agent's own declared `output_type` (union at construction vs.
  per-run override), and what constraint that puts on wrapped agents — determines
  whether `PydanticAIAgent` wraps the user's agent as-is or requires the union to be
  declared by the user.
- **Usage field mapping**: the framework's run-usage field names mapped onto
  `TokenUsage` (input/output/total), and whether per-request usage (rather than
  run-total) is retrievable for finer-grained accumulation.
- **History serialization stability**: whether the framework's message serialization
  is stable enough across the pinned range to be committed state, or whether the
  adapter should version-tag the scalar it writes (a golden-blob compat test decides).
- **Context carrier for tool executors**: shared activation contextvar (works
  unconditionally) vs. the framework's `deps` injection (cleaner, but occupies the
  user's `deps_type`) — leaning contextvar, revisit if `deps` proves compatible with
  user-owned deps.
