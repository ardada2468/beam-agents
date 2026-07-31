## Context

The runtime driver contract is fixed: an `Agent` is an async callable over an
`ActivationContext` returning `Complete | Suspend`
([agent.py:98](../../../src/beam_agents/adapters/langgraph/agent.py:98) shows the
LangGraph adapter targeting it). The context stages every effect — memory writes through
the `Memory` facade, model calls through cache-first `call_model`, intents through
`act()` ([context.py:547](../../../src/beam_agents/core/context.py:547)) and
`request_approval()` ([context.py:559](../../../src/beam_agents/core/context.py:559))
with deterministic step-indexed `intent_id`s, traces through `stage_trace_event`
([context.py:624](../../../src/beam_agents/core/context.py:624)) — and the DoFn commits
atomically with the bundle.

Google ADK brings its own seams: a **session** model (`BaseSessionService`:
`create_session` / `get_session` / `append_event`, sessions holding a `state` dict and an
`events` list, state mutated via each event's `state_delta`), a **runner**
(`Runner.run_async(user_id, session_id, new_message)` yielding an event stream), a
**tool** model (function tools; *long-running* function calls are ADK's native
pause-until-answered mechanism), and Gemini model calls through the `google-genai` SDK
(httpx-based). The adapter's job — same as the LangGraph adapter's — is to express each
ADK seam in terms of the runtime seam it corresponds to, owning no new correctness
machinery.

Constraints that shape everything below: state is protobuf blobs behind the `Memory`
facade (1 MiB working-memory cap, 100 KiB blob guidance), bundle retries must stage
byte-identical intents and traces, side effects only via intents, and the core package
must not import ADK. The conformance matrix (C22) already defines the acceptance bar:
`ScenarioSpec`-driven factories, the registry guard
([_registry.py:189](../../../tests/conformance/_registry.py:189)) that fails collection
for an unregistered importable adapter, and per-leg declarations in
[tests/conformance/_spec.py:34](../../../tests/conformance/_spec.py:34).

## Goals / Non-Goals

**Goals:**

- Run an existing ADK agent (single `LlmAgent` or a multi-agent tree) as a beam-agents
  activation with the full correctness envelope: atomic session commit, failover resume,
  effectively-once side effects, replay-cached model calls, deterministic traces.
- Adoption cost for an existing ADK agent = re-tag side-effectful tools with the
  runtime's `@tool(side_effect=True)` and wrap tools with the shim. No agent-tree
  restructuring, no session code.
- Land inside the conformance matrix: all seven scenarios on both legs, green, as a
  registered adapter — not a bespoke one-off test suite.
- Keep ADK an optional extra; zero ADK imports in core modules.

**Non-Goals:**

- No session *history* service semantics beyond one live session per key (no
  `list_sessions` browsing across users, no cross-session ADK memory service; the
  runtime's `Memory` facade and long-term MemoryStore remain the memory story).
- No ADK artifact service backing in v1 (the `Runner` gets an in-memory artifact service;
  durable artifacts are out of scope).
- No streaming/partial-event delivery to `.output` (`.output` gets the final result;
  the trace tee covers observability), and no ADK "live" (bidi/audio) modes — this
  runtime targets system-triggered agents, not interactive sessions.
- No interception of non-httpx model transports (e.g. grpc Vertex paths) — those take
  the existing warning-fallback degradation.
- No new `TraceEvent` proto event type in v1 (see D7 and Open Questions).

## Decisions

### D1. `AdkAgent` implements the runtime `Agent` protocol; the `Runner` runs inside the activation

Mirrors LangGraph D1: the adapter needs `Suspend` with a snapshot and re-entry with
`ctx.resume_result` / `ctx.resume_approval` / `ctx.snapshot`, which is exactly the
runtime driver contract. `AdkAgent(agent, chat_models=[...])` wraps a user-constructed
ADK agent; per activation it builds a `BeamSessionService` over `ctx.memory`, constructs
a `Runner` around the (never mutated) user agent, ensures the per-key session exists,
and drains `run_async(user_id=key_hex, session_id=key_hex, new_message=decoded_event)`
on the bridge loop. Per-key serialization makes one live session per key safe by
construction — the same argument as the LangGraph `thread_id` choice
([agent.py:127](../../../src/beam_agents/adapters/langgraph/agent.py:127)). The
activation is exposed to the shim/transport/tee through the shared contextvar (D6), the
pattern already proven at
[transport.py:45](../../../src/beam_agents/adapters/langgraph/transport.py:45).

*Alternative considered:* running the ADK `Runner` outside the DoFn (ADK-as-service,
results re-injected) — rejected: it would move state and model calls outside the staged
context, forfeiting invariants 1–3. The Runner is cheap to construct and holds no state
of its own once the session service is ours.

### D2. `BeamSessionService` stores one session per key in a reserved `__adk__/` memory namespace

The direct parallel of `BeamCheckpointSaver`
([checkpoint.py:83](../../../src/beam_agents/adapters/langgraph/checkpoint.py:83)): the
service holds the activation's `Memory` facade and persists the session as a facade
scalar under `__adk__/session` — the serialized session envelope (state dict + event
history; ADK sessions are pydantic models, serialized as canonical JSON bytes).
`append_event` applies the event's `state_delta` to the session state and appends the
event, then overwrites the scalar; `get_session` deserializes it; `create_session` is
idempotent for the fixed per-key identity and initializes an empty session. Because the
facade stages in memory and the DoFn commits the `MemoryBlob` with the bundle, session
durability *is* bundle atomicity — a failed activation leaves no partial session, and a
failover reloads the committed blob (invariant 1, by construction, no new machinery).
Retention is one session per key: `app_name` is an adapter constant, `user_id` and
`session_id` derive from the entity key, and `list_sessions` returns at most that one.

Cap interaction is identical to the checkpoint saver: an oversized session raises
`MemoryOverflow`, failing the activation closed to `.errors`; docs prescribe ADK-side
history trimming (the adapter exposes a `max_events` knob that drops oldest
non-load-bearing events on append, off by default). Compactors must not evict `__adk__/`
keys — same trust model as `__langgraph__/`.

*Alternative considered:* a dedicated state spec / proto field for sessions — rejected
for the same reasons as LangGraph D2 (new state surface, `--update` burden; the facade
already provides staging, caps, accounting).

### D3. Async-first service; all I/O is staged in-memory

Divergence from LangGraph D3, forced by the framework: ADK's `BaseSessionService` ABC is
async-first (`async def get_session` etc.), so the async methods are the implementation.
The body of every method touches only staged in-memory facade state — no network, no
blocking — so nothing ever blocks the bridge event loop and no executor hand-off is
needed. If the pinned ADK range still carries deprecated sync variants, they delegate to
the async core via the already-running loop only if the ABC requires them at all (Open
Questions; not load-bearing either way).

### D4. Side-effect tools become long-running function calls; one suspension covers all pending work

The shim declares `side_effect=True` tools as ADK long-running function tools. When the
model requests such a call, the tool body does **not** run; the shim records the call
(name, validated args, ADK function-call id) in a per-activation collector. When
`run_async`'s event stream completes with collected calls outstanding, the adapter
stages one `ToolIntent` per call via `ctx.act` (deterministic step-indexed `intent_id`,
invariant 2) — or `ctx.request_approval` for the approval shim (D5) — and returns a
single `Suspend(adapter="adk")`. The snapshot is the same JSON resume-map shape the
LangGraph adapter proved out ([agent.py:147](../../../src/beam_agents/adapters/langgraph/agent.py:147)):
`intent_id → {kind, function_call_id, tool_name}` plus results already collected. The
session itself is NOT in the snapshot — it is in memory (D2); the snapshot carries only
correlation the runtime doesn't track.

Resume mirrors the LangGraph accumulate-then-resume protocol exactly: each re-injected
`ToolResult`/`Approval` is recorded into the map (a result for an unknown intent fails
closed); while intents remain unanswered the activation re-suspends staging nothing new;
when all are answered, the adapter builds one user-role message whose parts are the
function responses (each carrying its original ADK function-call id and name) and runs
the `Runner` again — the committed session supplies the full history, so ADK's model
sees the tool round-trip exactly as if the tools had answered inline. Replayed bundles
stage byte-identical intents because intent bytes come from `ctx.act`'s step counter and
canonical-JSON args; snapshot bytes need not be byte-stable (ADK function-call ids are
model/framework-generated) and, as established in the LangGraph implementation, need not
be — the snapshot commits atomically with the session it references, and the
effectively-once argument rests on intent bytes alone.

*Alternative considered:* one suspension per call — rejected (serializes parallel calls,
multiplies re-injection round-trips; same reasoning as LangGraph D4).

### D5. The tagging shim wraps tools individually; approvals get a dedicated shim tool

Divergence from `BeamToolNode`
([toolnode.py:45](../../../src/beam_agents/adapters/langgraph/toolnode.py:45)): LangGraph
has a swappable tool *node*; ADK has per-tool objects on the agent, so the shim is a
per-tool wrapper plus a helper: `beam_tools([...])` maps a sequence of runtime `Tool`
objects to ADK tools — read-only tools to plain function tools that execute inline with
`argument_model`-validated args, side-effect tools to long-running declarations feeding
the D4 collector. Even a mis-wired agent cannot execute an effect in-pipeline: calling a
`side_effect=True` tool directly raises `SideEffectToolError` (registry design D3) — the
shim is the sanctioned detour. Adoption is exactly "re-tag + wrap": replace
`tools=[charge]` with `tools=beam_tools([charge])` where `charge` is now
`@tool(side_effect=True)`-decorated.

Approvals: ADK has no `interrupt()` primitive, so plain human-approval requests ride the
same long-running mechanism through a provided `BeamApprovalTool` — a long-running tool
on the runtime's approval channel whose pending call the adapter stages via
`ctx.request_approval` (APPROVAL-kind intent, HITL timer arms via `Suspend.timeout_ms`),
and whose function response on resume carries the decision
(`approved`/`approver`/`decided_at_ms`). This is the ADK-idiomatic equivalent of the
LangGraph `interrupt(...) → request_approval` mapping, and it is what the conformance
`approval_timeout_fallback` scenario drives.

### D6. The httpx transport hook is hoisted to `adapters/_transport.py` and taught the google-genai layout

`_ReplayTransport` and its contextvar depend only on `httpx` and `LlmRequest` — both
core dependencies, zero LangGraph imports
([transport.py:59](../../../src/beam_agents/adapters/langgraph/transport.py:59)) — so
the module moves to a shared private `beam_agents.adapters._transport`, with
`adapters/langgraph/transport.py` becoming a re-exporting shell (move-only; public names
and behavior unchanged, LangGraph adapter tests untouched). The recognition table gains
the `google-genai` async client layout so `AdkAgent(agent, chat_models=[...])` can
instrument the SDK clients ADK's model wrappers hold: recognized clients get the replay
transport (provider-shaped JSON body → `LlmRequest` → `ctx.call_model` → synthesized
`httpx.Response`), making bundle retries zero-provider-call on the cached path
(invariant 3); unrecognized model objects warn once per agent instance and increment the
existing `beam_agents.adapters/transport_fallback` counter
([transport.py:113](../../../src/beam_agents/adapters/langgraph/transport.py:113)) —
degradation, never an error. The exact attribute path from an ADK model wrapper to its
`httpx.AsyncClient`, and whether a custom `BaseLlm` implementation is the cleaner
sanctioned seam, are pinned down at implementation (Open Questions); the fallback path
guarantees the adapter is correct-if-slower even where recognition misses.

### D7. The event tee projects onto the existing trace vocabulary; determinism rules are strict

Every non-partial ADK event drained from `run_async` is teed into the activation trace,
but only through *deterministic projections* onto the existing `TraceEvent` vocabulary
(the proto's `EventType` enum is closed today —
[beam_agents.proto:95](../../../protos/beam_agents.proto:95)):

- **Inline tool executions** (read-only shim tools) stage `TOOL_CALL` events built with
  `ActivationTrace.tool_call`
  ([traces.py:255](../../../src/beam_agents/observability/traces.py:255)) — its
  dedicated `tool_index` counter exists precisely so tool spans never perturb the intent
  step cursor — enriched with the `beam_agents.adapter` attribute
  ([traces.py:76](../../../src/beam_agents/observability/traces.py:76)) and the ADK
  author name, then staged via `ctx.stage_trace_event`. This closes, for the ADK
  adapter, the inline-tool observability gap the conformance change recorded as a
  finding for `BeamToolNode` (its design.md, Findings): the shim executes tools while
  holding the activation contextvar, so it *has* the seam `BeamToolNode` lacked.
- **Model turns** need no tee: they surface as `LLM_CALL` events on the one existing
  `call_model` path via the transport hook (D6).
- **Suspensions and intents** likewise: `INTENT_EMITTED`/`SUSPENDED` are staged by
  `ctx.act`/the loop driver, with `adapter="adk"` on the SUSPENDED event.

Determinism: staged events use the activation clock and per-activation counters only.
ADK event ids, timestamps, and invocation ids are wall-clock/random and are **never**
copied into trace bytes — a replayed bundle must emit byte-identical traces (the
contract stated at the top of `traces.py`). Full-fidelity teeing of ADK-only events
(agent transfers in multi-agent trees, escalations) has no honest home in the current
enum; inventing attributes on unrelated event types would be worse than deferring. An
additive `ADAPTER_EVENT = 8` enum value (the `SUSPENDED = 7` precedent,
[beam_agents.proto:108](../../../protos/beam_agents.proto:108)) is the natural future
change; deferred, see Open Questions.

### D8. Packaging and import isolation follow the langgraph extra exactly — with one namespace-package wrinkle

New extra `adk = ["google-adk>=1.0,<2"]` (exact floor set at implementation against the
lockfile). `beam_agents.adapters.adk` imports ADK at module scope (it is the extra's own
module); `beam_agents/__init__.py` exposes `AdkAgent` via the existing lazy
`__getattr__` ([\_\_init\_\_.py:42](../../../src/beam_agents/__init__.py:42)) raising an
actionable `ImportError` naming `beam-agents[adk]`. Wrinkle: ADK imports as
`google.adk` under the `google` namespace package, which *is* present in core installs
(google-cloud dependencies), so the langgraph pattern's
`exc.name.partition(".")[0] in DISTRIBUTIONS` check would never match. The ADK branch
matches `exc.name` against the full dotted prefixes (`google.adk`, `google.genai`)
instead. The registry guard's `ImportError`-tolerant introspection
([_registry.py:169](../../../tests/conformance/_registry.py:169)) already handles the
absent-extra environment: unimportable subpackages are skipped, registered cells report
clean skips via `requires`.

### D9. Conformance registration: same factory shape, `requires="google.adk"`, one more Flink job

`tests/conformance/_adapters/adk.py` provides `build_adk_agent(spec)` /
`build_adk_provider(spec)` translating each `ScenarioSpec` — the same shared directive
vocabulary and `turn_response` bytes — into an ADK agent: a model seam that posts
provider-shaped JSON through an httpx client the transport hook instruments (the same
minimal-recognizable-layout trick as the LangGraph factory's `_ChatModel` in
[tests/conformance/_adapters/langgraph.py:44](../../../tests/conformance/_adapters/langgraph.py:44)),
`beam_tools`-wrapped scenario tools, and the approval shim for the approval scenarios.
ADK imports stay lazy inside the build functions so the module imports cleanly without
the extra; cells skip via `requires="google.adk"`. The registration is a third
`ConformanceAdapter` entry in
[ADAPTERS](../../../tests/conformance/_registry.py:58) with
`adapters_subpackage="adk"` — which is also what un-breaks collection once
`beam_agents/adapters/adk/` exists, per the guard. The Flink leg follows the
one-job-per-adapter multiplexing decision from the conformance design (its D5): one
additional job, per-scenario key prefixes, existing responder; per-leg skip declarations
(`bundle_retry_cache`, `ttl_expiry` on Flink) are scenario-level and apply to the new
adapter's cells automatically, and the meta-test's expected-cell accounting picks up the
third adapter with no new wiring.

## Risks / Trade-offs

- **[ADK API drift]** — `google-adk` is young and moves fast (session-service
  signatures, long-running tool surface) → pin `>=1.0,<2`; adapter tests exercise the
  public seams (session-service contract round-trip, long-running collect/resume, event
  drain) so a floor bump fails loudly in CI, not in users' pipelines. Uncertain surface
  details are resolved at implementation against the pinned version, not guessed
  (Open Questions).
- **[Session growth vs. the 1 MiB cap]** — ADK sessions append every event; long
  conversations overflow working memory → `MemoryOverflow` fails closed to `.errors`
  (no partial state); the `max_events` trimming knob plus documented guidance bound
  growth; soft-cap warning and counter give early signal. Same posture as LangGraph
  checkpoint sizing.
- **[Framework-generated non-determinism]** — ADK event ids/timestamps/function-call
  ids are not replay-stable → they are confined to committed session bytes and the
  suspension snapshot (both of which commit atomically with the state they describe and
  are never part of the effectively-once argument); intents and traces are built
  exclusively from runtime-deterministic inputs. The conformance `bundle_retry_cache`
  cell enforces this per adapter.
- **[Recognition misses on model clients]** — ADK model wrappers may hold their SDK
  clients at attribute paths the hook doesn't probe → warning + fallback counter makes
  the degradation visible; the conformance factory's model seam does not depend on
  real-SDK layouts, so the matrix stays green offline while real-client recognition is
  verified in the adapter unit suite with layout doubles.
- **[Transport hoist destabilizes the LangGraph adapter]** — the move is
  mechanical but touches a shipped adapter → move-only refactor gated by the existing
  LangGraph unit suite and its conformance cells running unchanged in the same PR.
- **[Multi-agent trees under one session]** — sub-agent transfers interleave authors in
  one event history; v1 persists them faithfully but traces them only through the D7
  projections → documented; full-fidelity adapter events are the deferred
  `ADAPTER_EVENT` follow-up.

## Migration Plan

Purely additive: new subpackage, new extra, new conformance cells; no state schema
change, no behavior change for existing users; the transport hoist preserves all public
names in `beam_agents.adapters.langgraph.transport`. Rollback = don't install/use the
extra (the `__adk__/` namespace only exists for keys that ran an ADK agent). No
`--update` concerns.

## Open Questions

- **Exact `BaseSessionService` surface in the pinned range**: the abstract method set
  and signatures (`GetSessionConfig`, `ListSessionsResponse`, remaining deprecated sync
  variants) shifted across ADK 1.x — settle the precise override list at implementation
  against the lockfile pin; D2/D3 are insensitive to the outcome.
- **Long-running call surfacing**: whether pending long-running function calls are best
  detected from the event stream (`long_running_tool_ids` on events) or solely from the
  shim's collector, and the exact resume shape ADK expects for function responses
  (`types.Content` role/parts composition, id/name matching rules) — verify both
  against the pinned version; the D4 protocol accommodates either detection path.
- **Model seam of record**: probe-and-wrap the `google-genai` httpx client (D6), or ship
  a small `BaseLlm` implementation that calls `ctx.call_model` directly and is passed as
  the agent's `model`? The transport hook is the v1 decision for zero-authoring-change
  adoption; if the `BaseLlm` route proves strictly cleaner against the real SDK it can
  replace the probe table without spec changes (the requirement is stated in terms of
  the cache-first path, not the mechanism).
- **Runner service requirements**: whether the pinned `Runner` requires artifact/memory
  services to be non-None (supply in-memory defaults if so).
- **`ADAPTER_EVENT` trace type**: whether to add the additive enum value (plus exporter
  name-mapping) for full-fidelity ADK event teeing (agent transfers, escalations) — a
  separate, additive wire-schemas change if wanted; v1 deliberately ships without it
  (D7).
