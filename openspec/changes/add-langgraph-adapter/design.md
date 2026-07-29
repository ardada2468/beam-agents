# Design: add-langgraph-adapter

## Context

The runtime driver contract already exists: an `Agent` is an async callable over an
`ActivationContext` returning `Complete | Suspend` (`core/agent.py`). The context stages
every effect — memory writes through the `Memory` facade, LLM calls through
`call_model()` (replay-cache-first), intents through `act()`/`request_approval()` with
deterministic step-indexed `intent_id`s — and the DoFn commits the staged state
atomically with the bundle. LangGraph brings its own persistence model
(`BaseCheckpointSaver`: checkpoints of channel values per superstep, pending writes),
its own HITL primitive (`interrupt()` raising `GraphInterrupt`, resumed with
`Command(resume=...)`), and its own tool execution node (`ToolNode`). The adapter's job
is to express each LangGraph seam in terms of the runtime seam it corresponds to,
without owning any new correctness machinery.

Constraints that shape everything below: Beam state is protobuf blobs behind the
`Memory` facade (1 MiB working-memory cap, 100 KiB per blob guidance), bundle retries
must be byte-identical (invariants 1–3), side effects only via intents (invariant 5),
and the core package must not import LangGraph.

## Goals / Non-Goals

**Goals:**

- Run a compiled LangGraph graph (`CompiledStateGraph`) as a beam-agents activation
  with the full correctness envelope: atomic checkpoint commit, mid-graph failover
  resume, effectively-once side effects, replay-cached model calls.
- Adoption cost for an existing graph = re-tag side-effectful tools with the runtime's
  `@tool(side_effect=True)` + swap `ToolNode` for the shim + pass the adapter's
  checkpointer. No topology changes.
- Keep LangGraph an optional extra; zero LangGraph imports in core modules.

**Non-Goals:**

- No checkpoint *history* (time-travel/forking across supersteps). Beam keyed state is
  the durability layer; we persist the latest checkpoint per key, not a log.
- No streaming of intermediate graph events to the main output (`.output` gets the
  final result; traces cover observability).
- No support for LangGraph subgraph-level distributed execution, cross-thread stores
  (`BaseStore`), or `langgraph-checkpoint-postgres`-style external checkpointers.
- No interception of non-httpx model clients (e.g. grpc Vertex) in v1 — those take the
  warning-fallback path.

## Decisions

### D1: `LangGraphAgent` implements the runtime `Agent` protocol, not `StreamAgent`

The adapter needs to return `Suspend` with a snapshot and to be re-entered with
`ctx.resume_result` / `ctx.resume_approval` / `ctx.snapshot` — that is exactly the
runtime driver contract (`Agent`), so the adapter targets it directly.
`LangGraphAgent(graph, tools=...)` wraps an already-compiled graph; per activation it
builds the LangGraph config (`thread_id` = entity key hex — per-key serialization makes
one live thread per key safe by construction), installs the checkpointer/transport, and
`await graph.ainvoke(input, config)` on the bridge loop.

*Alternative considered:* implementing `StreamAgent.activate()` and layering outcomes on
top — rejected; it would re-encode Suspend/resume through a second seam for no benefit.

### D2: `BeamCheckpointSaver` stores the latest checkpoint in a reserved memory namespace

The saver holds the activation's `Memory` facade and writes checkpoints as facade
scalars under the reserved `__langgraph__/` prefix: `__langgraph__/ckpt` (serialized
checkpoint + metadata) and `__langgraph__/writes` (pending writes for the interrupted
superstep). Serialization uses LangGraph's `JsonPlusSerializer` — the message history
lives inside the checkpoint's channel values, so the "message-history section" of the
`MemoryBlob` is this namespace. Latest-only retention: `put()` overwrites; `list()`
returns at most the one tuple. Because the facade stages in memory and the DoFn commits
`memory_blob()` with the bundle, checkpoint durability is *exactly* bundle atomicity —
no extra machinery, and invariant 1 holds by construction. Failover resume falls out the
same way: the re-delivered element reloads the committed blob, `aget_tuple()` returns
the last committed superstep, and `ainvoke` continues from there.

Cap interaction: a checkpoint that pushes memory past the hard cap raises
`MemoryOverflow`, which fails the activation cleanly to `.errors` (documented; users
bound graph state with LangGraph message trimming). The user's compactor hook can see
`__langgraph__/` keys; the adapter documents that compactors must not evict them (same
trust model as any other load-bearing memory key).

*Alternative considered:* a dedicated proto field / new state spec for checkpoints —
rejected: new state surface, `--update` compatibility burden, and the facade already
provides staging, caps, and accounting.

### D3: Sync-core saver; async methods delegate

All saver I/O is against in-memory staged state (no network, no blocking), so the sync
methods are the implementation and the async variants
(`aget_tuple`/`aput`/`aput_writes`/`alist`) delegate directly — with zero I/O no
event-loop hand-off is needed, and nothing blocks the bridge event loop (ASYNC rules).
*(Inverted from the original draft's async-core wording during implementation: the sync
core is strictly simpler and equally loop-safe.)*

### D4: One suspension covers all pending graph work; the snapshot is a resume-map

When `ainvoke` returns with `__interrupt__` (or the ToolNode shim raised through
`GraphInterrupt`), the adapter stages one intent per pending item — `request_approval()`
for a plain `interrupt(...)`, `act(tool_name, args)` for each side-effect tool call —
and returns a single `Suspend(adapter="langgraph")`. The snapshot is a small proto-free
serialized map (JSON bytes): `intent_id → {kind: approval|tool, tool_call_id, interrupt_id}`
plus results already collected. The checkpoint itself is NOT in the snapshot — it is in
memory (D2); the snapshot only carries the correlation the runtime doesn't track.

Resume: each re-injected `ToolResult`/`Approval` re-invokes the agent with the snapshot.
The adapter records the result in the map; if intents remain unanswered it re-suspends
(same seq, step-index seeded by the runtime, so no intent re-mint); when all are
answered it resumes the graph once — `Command(resume=...)` for approvals, and the
shim converts collected results to `ToolMessage`s with their original `tool_call_id`s.
Intent IDs stay deterministic because they come from `ctx.act()`'s step counter
(invariant 2); a replayed bundle stages byte-identical intents. The snapshot bytes
themselves are NOT byte-stable across replays — interrupt ids derive from LangGraph's
time-based checkpoint ids — and need not be: the snapshot commits atomically with the
checkpoint it references, so the pair is always mutually consistent, and the
effectively-once argument rests on intent bytes alone (implementation finding).

*Alternative considered:* one suspension per tool call (strictly sequential) — rejected:
it serializes parallel tool calls the graph author intentionally issued and multiplies
re-injection round-trips.

### D5: ToolNode shim interrupts on side-effect tools instead of executing them

`BeamToolNode(tools)` accepts runtime `Tool` objects (the `@tool` registry kind). For
each tool call in the incoming `AIMessage`: read-only tools execute inline through the
wrapped callable (validated args, same semantics as LangGraph's own node); tools with
`side_effect=True` are collected and raised as a `GraphInterrupt` carrying the tool
calls (name, args, `tool_call_id`). Calling a side-effect `Tool` directly raises
`SideEffectToolError` (registry D3), so even a mis-wired graph cannot execute an effect
in-pipeline — the shim is the sanctioned detour, and adoption is exactly "re-tag +
swap the node class". The interrupt resumes (LangGraph re-executes the node from its
start — its documented semantics) with the results map, and the node emits
`ToolMessage`s without re-raising for already-answered calls.

### D6: httpx transport hook rewrites recognized clients; unrecognized ones warn and fall back

`_install_transport(model, ctx)` recognizes provider clients whose HTTP stack is an
`httpx.AsyncClient` reachable through known attributes (langchain-anthropic /
langchain-openai style: `client._client` on the underlying SDK). Recognized clients get
their transport swapped for a `_ReplayTransport` that: parses the provider request body
(provider-shaped JSON — precisely what `LlmRequest.messages/tools_schema/
sampling_params` hold), builds `LlmRequest(model_id=body["model"], ...)`, awaits
`ctx.call_model(request)`, and materializes the `LlmResponse.response` bytes as an
`httpx.Response(200)`. Cache hit ⇒ zero provider calls on bundle retry (invariant 3);
cache miss ⇒ `call_model` reaches the runtime's configured `LLMClient` provider (the
single model seam — FakeLLM in tests, the real provider client in production) and the
context caches the response. The graph's model object supplies prompt shaping and
response parsing; the runtime's provider owns the wire. Unrecognized model objects log **one warning per DoFn instance**
(worker-local guard) naming the model class and stating that replay-cache protection is
off, then run untouched. Fallback is a degradation, never an error — the graph still
completes.

*Alternative considered:* a LangChain `BaseChatModel` wrapper users must adopt —
rejected as an authoring-surface change; the transport hook keeps existing graphs
unmodified. The wrapper remains possible later for non-httpx clients.

### D7: Packaging and import isolation

New extra `langgraph = ["langgraph>=0.2,<0.4", "langchain-core>=0.3"]` (exact pins set
at implementation against the lockfile). `beam_agents.adapters.langgraph` imports
LangGraph at module scope (it's the extra's own module); `beam_agents/__init__.py`
exposes `LangGraphAgent` via lazy `__getattr__` that raises an actionable `ImportError`
("install beam-agents[langgraph]") when the extra is absent. Unit tests importorskip;
CI adds the extra to the unit matrix so adapter tests actually run.

## Risks / Trade-offs

- **[LangGraph API drift]** (checkpointer ABC, interrupt internals evolve quickly) →
  pin a tested version range in the extra; adapter tests exercise the public seams
  (`BaseCheckpointSaver` contract test, `interrupt`/`Command` round-trip) so a bump
  fails loudly in CI, not in users' pipelines.
- **[Checkpoint size vs. 1 MiB cap]** — long message histories overflow working
  memory → `MemoryOverflow` routes the activation to `.errors` (fail-closed, no partial
  state); docs prescribe LangGraph-side trimming/summarization; soft-cap warning +
  counter give early signal.
- **[Node re-execution on resume]** — LangGraph re-runs the interrupted node from its
  start; code before `interrupt()`/the shim in that node executes again → documented
  loudly; model calls in re-run nodes are replay-cached so re-execution is cheap and
  deterministic; effects can only live behind intents anyway (invariant 5).
- **[Transport fallback hides replay-cache loss]** → warning names the model class and
  a Beam metrics counter (`beam_agents.adapters/transport_fallback`) makes it visible
  on dashboards, not just logs.
- **[Non-determinism inside user graphs]** (clocks, randomness in nodes) → same
  contract as hand-written agents: documented requirement, chaos/retry semantics tests
  cover the model/tool paths the runtime controls.

## Migration Plan

Purely additive: new package + new extra; no state schema change, no behavior change
for existing users. Rollback = don't use the adapter. No `--update` concerns (the
reserved namespace only exists for keys that ran a LangGraph agent).

## Open Questions

- Exact LangGraph version floor (settle at implementation time against the current
  release; the checkpointer ABC stabilized in `langgraph-checkpoint` ≥ 2.0).
- Whether `BeamToolNode` should also accept plain LangChain tools (treating them all as
  read-only) for zero-change adoption of graphs with no side effects — leaning yes if
  it costs nothing; the spec only mandates runtime-`Tool` handling.
