# Proposal: add-langgraph-adapter

## Why

beam-agents is an agent *runtime*, not an authoring framework: users are meant to bring
graphs written in LangGraph and run them as keyed, fault-tolerant Beam transforms. Today
there is no adapter — a LangGraph graph cannot use durable keyed memory, the outbox
side-effect path, HITL approvals, or the replay cache without being rewritten against
`AgentContext` by hand. This change adds the first framework adapter (the
`adapters/langgraph` slot already reserved in the module map) so an **existing** LangGraph
graph adopts the runtime's correctness guarantees with minimal changes — re-tagging its
side-effectful tools, not restructuring the graph.

## What Changes

- New `beam_agents.adapters` package with the shared adapter seam and the first concrete
  adapter, `LangGraphAgent`, which wraps a compiled LangGraph graph as a
  `StreamAgent`-compatible activation.
- **`BeamCheckpointSaver`**: a LangGraph `BaseCheckpointSaver` that persists graph
  checkpoints (channel values including message history, checkpoint metadata, pending
  writes) into a reserved section of the activation's working memory via the `Memory`
  facade. Checkpoints are therefore staged in the activation context and commit
  atomically with the Beam bundle — a failed/timed-out activation leaves no partial
  checkpoint (correctness invariant 1), and a worker failover resumes the graph from the
  last committed superstep on the same key.
- **Interrupt → intent/approval mapping with `Command` resume**: a LangGraph
  `interrupt(...)` inside the graph suspends the activation — the adapter stages an
  approval intent (deterministic `intent_id`, correctness invariant 2), persists a
  `Continuation`, and returns `Suspend`. When the `Approval`/`ToolResult` re-enters on
  the same key, the adapter resumes the graph with `Command(resume=<payload>)` from the
  committed checkpoint.
- **ToolNode shim**: a drop-in replacement for LangGraph's prebuilt `ToolNode` that
  understands the runtime's `@tool(side_effect=True)` tagging. Read-only tools execute
  inline (unchanged LangGraph semantics); side-effectful tools convert the tool call into
  a staged `ToolIntent` plus graph interruption instead of executing in-pipeline
  (correctness invariant 5). The re-injected `ToolResult` resumes the graph as that tool
  call's `ToolMessage`. Adopting the outbox requires only re-tagging tools with the
  runtime's `@tool` decorator and swapping the node class — no graph topology changes.
- **httpx transport hook**: chat-model calls made by the graph (LangChain provider
  clients built on httpx) are routed through the runtime's `LLMClient`/replay-cache path
  by installing a custom httpx transport on recognized provider clients, so bundle
  retries incur zero extra provider calls (correctness invariant 3). Unrecognized model
  clients that cannot be intercepted produce a **warning** (once per DoFn instance) and
  fall back to direct provider calls — the graph still runs, minus replay-cache
  protection.
- New optional dependency extra (`beam-agents[langgraph]`); the core package keeps zero
  LangGraph imports at module scope, and all tests run offline with FakeLLM.
- `LangGraphAgent` is re-exported from `beam_agents/__init__.py` (adapter classes are
  part of the sanctioned public API surface).

## Capabilities

### New Capabilities

- `langgraph-adapter`: running a compiled LangGraph graph as a beam-agents activation —
  checkpoint persistence in working memory with bundle-atomic commit and mid-graph
  failover resume, interrupt→approval mapping with `Command` resume, the side-effect
  ToolNode shim over the outbox, and the httpx transport hook routing chat models
  through the runtime `LLMClient` with warning fallback.

### Modified Capabilities

_None._ The adapter composes existing seams (`Memory` facade, `ActivationContext`
staging, `LLMClient`, `@tool` registry, `Suspend`/`Continuation`) without changing their
requirements; the checkpoint section lives under a reserved key prefix inside the
existing per-key `MemoryBlob`.

## Impact

- **New code**: `src/beam_agents/adapters/__init__.py`, `adapters/protocol.py` (shared
  adapter seam), `adapters/langgraph/` (agent wrapper, `BeamCheckpointSaver`, ToolNode
  shim, transport hook, checkpoint serde).
- **Dependencies**: new optional extra `langgraph` (`langgraph`, `langchain-core`;
  version-pinned). Core install unaffected; `make test-unit` stays offline — LangGraph
  is a test dependency for the adapter test module only, with `pytest.importorskip`
  guarding collection.
- **Public API**: `beam_agents.LangGraphAgent` (+ `BeamCheckpointSaver` and the ToolNode
  shim exported from `beam_agents.adapters.langgraph`).
- **State**: checkpoints live inside the existing `MemoryBlob` under a reserved
  `__langgraph__/` key namespace — no new state specs, no proto schema change; blob and
  working-memory caps (100 KiB / 1 MiB) apply to checkpoints, which bounds graph state.
- **Systems**: no new topics, effector unchanged (intents from the adapter are ordinary
  `ToolIntent`s), no CI workflow changes beyond the new extra in the test matrix.
