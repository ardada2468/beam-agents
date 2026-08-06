# Framework adapters

Agent *authoring* deliberately does not live here — it belongs to LangGraph,
Google ADK, Pydantic AI, or a plain async function. An adapter runs an agent
authored in one of those frameworks on this runtime, so it acquires the
guarantees the framework lacks: durable keyed checkpoints that commit
atomically with the Beam bundle, effectively-once side effects through the
staged-intent path, [HITL approvals](hitl.md) with the fail-closed timeout,
and replay-cached model calls.

There is **no adapter base class**. The runtime protocol *is* the seam: an
adapter is an async callable over the activation context returning
`Complete | Suspend`, exactly like a hand-written agent
([`StreamAgent`](api.md#agent-authoring-beam_agentscore)). Each adapter
subpackage owns its framework dependency behind an extra — `import
beam_agents` never imports a framework, and a missing extra surfaces as an
`ImportError` naming the extra to install.

| Adapter | Wraps | Extra |
| --- | --- | --- |
| `LangGraphAgent` | a compiled LangGraph `StateGraph` | `beam-agents[langgraph]` |
| `AdkAgent` | a Google ADK `LlmAgent`/`BaseAgent` tree | `beam-agents[adk]` |
| `PydanticAIAgent` | a `pydantic_ai.Agent` | `beam-agents[pydantic-ai]` |

All three share the same adoption shape: **the wrapped agent object is never
mutated**, side-effect tools are re-declared with the runtime's
`@tool(side_effect=True)` decorator, framework state persists latest-only
under a reserved working-memory namespace (the 1 MiB cap applies — trim or
summarize on the framework side), and recognized httpx-backed model clients
are routed through the runtime's replay-cached `LLMClient` path, with a
one-time warning and a `transport_fallback` metric for unrecognized ones.

## LangGraph

An existing graph adopts the runtime with three changes — no topology edits:

```python
from beam_agents import LangGraphAgent, RunAgent
from beam_agents.adapters.langgraph import BeamToolNode

# 1. re-declare side-effectful tools with @tool(side_effect=True)
# 2. swap LangGraph's prebuilt ToolNode for BeamToolNode(tools)
# 3. wrap the graph:
outputs = events | RunAgent(LangGraphAgent(graph, chat_models=[model]), config=config)
```

- **`BeamCheckpointSaver`** is a LangGraph checkpointer backed by keyed
  working memory (the reserved `__langgraph__/` namespace), so graph
  checkpoints commit with the bundle — latest-only, never a checkpoint
  history.
- **`interrupt(...)` becomes an approval intent.** A graph interrupt suspends
  the activation; the approval re-enters on the same key and resumes via
  `Command(resume=...)`. On resume the interrupted node re-runs from its
  start — LangGraph's own semantics — so keep pre-interrupt node code
  idempotent.
- **`BeamToolNode`** runs read-only tools inline through the runtime tool
  path and stages `side_effect=True` tools as intents; one suspension covers
  all pending graph work.

## Google ADK

```python
from beam_agents import AdkAgent, RunAgent
from beam_agents.adapters.adk import beam_tools

outputs = events | RunAgent(AdkAgent(agent, chat_models=[...]), config=config)
```

The activation builds a `BeamSessionService` over working memory (the
reserved `__adk__/` namespace, one session per key — safe by construction,
since Beam serializes per key), constructs a fresh ADK `Runner` around the
untouched user agent, and drains `run_async`. `beam_tools` builds the ADK
tool list from a runtime `ToolRegistry`: side-effect tools become
long-running function calls (`BeamLongRunningTool`) that stage intents and
suspend; `BeamApprovalTool` takes the approval channel; read-only tools run
inline (`BeamFunctionTool`). Re-injected results accumulate until every
pending intent is answered, then the run resumes once with the tool
round-trip presented exactly as if the tools had answered inline.

## Pydantic AI

Two changes, no restructuring of instructions, output types, or control flow:

```python
from beam_agents import PydanticAIAgent, RunAgent

# 1. re-declare side-effectful tools with @tool(side_effect=True); name any
#    read-only tool you want human-gated in approval_required
# 2. wrap the agent:
outputs = events | RunAgent(
    PydanticAIAgent(agent, tools=tools, approval_required=("close_account",)),
    config=config,
)
```

Message history persists latest-only under the reserved `__pydantic_ai__/`
namespace. A `side_effect=True` tool never executes in-pipeline: the tool is
declared *external*, the run ends cleanly at the call, the adapter stages one
`ToolIntent` per pending call and suspends; the re-injected result resumes a
fresh run seeded with the committed history plus the deferred results.
Approval-gated tools take the same shape through the approval channel;
read-only tools run inline through `BeamToolset` with validated arguments and
`TOOL_CALL` trace events.

## The conformance matrix

"Every adapter exhibits identical lifecycle semantics" is a
machine-verified release gate, not a code-review aspiration.
[`tests/conformance/`](https://github.com/ardada2468/beam-agents/tree/main/tests/conformance)
runs one shared scenario suite — completion, suspension/resume, approval and
its fail-closed timeout, retry determinism — across every registered adapter
*plus a reference agent* written directly against the runtime protocol, on
DirectRunner and on a real Flink cluster (the docker-backed `flink` leg, a
required check on `main`; a third `spark` leg runs weekly, best-effort).

The registry ([`tests/conformance/_registry.py`](https://github.com/ardada2468/beam-agents/blob/main/tests/conformance/_registry.py))
is an explicit list with a collection-time guard: shipping a new
`beam_agents.adapters` subpackage without registering its conformance factory
is a loud collection error, never a silently smaller matrix. Each factory is
also pinned to its scenario spec — tool names, effect classes, matcher count,
deadline — so an adapter cannot drift into testing a different conversation
than the one the spec declares.

Practically: if your agent behaves one way on DirectRunner in a test and
another way on Flink in production, that is a runtime defect covered by a
gate — [file it](https://github.com/ardada2468/beam-agents/issues), don't
work around it.

## API reference

The full adapter surface — every public class the three subpackages export —
is on the [API reference](api.md#adapters-beam_agentsadapters).
