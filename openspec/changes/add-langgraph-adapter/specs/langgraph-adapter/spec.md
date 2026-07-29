# Delta Spec: langgraph-adapter

## ADDED Requirements

### Requirement: LangGraphAgent runs a compiled graph as an activation

`LangGraphAgent` SHALL wrap a compiled LangGraph graph as a runtime `Agent`: an
activation invokes the graph on the bridge event loop with a per-key thread
(`thread_id` derived from the entity key), the adapter's checkpointer and transport
hook installed, and SHALL return `Complete` with the graph's final output when the
graph runs to completion without pending interrupts. The core package SHALL NOT import
LangGraph: `import beam_agents` MUST succeed without the `langgraph` extra installed,
and accessing `beam_agents.LangGraphAgent` without the extra MUST raise an
`ImportError` naming `beam-agents[langgraph]`.

#### Scenario: Fast-path graph completes in one activation

- **WHEN** a compiled graph whose nodes call only the model and read-only tools is run
  by `LangGraphAgent` for one element
- **THEN** the activation returns `Complete` carrying the graph's final output, and the
  committed working memory contains the graph's latest checkpoint under the reserved
  `__langgraph__/` namespace

#### Scenario: Core import works without the extra

- **WHEN** `beam_agents` is imported in an environment without the `langgraph` extra
- **THEN** the import succeeds, and accessing `beam_agents.LangGraphAgent` raises
  `ImportError` with a message naming `beam-agents[langgraph]`

### Requirement: Checkpoints commit atomically with the bundle

`BeamCheckpointSaver` SHALL persist the latest graph checkpoint (channel values
including message history, checkpoint metadata, and pending writes) through the
activation's `Memory` facade under the reserved `__langgraph__/` key namespace, staged
with the activation and committed only with the Beam bundle. A failed or timed-out
activation SHALL leave no checkpoint mutation. Retention SHALL be latest-only per key.

#### Scenario: Failed activation leaves no partial checkpoint

- **WHEN** a graph writes checkpoints for two supersteps and then a node raises,
  failing the activation
- **THEN** the element routes to the errors output and the committed `MemoryBlob` is
  byte-identical to its pre-activation state, containing neither checkpoint

#### Scenario: Worker failover resumes mid-graph

- **WHEN** an activation suspends mid-graph with a committed checkpoint at superstep N
  and the resuming element is processed by a fresh DoFn instance (as after worker loss)
- **THEN** the graph resumes from the committed superstep-N checkpoint — nodes
  completed before N do not re-execute — and completes with the same output the
  original worker would have produced

#### Scenario: Latest-only retention

- **WHEN** a graph runs multiple supersteps in one activation
- **THEN** the committed reserved namespace holds exactly one checkpoint tuple — the
  latest — and listing checkpoints through the saver returns at most that tuple

### Requirement: Graph interrupts map to approval intents and resume via Command

A LangGraph `interrupt(...)` raised inside the graph SHALL suspend the activation: the
adapter stages an approval intent through the activation context (deterministic
step-indexed `intent_id`, stamped TTL) and returns `Suspend` with `adapter`
`"langgraph"` so the HITL timer arms. When the matching `Approval` re-enters on the
same key, the adapter SHALL resume the graph from the committed checkpoint with
`Command(resume=<approval payload>)`. Replayed bundles SHALL stage byte-identical
intents.

#### Scenario: Interrupt suspends with a staged approval intent

- **WHEN** a graph node calls `interrupt(payload)` during an activation
- **THEN** the activation returns `Suspend`, exactly one approval intent is staged on
  the approval channel with a deterministic `intent_id` derived from
  `(entity_key, seq, step_index)`, and no graph node after the interrupting one has
  executed

#### Scenario: Approval resumes the graph with Command

- **WHEN** the approval for the staged intent re-enters on the same key and the agent
  is re-invoked with the suspension snapshot
- **THEN** the graph resumes from the committed checkpoint with
  `Command(resume=<approval payload>)` and the activation completes with the graph's
  final output

#### Scenario: Bundle replay stages byte-identical intents

- **WHEN** the same interrupting activation is executed twice from identical committed
  state (as under a bundle retry)
- **THEN** both executions stage byte-identical approval intents, including equal
  `intent_id`s

### Requirement: ToolNode shim converts side-effect tools to suspension

The adapter SHALL provide a ToolNode replacement that accepts runtime `Tool` objects.
Tool calls whose tool is `side_effect=False` SHALL execute inline with validated
arguments. Tool calls whose tool is `side_effect=True` SHALL NOT execute in the
pipeline: the shim SHALL convert them into staged `ToolIntent`s (one per tool call,
deterministic `intent_id`s) and suspend the activation. When all pending results have
re-entered, the graph SHALL resume with each `ToolResult` delivered as a `ToolMessage`
carrying the original `tool_call_id`. Adopting the outbox for an existing graph SHALL
require only re-tagging its tools with the runtime `@tool` decorator and swapping the
node class — no graph topology changes.

#### Scenario: Side-effect tool call suspends instead of executing

- **WHEN** the model requests a call to a `side_effect=True` tool and the shim node
  processes it
- **THEN** the tool's callable does not run, a `ToolIntent` with that tool's name and
  arguments is staged with a deterministic `intent_id`, and the activation returns
  `Suspend`

#### Scenario: Read-only tool executes inline

- **WHEN** the model requests a call to a `side_effect=False` tool and the shim node
  processes it
- **THEN** the tool executes inside the activation and its return value is delivered
  to the graph as a `ToolMessage` for that `tool_call_id`, with no intent staged

#### Scenario: ToolResult resumes the graph as a ToolMessage

- **WHEN** the `ToolResult` for a suspended side-effect call re-enters on the same key
- **THEN** the graph resumes and the tool's node emits a `ToolMessage` whose
  `tool_call_id` matches the original tool call and whose content carries the result
  payload

#### Scenario: Parallel side-effect calls resume after all results arrive

- **WHEN** one model turn requests two side-effect tool calls and their results
  re-enter one at a time
- **THEN** the first re-injection re-suspends without invoking the graph or staging new
  intents, and the second resumes the graph with both `ToolMessage`s present

#### Scenario: Adoption by re-tagging only

- **WHEN** an existing graph's tools are re-declared with `@tool(side_effect=True)` and
  its tool node is swapped for the shim, with no other graph changes
- **THEN** the graph compiles and runs under `LangGraphAgent` with its side effects
  flowing through staged intents

### Requirement: Chat model calls route through the runtime LLMClient

The adapter SHALL install an httpx transport hook on recognized chat-model clients so
their provider HTTP calls are served through the activation's cache-first model path
(`call_model`), making bundle retries incur zero additional provider calls on the
cached path. For model objects whose HTTP client cannot be recognized, the adapter
SHALL log a warning at most once per DoFn instance (naming the model class and the
lost replay-cache protection), increment a fallback metric, and leave the model
untouched so the graph still runs.

#### Scenario: Recognized client is replay-cached across retries

- **WHEN** a graph using a recognized httpx-backed chat model runs an activation twice
  from identical committed state (as under a bundle retry)
- **THEN** the second execution serves every model call from the replay cache with zero
  provider HTTP requests and the graph output is identical

#### Scenario: Unrecognized client warns once and falls back

- **WHEN** a graph uses a model object whose HTTP client the adapter does not recognize
  and two activations run on the same DoFn instance
- **THEN** the model's calls go directly to the provider, exactly one warning naming
  the model class is logged, the fallback counter increments, and the graph completes
  normally
