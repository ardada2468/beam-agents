# Delta Spec: pydantic-ai-adapter

## ADDED Requirements

### Requirement: PydanticAIAgent runs a Pydantic AI agent as an activation

`PydanticAIAgent` SHALL wrap a user-constructed Pydantic AI agent as a runtime `Agent`:
an activation invokes the framework's run on the bridge event loop with the adapter's
persisted message history, toolset, and transport hook installed, and SHALL return
`Complete` with the encoded run output when the run finishes without pending deferred
tool calls. The core package SHALL NOT import Pydantic AI: `import beam_agents` MUST
succeed without the `pydantic-ai` extra installed, and accessing
`beam_agents.PydanticAIAgent` without the extra MUST raise an `ImportError` naming
`beam-agents[pydantic-ai]`.

#### Scenario: Fast-path run completes in one activation

- **WHEN** a Pydantic AI agent whose conversation needs only the model and read-only
  tools is run by `PydanticAIAgent` for one element
- **THEN** the activation returns `Complete` carrying the encoded run output, and the
  committed working memory contains the run's message history under the reserved
  `__pydantic_ai__/` namespace

#### Scenario: Core import works without the extra

- **WHEN** `beam_agents` is imported in an environment without the `pydantic-ai` extra
- **THEN** the import succeeds, and accessing `beam_agents.PydanticAIAgent` raises
  `ImportError` with a message naming `beam-agents[pydantic-ai]`

### Requirement: Message history persists through the Memory facade and commits atomically

The adapter SHALL persist the run's serialized message history through the activation's
`Memory` facade under the reserved `__pydantic_ai__/` key namespace, staged with the
activation and committed only with the Beam bundle. A failed or timed-out activation
SHALL leave no history mutation. Retention SHALL be latest-only per key: each committed
run overwrites the stored history with the run's full message list. A later activation
on the same key SHALL observe the committed history as its conversation context, and
the history SHALL be subject to ordinary working-memory TTL garbage collection and the
blob/working-memory size caps (an overflowing history fails the activation closed).

#### Scenario: Failed activation leaves no history mutation

- **WHEN** a run appends messages and then fails before the activation completes
- **THEN** the element routes to the errors output and the committed `MemoryBlob` is
  byte-identical to its pre-activation state

#### Scenario: Conversation continues across activations on the same key

- **WHEN** a second event arrives on a key whose earlier activation committed message
  history
- **THEN** the second run receives the committed history as its message context, and
  the committed history after the second activation reflects both conversations

#### Scenario: History expires with the memory TTL

- **WHEN** the watermark passes the configured memory TTL after an activation committed
  history and a later event then arrives on the same key
- **THEN** the later run observes no prior history and starts a fresh conversation

### Requirement: Side-effect and approval tool calls map to intents and suspension

Tool calls on `side_effect=True` tools SHALL NOT execute in the pipeline: the adapter
SHALL declare them to the framework as deferred/externally-executed, and a run ending
at such calls SHALL stage one `ToolIntent` per pending call (deterministic
step-indexed `intent_id`s) and return `Suspend` with `adapter` `"pydantic_ai"` so the
HITL timer arms. Approval-requiring calls SHALL stage approval-kind intents through the
approval channel the same way. When re-injected `ToolResult`s/`Approval`s re-enter on
the same key, the adapter SHALL accumulate them, re-suspending without staging new
intents while any pending call is unanswered, and SHALL resume by re-running the agent
with the committed message history plus the collected deferred results (tool results
keyed by their original tool call IDs; approvals as approved/denied decisions).
Replayed bundles SHALL stage byte-identical intents.

#### Scenario: Deferred side-effect call suspends instead of executing

- **WHEN** the model calls a `side_effect=True` tool during a run
- **THEN** the tool's callable does not run, a `ToolIntent` with that tool's name and
  arguments is staged with a deterministic `intent_id`, and the activation returns
  `Suspend`

#### Scenario: Re-injected result resumes the run with the deferred result

- **WHEN** the `ToolResult` for a suspended deferred call re-enters on the same key
- **THEN** the agent re-runs from the committed history with that result supplied for
  the original tool call ID, and the activation completes with an output reflecting
  the injected result

#### Scenario: Parallel deferred calls resume after all results arrive

- **WHEN** one model turn issues two deferred tool calls and their results re-enter one
  at a time
- **THEN** the first re-injection re-suspends without staging new intents, and the
  second resumes the run with both results supplied

#### Scenario: Approval-requiring call maps to an approval intent

- **WHEN** the model calls an approval-requiring tool and the matching `Approval`
  later re-enters on the same key
- **THEN** the suspension staged exactly one approval-kind intent on the approval
  channel with a deterministic `intent_id`, and the resumed run receives the
  approved/denied decision for the original tool call

#### Scenario: Bundle replay stages byte-identical intents

- **WHEN** the same suspending activation is executed twice from identical committed
  state (as under a bundle retry)
- **THEN** both executions stage byte-identical intents, including equal `intent_id`s

### Requirement: Read-only tools execute inline through the runtime tool path

Tool calls on `side_effect=False` tools SHALL execute inline within the activation via
the runtime's `run_tool` path: arguments validated against the tool's argument model,
`SideEffectToolError` protection applied before execution, the call counted in the
activation tally, and a `TOOL_CALL` trace event staged. The tool's return value SHALL
be delivered to the run as that tool call's result, and no intent SHALL be staged.

#### Scenario: Read-only tool runs inline with a trace event

- **WHEN** the model calls a `side_effect=False` tool during a run
- **THEN** the tool executes inside the activation, its result is returned to the
  conversation, `.intents` receives nothing for the call, and the committed `.traces`
  include a `TOOL_CALL` event naming the tool

### Requirement: Model requests route through the runtime LLMClient

The adapter SHALL install the shared replay transport on recognized Pydantic AI model
objects (models whose provider SDK client exposes an `httpx.AsyncClient`) so their
provider HTTP calls are served through the activation's cache-first model path
(`call_model`), making bundle retries incur zero additional provider calls on the
cached path. For model objects whose HTTP client cannot be recognized, the adapter
SHALL log a warning at most once per agent instance (naming the model class and the
lost replay-cache protection), increment the transport-fallback metric, and leave the
model untouched so the run still completes.

#### Scenario: Recognized model is replay-cached across retries

- **WHEN** an activation using a recognized httpx-backed model runs twice from
  identical committed state (as under a bundle retry)
- **THEN** the second execution serves every model call from the replay cache with zero
  provider HTTP requests and produces identical output

#### Scenario: Unrecognized model warns once and falls back

- **WHEN** an agent uses a model object whose HTTP client the adapter does not
  recognize and two activations run on the same instance
- **THEN** the model's calls go directly to the provider, exactly one warning naming
  the model class is logged, the fallback counter increments, and the runs complete
  normally

### Requirement: Run usage accumulates into the activation tally

After each run segment, the adapter SHALL fold the framework's reported token usage
into the activation tally via `accumulate_usage`, so a Pydantic AI activation reports
its total tokens with usage observed. Per-call billed/unbilled usage attribution in
trace events SHALL remain the responsibility of the runtime's configured response
decode, unchanged by the adapter.

#### Scenario: Completed run reports usage in the tally

- **WHEN** an activation completes a run whose model responses carry token usage
- **THEN** the activation's tally reports a non-zero total token count with usage
  observed

### Requirement: The adapter passes the full conformance matrix

The adapter SHALL register a `pydantic_ai` conformance adapter (with
`requires="pydantic_ai"` and `adapters_subpackage="pydantic_ai"`) whose factories
translate every canonical `ScenarioSpec` into a behaviorally equivalent Pydantic AI
agent, and SHALL pass all seven conformance scenarios — single_shot,
multi_tool_inline, suspension_resume, approval_timeout_fallback,
restart_mid_suspension, bundle_retry_cache, ttl_expiry — on both the DirectRunner and
Flink legs, subject only to the matrix's existing per-scenario declared leg skips.
Where the extra is not installed, the adapter's cells SHALL skip cleanly with the
missing-package reason; where the subpackage is importable, the conformance registry
guard SHALL find it registered.

#### Scenario: All seven scenarios pass on both legs

- **WHEN** the conformance matrix runs with the `pydantic-ai` extra installed on the
  DirectRunner leg and on the Flink leg
- **THEN** every `pydantic_ai` cell for all seven scenarios passes on each leg, with
  only the matrix's declared per-leg skips (bundle_retry_cache and ttl_expiry on
  Flink) reported as skips, and the meta-test's cell accounting includes the
  `pydantic_ai` axis entry

#### Scenario: Missing extra skips cells without shrinking the matrix silently

- **WHEN** the conformance suite is collected in an environment without the
  `pydantic-ai` extra
- **THEN** the `pydantic_ai` cells report as skipped with the missing-package reason,
  collection does not error, and all other adapters' cells still run
