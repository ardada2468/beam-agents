# Delta Spec: adk-adapter

## ADDED Requirements

### Requirement: AdkAgent runs an ADK agent as an activation

`AdkAgent` SHALL wrap a Google ADK agent (an `LlmAgent` or `BaseAgent` tree) as a
runtime `Agent`: an activation SHALL construct an ADK `Runner` over the adapter's
session service, drive its event stream to completion on the bridge event loop under a
per-key session (`session_id` and `user_id` derived from the entity key), and return
`Complete` with the run's final output when no long-running tool calls are pending. The
adapter SHALL NOT restructure the user's agent: no sub-agent, tool, or model
substitution, and no adapter state stored on the agent (ADK's own `Runner` performs one
idempotent normalization — setting an unset root `mode` to `"chat"` — which the adapter
neither performs nor suppresses). The core package SHALL NOT import ADK:
`import beam_agents` MUST succeed without the `adk` extra installed, and accessing
`beam_agents.AdkAgent` without the extra MUST raise an `ImportError` naming
`beam-agents[adk]`.

#### Scenario: Fast-path run completes in one activation

- **WHEN** an ADK agent whose conversation needs only the model and read-only tools is
  run by `AdkAgent` for one element
- **THEN** the activation returns `Complete` carrying the run's final output, and the
  committed working memory contains the session under the reserved `__adk__/` namespace

#### Scenario: Core import works without the extra

- **WHEN** `beam_agents` is imported in an environment without the `adk` extra
- **THEN** the import succeeds, and accessing `beam_agents.AdkAgent` raises
  `ImportError` with a message naming `beam-agents[adk]`

### Requirement: Session state commits atomically with the bundle

`BeamSessionService` SHALL implement ADK's session-service contract over the
activation's `Memory` facade, persisting the per-key session (state dict and event
history, with each appended event's `state_delta` applied) under the reserved `__adk__/`
key namespace, staged with the activation and committed only with the Beam bundle. A
failed or timed-out activation SHALL leave no session mutation. Retention SHALL be one
session per key. An oversized session SHALL fail the activation closed via the memory
cap (no partial state).

#### Scenario: Failed activation leaves no partial session

- **WHEN** a run appends events to the session and then a later step raises, failing the
  activation
- **THEN** the element routes to the errors output and the committed `MemoryBlob` is
  byte-identical to its pre-activation state, containing none of the appended events

#### Scenario: Worker failover resumes from the committed session

- **WHEN** an activation suspends with a committed session and the resuming element is
  processed by a fresh DoFn instance (as after worker loss)
- **THEN** the run resumes over the committed session's state and event history and
  completes with the same output the original worker would have produced

#### Scenario: One session per key

- **WHEN** multiple activations run on the same key and the session service is asked to
  list sessions for that key's identity
- **THEN** at most one session is returned, holding the accumulated committed state and
  events for that key

### Requirement: Side-effect tools suspend via long-running function calls

The adapter SHALL provide a tool tagging shim mapping runtime `Tool` objects to ADK
tools. Tool calls whose tool is `side_effect=False` SHALL execute inline with validated
arguments. Tool calls whose tool is `side_effect=True` SHALL NOT execute in the
pipeline: the shim SHALL surface them as pending long-running function calls, the
adapter SHALL stage one `ToolIntent` per call (deterministic step-indexed `intent_id`s),
and the activation SHALL return `Suspend` with `adapter` `"adk"`. When all pending
results have re-entered on the same key, the run SHALL resume over the committed session
with each `ToolResult` delivered as a function response carrying the original ADK
function-call identity. Adopting the outbox for an existing ADK agent SHALL require only
re-tagging its tools with the runtime `@tool` decorator and wrapping them with the shim
— no agent-tree changes.

#### Scenario: Side-effect tool call suspends instead of executing

- **WHEN** the model requests a call to a `side_effect=True` tool during a run
- **THEN** the tool's callable does not run, a `ToolIntent` with that tool's name and
  arguments is staged with a deterministic `intent_id`, and the activation returns
  `Suspend`

#### Scenario: Read-only tool executes inline

- **WHEN** the model requests a call to a `side_effect=False` tool during a run
- **THEN** the tool executes inside the activation with validated arguments and its
  return value is delivered to the model as that call's function response, with no
  intent staged

#### Scenario: ToolResult resumes the run as a function response

- **WHEN** the `ToolResult` for a suspended side-effect call re-enters on the same key
  and all pending intents are answered
- **THEN** the run resumes from the committed session and the model receives a function
  response whose identity matches the original function call and whose content carries
  the result payload

#### Scenario: Parallel side-effect calls resume after all results arrive

- **WHEN** one model turn requests two side-effect tool calls and their results
  re-enter one at a time
- **THEN** the first re-injection re-suspends without resuming the run or staging new
  intents, and the second resumes the run with both function responses present

#### Scenario: Bundle replay stages byte-identical intents

- **WHEN** the same suspending activation is executed twice from identical committed
  state (as under a bundle retry)
- **THEN** both executions stage byte-identical `ToolIntent`s, including equal
  `intent_id`s

### Requirement: Approval requests map to approval intents

The adapter SHALL provide an approval shim tool through which an ADK agent requests
human approval: a pending approval call SHALL stage an approval-kind intent through the
activation context (deterministic `intent_id`, stamped TTL) and suspend the activation
so the HITL timer arms. When the matching `Approval` re-enters on the same key, the run
SHALL resume with the decision delivered as the approval call's function response.

#### Scenario: Approval request suspends with a staged approval intent

- **WHEN** the agent invokes the approval shim during a run
- **THEN** the activation returns `Suspend`, exactly one approval-kind intent is staged
  on the approval channel with a deterministic `intent_id`, and no agent step after the
  approval request has executed

#### Scenario: Approval decision resumes the run

- **WHEN** the approval for the staged intent re-enters on the same key
- **THEN** the run resumes from the committed session with the decision (approved flag,
  approver, decision time) delivered as the approval call's function response, and the
  activation completes with the run's final output

### Requirement: The ADK event stream is teed into the activation trace

The adapter SHALL tee the events drained from the ADK run into the activation's trace
surface using only deterministic projections: inline tool executions SHALL be staged as
`TOOL_CALL` trace events carrying the tool name and the `beam_agents.adapter` attribute
with value `"adk"`; model turns SHALL surface as `LLM_CALL` events via the cache-first
model path. Staged trace events SHALL derive timestamps from the activation clock and
identity from the runtime's deterministic span formulas only — ADK-generated event ids,
timestamps, and invocation ids SHALL NOT appear in trace bytes — so a replayed bundle
emits byte-identical trace events.

#### Scenario: Inline tool executions appear as TOOL_CALL trace events

- **WHEN** a run executes two read-only shim tools inline
- **THEN** the committed `.traces` for that activation contain a `TOOL_CALL` event per
  execution, each carrying the tool's name and the `beam_agents.adapter` attribute
  `"adk"`, correlated to the activation's trace and span identity

#### Scenario: Trace bytes are replay-deterministic

- **WHEN** the same activation is executed twice from identical committed state (as
  under a bundle retry)
- **THEN** the serialized trace events staged by the two executions are byte-identical

### Requirement: Model calls route through the runtime LLMClient

The adapter SHALL route recognized chat-model clients' provider HTTP calls through the
activation's cache-first model path (`call_model`), so bundle retries incur zero
additional provider calls on the cached path. For model objects whose HTTP client
cannot be recognized, the adapter SHALL log a warning at most once per agent instance
(naming the model class and the lost replay-cache protection), increment the transport
fallback metric, and leave the model untouched so the run still completes.

#### Scenario: Recognized client is replay-cached across retries

- **WHEN** an ADK agent using a recognized httpx-backed model client runs an activation
  twice from identical committed state (as under a bundle retry)
- **THEN** the second execution serves every model call from the replay cache with zero
  provider HTTP requests and the run output is identical

#### Scenario: Unrecognized client warns once and falls back

- **WHEN** an ADK agent uses a model object whose HTTP client the adapter does not
  recognize and two activations run on the same agent instance
- **THEN** the model's calls go directly to the provider, exactly one warning naming the
  model class is logged, the fallback counter increments, and the run completes normally

### Requirement: The ADK adapter passes the full conformance matrix

The ADK adapter SHALL be registered in the conformance registry
(`tests/conformance/_registry.py`) as a `ConformanceAdapter` whose factory translates
every conformance `ScenarioSpec` into an ADK agent using the shim, the session service,
and the transport-routed model seam. All seven conformance scenarios (`single_shot`,
`multi_tool_inline`, `suspension_resume`, `approval_timeout_fallback`,
`restart_mid_suspension`, `bundle_retry_cache`, `ttl_expiry`) SHALL pass for the ADK
adapter on both legs — DirectRunner and Flink — subject only to the matrix's declared
skips, which the meta-test counts as cells: the existing scenario-level per-leg
declarations, plus per-adapter declarations for scenarios whose *construction* is not
expressible in a framework's semantics. Exactly one such per-adapter declaration exists
for ADK: `bundle_retry_cache`, whose premise is a resume issuing no novel model request,
which ADK's resume semantics (a function response always drives one summarization turn)
make unreachable. In environments without the `adk` extra, the ADK cells SHALL report as
clean skips naming the missing package.

#### Scenario: All seven scenarios pass on both legs

- **WHEN** the conformance matrix runs with the ADK adapter registered and the extra
  installed
- **THEN** every ADK cell across the seven scenarios passes on the DirectRunner leg and
  on the Flink leg (declared per-leg and per-adapter skips reporting as skips carrying
  their reason, not failures), and the meta-test's expected cell count includes the ADK
  adapter's cells

#### Scenario: Unregistered ADK package fails collection

- **WHEN** `beam_agents.adapters.adk` is importable but the conformance registry has no
  `adk` registration
- **THEN** conformance collection fails with an error naming the `adk` subpackage
