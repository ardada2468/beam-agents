# agent-context Specification

## Purpose
TBD - created by archiving change add-agent-context. Update Purpose after archive.
## Requirements
### Requirement: Activation-scoped AgentContext surface

The system SHALL provide an `AgentContext` constructed once per activation and handed to the agent for exactly one element. It SHALL expose the activation scope as read-only attributes — `entity_key` (`bytes`), `seq` (`int`), and the injected `now_ms` clock — together with the activation's working `Memory`, its `LlmFacade`, and read-only tool execution. The `AgentContext` MUST NOT read wall-clock time, generate un-seeded randomness, or perform Beam state I/O directly; every non-determinism source is injected at construction so a replayed bundle behaves identically. Constructing an `AgentContext` SHALL NOT mutate keyed state or emit any output.

#### Scenario: Context exposes the injected activation scope

- **WHEN** an `AgentContext` is constructed with `entity_key`, `seq`, and `now_ms`
- **THEN** those values are readable on the context and are the only source the context uses for scope and time

#### Scenario: Constructing a context has no side effects

- **WHEN** an `AgentContext` is constructed
- **THEN** no memory mutation, cache insert, intent, trace, or output has been produced, and the drained result would be empty

### Requirement: Staged-effects accumulator applied only on success

The `AgentContext` SHALL accumulate every effect an activation produces — memory mutations, replay-cache inserts, `ToolIntent`s, `TraceEvent`s, and outputs — in activation-local state and SHALL NOT apply any of them to keyed state or emit any output during the activation. The context SHALL expose a single `drain()` operation that returns the accumulated effects as an `AgentResult` and marks the context drained. `drain()` SHALL be callable at most once; a second call SHALL raise. Only the caller that owns the activation (the DoFn, on activation success) invokes `drain()`; an activation that raises or times out is never drained, so it contributes nothing (correctness invariant #1: atomic commit — a failed activation mutates nothing).

#### Scenario: Effects are withheld until drain

- **WHEN** an activation writes memory, stages a trace, and emits an output but `drain()` has not been called
- **THEN** no keyed state is mutated and no output is emitted; the effects exist only inside the context

#### Scenario: Drain returns the full accumulated bundle

- **WHEN** `drain()` is called after an activation staged memory writes, cache inserts, intents, traces, and outputs
- **THEN** the returned `AgentResult` carries all of them and the context is marked drained

#### Scenario: A failed activation contributes nothing

- **WHEN** an activation stages effects and then raises before the owner calls `drain()`
- **THEN** the context is never drained and none of its staged effects are applied or emitted

#### Scenario: Draining twice is refused

- **WHEN** `drain()` is called a second time on the same context
- **THEN** it raises rather than replaying or duplicating the staged effects

### Requirement: Context is the model facade's staging sink

The `AgentContext` SHALL satisfy the model facade's `StagingSink` protocol structurally, providing `stage_trace_event(event)` and `accumulate_usage(usage)`. Trace events staged by the `LlmFacade` SHALL land in the context's staged trace bundle, and accumulated token usage SHALL be recorded on the context so it is available to the drained `AgentResult`. Replay-cache inserts performed by the `LlmFacade` during the activation SHALL likewise be staged and applied only on `drain()`.

#### Scenario: Facade-staged traces land in the context bundle

- **WHEN** the activation's `LlmFacade.complete` stages an `LLM_CALL` trace event
- **THEN** that event appears in the context's staged traces and is present on the drained `AgentResult`

#### Scenario: Accumulated usage survives to the result

- **WHEN** two cache-missing model calls accumulate token usage through the context
- **THEN** the drained `AgentResult` reports the summed billed usage

### Requirement: Side effects flow only through ctx.act as deterministic intents

The `AgentContext` SHALL expose `act(tool_name, arguments)` as the ONLY path to request a side effect. `act` SHALL record a `ToolIntent` into the staged bundle rather than executing anything; the underlying `side_effect=True` tool SHALL never run inside the pipeline (correctness invariant #5). Each intent's `intent_id` SHALL be derived deterministically as `uuid5(NAMESPACE, entity_key + seq + step_index)`, where `step_index` is a per-activation counter that advances by one on each `act` call, so a replayed activation that walks the same path produces byte-identical intents. The intent SHALL carry `entity_key`, `seq`, `step_index`, `tool_name`, and the arguments serialized as canonical JSON. Attempting to execute a `side_effect=True` tool through the read-only tool path SHALL raise.

#### Scenario: act stages an intent without executing the tool

- **WHEN** an activation calls `ctx.act("charge_card", {...})` for a `side_effect=True` tool
- **THEN** a `ToolIntent` for `charge_card` is added to the staged bundle and the tool's callable is never invoked

#### Scenario: Intent IDs are deterministic across replay

- **WHEN** the same activation path issues the same sequence of `act` calls under identical `entity_key` and `seq` on a replayed bundle
- **THEN** each produced `ToolIntent` has an `intent_id` equal to `uuid5(NAMESPACE, entity_key + seq + step_index)` and is byte-identical to the original run

#### Scenario: step_index advances per act call

- **WHEN** an activation issues three `act` calls
- **THEN** their intents carry `step_index` 0, 1, and 2 respectively and therefore three distinct `intent_id`s

#### Scenario: A side-effect tool cannot execute inline

- **WHEN** an activation tries to run a `side_effect=True` tool through the context's read-only tool path
- **THEN** the call raises and no intent is staged for it

### Requirement: Read-only tool execution through the context

The `AgentContext` SHALL execute `side_effect=False` tools inline on the fast path via the injected `ToolRunner`, validating arguments and awaiting async tools, and returning the tool's result to the agent. Read-only tool execution SHALL NOT stage an intent (it produces no side effect) but MAY stage a `TOOL_CALL` trace event. A read-only tool that raises SHALL propagate its error to the agent without being converted into an intent.

#### Scenario: A read-only tool runs inline and returns its value

- **WHEN** an activation invokes a registered `side_effect=False` tool through the context
- **THEN** the tool executes inline, its validated result is returned to the agent, and no `ToolIntent` is staged

### Requirement: Output staging

The `AgentContext` SHALL let an activation emit zero or more outputs, staging each into the accumulated bundle in emission order. Outputs SHALL NOT be yielded to the pipeline during the activation; they appear only on the drained `AgentResult`, preserving order.

#### Scenario: Outputs are ordered and withheld until drain

- **WHEN** an activation emits outputs A then B without draining
- **THEN** nothing is emitted downstream yet, and after `drain()` the `AgentResult` outputs are `[A, B]` in that order

### Requirement: AgentResult is the immutable drained bundle

The system SHALL provide an immutable `AgentResult` value type produced only by `AgentContext.drain()`. It SHALL expose the activation's staged outputs, `ToolIntent`s, `TraceEvent`s, accumulated `TokenUsage`, and the next-state material the DoFn commits — the resulting `MemoryBlob` (when memory is dirty) and the replay-cache inserts. `AgentResult` SHALL be a plain value with no behavior that mutates keyed state; the owning DoFn is responsible for committing it atomically with the Beam bundle.

#### Scenario: Result carries every staged effect category

- **WHEN** `drain()` produces an `AgentResult`
- **THEN** its outputs, intents, traces, usage, and next-state memory/cache material reflect exactly what the activation staged, and mutating the returned value does not retroactively change the context

#### Scenario: A clean activation yields an empty result

- **WHEN** an activation stages no effects and is drained
- **THEN** the `AgentResult` has empty outputs, intents, and traces, zero usage, and no memory or cache changes to commit

### Requirement: StreamAgent authoring protocol

The system SHALL define a `StreamAgent` runtime protocol with a single async entry point, `async def activate(self, ctx: AgentContext) -> None`, through which an activation performs all of its work using the context. `StreamAgent` SHALL be a structural `Protocol` (runtime-checkable) so any framework adapter or hand-written class satisfies it without inheritance. The protocol is a runtime contract only; it SHALL NOT introduce prompt templating, orchestration DSL, or other agent-authoring abstractions. `StreamAgent` SHALL be re-exported from `beam_agents/__init__.py` as public API.

#### Scenario: A class implementing activate satisfies the protocol

- **WHEN** a class defines `async def activate(self, ctx)` and is checked against `StreamAgent`
- **THEN** it is recognized as a `StreamAgent` with no base class required

#### Scenario: The activation drives all work through the context

- **WHEN** a `StreamAgent.activate` runs and uses `ctx` to call the model, read/write memory, run tools, and emit output
- **THEN** every effect is staged on the context and none is applied until the owner drains it

### Requirement: FunctionAgent wraps a plain async function

The system SHALL provide a `FunctionAgent` that adapts a plain `async` callable `fn(ctx: AgentContext) -> None` into a `StreamAgent`, so the simplest agents need no class. `FunctionAgent(fn).activate(ctx)` SHALL await `fn(ctx)` and stage effects identically to a class-based agent. `FunctionAgent` SHALL be re-exported from `beam_agents/__init__.py`.

#### Scenario: A function is adapted into a StreamAgent

- **WHEN** a plain `async def fn(ctx)` is wrapped as `FunctionAgent(fn)`
- **THEN** the wrapper satisfies `StreamAgent` and invoking `activate(ctx)` awaits `fn(ctx)`, staging its effects on the context
