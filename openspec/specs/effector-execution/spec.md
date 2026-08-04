# effector-execution Specification

## Purpose
TBD - created by archiving change add-reference-effector. Update Purpose after archive.
## Requirements
### Requirement: Tools are resolved from the shared ToolRegistry

The effector SHALL be constructed with a `ToolRegistry` — the same registry an agent declares its tools from — and SHALL resolve every intent's `tool_name` through it, so one `@tool` definition serves both the pipeline's fast path and the effector's execution path. An intent naming a tool absent from the registry SHALL NOT fail the service or block the partition.

#### Scenario: A registered side-effecting tool is resolved and invoked

- **GIVEN** a registry containing a `side_effect=True` tool named `charge`
- **WHEN** an intent with `tool_name = "charge"` is processed
- **THEN** the registry resolves that tool and its callable is invoked with the intent's arguments

#### Scenario: An unknown tool name is rejected without stalling the partition

- **WHEN** an intent names a tool that is not registered
- **THEN** a `ToolResult` with status `REJECTED` naming the unknown tool is published, the offset is committed, and processing of the partition continues

### Requirement: EffectorToolRunner is the only sanctioned executor of side-effecting tools

The effector SHALL provide an `EffectorToolRunner` whose `run` is the single, named execution path permitted to invoke a `side_effect=True` tool. It SHALL reach the wrapped callable through the `Tool` type's public unwrap accessor, never through a private attribute. It SHALL refuse a `side_effect=False` tool, because a read-only tool reaching the outbox indicates the pipeline failed to run it inline. It SHALL await the call result when it is awaitable, matching the in-pipeline `ToolRunner`'s behavior.

#### Scenario: A side-effecting tool executes through the effector runner

- **WHEN** `EffectorToolRunner.run` is called with a `side_effect=True` tool and valid arguments
- **THEN** the underlying callable is invoked and its return value is returned, with no `SideEffectToolError` raised

#### Scenario: A read-only tool is refused by the effector runner

- **WHEN** `EffectorToolRunner.run` is called with a `side_effect=False` tool
- **THEN** it raises, the callable is never invoked, and the intent is published as `REJECTED`

#### Scenario: An async side-effecting tool is awaited

- **WHEN** `EffectorToolRunner.run` is called with an `async def` `side_effect=True` tool
- **THEN** the coroutine is awaited to completion and the actual result is returned, not a coroutine object

#### Scenario: The in-pipeline guard is unchanged

- **WHEN** a `side_effect=True` tool is invoked directly or through the in-pipeline `ToolRunner`
- **THEN** a `SideEffectToolError` is still raised and the callable is never invoked

### Requirement: Arguments are validated before the callable runs

The effector SHALL parse each intent's `args_json` and validate the resulting arguments against the tool's Pydantic argument model before invoking the callable. Malformed JSON or arguments failing the model SHALL result in a `REJECTED` result and the callable SHALL NOT be invoked.

#### Scenario: Valid arguments are coerced and passed through

- **WHEN** an intent carries `args_json` satisfying its tool's model
- **THEN** the arguments are validated and the callable is invoked with the coerced values

#### Scenario: Invalid arguments are rejected before invocation

- **WHEN** an intent carries arguments that fail the tool's Pydantic model, or `args_json` that is not valid JSON
- **THEN** a `ToolResult` with status `REJECTED` carrying the validation message is published and the callable is never invoked

### Requirement: Expired intents are refused before execution, regardless of kind

The effector SHALL evaluate `hitl.refuse_expired(intent, now_ms)` before claiming or executing any intent, and SHALL publish the returned `ToolResult` with status `EXPIRED` instead of executing. The check SHALL apply to every intent kind, including `APPROVAL`. An intent with a non-positive `expires_at_ms` SHALL be treated as expired.

#### Scenario: An expired tool intent is refused

- **GIVEN** an intent whose `expires_at_ms` precedes the current time
- **WHEN** the effector processes it
- **THEN** a `ToolResult` with status `EXPIRED` correlating `intent_id`, `entity_key`, and `seq` is published and no tool is invoked

#### Scenario: An intent with no recorded expiry is refused

- **GIVEN** an intent whose `expires_at_ms` is zero
- **WHEN** the effector processes it
- **THEN** it is treated as expired and refused, never as unbounded

#### Scenario: An expired approval intent is refused rather than routed

- **GIVEN** an intent with `kind = APPROVAL` whose `expires_at_ms` has passed
- **WHEN** the effector processes it
- **THEN** a `ToolResult` with status `EXPIRED` is published and nothing is posted to the approval channel

### Requirement: Approval intents are routed to the approval channel and never executed

An intent whose `kind` is `APPROVAL` and which is not expired SHALL be published verbatim to the configured approval channel, keyed by `entity_key`, and SHALL NOT be executed against the tool registry. The effector SHALL mark such an intent terminal in the dedup store so redelivery does not post a second notification, and SHALL NOT publish a `ToolResult` for it — the decision returns separately as an approval envelope on the approvals topic. An intent whose `kind` is unspecified SHALL be treated as `TOOL`.

#### Scenario: An approval intent is posted to the approval channel

- **WHEN** a live intent with `kind = APPROVAL` is processed
- **THEN** the intent is published to the configured approval channel under its `entity_key`, no tool is invoked, and no `ToolResult` is published

#### Scenario: A redelivered approval intent does not double-notify

- **GIVEN** an approval intent already routed and marked terminal
- **WHEN** the same intent is redelivered
- **THEN** nothing is posted to the approval channel a second time and the offset is committed

#### Scenario: An unspecified kind is executed as a tool

- **WHEN** an intent is processed whose `kind` field is `TOOL_KIND_UNSPECIFIED`
- **THEN** it is executed against the tool registry exactly as an explicit `TOOL` intent

### Requirement: Every intent maps to exactly one terminal ToolResult status

The effector SHALL map each processed tool intent to exactly one terminal status, with `REJECTED` reserved for cases where the callable was never invoked and `ERROR` covering cases where it ran and its effect is unknown: `OK` when the tool returns (with the return value encoded as the result payload), `ERROR` when the tool raises or exceeds `tool_timeout_ms`, `EXPIRED` when the intent is past its expiry, and `REJECTED` for an unknown tool, a `side_effect=False` tool, or an argument-validation failure. Every published result SHALL carry the originating `intent_id`, `entity_key`, `seq`, and a `completed_at_ms`.

#### Scenario: A successful tool call publishes OK with its encoded return value

- **WHEN** a side-effecting tool returns a JSON-encodable value
- **THEN** a `ToolResult` with status `OK` is published whose payload decodes to that value

#### Scenario: A raising tool publishes ERROR with its message

- **WHEN** a side-effecting tool raises
- **THEN** a `ToolResult` with status `ERROR` is published whose `error_message` identifies the failure, and the tool is not retried

#### Scenario: A tool exceeding its timeout publishes ERROR, not REJECTED

- **GIVEN** a tool whose execution exceeds `tool_timeout_ms`
- **WHEN** the effector processes its intent
- **THEN** the execution is cancelled and a `ToolResult` with status `ERROR` is published, distinguishing it from the never-invoked `REJECTED` cases

#### Scenario: A result that cannot be encoded is an ERROR, not a lost intent

- **WHEN** a tool returns a value that cannot be encoded into the result payload
- **THEN** a `ToolResult` with status `ERROR` describing the encoding failure is published rather than the intent being dropped

#### Scenario: Every result correlates with its intent

- **WHEN** any terminal result is published for an intent
- **THEN** its `intent_id`, `entity_key`, and `seq` equal the intent's and `completed_at_ms` is populated
