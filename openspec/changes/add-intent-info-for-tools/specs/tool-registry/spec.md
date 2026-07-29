## ADDED Requirements

### Requirement: IntentInfo carries intent identity to opt-in tools

The system SHALL provide a frozen dataclass `IntentInfo` in `beam_agents.tools` with exactly the fields `intent_id: str`, `entity_key: bytes`, `seq: int`, `step_index: int`, and `attempt: int` — the deterministic identity of the `ToolIntent` being executed. Instances SHALL be immutable and hashable. `IntentInfo` SHALL be importable from `beam_agents.tools` without importing Beam, the effector, or generated protobuf modules.

#### Scenario: IntentInfo is frozen and hashable

- **WHEN** an `IntentInfo` is constructed and a field assignment is attempted
- **THEN** the assignment raises (`dataclasses.FrozenInstanceError`), and the instance is usable as a dict key (equal field values hash equal)

#### Scenario: IntentInfo imports standalone

- **WHEN** `IntentInfo` is imported from `beam_agents.tools`
- **THEN** the import succeeds without importing `apache_beam`, `beam_agents.effector`, or any `_pb2` module

### Requirement: Registration recognizes an opt-in keyword-only intent parameter

The `@tool` decorator SHALL inspect the wrapped callable's signature at decoration time. A keyword-only parameter named `intent` annotated `IntentInfo` SHALL mark the tool as accepting intent identity (`Tool.accepts_intent` is `True`), SHALL be excluded from the generated Pydantic argument model, and SHALL NOT appear in the provider-facing JSON `schema` — it is runtime-injected, never an LLM-visible or caller-supplied argument. A tool that does not declare the parameter SHALL be registered, schematized, and invoked exactly as before this capability existed.

Malformed declarations SHALL raise `ToolDefinitionError` at decoration time: a parameter annotated `IntentInfo` that is not the keyword-only parameter `intent` (wrong kind or wrong name), and a declaration of `intent: IntentInfo` on a `side_effect=False` tool (read-only tools execute inline in the pipeline, where no intent identity exists). A parameter named `intent` with any other annotation SHALL remain an ordinary schema argument.

#### Scenario: A declaring side-effect tool is marked and its schema excludes intent

- **WHEN** `@tool(side_effect=True)` wraps `def charge(key: str, *, intent: IntentInfo) -> str`
- **THEN** the resulting `Tool` has `accepts_intent` true, its argument model validates `{"key": "k"}` without requiring `intent`, and its JSON schema `parameters` lists only `key`

#### Scenario: A non-declaring tool is untouched

- **WHEN** `@tool(side_effect=True)` wraps a callable with no `intent` parameter
- **THEN** `accepts_intent` is false and the tool's argument model, JSON schema, and invocation behavior are byte-identical to the pre-capability behavior

#### Scenario: A positional IntentInfo parameter is rejected at decoration time

- **WHEN** `@tool(side_effect=True)` wraps `def charge(intent: IntentInfo, key: str) -> str` (not keyword-only)
- **THEN** decoration raises `ToolDefinitionError` naming the parameter and stating the keyword-only requirement

#### Scenario: A misnamed IntentInfo parameter is rejected at decoration time

- **WHEN** `@tool(side_effect=True)` wraps a callable with a keyword-only parameter annotated `IntentInfo` whose name is not `intent`
- **THEN** decoration raises `ToolDefinitionError`

#### Scenario: A read-only tool declaring intent is rejected at decoration time

- **WHEN** `@tool` (with `side_effect=False`) wraps a callable declaring a keyword-only `intent: IntentInfo`
- **THEN** decoration raises `ToolDefinitionError` stating that intent identity exists only for side-effecting tools

#### Scenario: An intent parameter with a different annotation stays an ordinary argument

- **WHEN** `@tool(side_effect=True)` wraps `def notify(intent: str) -> str`
- **THEN** decoration succeeds, `accepts_intent` is false, and `intent` appears in the JSON schema as an ordinary required string argument

#### Scenario: String annotations are recognized

- **WHEN** the wrapped callable's module uses `from __future__ import annotations` so the `intent` parameter's annotation is the string `"IntentInfo"`
- **THEN** recognition, exclusion, and validation behave identically to an evaluated annotation
