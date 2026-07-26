## MODIFIED Requirements

### Requirement: ToolIntent carries deterministic identity and expiry
`ToolIntent` SHALL contain `intent_id` (string), `entity_key` (bytes), `seq` (int64), `step_index` (uint32), `tool_name` (string), `args_json` (string holding canonical JSON), `created_at_ms`, `expires_at_ms`, `attempt` (uint32), and a `Kind` enum with values `TOOL_KIND_UNSPECIFIED = 0`, `TOOL`, `APPROVAL`. The schema SHALL NOT compute or default `intent_id`; it only transports the caller-computed uuid5 value. The `kind` field SHALL be additive under the existing `state_schema_version`: a reader SHALL treat `TOOL_KIND_UNSPECIFIED` and `TOOL` identically, so an intent written before `kind` existed is read as a tool call, and only an explicit `APPROVAL` SHALL be routed to a human approval channel rather than executed.

#### Scenario: All identity fields round-trip
- **WHEN** a `ToolIntent` with every field populated is serialized and parsed back
- **THEN** every field compares equal, including exact preservation of the `args_json` string and the `kind` value

#### Scenario: Expiry is representable for fail-closed enforcement
- **WHEN** an intent is created with `expires_at_ms` set
- **THEN** the parsed message exposes `expires_at_ms` so the effector can refuse expired intents without consulting external state

#### Scenario: Approval intents are distinguishable on the wire
- **WHEN** an intent created for a human approval request is serialized and parsed back
- **THEN** its `kind` is `APPROVAL`, distinguishable from a `TOOL` intent without inspecting `tool_name`

#### Scenario: An intent written before kind existed reads as a tool call
- **WHEN** a `ToolIntent` blob serialized without the `kind` field is parsed
- **THEN** `kind` reads as `TOOL_KIND_UNSPECIFIED` and is treated as a tool call, not as an approval request

### Requirement: Continuation persists framework-opaque resume state
`Continuation` SHALL contain `state_schema_version` (uint32, set to 1), `seq` (int64), `step_index` (uint32), repeated `pending_intent_ids` (string), `adapter` (string discriminator), `snapshot` (opaque bytes), `suspended_at_ms`, `deadline_ms`, and `escalations` (uint32, the number of timeout escalations already performed for this suspension). The runtime SHALL treat `snapshot` as opaque — no parsing, no schema. The `escalations` field SHALL be additive under the existing `state_schema_version`, defaulting to `0` for a blob written before it existed.

#### Scenario: Suspension state round-trips
- **WHEN** a `Continuation` with multiple `pending_intent_ids`, a non-empty `snapshot`, and a `deadline_ms` is serialized and parsed back
- **THEN** all fields compare equal and `snapshot` bytes are byte-identical

#### Scenario: Escalation count round-trips and defaults to zero
- **WHEN** a `Continuation` blob serialized without `escalations` is parsed, and a `Continuation` with `escalations` set is serialized and parsed back
- **THEN** the first reads `escalations == 0` and the second preserves its value exactly
