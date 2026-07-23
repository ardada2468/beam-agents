## ADDED Requirements

### Requirement: Proto package and committed generation
The project SHALL define all wire and state message schemas in `protos/beam_agents.proto` with `syntax = "proto3"` and package `beam_agents.v1`. Generated Python bindings SHALL be committed under `src/beam_agents/_protos/` and regenerating them via `scripts/gen_proto.sh` MUST produce no diff against the committed files.

#### Scenario: Regeneration is diff-clean
- **WHEN** `scripts/gen_proto.sh` is run on a clean checkout
- **THEN** `git diff --exit-code` over `src/beam_agents/_protos/` reports no changes

#### Scenario: Bindings are importable from the installed package
- **WHEN** a test imports the message classes from `beam_agents._protos`
- **THEN** all six top-level message classes (`MemoryBlob`, `ToolIntent`, `ToolResult`, `TraceEvent`, `AgentEnvelope`, `Continuation`) are available without any `sys.path` manipulation

### Requirement: MemoryBlob carries versioned, LRU-orderable working memory
`MemoryBlob` SHALL contain a `state_schema_version` field (uint32, set to 1 for this schema), a repeated `MemoryEntry` field where each entry has a string `key`, bytes `value`, and int64 `last_access_ms`, and a `total_value_bytes` field caching the sum of entry value sizes. Entry order SHALL be preserved exactly as written so LRU ordering is representable without a map field.

#### Scenario: Entries round-trip in insertion order
- **WHEN** a `MemoryBlob` is built with entries `["a", "b", "c"]` in that order, serialized, and parsed back
- **THEN** the parsed entries appear in the order `["a", "b", "c"]` with identical keys, values, and `last_access_ms`

#### Scenario: Schema version defaults are explicit
- **WHEN** a new `MemoryBlob` is constructed by runtime code
- **THEN** `state_schema_version` is set to 1, and a parsed blob with `state_schema_version` 0 is distinguishable as pre-versioned/uninitialized

### Requirement: ToolIntent carries deterministic identity and expiry
`ToolIntent` SHALL contain `intent_id` (string), `entity_key` (bytes), `seq` (int64), `step_index` (uint32), `tool_name` (string), `args_json` (string holding canonical JSON), `created_at_ms`, `expires_at_ms`, and `attempt` (uint32). The schema SHALL NOT compute or default `intent_id`; it only transports the caller-computed uuid5 value.

#### Scenario: All identity fields round-trip
- **WHEN** a `ToolIntent` with every field populated is serialized and parsed back
- **THEN** every field compares equal, including exact preservation of the `args_json` string

#### Scenario: Expiry is representable for fail-closed enforcement
- **WHEN** an intent is created with `expires_at_ms` set
- **THEN** the parsed message exposes `expires_at_ms` so the effector can refuse expired intents without consulting external state

### Requirement: ToolResult correlates outcomes with terminal statuses
`ToolResult` SHALL contain `intent_id` (string), `entity_key` (bytes), `seq` (int64), a `Status` enum with values `STATUS_UNSPECIFIED = 0`, `OK`, `ERROR`, `EXPIRED`, `REJECTED`, a `payload` (bytes), `error_message` (string), and `completed_at_ms`.

#### Scenario: Every status value is representable
- **WHEN** a `ToolResult` is constructed with each declared status value in turn and round-tripped
- **THEN** each parses back to the same status, and an unset status reads as `STATUS_UNSPECIFIED`

### Requirement: TraceEvent aligns with OTel GenAI conventions
`TraceEvent` SHALL contain `trace_id`, `span_id`, `parent_span_id` (bytes), `entity_key` (bytes), `seq` (int64), `step_index` (uint32), an `EventType` enum (`EVENT_TYPE_UNSPECIFIED = 0`, `ACTIVATION_START`, `LLM_CALL`, `TOOL_CALL`, `INTENT_EMITTED`, `ACTIVATION_END`, `ERROR`), a `map<string, string> attributes` field for OTel GenAI semantic-convention attributes, and `start_ms`/`end_ms`.

#### Scenario: GenAI attributes survive round-trip
- **WHEN** a `TraceEvent` carrying attributes such as `gen_ai.request.model` and `gen_ai.usage.input_tokens` is serialized and parsed back
- **THEN** all attribute keys and values compare equal

### Requirement: AgentEnvelope is the single keyed input type
`AgentEnvelope` SHALL contain `entity_key` (bytes), `event_time_ms` (int64), and a `oneof payload` with exactly three variants: `external_event` (opaque bytes), `tool_result` (`ToolResult`), and `approval` (a nested `Approval` message with `intent_id`, `approved` bool, `approver` string, `decided_at_ms`). The runtime SHALL NOT impose any schema on `external_event` bytes.

#### Scenario: Exactly one payload variant is set
- **WHEN** an `AgentEnvelope` is constructed with a `tool_result` payload and then assigned an `approval` payload
- **THEN** the `oneof` reports only `approval` as set and the `tool_result` field is cleared

#### Scenario: All three variants round-trip
- **WHEN** one envelope of each payload variant is serialized and parsed back
- **THEN** each parsed envelope reports the correct `oneof` case with field-equal contents

### Requirement: Continuation persists framework-opaque resume state
`Continuation` SHALL contain `state_schema_version` (uint32, set to 1), `seq` (int64), `step_index` (uint32), repeated `pending_intent_ids` (string), `adapter` (string discriminator), `snapshot` (opaque bytes), `suspended_at_ms`, and `deadline_ms`. The runtime SHALL treat `snapshot` as opaque — no parsing, no schema.

#### Scenario: Suspension state round-trips
- **WHEN** a `Continuation` with multiple `pending_intent_ids`, a non-empty `snapshot`, and a `deadline_ms` is serialized and parsed back
- **THEN** all fields compare equal and `snapshot` bytes are byte-identical

### Requirement: Schema evolution is additive and golden-blob guarded
Schema changes SHALL be additive only (new fields with new numbers; removals use `reserved` statements); any breaking change REQUIRES a `state_schema_version` bump. Golden serialized blobs for all six message types SHALL be committed as fixtures, and a compat test SHALL decode every golden blob with the current bindings and assert field-level equality against expected values. Golden tests SHALL NOT require byte-identical re-encoding.

#### Scenario: Golden blobs decode with current bindings
- **WHEN** the compat test parses each committed golden blob under `tests/core/golden/`
- **THEN** every blob decodes without error and all expected field values compare equal

#### Scenario: Unknown fields are tolerated on decode
- **WHEN** a blob containing an unknown (future) field number is parsed with the current bindings
- **THEN** parsing succeeds and all known fields compare equal, demonstrating forward-compatible decode

#### Scenario: Unknown fields survive re-encode
- **WHEN** a blob containing an unknown (future) field number is parsed with the current bindings and then re-serialized
- **THEN** the re-encoded bytes still contain the unknown field's data, so an older reader rewriting state during pipeline `--update` never drops fields written by a newer schema
