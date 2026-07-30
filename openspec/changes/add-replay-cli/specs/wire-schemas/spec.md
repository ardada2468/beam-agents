## MODIFIED Requirements

### Requirement: AgentEnvelope is the single keyed input type
`AgentEnvelope` SHALL contain `entity_key` (bytes), `event_time_ms` (int64), and a `oneof payload` with exactly four variants: `external_event` (opaque bytes), `tool_result` (`ToolResult`), `approval` (a nested `Approval` message with `intent_id`, `approved` bool, `approver` string, `decided_at_ms`), and `export_request` (a nested `StateExportRequest` message carrying a `request_id` string). The runtime SHALL NOT impose any schema on `external_event` bytes. `export_request` is additive under `state_schema_version = 1`: an envelope written before the variant existed decodes unchanged, and a reader that predates it sees the new variant as an unknown field rather than a parse failure.

#### Scenario: Exactly one payload variant is set

- **WHEN** an `AgentEnvelope` is constructed with a `tool_result` payload and then assigned an `approval` payload
- **THEN** the `oneof` reports only `approval` as set and the `tool_result` field is cleared

#### Scenario: All four variants round-trip

- **WHEN** one envelope of each payload variant is serialized and parsed back
- **THEN** each parsed envelope reports the correct `oneof` case with field-equal contents

#### Scenario: An envelope written before export_request still decodes

- **WHEN** a golden `AgentEnvelope` blob predating the `export_request` variant is parsed with the current bindings
- **THEN** parsing succeeds and every field compares equal to its expected value

## ADDED Requirements

### Requirement: StateSnapshot carries a versioned per-key state image

`StateSnapshot` SHALL contain `state_schema_version` (uint32, set to 1), `entity_key` (bytes), `seq` (int64, the key's `SEQ` counter at export), `snapshot_at_ms` (int64, the export request's `event_time_ms`), `memory` (`MemoryBlob`), `llm_cache` (`LlmCacheBlob`), `continuation` (`Continuation`, present only when the key is suspended), repeated `pending` (`ToolIntent`), and `request_id` (string, echoed from the originating `StateExportRequest`). Embedded blobs SHALL be carried verbatim as committed — the snapshot SHALL NOT re-version, migrate, or reorder their contents at export time.

#### Scenario: A populated snapshot round-trips

- **WHEN** a `StateSnapshot` with a populated memory blob, cache blob, continuation, and two pending intents is serialized deterministically and parsed back
- **THEN** every field compares equal, including entry order within the embedded blobs

#### Scenario: Embedded blobs keep their own schema versions

- **WHEN** a snapshot embeds a `MemoryBlob` whose `state_schema_version` differs from the snapshot envelope's
- **THEN** the round-tripped blob reports its original version unchanged, leaving migration to the loader
