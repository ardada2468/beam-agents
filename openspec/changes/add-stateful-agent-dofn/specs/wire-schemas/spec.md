## ADDED Requirements

### Requirement: RuntimeError carries typed dead-letter failures
`RuntimeError` SHALL contain an `ErrorType` enum with `ERROR_TYPE_UNSPECIFIED = 0`, `INVALID_ENVELOPE`, `BUSY_KEY`, `ORPHANED_RESULT`, `ACTIVATION_TIMEOUT`, `ACTIVATION_FAILED`, and `TIMEOUT_HANDLING_FAILED`, plus `entity_key` (bytes), `seq` (int64), `intent_id` (string), `message` (string), and `observed_at_ms` (int64).

#### Scenario: Runtime failure details round-trip
- **WHEN** a `RuntimeError` with every data field populated is serialized and parsed
- **THEN** its error type, entity key, sequence, intent correlation, message, and observation time compare equal

#### Scenario: Every runtime error type is representable
- **WHEN** a `RuntimeError` is constructed with each declared error type in turn and round-tripped
- **THEN** each parses back to the same type and an unset type reads as `ERROR_TYPE_UNSPECIFIED`

## MODIFIED Requirements

### Requirement: Proto package and committed generation
The project SHALL define all wire and state message schemas in `protos/beam_agents.proto` with `syntax = "proto3"` and package `beam_agents.v1`. Generated Python bindings SHALL be committed under `src/beam_agents/_protos/` and regenerating them via `scripts/gen_proto.sh` MUST produce no diff against the committed files.

#### Scenario: Regeneration is diff-clean
- **WHEN** `scripts/gen_proto.sh` is run on a clean checkout
- **THEN** `git diff --exit-code` over `src/beam_agents/_protos/` reports no changes

#### Scenario: Bindings are importable from the installed package
- **WHEN** a test imports the message classes from `beam_agents._protos`
- **THEN** all eight top-level message classes (`MemoryBlob`, `ToolIntent`, `ToolResult`, `TraceEvent`, `AgentEnvelope`, `Continuation`, `LlmCacheBlob`, `RuntimeError`) are available without any `sys.path` manipulation

### Requirement: Schema evolution is additive and golden-blob guarded
Schema changes SHALL be additive only (new fields with new numbers; removals use `reserved` statements); any breaking change REQUIRES a `state_schema_version` bump. Golden serialized blobs for all eight message types SHALL be committed as fixtures, and a compat test SHALL decode every golden blob with the current bindings and assert field-level equality against expected values. Golden tests SHALL NOT require byte-identical re-encoding.

#### Scenario: Golden blobs decode with current bindings
- **WHEN** the compat test parses each committed golden blob under `tests/core/golden/`
- **THEN** every blob decodes without error and all expected field values compare equal

#### Scenario: Unknown fields are tolerated on decode
- **WHEN** a blob containing an unknown (future) field number is parsed with the current bindings
- **THEN** parsing succeeds and all known fields compare equal, demonstrating forward-compatible decode

#### Scenario: Unknown fields survive re-encode
- **WHEN** a blob containing an unknown (future) field number is parsed with the current bindings and then re-serialized
- **THEN** the re-encoded bytes still contain the unknown field's data, so an older reader rewriting state during pipeline `--update` never drops fields written by a newer schema
