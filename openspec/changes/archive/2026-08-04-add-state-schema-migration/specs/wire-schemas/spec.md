# wire-schemas Delta Specification

## MODIFIED Requirements

### Requirement: Schema evolution is additive and golden-blob guarded
Schema changes SHALL be additive by default (new fields with new numbers; removals use `reserved` statements). A breaking change is permitted ONLY for the three versioned keyed-state messages (`MemoryBlob`, `Continuation`, `LlmCacheBlob`) and ONLY through a `state_schema_version` bump accompanied by a registered migration function for every versioned message and a frozen golden corpus entry for the outgoing version (see `state-migration`); the unversioned wire messages (`ToolIntent`, `ToolResult`, `TraceEvent`, `AgentEnvelope`, `ActivationErrorRecord`) remain additive-only with no bump escape hatch. Every schema change, breaking or not, MUST keep previously written bytes parseable by the current descriptor: an existing field number SHALL never be retyped or reused, because migration operates on decoded messages and runs only after a successful parse. Golden serialized blobs SHALL be committed per schema version under `tests/core/golden/v<N>/`, and a compat test SHALL decode every golden blob with the current bindings, migrate historical versions to current, and assert field-level equality against expected values. Golden tests SHALL NOT require byte-identical re-encoding.

#### Scenario: Golden blobs decode with current bindings
- **WHEN** the compat test parses each committed golden blob under `tests/core/golden/v<N>/`
- **THEN** every blob decodes without error and, after migration to the current version, all expected field values compare equal

#### Scenario: Golden corpus is laid out per version
- **WHEN** the compat suite collects its fixtures
- **THEN** every committed blob lives under a `v<N>` directory matching the `state_schema_version` regime its builders were frozen at, and no fixture sits outside a version directory

#### Scenario: Unknown fields are tolerated on decode
- **WHEN** a blob containing an unknown (future) field number is parsed with the current bindings
- **THEN** parsing succeeds and all known fields compare equal, demonstrating forward-compatible decode

#### Scenario: Unknown fields survive re-encode
- **WHEN** a blob containing an unknown (future) field number is parsed with the current bindings and then re-serialized
- **THEN** the re-encoded bytes still contain the unknown field's data, so an older reader rewriting state during pipeline `--update` never drops fields written by a newer schema
