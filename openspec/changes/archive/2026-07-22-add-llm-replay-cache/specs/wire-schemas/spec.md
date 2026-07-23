# wire-schemas Delta Specification

## ADDED Requirements

### Requirement: LlmCacheBlob carries versioned, LRU-orderable replay-cache entries
`LlmCacheBlob` SHALL contain a `state_schema_version` field (uint32, set to 1 for this schema), a repeated `LlmCacheEntry` field where each entry has a string `cache_key`, bytes `response`, bytes `response_digest`, int64 `created_at_ms`, int64 `last_access_ms`, and bool `digest_only`, and a `total_response_bytes` field caching the sum of stored response sizes. Entry order SHALL be preserved exactly as written so LRU ordering is representable without a map field.

#### Scenario: Cache entries round-trip in insertion order
- **WHEN** an `LlmCacheBlob` is built with entries keyed `["a", "b", "c"]` in that order, serialized, and parsed back
- **THEN** the parsed entries appear in the order `["a", "b", "c"]` with identical field values for every entry field

#### Scenario: Digest-only entries are representable
- **WHEN** an entry is written with `digest_only` `True`, an empty `response`, and a 32-byte `response_digest`, then round-tripped
- **THEN** the parsed entry preserves `digest_only` `True`, empty `response` bytes, and the exact digest bytes

## MODIFIED Requirements

### Requirement: Proto package and committed generation
The project SHALL define all wire and state message schemas in `protos/beam_agents.proto` with `syntax = "proto3"` and package `beam_agents.v1`. Generated Python bindings SHALL be committed under `src/beam_agents/_protos/` and regenerating them via `scripts/gen_proto.sh` MUST produce no diff against the committed files.

#### Scenario: Regeneration is diff-clean
- **WHEN** `scripts/gen_proto.sh` is run on a clean checkout
- **THEN** `git diff --exit-code` over `src/beam_agents/_protos/` reports no changes

#### Scenario: Bindings are importable from the installed package
- **WHEN** a test imports the message classes from `beam_agents._protos`
- **THEN** all seven top-level message classes (`MemoryBlob`, `ToolIntent`, `ToolResult`, `TraceEvent`, `AgentEnvelope`, `Continuation`, `LlmCacheBlob`) are available without any `sys.path` manipulation

### Requirement: Schema evolution is additive and golden-blob guarded
Schema changes SHALL be additive only (new fields with new numbers; removals use `reserved` statements); any breaking change REQUIRES a `state_schema_version` bump. Golden serialized blobs for all seven message types SHALL be committed as fixtures, and a compat test SHALL decode every golden blob with the current bindings and assert field-level equality against expected values. Golden tests SHALL NOT require byte-identical re-encoding.

#### Scenario: Golden blobs decode with current bindings
- **WHEN** the compat test parses each committed golden blob under `tests/core/golden/`
- **THEN** every blob decodes without error and all expected field values compare equal

#### Scenario: Unknown fields are tolerated on decode
- **WHEN** a blob containing an unknown (future) field number is parsed with the current bindings
- **THEN** parsing succeeds and all known fields compare equal, demonstrating forward-compatible decode

#### Scenario: Unknown fields survive re-encode
- **WHEN** a blob containing an unknown (future) field number is parsed with the current bindings and then re-serialized
- **THEN** the re-encoded bytes still contain the unknown field's data, so an older reader rewriting state during pipeline `--update` never drops fields written by a newer schema
