# wire-schemas Delta Specification

## ADDED Requirements

### Requirement: ActivationErrorRecord carries dead-letter triage fields
`ActivationErrorRecord` SHALL be a top-level message in `protos/beam_agents.proto` containing `entity_key` (bytes), `reason` (string), `detail` (string), and `event_time_ms` (int64). It is the wire form of a `.errors` dead letter; when published to a message-bus errors sink it SHALL travel inside `AgentEnvelope.external_event` as deterministically serialized bytes. The `AgentEnvelope` schema itself SHALL NOT change: `external_event` remains opaque bytes, and carrying an `ActivationErrorRecord` there is a documented convention, not a schema constraint.

#### Scenario: All fields round-trip
- **WHEN** an `ActivationErrorRecord` with every field populated is serialized and parsed back
- **THEN** `entity_key`, `reason`, `detail`, and `event_time_ms` all compare equal

#### Scenario: Deterministic serialization is byte-stable
- **WHEN** the same `ActivationErrorRecord` is serialized twice with deterministic serialization
- **THEN** both byte strings are identical

## MODIFIED Requirements

### Requirement: Proto package and committed generation
The project SHALL define all wire and state message schemas in `protos/beam_agents.proto` with `syntax = "proto3"` and package `beam_agents.v1`. Generated Python bindings SHALL be committed under `src/beam_agents/_protos/` and regenerating them via `scripts/gen_proto.sh` MUST produce no diff against the committed files.

#### Scenario: Regeneration is diff-clean
- **WHEN** `scripts/gen_proto.sh` is run on a clean checkout
- **THEN** `git diff --exit-code` over `src/beam_agents/_protos/` reports no changes

#### Scenario: Bindings are importable from the installed package
- **WHEN** a test imports the message classes from `beam_agents._protos`
- **THEN** all eight top-level message classes (`MemoryBlob`, `ToolIntent`, `ToolResult`, `TraceEvent`, `AgentEnvelope`, `Continuation`, `LlmCacheBlob`, `ActivationErrorRecord`) are available without any `sys.path` manipulation
