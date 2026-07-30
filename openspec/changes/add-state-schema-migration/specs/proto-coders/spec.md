# proto-coders Delta Specification

## ADDED Requirements

### Requirement: Coders are migration-invariant and version-agnostic
`DeterministicProtoCoder` SHALL NOT perform schema migration: `decode` SHALL return the parsed message at whatever `state_schema_version` the bytes carry, and `encode` SHALL remain raw `SerializeToString(deterministic=True)` for messages of every version. Migration is applied above the codec, at the DoFn's keyed-state read sites (see `state-migration`). This keeps encoded keyed state a pure, version-agnostic function of message content, so a Dataflow `--update` can restore state written by any prior schema version and hand it, unaltered, to the read-path migration hook. The state spec IDs (`"memory"`, `"continuation"`, `"llm_cache"`) and their coder class SHALL NOT change across a `state_schema_version` bump — a spec rename or coder swap is a state-compatibility break that no version bump can license.

#### Scenario: Decoding an old-version blob does not migrate it
- **WHEN** a blob stamped with a historical `state_schema_version` `n` is passed through `decode(encode(blob))`
- **THEN** the result still reads `state_schema_version == n` and compares field-equal to the input, with no migration function invoked

#### Scenario: Wire compatibility holds across a version bump
- **WHEN** state bytes written under schema version `n` are decoded by a binary whose current version exceeds `n`
- **THEN** parsing succeeds through the same coder with all version-`n` fields intact, ready for the read-path migration hook — no coder configuration or state spec change is required by the bump
