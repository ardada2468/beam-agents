## ADDED Requirements

### Requirement: Deterministic proto coder for all six message types
`beam_agents.core.coders` SHALL provide a Beam coder that encodes messages with protobuf deterministic serialization (`SerializeToString(deterministic=True)`) and SHALL support all six message types (`MemoryBlob`, `ToolIntent`, `ToolResult`, `TraceEvent`, `AgentEnvelope`, `Continuation`). Encoding the same message value MUST always produce identical bytes within a single protobuf library version.

#### Scenario: Repeated encoding is byte-identical
- **WHEN** the same message value is encoded twice through the coder
- **THEN** the two byte strings are identical, for property-based (hypothesis-generated) instances of all six message types

#### Scenario: Map insertion order does not affect encoding
- **WHEN** two `TraceEvent` messages carry equal `attributes` maps populated in different insertion orders and both are encoded
- **THEN** the encoded bytes are identical

### Requirement: Lossless round-trip through the coder
For every supported message type, decoding an encoded message SHALL yield a message equal to the original, including empty/default field values, `oneof` cases, and opaque bytes fields.

#### Scenario: Round-trip equality for all six types
- **WHEN** property-based instances of each of the six message types are passed through `decode(encode(msg))`
- **THEN** the decoded message compares equal to the original

### Requirement: Coder advertises determinism and is usable as a key coder
The coder SHALL report `is_deterministic() == True` so Beam accepts these types as grouping keys and state values without deterministic-coder fallback wrapping.

#### Scenario: Message type works as a GroupByKey key
- **WHEN** a `TestPipeline` groups elements keyed by a `ToolIntent` after coder registration
- **THEN** the pipeline runs without a non-deterministic-coder error and grouping is by message value equality

### Requirement: Explicit registration, no import side effects
The module SHALL expose a `register_coders()` function that registers the deterministic coder for all six message types with Beam's coder registry. Importing `beam_agents.core.coders` SHALL NOT mutate the registry, and `register_coders()` MUST be idempotent.

#### Scenario: Registry resolves the deterministic coder after registration
- **WHEN** `register_coders()` is called and the registry is asked for the coder of each of the six message types
- **THEN** each lookup returns the deterministic proto coder, not a pickle-based fallback coder

#### Scenario: Import alone does not register
- **WHEN** `beam_agents.core.coders` is imported without calling `register_coders()`
- **THEN** the Beam coder registry's mapping for the six message types is unchanged

#### Scenario: Double registration is harmless
- **WHEN** `register_coders()` is called twice in the same process
- **THEN** no error is raised and registry lookups still return the deterministic proto coder

### Requirement: Pipeline elements never fall back to pickle
After registration, the six message types SHALL flow through pipeline transforms using the deterministic proto coder end-to-end; pickled bytes of these messages MUST NOT appear on any wire or state path.

#### Scenario: Elements round-trip through a TestPipeline
- **WHEN** a `TestPipeline` passes instances of each of the six message types through a shuffle boundary (e.g., a GroupByKey on a fixed key) after `register_coders()`
- **THEN** the output messages compare equal to the inputs and the resolved element coder for each type is the deterministic proto coder
