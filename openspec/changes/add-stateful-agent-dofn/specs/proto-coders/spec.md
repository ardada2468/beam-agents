## ADDED Requirements

### Requirement: Runtime errors use deterministic protobuf coding
`beam_agents.core.coders` SHALL support `RuntimeError` with `DeterministicProtoCoder`, and `register_coders()` SHALL explicitly and idempotently register that coder with Beam.

#### Scenario: Runtime error encoding is deterministic
- **WHEN** an arbitrary `RuntimeError` is encoded twice with `DeterministicProtoCoder`
- **THEN** the two byte strings are identical

#### Scenario: Runtime error round-trips
- **WHEN** an arbitrary `RuntimeError` is encoded and decoded
- **THEN** the decoded message equals the original

#### Scenario: Runtime error registration is explicit
- **WHEN** `register_coders()` is called one or more times
- **THEN** Beam's registry returns `DeterministicProtoCoder` for `RuntimeError`
