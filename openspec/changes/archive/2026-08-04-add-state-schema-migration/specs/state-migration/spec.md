# state-migration Delta Specification

## ADDED Requirements

### Requirement: The runtime owns a single current state schema version
`core/migration.py` SHALL define `CURRENT_STATE_SCHEMA_VERSION` (equal to `1` at introduction) as the one authoritative version for keyed state. The three versioned state messages — `MemoryBlob`, `Continuation`, `LlmCacheBlob` — SHALL be stamped from this constant by every runtime writer (`WorkingMemory.to_blob`, `ReplayCacheView.to_blob`, `build_continuation`), replacing hardcoded literals. A parsed `state_schema_version` of `0` SHALL be treated as version `1` (proto3 zero-default; the runtime has only ever written `1`, and the wire-schemas spec designates `0` as pre-versioned).

#### Scenario: Writers stamp the current version
- **WHEN** an activation commits and its `MemoryBlob`, `LlmCacheBlob`, and (on suspension) `Continuation` are written to keyed state
- **THEN** every written blob carries `state_schema_version == CURRENT_STATE_SCHEMA_VERSION`

#### Scenario: Version zero reads as the baseline
- **WHEN** a blob with `state_schema_version` `0` is passed to `migrate_to_current`
- **THEN** it is treated as version `1` and migrates to the current version without error

### Requirement: Migrations form a per-message chain of single-step pure functions
`core/migration.py` SHALL provide a registry mapping `(message type, from_version)` to a migration function that takes a version-`n` message and returns a version-`n+1` message, and a `migrate_to_current` entry point that composes registered steps until the message reads `CURRENT_STATE_SCHEMA_VERSION`. Migration functions MUST be pure and deterministic — no clocks, randomness, or I/O — because they run on the element path and a replayed bundle must produce an identical migrated view. A current-version message SHALL be returned unchanged with no copy (identity fast path). A missing step in a chain SHALL raise a typed error naming the message type and the gap; `migrate_to_current` SHALL verify that each applied step advanced the version stamp and that the final result reads the current version.

#### Scenario: A single-step migration upgrades one version
- **WHEN** a migration step is registered for a message type at `from_version` `n` and a version-`n` blob is passed to `migrate_to_current` with the current version equal to `n + 1`
- **THEN** the returned blob is the step's output with `state_schema_version == n + 1`

#### Scenario: Chains compose across multiple versions
- **WHEN** steps are registered for versions `1 -> 2` and `2 -> 3` (test doubles), the current version is `3`, and a version-`1` blob is passed to `migrate_to_current`
- **THEN** both steps run in order and the result reads version `3` with both transformations applied

#### Scenario: A gap in the chain is a hard error
- **WHEN** the current version is `3`, a step exists for `1 -> 2` but none for `2 -> 3`, and a version-`1` blob is migrated
- **THEN** a typed missing-migration error naming the message type and the missing `from_version` is raised, and no partially-migrated value is returned

#### Scenario: A current-version blob passes through untouched
- **WHEN** a blob already at `CURRENT_STATE_SCHEMA_VERSION` is passed to `migrate_to_current`
- **THEN** the identical object is returned with no migration function invoked and no copy made

### Requirement: Keyed state is migrated lazily on first read inside the DoFn
`_AgentDoFn` SHALL pass every keyed-state read of `MEMORY`, `CONTINUATION`, and `LLM_CACHE` through `migrate_to_current` before any field of the blob is interpreted — the `MEMORY`/`LLM_CACHE` reads in `_start`, the `CONTINUATION`/`MEMORY`/`LLM_CACHE` reads in `_resume`, and the `CONTINUATION` reads in `on_ttl` and `on_hitl`. Migration at read time SHALL NOT write keyed state: the migrated value reaches durable state only through the next successful commit's existing writes (which stamp the current version), preserving the atomic-commit invariant that a failed activation, refused resume, or timer no-op mutates nothing.

#### Scenario: An old blob is migrated on read and committed at the current version
- **WHEN** a key's stored blob reads version `n`, the binary's current version is `n + 1` with a registered step, and an element for that key activates and commits successfully
- **THEN** the activation observes the migrated (version `n + 1`) view of the state, and the blob written back at commit carries `state_schema_version == n + 1`

#### Scenario: A failed activation leaves old-version bytes untouched
- **WHEN** a key's stored blob reads version `n` under a version-`n + 1` binary and the activation raises or times out
- **THEN** the element is dead-lettered per the existing failure routes and the stored state still carries its original version-`n` bytes, unmodified

#### Scenario: Timer callbacks interpret only migrated continuations
- **WHEN** `on_hitl` or `on_ttl` fires for a key whose stored `Continuation` reads version `n` under a version-`n + 1` binary
- **THEN** the callback's reads of `deadline_ms`, `seq`, `escalations`, and `pending_intent_ids` observe the migrated view, and an escalation writes the migrated continuation back at the current version

### Requirement: A state version from the future fails fast
When a keyed-state blob's `state_schema_version` exceeds `CURRENT_STATE_SCHEMA_VERSION`, `migrate_to_current` SHALL raise a typed error naming the message type, the found version, and the binary's current version, before any field of the blob is interpreted. The DoFn SHALL NOT catch this error: the bundle fails and the runner retries, wedging the key until the binary is rolled forward. The element SHALL NOT be dead-lettered and no keyed state SHALL be mutated — future-version state means a newer binary already ran on this key, and both dropping the element and interpreting the blob under older semantics would lose data that a roll-forward recovers losslessly.

#### Scenario: A future-version blob fails the bundle
- **WHEN** a key's stored blob reads `CURRENT_STATE_SCHEMA_VERSION + 1` and an element for that key is processed
- **THEN** a typed from-the-future error propagates out of `process`, no record is emitted on `.errors` or any other output, and keyed state is unchanged

#### Scenario: Rolling forward recovers the key
- **WHEN** the same key is subsequently processed by a binary whose `CURRENT_STATE_SCHEMA_VERSION` is at least the blob's version
- **THEN** the element processes normally with no residue from the failed attempts

### Requirement: A cross-version golden corpus replays every historical version to current
Golden fixtures SHALL be organized per schema version under `tests/core/golden/v<N>/`, with `v1/` holding the existing baseline blobs byte-for-byte. The generator SHALL keep per-version builders, SHALL freeze a version's builders once a newer version exists, and SHALL only ever write the current version's directory. A CI corpus test SHALL, for every fixture of every historical version: decode the committed bytes with current bindings, apply `migrate_to_current`, and assert field-level equality against the expected current-version message. The corpus test SHALL NOT require byte-identical re-encoding. The corpus replay and completeness tests SHALL carry the offline `semantics` marker in addition to running in the plain unit tier, per the project's designation of state compat as a semantics gate.

#### Scenario: Every historical version replays to current
- **WHEN** the corpus test runs over all committed `tests/core/golden/v<N>/*.bin` fixtures
- **THEN** each blob decodes without error, migrates to `CURRENT_STATE_SCHEMA_VERSION`, and compares field-equal to its expected current-version message

#### Scenario: The corpus cannot silently shrink
- **WHEN** the completeness meta-test runs
- **THEN** it fails unless every version in `1..CURRENT_STATE_SCHEMA_VERSION` has a corpus directory and every versioned message type has a fixture in every version directory since its introduction

#### Scenario: Historical corpus directories are frozen
- **WHEN** the generator is run by hand at current version `N`
- **THEN** it writes only `tests/core/golden/v<N>/` and leaves every `v<M>` for `M < N` untouched

### Requirement: Breaking proto changes are gated on migration and corpus artifacts
This capability is the enforcement gate for breaking proto changes: a change that bumps `CURRENT_STATE_SCHEMA_VERSION` to `n` SHALL NOT merge unless the registry contains a `(type, n - 1)` migration step for every versioned state message and the corpus contains a frozen `v<n - 1>/` directory plus `v<n>/` fixtures. Enforcement SHALL be executable: CI completeness tests SHALL fail whenever the constant exceeds the registry's highest fully-populated chain or the corpus's highest version directory. The policy — including the non-waivable rule that existing field numbers are never retyped or reused, since migration runs after decode — SHALL be documented in `docs/state-migration.md`.

#### Scenario: A version bump without migration functions fails CI
- **WHEN** `CURRENT_STATE_SCHEMA_VERSION` is raised to `n` and any versioned message type lacks a registered `(type, n - 1)` step
- **THEN** the registry completeness test fails, naming the message type and missing version

#### Scenario: A version bump without a corpus entry fails CI
- **WHEN** `CURRENT_STATE_SCHEMA_VERSION` is raised to `n` and `tests/core/golden/` lacks a `v<n - 1>/` directory or `v<n>/` fixtures
- **THEN** the corpus completeness test fails, naming the missing directory
