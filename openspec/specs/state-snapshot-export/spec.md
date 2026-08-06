# state-snapshot-export Specification

## Purpose
TBD - created by archiving change add-replay-cli. Update Purpose after archive.
## Requirements
### Requirement: An export request yields one snapshot from the keyed stream

The stateful DoFn SHALL handle an `AgentEnvelope` whose payload is `export_request` by reading the key's `MEMORY`, `CONTINUATION`, `LLM_CACHE`, and `PENDING` state cells and the `SEQ` counter, building exactly one `StateSnapshot`, and emitting it on the `.snapshots` tagged output. Because elements are processed one at a time per key, the snapshot SHALL reflect a consistent point in the key's serial history: every activation committed before the request on the key's stream, and none after it. `StateSnapshot.snapshot_at_ms` SHALL be the request envelope's `event_time_ms` — a replay-deterministic time, never a wall-clock reading.

#### Scenario: Snapshot captures the blobs a subsequent activation would load

- **WHEN** an activation for key K commits, an `export_request` for K is processed next, and the emitted `StateSnapshot` is compared against the state a following activation for K loads
- **THEN** the snapshot's `memory`, `llm_cache`, `continuation`, and `pending` contents equal those loaded blobs field-for-field, and `seq` equals the committed `SEQ` counter value

#### Scenario: A retried bundle re-emits a byte-identical snapshot

- **WHEN** the bundle containing an `export_request` is retried and state is unchanged
- **THEN** the re-emitted `StateSnapshot`, serialized deterministically, is byte-identical to the first emission

### Requirement: Export is read-only

Handling an `export_request` MUST NOT mutate any state cell, set or clear any timer, or increment `SEQ`. No activation runs: the agent is not invoked, no provider call is made, and nothing is emitted on `.output`, `.intents`, `.traces`, or `.errors` for the request.

#### Scenario: State and seq are untouched by an export

- **WHEN** an `export_request` is processed for a key holding populated memory, cache, and continuation state
- **THEN** every state cell reads back unchanged afterwards, `SEQ` is not incremented, and the next activation for the key observes identical inputs to what it would have observed had the export never happened

#### Scenario: An export produces no activation outputs

- **WHEN** an `export_request` is processed
- **THEN** the only element emitted is the `StateSnapshot` on `.snapshots`, with nothing on the main output, `.intents`, `.traces`, or `.errors`

### Requirement: The snapshots output resolves a sink like traces

`RunAgent` SHALL expose `.snapshots` as a tagged output of `PCollection[StateSnapshot]`, and `AgentConfig` SHALL accept a `snapshots_to` URI resolved by the default sink resolver with a serialization step, mirroring `traces_to`: message-bus schemes SHALL receive `(entity_key, deterministic proto bytes)` pairs keyed by `entity_key`. A snapshot is an opaque per-key state image with no row encoding, so the default resolver SHALL refuse a `bigquery://` `snapshots_to` at `AgentConfig` construction with an actionable error naming the schemes that do carry it. When no `snapshots_to` is configured, the tagged output SHALL remain exposed and unconsumed; configuring a sink SHALL NOT be required for pipeline construction.

#### Scenario: A configured snapshots sink receives serialized snapshots keyed by entity

- **WHEN** a pipeline runs with `snapshots_to` set to a message-bus scheme and an `export_request` is processed for key K
- **THEN** the sink receives one message whose key is K and whose value parses as the emitted `StateSnapshot`

#### Scenario: No sink configured still constructs and runs

- **WHEN** a pipeline is constructed without `snapshots_to` and an `export_request` is processed
- **THEN** construction and execution succeed, and the snapshot element is dropped with the unconsumed output
