# errors-sink Delta Specification

## ADDED Requirements

### Requirement: ActivationError carries a deterministic event time
`ActivationError` SHALL carry an `event_time_ms` field (int64 semantics, default `0`) alongside `entity_key`, `reason`, and `detail`. Every emission site SHALL populate it from replay-deterministic time only: the element's event time for element-path dead letters, or the timer's scheduled firing timestamp for timer-path dead letters. No emission site SHALL read a wall clock to populate it.

#### Scenario: Element-path dead letters carry the element's event time
- **WHEN** an activation for an envelope with `event_time_ms = T` fails and is routed to `.errors`
- **THEN** the emitted `ActivationError` has `event_time_ms = T`

#### Scenario: Timer-path dead letters carry the scheduled firing time
- **WHEN** a HITL or TTL timer callback emits a dead letter for a timer scheduled at time `T`
- **THEN** the emitted `ActivationError` has `event_time_ms` equal to `T`, not a wall-clock reading

#### Scenario: Replay produces identical records
- **WHEN** a bundle is retried and the same failure path is walked again
- **THEN** the re-emitted `ActivationError` compares equal to the original, including `event_time_ms`

### Requirement: Errors encode as AgentEnvelope-wrapped ActivationErrorRecord for message-bus sinks
For `kafka://` and `pubsub://` errors sinks, each `ActivationError` SHALL be encoded as `KV[bytes, bytes]`: the key is `entity_key`, and the value is an `AgentEnvelope` serialized with deterministic protobuf serialization, where `AgentEnvelope.entity_key` and `AgentEnvelope.event_time_ms` are copied from the error and `AgentEnvelope.external_event` holds the deterministically serialized `ActivationErrorRecord` (`entity_key`, `reason`, `detail`, `event_time_ms`). Encoding the same `ActivationError` twice MUST produce byte-identical output.

#### Scenario: Encoded record round-trips through AgentEnvelope
- **WHEN** an `ActivationError` is encoded and the value bytes are parsed as `AgentEnvelope`, then its `external_event` bytes are parsed as `ActivationErrorRecord`
- **THEN** the envelope's `entity_key` and `event_time_ms` equal the error's, and the record's `entity_key`, `reason`, `detail`, and `event_time_ms` all equal the error's fields

#### Scenario: Encoding is deterministic
- **WHEN** the same `ActivationError` is encoded twice
- **THEN** both `(key, value)` pairs are byte-identical

### Requirement: Errors encode as row mappings for BigQuery sinks
For a `bigquery://` errors sink, each `ActivationError` SHALL be encoded as a flat row mapping with `entity_key` rendered as lowercase hex (matching the trace-row convention) and `reason`, `detail`, and `event_time_ms` carried as their native values.

#### Scenario: Row carries all triage fields
- **WHEN** an `ActivationError` with all fields populated is encoded as a row
- **THEN** the row's `entity_key` is the hex encoding of the key bytes and `reason`, `detail`, and `event_time_ms` equal the error's fields

### Requirement: The errors encoder is registered with the RunAgent sink resolver
`DefaultSinkResolver.resolve` for the `errors_to` field SHALL return a write transform that first applies the scheme-appropriate error encoding (envelope bytes for `kafka://`/`pubsub://`, rows for `bigquery://`) and then writes via the scheme's writer. The bare scheme writer SHALL never receive raw `ActivationError` objects.

#### Scenario: errors_to kafka URI resolves to an encoding writer
- **WHEN** `DefaultSinkResolver.resolve("errors_to", "kafka://...")` is called and the result is applied to a `PCollection[ActivationError]`
- **THEN** the underlying Kafka writer receives `KV[bytes, bytes]` envelope-encoded elements

#### Scenario: errors_to bigquery URI resolves to a row-encoding writer
- **WHEN** `DefaultSinkResolver.resolve("errors_to", "bigquery://...")` is called and the result is applied to a `PCollection[ActivationError]`
- **THEN** the underlying BigQuery writer receives row mappings, not dataclasses

### Requirement: Intent dead letters unify into the error record schema
Intent serialization dead letters routed to a configured `errors_to` sink SHALL be mapped into `ActivationError` records — `entity_key` from the failed element's key, reason `intent_dead_letter`, and a JSON `detail` carrying the failure reason and the intent's identifying fields (`intent_id`, `seq`, `tool_name`) — and SHALL flow through the same errors encoder as activation dead letters, for every supported scheme.

#### Scenario: A dead-lettered intent reaches the errors sink as a unified record
- **WHEN** `intents_to` and `errors_to` are configured, and an intent fails serialization
- **THEN** the errors sink receives an envelope-encoded (or row-encoded, per scheme) `ActivationErrorRecord` with reason `intent_dead_letter` whose `detail` JSON includes the failure reason, `intent_id`, `seq`, and `tool_name`

#### Scenario: BigQuery errors sinks accept intent dead letters
- **WHEN** `errors_to` is a `bigquery://` URI and an intent dead letter is routed to it
- **THEN** the dead letter is delivered as a row mapping, not as raw `KV[bytes, bytes]`

### Requirement: Downstream failure-streak alarm consumption is documented and test-backed
The project SHALL document, in `docs/errors.md`, the errors-sink record schema and a complete downstream failure-streak alarm example: a plain Beam stateful DoFn that consumes envelope-encoded error records keyed by `entity_key`, counts dead letters per key, emits one alarm when the count reaches a threshold `N`, and resets its count after alarming. The example SHALL be exercised by an offline TestStream-driven test that feeds encoded error envelopes through the documented pipeline shape.

#### Scenario: The alarm fires once at the threshold
- **WHEN** `N` encoded error records for the same `entity_key` are fed through the example pipeline with threshold `N`
- **THEN** exactly one alarm is emitted for that key, carrying the key and the streak count

#### Scenario: Below-threshold keys stay silent and counts reset after alarming
- **WHEN** one key receives `N - 1` error records and another receives `2N - 1`
- **THEN** the first key emits no alarm and the second emits exactly one

#### Scenario: The example consumes the documented wire format
- **WHEN** the test feeds values produced by the errors-sink envelope encoder
- **THEN** the example pipeline decodes them via `AgentEnvelope`/`ActivationErrorRecord` parsing alone, with no dependence on runtime internals
