# write-intents-sink Specification

## Purpose
TBD - created by archiving change add-writeintents-transform. Update Purpose after archive.
## Requirements
### Requirement: WriteIntents consumes pre-keyed ToolIntent KV input and validates it at construction
`WriteIntents` SHALL be a `PTransform` consuming a `PCollection[KV[bytes, ToolIntent]]` whose key is the intent's `entity_key`. `WriteIntents` SHALL NOT re-key elements itself. At pipeline-construction (`expand`) time it SHALL raise `ValueError` with an actionable message when the input is positively not KV-shaped (for example a bare `PCollection[ToolIntent]`), directing the caller to key by `entity_key` upstream.

#### Scenario: Pre-keyed KV input flows through
- **WHEN** a `PCollection[KV[bytes, ToolIntent]]` is passed to `WriteIntents`
- **THEN** each keyed intent reaches the outbox writer under its `entity_key` and no additional keying step is inserted

#### Scenario: Non-KV input is rejected at construction
- **WHEN** a `PCollection[ToolIntent]` (not KV-shaped) is passed to `WriteIntents`
- **THEN** `expand` raises `ValueError` explaining that KV input keyed by `entity_key` is required, before the pipeline runs

### Requirement: WriteIntents dispatches on outbox URI scheme and validates the URI at construction
`WriteIntents` SHALL accept a single outbox URI and select the writer via a scheme registry. The registry SHALL recognize `kafka://<brokers>/<topic>` and `pubsub://<project>/<topic>`. At construction time `WriteIntents` SHALL raise `ValueError` naming the offending URI when the scheme is unrecognized, or when a recognized scheme is missing required components (Kafka: brokers and topic; Pub/Sub: project and topic). Validation SHALL occur before any pipeline runs.

#### Scenario: Recognized kafka URI resolves to the Kafka writer
- **WHEN** `WriteIntents` is constructed with `kafka://broker:9092/agent-intents`
- **THEN** construction succeeds and the Kafka outbox writer is selected

#### Scenario: Recognized pubsub URI resolves to the Pub/Sub writer
- **WHEN** `WriteIntents` is constructed with `pubsub://my-project/agent-intents`
- **THEN** construction succeeds and the Pub/Sub outbox writer is selected

#### Scenario: Unknown scheme is rejected at construction
- **WHEN** `WriteIntents` is constructed with a URI whose scheme is neither `kafka` nor `pubsub` (for example `sqs://q`)
- **THEN** construction raises `ValueError` naming the offending URI, and no pipeline is built

#### Scenario: Recognized scheme missing required components is rejected
- **WHEN** `WriteIntents` is constructed with a `kafka://` or `pubsub://` URI missing its brokers/project or topic
- **THEN** construction raises `ValueError` naming the offending URI and the missing component

### Requirement: WriteIntents routes each key to a single outbox partition or ordering key
For every intent, `WriteIntents` SHALL set the outbox partition/ordering key to the element's `entity_key` (Kafka message key; Pub/Sub `orderingKey`) so that all intents for a given entity are routed to a single partition/ordering key rather than scattered across the topic. `WriteIntents` SHALL NOT itself reorder a key's intents relative to how its input `PCollection` presents them, batch across keys in a way that interleaves a single key's intents, or coalesce intents that share an `entity_key`. Enabling Pub/Sub ordered delivery (message ordering) SHALL be part of configuring the Pub/Sub writer. Beam does not guarantee intra-`PCollection` element order is preserved across bundle retries or splits on a distributed runner, so this routing is what lets a consumer *group* a key's intents together; it is not, by itself, an end-to-end guarantee that wire order equals emission order. A consumer needing a total order for a key SHOULD order by `ToolIntent.seq`.

#### Scenario: Two intents on the same key route to the same partition
- **WHEN** intents `i1` then `i2` with the same `entity_key` are written
- **THEN** both carry that `entity_key` as the outbox ordering key, landing on the same destination partition

#### Scenario: Intents on different keys may be independently partitioned
- **WHEN** intents with distinct `entity_key`s are written
- **THEN** each is keyed by its own `entity_key` and no cross-key ordering is asserted

### Requirement: WriteIntents serializes intents with a canonical, deterministic encoding
`WriteIntents` SHALL serialize each `ToolIntent` to the outbox payload using a deterministic protobuf encoding, such that two byte-identical `ToolIntent` messages produce byte-identical payloads. The outbox message key SHALL be the raw `entity_key` bytes.

#### Scenario: Identical intents serialize identically
- **WHEN** two `ToolIntent` messages with equal field values are serialized by `WriteIntents`
- **THEN** the produced payload bytes are identical

### Requirement: WriteIntents routes serialization failures to a dead-letter output
When serializing an intent fails, `WriteIntents` SHALL NOT fail the bundle and SHALL NOT drop the intent. Instead it SHALL emit the offending element, together with the failure reason, to a dead-letter tagged output. `WriteIntents.expand` SHALL return a typed result object exposing this dead-letter `PCollection` as `.dead_letter`. Intents that serialize successfully SHALL NOT appear on `.dead_letter`.

#### Scenario: A serialization failure is dead-lettered, not dropped
- **WHEN** an element cannot be serialized to the outbox payload
- **THEN** the element and its failure reason appear on `.dead_letter` and are not written to the outbox topic, and the bundle does not fail

#### Scenario: Successful intents do not appear on dead-letter
- **WHEN** an intent serializes successfully and is written to the outbox
- **THEN** it does not appear on `.dead_letter`

### Requirement: WriteIntents is registered with the RunAgent sink resolver for intents URIs
`WriteIntents` SHALL be registered such that the sink resolver used by `RunAgent` resolves an `intents_to` URI with a `kafka://` or `pubsub://` scheme to a `WriteIntents` write branch attached to the `.intents` output. Registration SHALL NOT change `RunAgent`'s documented output surface or attach any writer to `.output`.

#### Scenario: intents_to kafka URI resolves to WriteIntents
- **WHEN** `RunAgent` runs with an `AgentConfig` whose `intents_to` is a `kafka://` URI
- **THEN** the sink resolver attaches a `WriteIntents` Kafka write branch to the `.intents` output and `.intents` remains exposed on `RunAgentOutputs`

#### Scenario: intents_to pubsub URI resolves to WriteIntents
- **WHEN** `RunAgent` runs with an `AgentConfig` whose `intents_to` is a `pubsub://` URI
- **THEN** the sink resolver attaches a `WriteIntents` Pub/Sub write branch to the `.intents` output
