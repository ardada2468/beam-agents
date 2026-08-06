## Why

`RunAgent` already exposes a `.intents` output and resolves a configured `intents_to` URI to a "write transform" via its sink resolver, but no concrete outbox sink exists yet — the resolver has nothing real to hand back. Effectively-once side effects depend on `ToolIntent`s reaching the outbox durably and in the right order: the effector dedups on `intent_id`, but ordering per entity still matters because a later intent on a key may supersede or depend on an earlier one, and a silent serialization failure would drop an effect entirely. This change supplies the durable, order-preserving, loss-proof outbox writer the runtime has been designed around.

## What Changes

- Add a `WriteIntents` `PTransform` (in the `actions/` module) that consumes `PCollection[KV[bytes, ToolIntent]]` (key = `entity_key`) and writes serialized intents to an outbox topic.
- Support two outbox URI schemes: `kafka://<brokers>/<topic>` and `pubsub://<project>/<topic>`, dispatched by a scheme registry. Unknown/malformed schemes are rejected at pipeline-construction time with an actionable `ValueError`.
- Register `WriteIntents` with the existing sink resolver so a `RunAgent(AgentConfig(intents_to=...))` with a `kafka://` or `pubsub://` URI resolves to this transform (closing the loop left open by `add-runagent-transform`).
- Preserve **per-key intent order**: each intent is written to the outbox using its `entity_key` as the partition/ordering key (Kafka message key; Pub/Sub `orderingKey`), so intents for a given entity land on one partition in emission order.
- Serialize each intent with a deterministic, canonical protobuf encoding; on serialization failure, route the offending element to a **dead-letter** tagged output rather than failing the bundle or dropping the intent.
- Expose the transform's outputs as a typed result object exposing the dead-letter `PCollection` (`.dead_letter`) so callers can wire it to an errors sink; the main write branch has no meaningful main output.

## Capabilities

### New Capabilities
- `write-intents-sink`: A key-partitioned, order-preserving outbox writer for `ToolIntent`s over `kafka://` and `pubsub://` schemes, with construction-time URI validation and a dead-letter output for serialization failures.

### Modified Capabilities
<!-- No spec-level requirement changes to existing capabilities. The RunAgent sink-resolver contract from run-agent-transform is satisfied, not modified: this change registers a concrete resolver entry without altering RunAgent's documented behavior. -->

## Impact

- **New code:** `src/beam_agents/actions/write_intents.py` (transform + scheme registry + serializer), `actions/__init__.py` export; registration hook into the sink resolver used by `core/transform.py`.
- **Schemas:** consumes the existing `ToolIntent` proto (`protos/beam_agents.proto`); no proto changes.
- **Dependencies:** Kafka path uses Beam's `KafkaIO`/`WriteToKafka` (cross-language) and requires the expansion service in integration/CI; Pub/Sub path uses `apache_beam.io.gcp.pubsub.WriteToPubSub` with ordering keys. Both are within the committed `apache-beam[gcp]` dependency.
- **Tests:** unit tests run with no docker (fake/in-memory sink for order + dead-letter assertions); Kafka/Pub/Sub wiring exercised in the `integration` CI lane against Redpanda / Pub/Sub emulator.
- **Downstream:** completes the `.intents → outbox topic → effector → results topic` leg of the Dataflow shape; the effector and `ToolResult` re-injection are out of scope for this change.
