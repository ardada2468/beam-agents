## Context

`add-runagent-transform` shipped `AgentConfig`, `RunAgent`, and a `DefaultSinkResolver` (`core/transform.py`). For `intents_to`, `resolve()` today returns a **bare** `WriteToKafka(...)` / `WriteToPubSub(topic=...)`. That is a placeholder, not a correct outbox writer:

- `.intents` is a `PCollection[ToolIntent]` (proto messages). `WriteToKafka` expects `KV[bytes, bytes]` and `WriteToPubSub` expects `bytes` — the proto is never serialized, and no message key is set.
- With no message key, Kafka round-robins across partitions, so **per-key intent order is lost** — two intents for the same `entity_key` can land on different partitions and be consumed out of order.
- A serialization error inside a bare write would fail the whole bundle, and on retry the runtime's staged-effects model means the intent could be silently dropped.

The `ToolIntent` proto already carries `entity_key` (bytes), `intent_id`, `seq`, `step_index`, and `expires_at_ms`; the effector dedups on `intent_id`. Ordering per entity still matters because a later intent on a key can supersede or depend on an earlier one. This change replaces the placeholder with a real `WriteIntents` transform in the `actions/` module and points the resolver at it.

## Goals / Non-Goals

**Goals:**
- A `WriteIntents` `PTransform` consuming `PCollection[KV[bytes, ToolIntent]]` that serializes each intent, writes it key-partitioned by `entity_key`, preserves per-key order, and dead-letters serialization failures.
- Support `kafka://<brokers>/<topic>` and `pubsub://<project>/<topic>`, reusing the existing URI grammar and construction-time validation semantics.
- Wire the resolver so `intents_to` with a `kafka://`/`pubsub://` scheme resolves to `WriteIntents`, closing the RunAgent → outbox leg.
- Unit-testable with no docker via an injectable in-memory writer.

**Non-Goals:**
- The effector, `ToolResult` re-injection, and dedup on `intent_id` (separate change).
- BigQuery as an intents destination (`bigquery://` remains a traces/errors sink only).
- Encryption, schema-registry integration, or payload compression.
- Changing `RunAgent`'s output surface, `.output` handling, or the traces/errors sinks.

## Decisions

### 1. `WriteIntents` lives in `actions/`, keeps keying explicit
Per the module map, `actions/` owns "intents + outbox sinks". `WriteIntents.expand` requires `PCollection[KV[bytes, ToolIntent]]` and **does not re-key** — mirroring `RunAgent`'s "pre-keyed KV, validate at construction" contract. `.intents` from `RunAgent` is currently `PCollection[ToolIntent]`; the resolver-attached branch prepends a `WithKeys(lambda i: i.entity_key)` (a pure, deterministic keying step) before `WriteIntents`, so `WriteIntents` itself always sees KV and its ordering guarantee is unambiguous. `expand` raises `ValueError` when handed non-KV input.

### 2. Scheme dispatch via a small registry, validation stays import-free
Reuse the existing URI grammar and `UnknownSinkSchemeError`. `WriteIntents` validates its URI in `__init__` (cheap, import-free — no Beam IO imports), and builds the concrete writer only in `expand`. A `_WRITERS` registry maps `"kafka"`/`"pubsub"` to a builder callable; adding a scheme is one entry. Validation rejects unknown schemes and recognized-but-incomplete URIs (missing brokers/project or topic) at construction, before any pipeline is built.

### 3. Serialization: canonical protobuf, keyed by raw `entity_key`
Each element `KV(entity_key, intent)` becomes `KV(entity_key_bytes, serialize(intent))`. Serialization uses `intent.SerializeToString(deterministic=True)` so byte-identical intents yield byte-identical payloads (consistent with the runtime's replay/determinism invariants). The Kafka message **key** is the raw `entity_key` bytes; the Pub/Sub `orderingKey` is a stable string derived from `entity_key` (hex of the bytes). This runs in a `DoFn` (not a `Map`) so it can emit to a tagged dead-letter output.

### 4. Order preservation is a property of the message key + destination config
- **Kafka:** setting the message key = `entity_key` makes the default partitioner hash all of a key's intents to one partition; within a partition Kafka preserves append order, and the upstream stateful DoFn emits a key's intents in `seq`/`step_index` order. `WriteIntents` sets `producer_config` (idempotent producer: `enable.idempotence=true`) but does **not** add cross-key batching that would interleave a single key's records.
- **Pub/Sub:** publishes directly via `google.cloud.pubsub_v1.PublisherClient(publisher_options=PublisherOptions(enable_message_ordering=True))` with `ordering_key=entity_key.hex()` per message, **bypassing** `apache_beam.io.gcp.pubsub.WriteToPubSub`. Verified against a live Pub/Sub emulator: as of the apache-beam version pinned here (2.72.0), `WriteToPubSub`'s DirectRunner path (`_PubSubWriteDoFn._flush()`) deserializes the `PubsubMessage` but calls the client's `.publish()` with only `data`/`attributes` — it never forwards `ordering_key`, so every message publishes unordered regardless of what key is set on it. This is a real bug in that Beam version's Pub/Sub sink, not a theoretical risk; publishing directly is the only way to get actual per-key order on this scheme today. Ordered delivery must still be enabled on the subscription downstream (a deployment precondition `WriteIntents` cannot enforce).

### 5. Dead-letter, never drop, never fail the bundle
The serializer `DoFn` wraps `SerializeToString` in try/except. On failure it emits the original element plus a reason string to a `dead_letter` tagged output instead of raising. `expand` returns a small typed `WriteIntentsResult` frozen dataclass exposing `.dead_letter: PCollection`. The resolver attaches this to the errors leg when `errors_to` is set, or leaves it exposed. Successful intents never appear on `.dead_letter`. Because the failure path emits rather than raises, a poison intent cannot wedge the bundle or vanish under retry.

### 6. Resolver integration is additive
`DefaultSinkResolver.resolve` gains a branch: for the intents field, return the `WithKeys → WriteIntents` composite rather than a bare `WriteToKafka`/`WriteToPubSub`. Since `resolve` is only called at `expand`, and the resolver already parses/validates the URI, no new validation surface is added to `AgentConfig`. `traces_to`/`errors_to` keep their existing bare writers. `_SINK_LABELS["intents_to"]` is already `"WriteIntents"`, so no label change is needed.

### 7. Testability via an injectable writer
`WriteIntents` accepts an optional `writer_factory` (default: the real Kafka/Pub/Sub builder). Unit tests inject an in-memory writer that records `(key, payload)` tuples, letting tests assert (a) per-key order, (b) deterministic serialization, and (c) dead-letter routing on a forced serialization error — all with no docker. Real Kafka/Pub/Sub wiring is exercised in the `integration` lane (Redpanda + Pub/Sub emulator).

## Risks / Trade-offs

- **Kafka cross-language expansion service** is required to construct `WriteToKafka`; it is unavailable in pure unit tests. Mitigated by the `writer_factory` seam — unit tests never touch `KafkaIO`; only integration does. Confirmed live against Redpanda and root-caused precisely: `org.apache.beam.sdk.io.kafka.upgrade.KafkaIOTranslation$WriteRegistrar` (a Java-native `PTransformTranslator` for pipeline `--update`/drain snapshot compatibility) unconditionally tags `KafkaIO.Write` with `beam:transform:org.apache.beam:kafka_write:v2` whenever it passes through Java's proto serialization — including inside the ExpansionService's own response to Python — and `PTransform.from_runner_api` has no fallback for an unrecognized non-empty urn (only for an empty one), so it raises `KeyError`. Confirmed version-independent (reproduced against both Beam 2.60.0 and 2.72.0 via `JavaJarExpansionService`, ruling out jar-pinning as a fix); a `_known_urns` monkeypatch clears the `KeyError` but surfaces a second, unrelated Beam bug in `AnyOfEnvironment.resource_hints()`. This is a genuine, reproducible upstream Beam defect, not fixable from `WriteIntents`. The integration test is marked `xfail(strict=False)` with the full trace; recommend filing against apache/beam.
- **Pub/Sub ordering** requires the subscription to have message ordering enabled; `WriteIntents` publishes directly with `enable_message_ordering=True` and a per-message `ordering_key` (see Decision 4) after confirming Beam's own `WriteToPubSub` silently drops the key. Verified passing end-to-end against a live Pub/Sub emulator.
- **Idempotent producer vs. throughput:** `enable.idempotence=true` plus single-partition-per-key caps per-key throughput to one partition's capacity. Acceptable — intents per entity are low-volume and correctness (order + no-dup) dominates; cross-key parallelism is unaffected.
- **Dead-letter payload shape:** serialization failures for a proto are rare (mostly programming errors), so `.dead_letter` mainly guards against unexpected oversized/invalid intents. The reason string is best-effort; the element is preserved verbatim so it can be re-driven after a fix.
- **`WithKeys` re-derivation:** keying off `intent.entity_key` in the resolver branch assumes `.intents` elements are unkeyed `ToolIntent`s. If a future change makes `.intents` already-KV, the resolver branch must drop the `WithKeys` step; the `WriteIntents` KV contract stays stable either way.
