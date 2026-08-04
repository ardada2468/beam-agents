# effector-service Specification

## Purpose
TBD - created by archiving change add-reference-effector. Update Purpose after archive.
## Requirements
### Requirement: The effector is a standalone service with no pipeline dependency

The reference effector SHALL live in `beam_agents.effector`, and its own import closure SHALL contain neither Apache Beam nor `beam_agents.core`: it SHALL depend only on `beam_agents.tools`, `beam_agents.hitl`, and `beam_agents._protos`, each of which is already Beam-free. It SHALL be importable and runnable without any of the optional transport or dedup client libraries installed. Concrete client libraries (Kafka, Pub/Sub, Redis, Bigtable) SHALL be imported lazily inside their adapters, never at module import time. Effector symbols SHALL NOT be re-exported from `beam_agents/__init__.py`.

The closure is the guarantee, not the import statement: importing `beam_agents.effector` through the normal machinery also executes the parent `beam_agents/__init__.py`, which re-exports the Beam-facing pipeline surface. That is a property of the package layout — a standalone deployment of these modules never evaluates it — so the requirement is stated over, and verified against, the effector's own dependencies.

#### Scenario: No effector module imports Beam or the pipeline runtime

- **WHEN** every module under `beam_agents.effector` is inspected for its imports
- **THEN** none imports `apache_beam` or any module under `beam_agents.core`, whether at module level or lazily inside a function

#### Scenario: The package imports with Beam unavailable

- **GIVEN** `apache_beam` is made unimportable and the parent package's re-export module is not evaluated
- **WHEN** `beam_agents.effector` and every module under it are imported
- **THEN** all imports succeed and neither `apache_beam` nor `beam_agents.core` appears in `sys.modules`

#### Scenario: The package imports with no optional client libraries installed

- **GIVEN** none of `aiokafka`, `google.cloud.pubsub_v1`, `redis`, or `google.cloud.bigtable` is importable
- **WHEN** `beam_agents.effector` is imported and an `EffectorConfig` is constructed and validated
- **THEN** both succeed, and the `ImportError` for a missing client surfaces only when the corresponding adapter is constructed

#### Scenario: The effector is absent from the public API

- **WHEN** the names re-exported by `beam_agents/__init__.py` are inspected
- **THEN** no effector symbol appears among them

### Requirement: Intent sources and result sinks are protocol seams with Kafka and Pub/Sub adapters

The effector SHALL define an `IntentSource` protocol (yielding intents with an associated acknowledgement handle, plus a `commit` operation) and a `ResultSink` protocol (publishing a `ToolResult` under a partition/ordering key). It SHALL provide a Kafka adapter for `kafka://<brokers>/<topic>` and a Pub/Sub adapter for `pubsub://<project>/<subscription>` on the source side, and the corresponding topic-addressed adapters on the sink side. The service SHALL depend only on the protocols, never on a concrete adapter.

#### Scenario: The service loop runs against in-memory implementations

- **GIVEN** an in-memory `IntentSource`, `ResultSink`, and dedup store
- **WHEN** the service processes a scripted batch of intents
- **THEN** the loop completes with no network access and every published result is observable on the in-memory sink

#### Scenario: Results are published under the originating entity key

- **WHEN** the effector publishes a `ToolResult` for an intent carrying `entity_key = K`
- **THEN** the result is published with partition/ordering key `K` and its `entity_key`, `intent_id`, and `seq` fields equal the originating intent's

### Requirement: Configuration is validated eagerly, before any client is constructed

`EffectorConfig` SHALL carry the intents source URI, results sink URI, approval channel URI, dedup store URI, consumer group id, and the `lease_ms`, `result_ttl_ms`, and `tool_timeout_ms` budgets plus concurrency bounds. `validate()` SHALL parse and reject malformed or unknown-scheme URIs and SHALL reject a budget configuration where `lease_ms` does not exceed `tool_timeout_ms`, raising `ValueError` with an actionable message. Validation SHALL NOT import any transport or dedup client library.

#### Scenario: An unknown source scheme is rejected at construction

- **WHEN** an `EffectorConfig` is constructed with an intents URI whose scheme is neither `kafka` nor `pubsub`
- **THEN** a `ValueError` naming the offending URI and the supported schemes is raised at construction time

#### Scenario: A lease shorter than the tool timeout is rejected

- **WHEN** an `EffectorConfig` is constructed with `lease_ms <= tool_timeout_ms`
- **THEN** a `ValueError` is raised explaining that the lease must outlive a tool execution so an unexpired lease implies a live owner

#### Scenario: Validation performs no client imports

- **GIVEN** every optional client library is made unimportable
- **WHEN** a fully populated `EffectorConfig` is constructed and validated
- **THEN** validation succeeds

### Requirement: Each intent is processed in the order refuse-expired, claim, execute, complete, publish, commit

For every consumed intent the effector SHALL evaluate expiry first, then acquire a dedup claim, then execute, then write the terminal result into the dedup store, then publish it, and only then commit the offset/ack. The expiry check SHALL precede any dedup store access. The terminal result SHALL be durable in the dedup store before it is published. The offset/ack SHALL NOT be committed until publishing has succeeded.

#### Scenario: Expiry is decided before the dedup store is touched

- **GIVEN** an intent whose `expires_at_ms` is in the past
- **WHEN** the effector processes it
- **THEN** a `ToolResult` with status `EXPIRED` is published, the offset is committed, and the dedup store records no claim for that `intent_id`

#### Scenario: A crash between completion and publication does not re-execute

- **GIVEN** an intent whose tool has executed and whose result has been written to the dedup store
- **WHEN** the process is killed before publishing and the intent is redelivered to a new worker
- **THEN** the stored result is republished verbatim, the tool is not invoked a second time, and the offset is committed

#### Scenario: A crash before publication does not commit the offset

- **GIVEN** an intent being processed
- **WHEN** the process is killed at any point before the result is published
- **THEN** no offset is committed for that intent and it is redelivered

#### Scenario: Exactly one terminal result per intent id

- **WHEN** a stream of intents is replayed with process kills injected at every phase boundary
- **THEN** each distinct `intent_id` results in at most one tool invocation and exactly one terminal `ToolResult` status observed downstream

### Requirement: Per-key order is preserved through consumer-group partition affinity

The effector SHALL preserve the per-entity order in which intents were written. On Kafka it SHALL consume through a configured **consumer group**, relying on the `entity_key` message key set by the outbox writer to confine a key to one partition, and SHALL process one intent at a time per assigned partition, awaiting a terminal outcome before consuming the next from that partition. On Pub/Sub it SHALL consume an ordered subscription and process one message at a time per `ordering_key`. Concurrency SHALL apply across partitions/ordering keys only, bounded by a configured maximum.

#### Scenario: Intents for one key execute in emission order

- **GIVEN** three intents for the same `entity_key` delivered in `seq`/`step_index` order on one partition
- **WHEN** the effector processes that partition
- **THEN** their tools are invoked in that same order, each completing before the next begins

#### Scenario: Distinct partitions progress concurrently

- **GIVEN** intents on two partitions where the first partition's tool blocks
- **WHEN** the effector processes both
- **THEN** the second partition's intent completes without waiting for the first, up to the configured concurrency bound

#### Scenario: A revoked partition releases unexecuted claims

- **GIVEN** an assigned partition with an intent that has been claimed but not yet executed
- **WHEN** the partition is revoked by a consumer-group rebalance
- **THEN** the claim is released so the new owner can proceed immediately, and no offset is committed for that intent

### Requirement: Infrastructure operations retry with backoff; the tool never does

The effector SHALL retry idempotent infrastructure operations — dedup store RPCs and result publication — with bounded exponential backoff. It SHALL NOT re-invoke a tool callable after a failure within the same claim. When publication cannot succeed within its retry budget, the effector SHALL NOT commit the offset.

#### Scenario: A transient publish failure is retried and then committed

- **GIVEN** a result sink that fails the first publish attempt and succeeds on the second
- **WHEN** the effector publishes a result
- **THEN** the publish is retried after a backoff, succeeds, and the offset is committed once

#### Scenario: A failed tool is not re-invoked

- **GIVEN** a side-effecting tool that raises
- **WHEN** the effector processes its intent
- **THEN** the callable is invoked exactly once and a `ToolResult` with status `ERROR` is published

#### Scenario: Exhausted publish retries leave the offset uncommitted

- **GIVEN** a result sink that fails every attempt
- **WHEN** the retry budget is exhausted
- **THEN** no offset is committed for that intent, and the failure is surfaced rather than swallowed
