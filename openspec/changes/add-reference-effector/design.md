## Context

The runtime side of effects is complete: `ctx.act(...)` stages a `ToolIntent` whose `intent_id` is `uuid5(NAMESPACE, entity_key + seq + step_index)` (correctness invariant 2), `WriteIntents` lands it on the outbox topic keyed by `entity_key` with per-key order preserved, and `hitl.intent_expired` / `hitl.refuse_expired` are already the pure, Beam-free, I/O-free layer-2 expiry guard (correctness invariant 6). Nothing consumes the outbox. The `ToolResult` re-injection path in `core/dofn.py::_resume` is fully implemented and completely unreachable.

The constraints this design lives under are fixed by the project:

- **The effector is external.** Module map: `effector/` is "a separate reference service: consume intents → dedup → execute → publish results". It must not import Beam, must not import `core/`, and must not be part of the `beam_agents/__init__.py` public API.
- **Determinism is the pipeline's job; dedup is the effector's.** The pipeline guarantees a replayed bundle re-mints byte-identical `intent_id`s. That guarantee is worth nothing unless the executor collapses duplicates on `intent_id`. Together they are the entire effectively-once argument.
- **Unit tests must pass offline with no docker.** Kafka, Pub/Sub, Redis, and Bigtable clients are all optional dependencies and must sit behind seams that in-memory fakes can fill.
- **Side effects are guarded in-pipeline.** `Tool.__call__` and `ToolRunner.run` both raise `SideEffectToolError` for `side_effect=True`. The effector is precisely where those tools *must* run, so the guard needs one named, auditable exception rather than a private-attribute bypass.

## Goals / Non-Goals

**Goals:**

- An asyncio service that consumes `ToolIntent`s from Kafka or Pub/Sub, refuses expired ones, dedups on `intent_id` through a pluggable store, executes side-effecting tools from a shared `ToolRegistry`, and publishes exactly one `ToolResult` per intent keyed by `entity_key`.
- Per-key ordering preserved under horizontal scale-out, via consumer-group partition affinity (Kafka) and ordered subscriptions (Pub/Sub).
- No intent lost (commit strictly after the outcome is durable) and no intent executed twice for one `intent_id`, except in a precisely stated lease-expiry window.
- Two production dedup backends (Redis, Bigtable) behind one protocol, plus an in-memory one for tests.
- Every failure mode maps to a terminal `ToolResult` status; nothing silently disappears.

**Non-Goals:**

- Hosting, containerization, autoscaling, or a control plane ("no hosted effector" is an explicit project non-goal). This ships a library plus an entry point.
- Owning the approval loop. Approval intents are routed to a channel; the human decision returns as `AgentEnvelope.Approval` on the approvals topic, produced by whatever fronts that channel.
- Transactional coupling between the tool's own side effect and the dedup record. Tools are arbitrary external systems; exactly-once *effects* would require the tool to participate (idempotency key or its own transaction). We give exactly-once *dispatch* per `intent_id` and state the residual window honestly.
- Re-issuing intents. `ToolIntent.attempt` is the pipeline's field for its own re-issue; the effector never increments it and never re-runs a tool under the same claim.
- Schema changes. `ToolIntent`/`ToolResult` as they stand are sufficient.

## Decisions

### D1. `effector/` is a standalone asyncio service with a hard import boundary

Layout: `effector/config.py` (`EffectorConfig` + eager, import-free URI validation), `effector/sources.py` (`IntentSource` protocol + Kafka/Pub/Sub adapters), `effector/sinks.py` (`ResultSink` protocol + adapters), `effector/dedup.py` (`DedupStore` protocol + Redis/Bigtable/in-memory), `effector/runner.py` (`EffectorToolRunner`), `effector/service.py` (the loop), `effector/__main__.py` (entry point).

`effector/` imports only `beam_agents.tools`, `beam_agents.hitl`, and `beam_agents._protos` — all three are already Beam-free and side-effect-free on import. A unit test asserts the boundary by importing `beam_agents.effector` with `apache_beam` blocked from `sys.modules` and asserting the import succeeds. Concrete client libraries (`aiokafka`, `google.cloud.pubsub_v1`, `redis.asyncio`, `google.cloud.bigtable`) are imported **inside** the adapter constructors, the same lazy-import pattern (and the same `PLC0415` per-file ignore) already used by `core/transform.py` and `actions/write_intents.py`.

*Alternative rejected:* a separate distribution package. Premature — it splits the shared `@tool` definitions and the shared protos across two release trains for a v0.x reference service. An optional dependency group gets the same install-time isolation.

### D2. Asyncio, one task per partition, sequential within a partition

The service runs an asyncio loop consistent with the project's async-first internals. Ordering is not enforced by the effector's own bookkeeping — it is inherited from the transport and preserved by *not* breaking it:

- **Kafka:** intents are written with the Kafka message key = `entity_key`, so a key's intents hash to one partition. A consumer group assigns each partition to exactly one member. The service runs one processing task per assigned partition and awaits each intent to a terminal outcome before pulling the next one from that partition. Scale-out = more members, up to the partition count; per-key order survives because a key never spans partitions.
- **Pub/Sub:** the subscription must have message ordering enabled (a deployment precondition, the mirror of the one `WriteIntents` already documents). Messages are grouped by `ordering_key` and processed one at a time per key; Pub/Sub itself withholds the next message of an ordering key until the previous is acked.

Cross-partition concurrency is bounded by `max_concurrent_partitions`. Within a partition, a slow tool blocks the other keys sharing that partition. That is the accepted cost of ordering — mitigated by partition sizing and the per-tool execution timeout (D6), not by out-of-order execution.

*Alternative rejected:* a global work pool with a per-key mutex. It reproduces the ordering guarantee the transport already provides, adds a distributed-lock dependency, and breaks the moment two workers hold the same key.

### D3. Phase order per intent: refuse-expired → claim → execute → complete → publish → commit

This exact order is load-bearing; each edge is a correctness argument.

1. **Refuse-expired first.** `hitl.refuse_expired(intent, now_ms)` runs before the dedup store is touched. An expired intent must never consume a claim, never reach a tool, and never depend on store availability to be refused. The refusal `ToolResult(EXPIRED)` is published and the offset committed. Expiry is **kind-agnostic** — an expired approval intent is refused the same way, so the agent's continuation gets its `EXPIRED` result immediately instead of waiting out the layer-1 HITL timer.
2. **Claim before execute.** Nothing runs without an exclusive claim on `intent_id`.
3. **Complete before publish.** The terminal `ToolResult` is written into the dedup record *before* it is published. Crash between `complete` and `publish` → redelivery finds `DONE` and **republishes the stored result without re-executing**. Publishing first would invert this: a crash before `complete` would leave the record claimed-but-unresolved and redelivery would re-execute an already-performed effect. This is why `DONE` stores the whole result and not just a tombstone.
4. **Commit after publish.** Offsets/acks commit only once the result is on the results topic. A crash anywhere earlier re-delivers the intent, which is safe by construction: every earlier phase is either idempotent or guarded by the claim. Delivery is at-least-once; *execution* is at-most-once per claim.

Duplicate re-publishing of a `ToolResult` is harmless downstream: `_resume` admits a result only against a live continuation with a matching pending `intent_id`, and routes anything else to `.errors` as `orphaned_result`.

### D4. Three-state claim protocol, and `IN_FLIGHT` waits rather than skips

`DedupStore.claim(intent_id, lease_ms)` returns exactly one of:

- `Claimed` — the caller owns execution (carries an opaque `token` identifying this claim).
- `InFlight` — a live lease is held by someone else.
- `Done(result)` — a terminal `ToolResult` is already stored.

Plus `complete(intent_id, token, result, ttl_ms)` (conditional on still owning `token`) and `release(intent_id, token)` (voluntary abandon, e.g. on partition revocation).

`InFlight` is handled by **waiting with backoff and re-claiming, never by skipping-and-committing**. Skipping would commit an offset for an intent whose outcome is owned by a worker that may be dead, and if that worker never completes, the effect is silently dropped. Waiting turns the worst case into bounded head-of-line blocking (bounded by `lease_ms`), which is recoverable; skipping turns it into a lost effect, which is not.

`lease_ms` MUST exceed the per-tool execution timeout plus publish/commit budget (validated at config time). A lease therefore only expires when the owner is genuinely presumed dead.

The `DONE` record's TTL (`result_ttl_ms`, default 24h) bounds store growth. It must exceed the maximum plausible redelivery lag; past it, a redelivered intent re-executes. That is the same trade every dedup window makes, and it is why the TTL is configuration, not a constant.

### D5. Redis via `SET NX PX`; Bigtable via `CheckAndMutateRow`

Both implement the same protocol; the state is encoded in a single value so claim and completion are one atomic operation each.

- **Redis.** Value framing is a one-byte tag: `C<token>` for a claim, `D<serialized ToolResult>` for a terminal result. `claim` is `SET intent_id "C<token>" NX PX lease_ms`; on `nil` it `GET`s and decodes the tag to distinguish `InFlight` from `Done`. `complete` and `release` are small Lua scripts doing compare-and-set / compare-and-delete on the token, so a worker whose lease expired mid-flight can never clobber the new owner's record. Server-side `PX` expiry gives lease and TTL semantics for free.
- **Bigtable.** Row key is `intent_id` (a uuid5 — already uniformly distributed, so no salting needed). Column family `d` with columns `claim` (big-endian int64 lease-expiry ‖ token) and `result` (serialized `ToolResult`). `claim` is a `CheckAndMutateRow` whose predicate is a filter chain matching a live claim — column `claim` present **and** a `ValueRange` whose lower bound is the big-endian encoding of `now_ms`; big-endian fixed-width encoding makes Bigtable's lexicographic value ordering agree with numeric ordering, so "lease not yet expired" is expressible as a range filter. Predicate true → `InFlight`; predicate false → the false-branch mutation writes the new claim, and a `result`-column check distinguishes `Done`. Worst case is two RPCs (claim check, then a read for a stored result); the common path is one. `complete` is a `CheckAndMutateRow` conditional on the claim column still carrying this worker's token. Row TTL comes from the column family's `maxage` GC rule, set to `result_ttl_ms`.

*Alternative rejected for Bigtable:* `ReadModifyWriteRow`. It is atomic but unconditional — it cannot express "only if unclaimed", which is the entire operation.

### D6. `EffectorToolRunner`: the inverted guard, and one attempt per claim

`EffectorToolRunner.run(tool, arguments)` is the mirror image of `ToolRunner.run`:

- It **requires** `side_effect=True` and raises for `side_effect=False`. A read-only tool arriving on the outbox means the pipeline should have run it inline — that is a bug to surface as `REJECTED`, not an effect to perform.
- It validates arguments against the tool's Pydantic model first; a failure is `REJECTED` and the callable is never invoked.
- It awaits awaitable results, matching `ToolRunner`'s behavior.
- It wraps the call in `asyncio.wait_for(..., tool_timeout_ms)`.

To reach the callable it uses a named accessor on `Tool` (`Tool.unwrap()`), not `tool._func`. The one sanctioned bypass of correctness invariant 5 should be greppable, documented, and testable — a private-attribute poke is none of those.

**The tool is invoked at most once per claim.** There is no retry around the callable: a side-effecting tool that raised may well have performed part of its effect, so a blind re-invocation is a second effect, not a retry. Retries with exponential backoff apply only to the *idempotent* infrastructure operations — dedup RPCs and result publishing.

Status mapping is total, and the dividing line is whether the callable ran:

| Condition | Status |
|---|---|
| tool returned | `OK` (payload = canonical-JSON-encoded return value) |
| tool raised, or exceeded `tool_timeout_ms` | `ERROR` (effect unknown) |
| `expires_at_ms` passed | `EXPIRED` |
| unknown tool name / `side_effect=False` tool / argument validation failure | `REJECTED` (never invoked) |

### D7. Approval intents are routed and marked terminal, without a `ToolResult`

`ToolIntent.kind == APPROVAL` (after the expiry check, which is kind-agnostic per D3) is published verbatim to the configured approval channel keyed by `entity_key`, then marked terminal in the dedup store with a sentinel record so redelivery cannot double-notify a human. **No `ToolResult` is published** — the answer to an approval is an `AgentEnvelope.Approval` on the approvals topic, minted by whoever fronts the channel, and the layer-1 HITL timer already covers the never-answered case. `TOOL_KIND_UNSPECIFIED` is treated as `TOOL`, matching the proto's documented additive-decode contract.

### D8. Configuration reuses the existing URI grammar; validation is eager and import-free

`EffectorConfig` is a frozen dataclass carrying the intents source URI, results sink URI, approval channel URI, dedup URI (`redis://`, `bigtable://<project>/<instance>/<table>`, `memory://`), consumer group id, lease/TTL/timeout budgets, and concurrency bounds. `validate()` parses every URI and checks the budget invariant from D4 (`lease_ms > tool_timeout_ms + publish budget`) **without importing any client library**, mirroring `AgentConfig.__post_init__`. Misconfiguration raises `ValueError` with an actionable message at construction, not on first message.

### D9. Testability: protocol seams everywhere, in-memory fakes for the whole loop

`IntentSource`, `ResultSink`, and `DedupStore` are `typing.Protocol`s; the service depends only on them. Unit tests drive the full loop with an in-memory source (a scripted list with a recording `commit`), an in-memory sink, and an in-memory dedup store — no docker, no network. Crash injection is a fake that raises at a named phase boundary, which is how "complete before publish" and "commit after publish" become assertable properties rather than review comments. The `-m semantics` effectively-once gate replays an intent stream with kills injected at every phase boundary and asserts exactly one execution per `intent_id` and one terminal result per `intent_id`. Redpanda/Redis/Pub/Sub-emulator wiring is exercised separately under `-m integration`.

## Risks / Trade-offs

- **Lease expiry can double-execute.** → A tool that outlives its lease while the owner is alive-but-slow can be re-claimed and re-run. Mitigated by validating `lease_ms > tool_timeout_ms + slack` at config time, so an un-expired lease implies a live owner; and by `complete` being conditional on the claim token, so the loser of a race cannot overwrite the winner's result. Residual: a partitioned-but-alive worker. Documented in the spec — exactly-once *effects* need the tool to be idempotent on `intent_id`, and the recommended pattern is to pass `intent_id` as the tool's own idempotency key.
- **`IN_FLIGHT` blocks the partition head.** → Bounded by `lease_ms`; a wedged claim delays a partition for at most one lease. Deliberately preferred over skipping (D4), which would lose effects.
- **A slow tool blocks unrelated keys sharing a partition.** → Per-tool timeout plus partition-count sizing. Ordering per key is a stated invariant; out-of-order execution is not an available trade.
- **Kafka rebalance mid-execution.** → The revoked partition's in-flight intent keeps its claim; the new owner observes `InFlight` and waits (D4). On a clean revocation the service calls `release` for intents not yet executed, so the new owner proceeds immediately. Offsets are never committed for an intent whose result was not published.
- **Pub/Sub ordered delivery is a deployment precondition.** → The subscription must be created with message ordering enabled; the effector cannot enforce this. Same class of precondition `WriteIntents` already documents on the publish side. Startup logs a warning when the subscription's ordering flag is readable and false.
- **Dedup TTL vs. redelivery lag.** → A redelivery arriving after `result_ttl_ms` re-executes. Default 24h, configurable; the spec states the bound explicitly rather than implying an unbounded guarantee.
- **Four new optional dependencies.** → Confined to an `effector` extra, imported lazily inside adapters. A boundary test asserts `import beam_agents.effector` works with none of them installed, so the core install and the offline unit lane stay clean.
- **`Tool.unwrap()` widens the invariant-5 guard.** → It is one named, documented accessor, greppable in review, and the `tool-registry` spec delta states the exact sanctioned caller. The inverse guard in `EffectorToolRunner` (refusing `side_effect=False`) keeps the two paths disjoint rather than overlapping.

## Migration Plan

New service, no data migration, no schema change. Rollout: create the dedup store (Redis instance or Bigtable table with the `d` column family and a `maxage` GC rule), create the results topic and the ordered subscription, then start effector replicas with a consumer group id. Until they start, intents accumulate on the outbox with retention — nothing is lost, agents simply suspend and eventually hit their HITL deadlines. Rollback: stop the replicas; consumer-group offsets and the dedup records persist, so a restart resumes without re-executing completed intents.

## Open Questions

- **Result payload encoding.** `ToolResult.payload` is opaque bytes; this design encodes a tool's return value as canonical JSON for symmetry with `args_json`. A tool returning non-JSON-serializable data currently becomes `ERROR`. Whether to add a per-tool return codec is deferred until a real tool needs it.
- **Metrics surface.** The effector emits counters (claimed / deduped / expired / rejected / errored, per-tool latency) through a small injectable sink. Wiring them to OTel is deferred to the `observability/` change rather than pulling an exporter dependency into this one.
