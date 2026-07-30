# Design: add-longterm-memory-stores

## Context

Working memory is the only memory tier: a per-key `MemoryBlob` in `ReadModifyWriteState`, staged by the `Memory` facade ([facade.py:105](../../../src/beam_agents/memory/facade.py:105)), capped at 1 MiB with LRU compaction and event-time GC. It is deliberately small and deliberately evictable — the opposite of what an entity profile or case history needs. The module map has promised the second tier since the constitution was written: `memory/` "stores (Bigtable/Redis/Firestore/SQL)" ([project.md:60](../../project.md:60)), with Bigtable/Redis/Firestore/SQLAlchemy named again under external dependencies ([project.md:120](../../project.md:120)).

The hard part is not the stores; it is that a long-term store is *external I/O touched from inside an activation*, which collides with three invariants at once:

- **Invariant 1 (atomic commit):** all effects stage in the activation context and apply only on success ([context.py](../../../src/beam_agents/core/context.py:370) stages everything; [`_commit`](../../../src/beam_agents/core/dofn.py:514) applies in fixed order). An external write cannot join the Beam bundle transaction.
- **Invariant 2 (deterministic replay):** a replayed bundle must walk the same path and mint byte-identical intents ([`intent_id_for`](../../../src/beam_agents/core/agent.py:43)). An external read whose answer changes between attempts can fork the path.
- **Invariant 5 (side effects only via intents):** external writes never execute inside the pipeline — *"exception: documented idempotent upserts to the long-term MemoryStore keyed by `(key, seq)`"* ([project.md:46](../../project.md:46)). The constitution pre-authorizes exactly one shape of in-pipeline write; this design has to earn it.

Prior art in-repo: the effector's dedup stores ([dedup.py](../../../src/beam_agents/effector/dedup.py:205)) already solved lazy optional-client imports, big-endian comparable-bytes encoding for store-side predicates, per-backend atomic conditional writes, and the `build_dedup_store(scheme, parts)` URI factory ([dedup.py:496](../../../src/beam_agents/effector/dedup.py:496)). This design reuses every one of those patterns rather than inventing parallel ones.

## Goals / Non-Goals

**Goals:**

- A `MemoryStore` ABC — async `load`/`save`/`search` — with four production backends (Bigtable, Redis, Firestore, SQLAlchemy) behind one optional extra, plus an in-memory store so the whole contract is testable offline.
- An explicit `ctx.memory.longterm` surface: nothing implicit, nothing hydrated automatically, unconfigured pipelines behave exactly as today.
- A write path whose duplicates converge: staged during the activation, flushed in the commit tail, idempotent upsert guarded by `(entity_key, key, seq)` so bundle retries and late replays cannot duplicate or regress rows.
- A read path with a stated, testable determinism discipline instead of a hand-wave.
- One record envelope (`LongTermRecord` proto) stored byte-identically by every backend, so backend choice is an ops decision, not a semantics decision.

**Non-Goals:**

- No vector/semantic search, no embeddings, no summarization policies — retrieval strategy is agent-framework territory (runtime-not-framework principle), and embedding calls inside an activation have no replay-determinism story.
- No automatic promotion/demotion between the working and long-term tiers; compaction strategies that spill to the store can ship later against the existing `Compactor` seam.
- No retention/GC of long-term rows in v0 (see Open Questions).
- No change to the working-tier contract: caps, ring semantics, LRU, and the `MemoryBlob` schema are untouched.
- No cross-entity queries via `ctx` — per-key serialization is the isolation model, and an agent reads only its own entity's rows.

## Decisions

### D1. `memory/stores/` is a subpackage around an ABC, not a Protocol

`beam_agents.memory.stores` holds `base.py` (the `MemoryStore` ABC, the `LongTermRecord` handling, the big-endian seq encoding, `build_memory_store`, and `InMemoryMemoryStore`) plus one module per backend. Unlike `DedupStore` — a pure structural seam where a `Protocol` is right — `MemoryStore` is an `abc.ABC` because the base class owns correctness-bearing shared behavior the backends must inherit, not re-implement: envelope encode/decode with deterministic serialization, the seq-guard comparison rule (one definition of "applies iff `seq >= stored_seq`"), and argument validation (non-empty `key`, non-negative `seq`, `limit > 0`). A backend only implements the storage primitives; the semantics live once. Client libraries import inside constructors (`PLC0415` per-file ignores, the established pattern from `effector/dedup.py`), so `import beam_agents.memory.stores` succeeds with no client installed.

*Alternative rejected:* one flat `stores.py` like `dedup.py`. Four backends plus search plus the envelope is roughly double `dedup.py`'s surface; per-backend modules also give the lazy-import lint ignores a natural file boundary.

### D2. One versioned envelope, stored byte-identically everywhere; `seq` duplicated natively for guards

All four backends store the same value: a `LongTermRecord` proto (`state_schema_version`, `key`, `value` bytes, `seq`, `updated_at_ms`) serialized with `SerializeToString(deterministic=True)`. This is what makes the wire-schemas conventions (C02) the right dependency: schema evolution of long-term rows follows the same additive-only, version-stamped rules as every blob in `protos/beam_agents.proto`, a golden-bytes compat test pins the encoding, and a pipeline can migrate between backends by copying values verbatim.

`seq` is *also* written backend-natively next to the envelope — a big-endian u64, reusing the order-preserving encoding argument proven for [`encode_lease_expiry`](../../../src/beam_agents/effector/dedup.py:264) — because the upsert guard must be evaluable by each backend's own atomic primitive (Bigtable value-range filters, Redis Lua byte compare, SQL integer columns), and none of them can compare a field *inside* a serialized proto.

*Alternative rejected:* raw `(value, seq)` columns with no envelope. Loses cross-backend byte-portability and re-opens the "state is protobuf, never pickle" question per backend; the envelope costs a few bytes and closes it once.

### D3. Writes are staged in the activation and flushed in the commit tail — the invariant-5 exception, earned by determinism plus the seq guard

`ctx.memory.longterm.save(key, value)` performs **no I/O during the agent's turn**. It stages an upsert record stamped with the activation's `seq` and `now_ms` — both already frozen per activation — alongside intents, traces, and blobs. After the agent coroutine returns successfully, the loop driver flushes the staged upserts through the store (still on the bridge loop, inside `activation_timeout`), and only then does the DoFn commit the bundle-atomic effects. A failed or timed-out activation flushes nothing, exactly like every other staged effect.

The flush is *outside* the Beam transaction, so the failure to reason about is: **flush succeeded, bundle commit failed, bundle retries.** The retry re-runs the activation deterministically (same event, same committed state, same frozen `seq`/`now_ms`, replay-cached model path), re-stages *byte-identical* upserts, and re-flushes them. The store-side guard — apply iff incoming `seq >= stored_seq` for `(entity_key, key)` — makes that re-flush an identical overwrite: the store converges on the same row regardless of how many times the bundle retries. The `>=` (not `>`) is deliberate: an equal-seq flush must be *accepted*, because the retry legitimately re-writes its own row; it is harmless because determinism guarantees the bytes are identical, and the retry-determinism chaos gate is extended to assert exactly that. The strict inequality does the other half of the job: a delayed or duplicated flush of seq `N` arriving after seq `N+1` has written cannot regress the newer row.

Flush failure fails the activation (fail closed, routed to `.errors`, nothing committed) — safe for the same reason the retry is: the next attempt re-stages identical upserts and partially-applied flushes are absorbed by the guard.

*Alternative rejected:* **inline `await store.save(...)` at the call site.** A write that executes mid-activation lands even when the activation later fails, breaking the "failed activation mutates nothing" reading of invariant 1 for no benefit — the agent cannot observe its own store write any sooner than the overlay (D5) already shows it.

*Alternative rejected:* **outbox-in-state deferral** — commit staged upserts into a new keyed-state blob atomically with the bundle, flush them at the start of the *next* activation (or a new real-time timer), clearing on success. This fully closes even the read-back window in D4, because failed attempts never flush at all. Rejected for v0 on cost: a new state spec + proto + timer + migration surface, pending upserts competing with the 100 KiB blob cap, and write visibility deferred indefinitely for quiet keys unless a timer is added. The commit-tail flush achieves convergence with none of that mechanism; the deferral remains the documented escalation path if the D4 residual ever matters in practice.

### D4. Reads are inline and point-in-time; determinism is preserved by a stated write discipline, not by pretending the store is a snapshot

`load`/`search` execute inline on the bridge loop (they are reads, not side effects — invariant 5 does not apply; failures raise through the agent and fail the activation closed). The determinism question is: can a replayed activation read different bytes than its first attempt? Per-key serialization (invariant 4) means the only in-pipeline writer of an entity's rows is that entity's own key, and D3 means failed attempts flush nothing. That leaves exactly one residual window: a bundle that **flushed and then failed to commit** — its retry can read the first attempt's own flushed rows.

Closing that window mechanically requires the rejected outbox-in-state design, so instead the spec states the discipline that makes the window harmless, as a requirement with a chaos-gate scenario rather than as advice:

1. **Blind upserts:** a long-term write MUST be computed from replay-stable inputs — the event, working memory, replay-cached model output — and MUST NOT be conditioned on a same-activation long-term read of the same key (no check-then-set across the long-term boundary). Under this rule the retry stages the same upsert whether or not it observes the earlier flush, so the path and the intents stay byte-identical.
2. **Reads for path-decisions belong upstream:** data that determines *which* path an activation takes should be joined in the enrichment stage of the dataflow (the shape in project.md already reserves the slot), where it is part of the replayed element and therefore replay-stable by construction.
3. **Read-your-writes overlay (D5)** keeps intra-activation reads consistent regardless of flush timing.

This mirrors how the effector handles its own residual window: state it precisely, bound it, and gate the invariant that keeps it harmless — rather than claim it away.

### D5. `ctx.memory.longterm` is an activation-scoped handle with a read-your-writes overlay; the facade itself stays I/O-free

`Memory` gains a `longterm` property returning a `LongtermMemory` handle, or raising an actionable error naming `AgentConfig.longterm_memory` when no store is configured. The handle — constructed per activation with the frozen `entity_key`/`seq`/`now_ms` and the worker-shared store — is the only object that touches the network: `Memory`'s own methods remain pure in-memory staging, so every existing facade requirement holds verbatim. The handle's `save` appends to the activation's staged-upsert list; its `load`/`search` consult that list first and merge it over store results, so an agent always observes its own writes in program order even though nothing has flushed yet. Both context surfaces (`ActivationContext` and `AgentContext`) construct `Memory` with the handle and expose the staged upserts to their owners (`ActivationResult` / `AgentResult`).

*Alternative rejected:* `ctx.longterm` as a sibling of `ctx.memory`. The roadmap and module map treat long-term storage as a memory tier; hanging it off `Memory` keeps one discoverable memory surface and lets a future spill-compactor reach both tiers through the object it is already handed.

### D6. Store lifecycle: one client per DoFn instance, built by a URI factory; validation is import-free

`AgentConfig` gains `longterm_memory: str | None = None`. `__post_init__` validates the URI grammar without importing any client (the [`DefaultSinkResolver.validate`](../../../src/beam_agents/core/transform.py:284) pattern): recognized schemes are `memory://`, `redis://`, `bigtable://<project>/<instance>/<table>`, `firestore://<project>/<collection>`; any other scheme is accepted as a SQLAlchemy async URL (e.g. `postgresql+asyncpg://…`, `sqlite+aiosqlite://…`) and fully parsed only at construction, where SQLAlchemy itself is the authority on its own URL grammar. `build_memory_store(scheme, parts)` mirrors [`build_dedup_store`](../../../src/beam_agents/effector/dedup.py:496). The DoFn constructs the store once per instance in `setup()` on the bridge loop and closes it in `teardown()` — a worker-local client pool shared across keys, which is the sanctioned kind of sharing (like the httpx pools), not cross-key mutable state.

### D7. `search` is a bounded, per-entity key-prefix scan

`search(entity_key, prefix, limit)` returns at most `limit` records whose `key` starts with `prefix`, ordered by `key` ascending, scoped to one entity. Every backend expresses it natively: Bigtable as a row-range scan over `hex(entity_key) + "#" + prefix`; Redis as `HSCAN` with a match pattern over the per-entity hash (bounded client-side sort — acceptable because the namespace is one entity's rows, not the keyspace); Firestore as an ordered range query (`key >= prefix` and `key < prefix + "\uffff"`); SQL as an escaped `LIKE prefix%` with `ORDER BY key LIMIT n` (`%`/`_` in the prefix are escaped, so a prefix is always a literal). `limit` is required to be positive — an unbounded scan inside an activation is a latency and determinism hazard, so the API refuses to express one.

### D8. Backend write-guard mappings

Each backend enforces "apply iff `seq >= stored_seq`" with its own atomic primitive; none of them needs a distributed lock because per-key serialization already guarantees at most one in-pipeline writer per entity:

- **Bigtable:** row key `hex(entity_key) + "#" + key`, column family `m`, columns `seq` (big-endian u64) and `rec` (envelope). `save` is one `CheckAndMutateRow`: predicate = latest `seq` cell (`CellsColumnLimitFilter(1)`, the lesson already encoded in the dedup store) strictly greater than the incoming seq; true branch → no-op, false branch writes both cells. The big-endian encoding is what makes "greater" expressible as a `ValueRangeFilter`.
- **Redis:** per-entity hash `beam-agents:ltm:<hex(entity_key)>`, field = `key`, value = 8-byte big-endian seq ‖ envelope. `save` is a server-side Lua compare-and-set (read field, compare the 8-byte prefix, `HSET` iff `incoming >= stored`) — the same conditional-write-needs-a-script reasoning as the dedup store's `complete`.
- **Firestore:** document `<hex(entity_key)>#<key>` with `seq` and `rec` fields; `save` runs in a transaction (read, compare, write). Firestore has no CAS primitive, so the transaction *is* the atomic guard.
- **SQLAlchemy:** table `beam_agents_longterm(entity_key BYTEA, key TEXT, seq BIGINT, rec BYTEA, updated_at_ms BIGINT, PRIMARY KEY (entity_key, key))`; `save` is a transactional read-compare-write (`SELECT … FOR UPDATE` where the dialect supports it). Deliberately portable rather than `ON CONFLICT DO UPDATE … WHERE`, which is dialect-specific; a per-dialect fast path can land later without changing the contract. Async engine (`create_async_engine`) throughout — the store runs on the bridge loop and must never block it (ruff ASYNC rules apply).

### D9. Testing shape

The contract tests are written once against the ABC and run as a shared conformance suite over every store — the same pattern the dedup stores use. Offline (`make test-unit`): the in-memory store and `sqlite+aiosqlite` run the full suite, docker-free; an import-boundary test imports every store module with all four client roots blocked; a golden-bytes test pins the envelope encoding; property tests pin the big-endian order-preservation and the seq-guard algebra (apply/no-op matrix over seq pairs). `-m integration`: the suite runs against testcontainers Redis and the compose Bigtable/Firestore emulators (`google/cloud-sdk` image, already used for Pub/Sub and Bigtable). The activation-semantics scenarios (staging, commit-tail flush, flush-failure fail-closed, overlay) run offline against the in-memory store with a scripted flush-failure fake; the retry-determinism chaos gate gains a scenario forcing a bundle retry across a completed flush and asserting byte-identical intents and identical store rows.

## Risks / Trade-offs

- **[Residual read-back window]** A bundle that flushed but failed to commit lets its retry observe its own rows (D4). → Bounded to same-key read-after-write within one activation; made harmless by the blind-upsert requirement; gated by the chaos scenario rather than left as prose. Escalation path (outbox-in-state deferral) documented in D3.
- **[Latency inside the activation]** Store round-trips count against `activation_timeout` and sit near the p50 < 15 ms overhead budget. → The budget excludes agent-elective external time (like LLM/tool calls); the runtime's own addition is one batched flush per activation, only when upserts were staged. The flush is included in the benchmark suite so the claim is measured, not assumed.
- **[Seq guard assumes deterministic values per seq]** A nondeterministic agent could stage different bytes for the same seq across retries; `>=` would then let attempts disagree about the final row. → That agent already violates invariant 2 (its intents diverge too); the retry-determinism gate catches it, and the store cannot be asked to repair it.
- **[Cross-backend isolation differences]** Lua scripts, `CheckAndMutateRow`, Firestore transactions, and SQL transactions have different concurrency envelopes. → Per-key serialization means the guard only ever races replay duplicates of *itself*, not concurrent distinct writers; the shared conformance suite pins identical observable semantics anyway.
- **[Four more optional clients]** → One `memory-stores` extra, lazy constructor imports, an import-boundary test, and the `integration` group mirror — all precedented by the effector extra; the offline lane never installs them.
- **[SQL `LIKE` metacharacters]** A prefix containing `%`/`_` would silently widen the scan. → Escaped in the store with a scenario pinning it.
- **[Unbounded store growth]** No TTL/GC in v0. → Documented operator concern (backends all have native TTL/retention mechanisms); see Open Questions.

## Migration Plan

Everything is additive. The proto gains one new top-level message (no existing message or field touched, regen diff-clean); keyed-state layout is unchanged, so pipeline `--update` compatibility is unaffected. `AgentConfig.longterm_memory` defaults to `None`: unconfigured pipelines construct no store, `ctx.memory.longterm` raises with an actionable message, and every existing test passes untouched. Enabling the tier is a config change plus backend provisioning (Bigtable table with family `m`; nothing to pre-create for Redis/Firestore; one `CREATE TABLE` for SQL — DDL shipped as a documented statement, not runtime auto-migration). Rollback is unsetting the URI; rows already written are inert. Backend migration is a verbatim copy of `(entity_key, key, seq, rec)` rows, guaranteed by the byte-identical envelope (D2).

## Open Questions

- **Retention/GC of long-term rows.** Backends have native mechanisms (Bigtable `maxage`, Redis `EXPIRE`, Firestore TTL policies, SQL jobs) with different semantics; whether the runtime should own a portable retention policy or leave it to operators is deferred until a deployment needs one.
- **Spill-to-store compaction.** A `Compactor` that demotes evicted working-memory entries into the long-term store is the obvious next consumer of this tier; it needs its own change (it interacts with the soft-cap path and the blind-upsert rule).
- **Bulk flush APIs.** Each backend has a batch write primitive (`mutate_rows`, pipelined `HSET`, Firestore batched writes, `executemany`); v0 flushes upserts sequentially per activation, which is expected to be 1–3 rows. Revisit if the benchmark shows flush cost scaling with row count.
