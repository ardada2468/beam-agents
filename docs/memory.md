# Memory

beam-agents has two memory tiers. They are separate on purpose: nothing is
promoted, demoted, or hydrated between them automatically.

| | Working tier | Long-term tier |
|---|---|---|
| Storage | Beam keyed state (`MemoryBlob`) | External `MemoryStore` backend |
| Access | `ctx.memory.get/set/append/ring/delete` | `ctx.memory.longterm` (explicit) |
| Scope | One key, one pipeline | One entity, durable across pipelines |
| Size | 1 MiB hard cap, LRU-compacted | Operator-bounded |
| Lifetime | Reclaimed by `TTL_TIMER` | No runtime GC (see *Retention*) |
| Enabled | Always | Only when `AgentConfig.longterm_memory` is set |

The working tier is documented by the facade itself (`beam_agents.memory.facade`).
This page covers the long-term tier.

## Enabling the tier

```python
config = AgentConfig(
    provider_factory=my_provider,
    longterm_memory="bigtable://my-project/my-instance/agent-memory",
)
```

The URI grammar is validated at `AgentConfig` construction — before any
pipeline exists — without importing any client library:

| URI | Backend |
|---|---|
| `memory://` | in-process reference store (tests, single-worker dev) |
| `redis://<url>` | Redis, per-entity hash + Lua compare-and-set |
| `bigtable://<project>/<instance>/<table>` | Bigtable, `CheckAndMutateRow` guard |
| `firestore://<project>/<collection>` | Firestore, transactional guard |
| anything else | a SQLAlchemy async URL (`postgresql+asyncpg://…`, `sqlite+aiosqlite://…`) |

Backends ship in the optional `memory-stores` extra:

```
pip install 'beam-agents[memory-stores]'
```

Unset (the default) leaves the tier off entirely: no store is constructed, no
external I/O happens, and `ctx.memory.longterm` raises an error naming
`AgentConfig.longterm_memory`.

## The API

```python
async def my_agent(ctx):
    ltm = ctx.memory.longterm
    profile = await ltm.load("profile")                  # MemoryRecord | None
    recent = await ltm.search("case/", limit=20)         # ordered, bounded
    ltm.save("case/2024-11", summarize(ctx.event))       # staged, no I/O yet
    return Complete(output=b"ok")
```

`search` is a **key-prefix scan**, scoped to the activation's own entity,
ordered by key ascending, and always bounded by `limit`. It is deliberately not
vector or semantic search: retrieval strategy belongs to the agent framework,
and embedding calls inside an activation have no replay-determinism story.

## Why in-pipeline writes are allowed here

Correctness invariant 5 says external writes never execute inside the pipeline
— with exactly one documented exception: *idempotent upserts to the long-term
MemoryStore keyed by `(key, seq)`*. This tier is that exception, and it earns
it with two mechanisms:

1. **Staging + commit-tail flush.** `save` performs no I/O. It stages an upsert
   stamped with the activation's frozen `seq` and `now_ms`. The loop driver
   flushes staged upserts *after* the agent returns successfully and *before*
   the DoFn commits the bundle-atomic effects. A failed or timed-out activation
   flushes nothing; a flush failure fails the activation closed (routed to
   `.errors`, nothing committed).
2. **A seq-guarded upsert.** Every backend applies a write iff the incoming
   `seq` is `>=` the seq currently stored for `(entity_key, key)`, enforced by
   that backend's own atomic primitive (`CheckAndMutateRow`, a Lua script, a
   Firestore transaction, a SQL transaction). The `>=` is deliberate: a replayed
   activation legitimately rewrites its own byte-identical row. The strict half
   does the other job — a delayed duplicate flush of seq *N* can never regress a
   row already at *N+1*.

Together these make the failure that matters — *flush succeeded, bundle commit
failed, bundle retries* — converge: the retry re-runs deterministically,
re-stages byte-identical upserts, and re-flushes them onto an identical row.

## The blind-upsert discipline (normative)

A long-term write **MUST** be computed from replay-stable inputs — the event,
working memory, replay-cached model output — and **MUST NOT** be conditioned on
a same-activation long-term read of the same key.

Do this:

```python
# The value is a function of the event and working memory only. Whether the
# read observes a previous attempt's flush cannot change what is staged.
history = await ctx.memory.longterm.search("case/", limit=10)   # for the model's context
ctx.memory.longterm.save(f"case/{ctx.event_id}", summarize(ctx.event))
```

Not this:

```python
# check-then-set across the long-term boundary: the retry can observe its own
# discarded attempt's row and stage something different — the path forks.
existing = await ctx.memory.longterm.load("counter")
ctx.memory.longterm.save("counter", str(int(existing.value) + 1).encode())
```

Data that decides *which path* an activation takes belongs in the enrichment
stage of the dataflow, where it is part of the replayed element and therefore
replay-stable by construction.

### The residual read-back window

Reads are point-in-time, not snapshots. Per-key serialization means the only
in-pipeline writer of an entity's rows is that entity's own key, and failed
attempts flush nothing — which leaves exactly one window: a bundle that
*flushed and then failed to commit* lets its retry read its own flushed rows.
Under the blind-upsert rule that window is harmless: the retry stages the same
upsert whether or not it sees the earlier flush, so the path, the intents, and
the rows stay byte-identical. The retry-determinism gate
(`tests/semantics/test_longterm_retry_determinism.py`) forces exactly this
sequence and asserts it.

Within one activation, reads always see that activation's own staged saves
first (a read-your-writes overlay), so program order is never surprising even
though nothing has flushed yet.

## Provisioning

Nothing is auto-created at runtime.

- **Bigtable:** create the table with column family `m` (columns `seq`, `rec`).
- **Redis:** nothing to pre-create.
- **Firestore:** nothing to pre-create.
- **SQLAlchemy:** run the shipped DDL
  (`beam_agents.memory.stores.sql.DDL`):

  ```sql
  CREATE TABLE beam_agents_longterm (
    entity_key BYTEA NOT NULL,
    key TEXT NOT NULL,
    seq BIGINT NOT NULL,
    rec BYTEA NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (entity_key, key)
  );
  ```

Every backend stores the same value bytes — a deterministically serialized
`LongTermRecord` envelope — so migrating between backends is a verbatim copy of
`(entity_key, key, seq, rec)` rows.

## Retention

The runtime does **not** garbage-collect long-term rows; growth is an operator
concern in v0. Each backend has a native mechanism with different semantics:
Bigtable column-family `maxage`, Redis `EXPIRE`, Firestore TTL policies, a
scheduled SQL job. Whether the runtime should own a portable retention policy is
deferred until a deployment needs one.

## Metrics

`beam_agents.runtime.longterm_upserts` counts rows flushed, per committed
activation. A failed activation flushes nothing and a failed flush fails the
activation, so the counter only ever reflects durable writes on the committed
path.
