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
This page covers the long-term tier, plus the [compaction](#compaction) that
keeps the working tier inside its caps and the [expiry hook](#on_expire-demoting-expiring-memory)
that bridges the two at TTL.

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

```sh
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

## Compaction

Working memory is capped at 1 MiB per key. Compaction is what keeps a
long-lived key under that cap, and it comes in two tiers, split by *where each
is allowed to run*.

| | Tier 1 — `AgentConfig.compactor` | Tier 2 — `AgentConfig.summarizer` |
|---|---|---|
| Strategy | `DropOldestCompactor` (the default) | `SummarizeCompactor` (opt-in) |
| Runs | synchronously, inside a memory write | inside the activation, after the agent returns |
| Trigger | the facade's soft cap (75%) and hard cap | staged `size_bytes >= trigger_bytes` |
| Model calls | never (it cannot `await`) | through `ctx.call_model` only |
| Effect | deletes LRU entries down to `target_bytes` | folds old ring items into a summary entry |

### Tier 1: `DropOldestCompactor` (default, behavior change)

`AgentConfig.compactor` defaults to `DropOldestCompactor()`. **This changes
default behavior:** a write whose prospective total crosses the 1 MiB hard cap
used to raise `MemoryOverflow`, failing the activation and dead-lettering the
element — permanently, on every subsequent over-cap write. It now succeeds after
evicting least-recently-used entries down to `target_bytes` (default 524 288,
half the cap, chosen for hysteresis below the 786 432-byte soft cap).

An agent may therefore observe a previously-written key as absent. To restore
the old contract:

```python
config = AgentConfig(provider_factory=my_provider, compactor=None)  # strict overflow
```

Keys matching `protected_prefixes` (default `("__langgraph__/",)`) are never
evicted: those hold a suspended LangGraph agent's resume state. If only
protected entries remain and the target is still exceeded, compaction stops and
the write raises `MemoryOverflow` as before — silently dropping resume state to
admit a write would corrupt the suspension.

Eviction reads only staged entries and the compactor's frozen configuration — no
clock, no randomness, no I/O — so a replayed activation evicts an identical set.

### Tier 2: `SummarizeCompactor` (opt-in)

Tier 1 discards; tier 2 preserves meaning by folding a ring's older items into
one summary entry. Because that needs the model, **where it runs is the whole
design**: it runs inside the activation, and its model calls go through
`ctx.call_model` and nothing else. That makes each call keyed by
`(content, key, seq)`, staged in the replay cache, committed atomically with the
bundle, and served from keyed state with zero provider calls on a bundle retry.
A summarizer that called a provider from a timer, a side transform, or a raw
client would break exactly that.

```python
def build_request(items: tuple[bytes, ...], prior_summary: bytes | None) -> LlmRequest:
    ...  # your prompt; MUST be a pure function of these two inputs

def extract_summary(response: bytes) -> bytes:
    ...  # your provider's response parsing

config = AgentConfig(
    provider_factory=my_provider,
    summarizer=SummarizeCompactor(
        build_request=build_request,
        extract_summary=extract_summary,
        source_keys=("log",),
        summary_key="summary",   # written as a scalar
        keep_recent=8,           # newest items survive verbatim
        trigger_bytes=786_432,   # the soft cap, by default
    ),
)
```

The runtime owns *when* to summarize, *what* to feed, *where* the call runs, and
*how* the result lands. It owns nothing about the prompt: beam-agents is a
runtime, not a framework, and a shipped summarization prompt would be prompt
templating.

Two contract points to hold up your end of:

- **`build_request` must be pure.** An impure builder hashes to a different
  cache key on replay, misses the cache, and re-calls the provider — which the
  retry-determinism gate detects rather than silently absorbing.
- **The summary must shrink.** An `extract_summary` result no smaller than the
  bytes it replaces raises `ValueError`, failing the activation closed instead of
  committing memory growth.

Anything the summarizer raises fails the activation atomically: nothing commits,
and no half-summarized blob is observable.

## `on_expire`: demoting expiring memory

`TTL_TIMER` reclaims an idle key's working memory. By default the memory is
simply gone. `AgentConfig.on_expire` gives it somewhere to go — the long-term
tier — and requires `longterm_memory` to be configured (setting one without the
other raises at `AgentConfig` construction):

```python
config = AgentConfig(
    provider_factory=my_provider,
    longterm_memory="bigtable://my-project/my-instance/agent-memory",
    on_expire=FlushToLongterm(),           # upserts under key "working_memory"
)
```

Before the wipe, the timer callback reads the key's committed `MemoryBlob` and
`seq` and performs **one** idempotent upsert keyed by `(entity_key, seq)` — the
same invariant-5 carve-out the rest of this page documents — on the DoFn's async
bridge, under a bounded timeout. The upsert's content is a pure function of
committed state and the timer's firing time, so a retried timer bundle produces
byte-identical bytes that the seq guard collapses onto one row. A key with empty
working memory is wiped with no store call.

**Fail-closed, and the trade-off that comes with it.** The wipe runs only after
the flush succeeds. A flush failure propagates out of the callback, failing the
timer bundle so the runner retries it against state that has deliberately not
been wiped. During a long store outage this leaves the key wedged — retrying
until the store recovers — which is the meaning of configuring `on_expire`: this
memory must not be lost. Flush failures are visible as failed timer bundles at
the runner level; they cannot be dead-lettered, because a raising callback's
bundle discards its outputs.

Unset (the default), expiry behaves exactly as it always has: no store
interaction, the same wipe, and the same `ttl_wiped_suspension` dead letter when
GC reaches a key that was still waiting on an answer.
