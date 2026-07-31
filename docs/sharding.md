# Hot keys and shard fan-out

`RunAgent` is keyed by construction: it consumes a pre-keyed
`PCollection[KV[bytes, AgentEnvelope]]`, and all five state cells — `MEMORY`,
`CONTINUATION`, `LLM_CACHE`, `PENDING`, `SEQ` — plus both timers live under that
one key. Beam stateful DoFns process **one element at a time per key**, which is
exactly what makes working memory race-free without a lock.

The flip side is a hard ceiling. One hot `entity_key` runs one activation at a
time no matter how many workers the runner gives you. Cross-key parallelism
scales; within-key parallelism does not exist, on purpose.

For agents that carry **no cross-event state**, that serial lane is pure waste —
nothing about the computation needs one lane per logical entity. The escape
hatch is key sharding: fan one logical key across `N` physical keys
`key#0 … key#N-1`, and let the runner schedule them independently.

```python
from beam_agents import ShardKeys, shard_key, unshard_key
```

!!! warning "Memory-free agents only"

    Sharding is safe when, and only when, the agent keeps no per-key memory,
    needs no per-key ordering, and takes no HITL approval keyed by the logical
    entity. The runtime performs no detection — a shard key is an ordinary
    `bytes` key to every coder and state spec. Read
    [when not to shard](#when-not-to-shard) before you reach for this.

## The convention

`shard_key(key, n, *, payload)` returns `key + b"#" + <decimal index>`, where the
index is `int.from_bytes(sha256(payload).digest()[:8]) % n`:

```pycon
>>> shard_key(b"hot-entity", 4, payload=b"evt-a")
b'hot-entity#1'
>>> unshard_key(b"hot-entity#1")
b'hot-entity'
```

Three properties are load-bearing:

- **Deterministic, from the payload alone.** Shard assignment is a *correctness*
  input, not a load-balancing detail. `intent_id = uuid5(NAMESPACE, key + seq +
  step_index)` and the replay-cache key both contain the physical key
  ([correctness invariants 2 and 3](https://github.com/ardada2468/beam-agents/blob/main/openspec/project.md)).
  An assignment that differed between a bundle's first attempt and its retry
  would re-mint intent IDs — the effector's dedup would no longer suppress the
  duplicate side effect — and miss the replay cache, paying for provider calls a
  retry is supposed to get for free. SHA-256 and not Python's `hash()`, which is
  salted per process by `PYTHONHASHSEED` and would assign the same element
  differently on different workers.
- **`n = 1` still suffixes `#0`.** The shape of a sharded key never depends on
  the shard count, so `unshard_key` works uniformly and changing `N` does not
  change the *shape* of anything downstream.
- **The inverse fails loudly.** `unshard_key` strips exactly one trailing
  `#<digits>` group and raises `ValueError` on a key that has none, rather than
  passing it through and silently merging a regroup under the wrong key. One
  ambiguity survives and cannot be removed while the `key#shard_n` shape is the
  convention: a logical key that *itself* ends in `#<digits>` is
  indistinguishable from a sharded key. Don't shard those keys.

## Where `ShardKeys` goes in the dataflow shape

On the **events branch only** — after `WithKeys`, before the `Flatten` with tool
results and approvals:

```text
Kafka/PubSub events ──► WithKeys(entity_id) ──► ShardKeys(N) ──┐
tool-results topic ────────────────────────────────────────────┼─► Flatten ─► RunAgent
approvals topic ───────────────────────────────────────────────┘
```

Results and approvals must **not** pass through `ShardKeys`. They already carry
the physical shard key: the runtime stamps `ToolIntent.entity_key` with it, the
effector echoes it onto the `ToolResult`, and resume admission looks the
continuation up under the key the element arrives on. Re-sharding them would
either double-suffix the key or (hash mode, different payload) route the result
to the wrong shard, where it finds no continuation and dead-letters as
`orphaned_result`.

`ShardKeys` rewrites the KV key *and* the envelope's own `entity_key` field to
the same value, so the state layout and the envelope can never disagree.

## The throughput math

The quantities below are the runtime's published `beam_agents.runtime` metrics
([metrics](metrics.md)), so every formula can be evaluated against a live
pipeline's own dashboard rather than a model of it.

### Per-key ceiling

Per-key serialization means a key's sustainable input rate is bounded by how
long its activations occupy the lane:

```text
λ_key ≤ 1000 / E[activation_ms]     activations per second, per key
```

`activation_ms` is wall time for one element — **including** model and tool time,
because that is what occupies the key's serial lane. `overhead_ms` is the same
activation minus its model and tool time: the runtime's own cost, and the part
`project.md`'s p50 < 15 ms / p99 < 60 ms budget gates. The gap between them is
the provider, and on any LLM-bearing agent the gap is almost the whole number.
`activations` counts the committed activations the average is over.

That is the entire hot-key problem in one line: at a 2-second `activation_ms`,
one key sustains 0.5 activations/sec, and adding workers changes nothing.

### Suspension dwell is latency, not occupancy

A suspending activation (`Suspend` → `CONTINUATION` persisted, HITL timer armed)
**commits and releases the key**. The key is idle for the whole dwell — waiting
on an effector, a tool, or a human — so dwell time does not consume the key's
serial budget. What it costs is end-to-end latency per logical event.

What *does* consume the budget is that a suspending flow spends **two**
activations per logical event, the start and the resume:

```text
λ_key ≤ 1000 / (E[activation_ms_start] + E[activation_ms_resume])
```

So a five-minute approval dwell does not halve a key's throughput; the second
activation does. Watch `suspensions` against `activations` to see what fraction
of the traffic pays the two-activation price.

### Fan-out

`N` shards give the logical entity `N` independent lanes:

```text
λ_logical ≤ N × λ_key
```

bounded above by three things the formula does not know about:

1. **runner parallelism** — shards beyond the available keyed-work slots queue
   rather than run; `N` above the runner's effective parallelism buys nothing;
2. **source partition count** — a topic with 4 partitions feeding 32 shards is
   still gated by what the source can deliver in parallel;
3. **hash uniformity** — see the [skew warning](#hash-skew-verify-the-fan-out)
   below. The effective `N` is the number of shards that actually receive
   traffic, not the number you configured.

### Worked examples

The measured inputs come from the benchmark suite ([benchmarks](benchmarks.md)),
which runs offline on `FakeLLM` — `make bench && make bench-gate` writes
`bench-report.md`. Use *your* report's numbers; the point of the arithmetic is
the shape, not any particular machine's figures.

| Input | Where it comes from |
|---|---|
| Runtime ceiling, zero agent work | `noop_throughput` — the report derives activations/sec from its median |
| Runtime overhead per activation | `overhead_50ms` (the gated tier), with `overhead_500ms` / `overhead_2000ms` proving the overhead does not scale with the provider wait |
| Suspension round-trip cost | `suspension_roundtrip` — one `Suspend`-committing activation plus one admitted `ToolResult` resume, summed |
| Cost of carrying memory | `state_commit_{1,16,64,100}kib` — activation cost against committed `MemoryBlob` size (a curve you only pay on agents that cannot be sharded anyway) |

**Example 1 — a fast-path agent on the harness's 50 ms tier.** The harness's
gated tier configures a 50 ms `FakeLLM` latency, and its whole claim is that
runtime overhead stays in the low single-digit milliseconds and does *not* grow
with the provider wait (that is what the 500 ms and 2000 ms tiers prove). So
`E[activation_ms] ≈ 50 + overhead_50ms`, and one key sustains roughly
`1000 / 53 ≈ 19` activations/sec. A logical entity arriving at 60 events/sec
needs `N ≥ 60 / 19 ≈ 4` shards.

**Example 2 — a real provider.** Substitute the tier you actually live on. At
the harness's 2000 ms tier, `λ_key ≈ 1000 / 2003 ≈ 0.5` activations/sec: the
same 60 events/sec entity would need `N ≥ 120`, which is where you stop and ask
whether one entity should really be absorbing that traffic — long before it is
where you set `N`.

**Example 3 — a suspending flow.** Two activations per logical event, so use
their sum. `suspension_roundtrip` measures exactly that pair, both element hops
over shared handles, deliberately excluding the effector and the bus (deployment
latency, not runtime cost). The dwell between them stretches completion time and
leaves the ceiling alone.

**Example 4 — the runtime floor.** `noop_throughput` is the ceiling with zero
agent work: sub-millisecond per activation, so thousands of activations/sec per
key. Any real number you compute above is dominated by the provider, not by the
runtime — which is why sharding buys throughput and micro-optimizing the runtime
does not.

## The fan-out example

The pipeline below is executed verbatim by
`tests/examples/test_shard_fanout.py`. Changing one without the other is a
defect: the doc is the contract that test holds the runtime to.

```python
# One logical entity, hot enough that per-key serialization is the bottleneck.
HOT_KEY = b"hot-entity"
VERDICT = b"scored"


def make_provider() -> FakeLLM:
    return FakeLLM([(match_any(), respond_with(VERDICT))])


async def stateless_scorer(ctx: ActivationContext) -> Complete:
    """Memory-free by construction: no `ctx.memory` read or write, no ordering
    assumption, no `ctx.act(...)` against a logically-keyed approval channel.
    Those three absences are the whole precondition for sharding this agent.
    """
    response = await ctx.call_model(
        LlmRequest(
            model_id="fake-model",
            messages=[ctx.event.decode()],
            tools_schema=None,
            sampling_params=None,
        )
    )
    # Carry the physical key on the output so the regroup below has something
    # to unshard; a real pipeline would key its sink the same way.
    return Complete(output=ctx.entity_key + b"|" + ctx.event + b"|" + response.response)


def split_output(payload: bytes) -> tuple[bytes, bytes]:
    physical_key, _, rest = payload.partition(b"|")
    return physical_key, rest


def build(pipeline: beam.Pipeline, events: list[AgentEnvelope]) -> RunAgentOutputs:
    """Key by entity, fan the hot key across four shards, then run the agent.

    `ShardKeys` goes on the events branch only — after `WithKeys`, before any
    `Flatten` with tool-results or approvals, which already carry the physical
    key from `ToolIntent.entity_key`.
    """
    keyed = (
        pipeline
        | "Events" >> beam.Create(events)
        | "KeyByEntity"
        >> beam.WithKeys(lambda env: env.entity_key).with_output_types(tuple[bytes, AgentEnvelope])
        | "Shard" >> ShardKeys(4)
    )
    return keyed | RunAgent(stateless_scorer, config=AgentConfig(provider_factory=make_provider))


def regroup(outputs: beam.pvalue.PCollection) -> beam.pvalue.PCollection:
    """Reassemble the logical entity downstream: ordinary Beam, no runtime help."""
    return (
        outputs
        | "SplitKey" >> beam.Map(split_output)
        | "Unshard" >> beam.MapTuple(lambda key, rest: (unshard_key(key), rest))
    )
```

Eight events for `b"hot-entity"` land on shards 3, 1, 2, 1, 3, 1, 1, 3 — three of
the four. That is the skew warning below, visible in the doc's own example rather
than hidden behind a hand-picked payload set. Regrouping puts all eight outputs
back under `b"hot-entity"`: nothing is lost or duplicated by the fan-out.

Regrouping is ordinary Beam. The module gives you the key function; whatever
aggregation you want on top (`GroupByKey`, `CombinePerKey`, a windowed count) is
yours to write — there is no cross-shard aggregation transform, and there is not
going to be one.

## When not to shard

### Memory-carrying agents

Each physical shard has its own `MEMORY` cell. A sharded memory-carrying agent
does not "share memory more slowly" — it accumulates **`N` independent, divergent
working memories**, each seeing roughly 1/N of the entity's events, and each
agent activation recalling only its own shard's history. Nothing errors. The
outputs just get quietly worse.

`tests/keys/test_shard_keys_transform.py` pins this as behavior: the same four
events for one logical entity accumulate into one ring unsharded, and into two
disjoint rings behind `ShardKeys(n=2)`. The symptom is observable in existing
instruments — per-shard `memory_bytes`, and recall that contradicts itself
between activations — but nothing will tell you it is happening.

The same applies to the `LLM_CACHE`: a cache hit is scoped to `(key, seq)`, so
sharding also splits the replay cache `N` ways.

### Ordering-sensitive flows

Per-key serialization is the *only* ordering guarantee the runtime offers.
Sharding trades one serial lane for `N` concurrent ones, which is the entire
point — so any agent whose correctness depends on seeing an entity's events in
order (state machines, "last write wins" logic, deduplication against the
previous event) must not be sharded.

### HITL approval affinity

The intent → effector → result path survives sharding automatically: the
physical key rides on `ToolIntent.entity_key` and the result comes back on the
shard that emitted it.

What does not survive is an approvals stream keyed by the **logical** entity — a
hand-wired approvals topic, an ops console that knows entities and not shards, a
Slack callback that keys on the customer ID. The continuation lives under
`entity#3`; an approval arriving under `entity` finds no continuation there and
dead-letters as `orphaned_result` on `.errors`. The loss is visible
([errors](errors.md)) but it is still a loss, and the HITL timer then fires the
fail-closed fallback for a decision a human actually made.

Either don't shard HITL flows, or make every approval producer carry the physical
key end to end.

### Hash skew: verify the fan-out

Hash assignment's failure mode is low-entropy payloads. If many events are
byte-identical, they all hash to the same shard and the promised `×N` never
materializes — you get `N` keys and one busy lane.

Verify before you trust `N`: run the traffic through and count elements per
physical key (the example above makes the spread visible in eight events).
`activations` per shard on the dashboard shows the same thing in production.

If the payloads genuinely cannot spread, `assignment="round_robin"` exists — and
carries a real cost:

!!! danger "`round_robin` forfeits retry determinism"

    Round-robin assigns from a worker-local counter, so it is **not** a pure
    function of the element. A retried bundle's counter state differs, the
    element lands on a different shard, and both replay properties break: the
    activation mints different `intent_id`s (the effector's dedup no longer
    suppresses the duplicate side effect) and misses the `(key, seq)` replay
    cache (extra provider calls on every retry).

    Use it only for agents that emit no intents — or whose effects are
    idempotent independently of `intent_id` — and only where duplicate provider
    calls on a retry are an accepted cost. It is never the default.

## Adopting and rolling back

Sharding is purely additive: a shard key is an ordinary `bytes` key, so there is
no wire, state, coder, or `state_schema_version` implication, and existing
pipelines are untouched.

Turning sharding *on* for a live pipeline is a state migration for that
pipeline's keys — state under `entity` does not follow to `entity#i`. For a
memory-free agent there is no state to strand, which is precisely why the
contract is what it is. Rolling back is deleting the `ShardKeys` step;
suspensions already in flight under shard keys drain normally, because their
results carry the physical key.

Changing `N` has the same character: the assignment of every payload moves, so
do it on a memory-free agent (where nothing is stranded) and expect in-flight
suspensions under the old shard keys to drain on the old `N` first.
