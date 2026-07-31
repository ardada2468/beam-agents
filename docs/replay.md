# Exporting state and replaying an activation

When an operator asks *"why did the activation for key `K` at seq 7 emit that
intent"*, the answer is to run it again — locally, offline, against the exact
model responses it consumed. Two pieces make that possible:

- an **in-band state export**: publish an `export_request` envelope to the
  events topic and the pipeline answers with a `StateSnapshot` of that key on
  the new `.snapshots` output;
- the **`beam-agents-replay` CLI**: it reconstructs the activation from that
  snapshot plus its traces and re-runs it against a provider that can serve
  nothing, then diffs the re-run against the traced record.

Nothing in the replay path opens a network connection, and nothing writes back:
replay reads.

## 1. Exporting a key's state

Point `snapshots_to` at a topic, then publish an `export_request` keyed by the
entity you want:

```python
config = AgentConfig(
    provider_factory=make_client,
    traces_to="kafka://broker:9092/traces",
    snapshots_to="kafka://broker:9092/snapshots",  # or pubsub://
)
```

```python
from beam_agents._protos import AgentEnvelope

request = AgentEnvelope(
    entity_key=b"customer-42",
    event_time_ms=now_ms,          # a replay-deterministic time, not a clock read
    export_request=AgentEnvelope.StateExportRequest(request_id="incident-1734"),
)
# ...publish to the same events topic RunAgent reads.
```

Because Beam processes one element at a time per key, the request observes state
at a well-defined point in that key's serial history: **after** every activation
that committed before it on the stream, **before** every one that follows. The
handler is strictly read-only — no activation runs, no state cell is written, no
timer is set, and `SEQ` is not incremented. The one element it emits is a
`StateSnapshot` on `.snapshots`, carrying:

| Field | What it holds |
|---|---|
| `state_schema_version` | the exporting binary's schema version |
| `entity_key`, `seq` | the key, and its `SEQ` counter at export |
| `snapshot_at_ms` | the request envelope's `event_time_ms` |
| `memory`, `llm_cache` | the committed working-memory and replay-cache blobs |
| `continuation` | present **only** while the key is suspended |
| `pending` | the pending `ToolIntent`s |
| `request_id` | echoed from your request |

Sink encodings mirror `traces_to`: a `kafka://`/`pubsub://` sink receives
`(entity_key, deterministic proto bytes)`. `bigquery://` is refused at
`AgentConfig` construction — a snapshot is an opaque per-key state image with no
row layout — and `otlp://` is a traces-only exporter. With no `snapshots_to`
configured, `.snapshots` stays exposed on `RunAgentOutputs` and unconsumed.

Two limits worth stating plainly:

- **Export works only against a running pipeline.** A drained or crashed
  pipeline's state is inside the runner and unreachable until it resumes.
- **An export request is a state-disclosure primitive on the events topic.**
  Anyone who can produce to that topic can ask for a key's memory, exactly as
  they can already forge a `ToolResult`. The events topic is the trust boundary;
  egress is controlled by controlling `snapshots_to`, which has no default.

## 2. Collecting the three inputs

`beam-agents-replay` reads local files. Getting bytes out of a topic or a table
is your existing operator tooling — the CLI deliberately ships no fetcher.

| Flag | What to put in the file |
|---|---|
| `--snapshot` | one serialized `StateSnapshot` (the sink's message value) |
| `--traces` | a varint-length-delimited stream of `TraceEvent` payloads |
| `--event` | the serialized `AgentEnvelope` that triggered the activation |

The trace stream is the binary interchange: each frame is a varint length
followed by the bytes `serialize_trace_event` produces. If you dump traces from
BigQuery you have rows, not protos; converting rows back to `TraceEvent`s is a
few lines against the published bindings.

The envelope comes off the durable bus. Traces carry positions and identities,
never payloads, and the DoFn retains no consumed envelope, so the events (or
results, or approvals) topic is the only place the triggering bytes exist.

## 3. Replaying

```console
$ beam-agents-replay \
    --snapshot snapshot.pb --traces traces.pb --event event.pb \
    --agent myapp.agent:AGENT --registry myapp.agent:TOOLS
beam-agents-replay 0.1.0
  agent:  myapp.agent:AGENT
  key:    637573746f6d65722d3432  seq: 7  now_ms: 1734000000000
  kind:   start
reproduced
  provider calls: 0
  outputs[0]: 24 bytes, sha256=...
  memory blob: 118 bytes, sha256=... (no traced counterpart — reported, not diffed)
```

What is reconstructed from where: state blobs (and, for a resume, `step_index`
and the adapter snapshot) come from the snapshot; the target `(entity_key, seq)`
and the activation clock come from the trace — `now_ms` is the traced
`ACTIVATION_START.start_ms`, which *is* the clock the activation ran on, so the
replayed events' timestamps land byte-identical. `--seq` picks an older
activation than the highest traced one.

Model calls are served by the ordinary cache-first path from the snapshot's
`llm_cache`. The injected provider holds no transport and its `complete` raises
unconditionally: reaching it *is* a cache miss. Read-only tools re-execute for
real; side effects cannot happen, because `ctx.act` only stages intents and a
`side_effect=True` tool is refused before it runs — so a replay can be run any
number of times without performing an effect.

### Exit codes

| Code | Meaning |
|---|---|
| `0` | reproduced — the re-run matched the traced record |
| `1` | diverged — a structured diff naming the first diverging event is printed |
| `2` | usage/configuration error, or a snapshot newer than the installed package |
| `3` | irreproducible — a cache miss, a digest-only entry, or a migration gap |

Two attributes are normalized before the trace comparison, and the list is
closed: a replayed `LLM_CALL` reports `beam_agents.cache_hit = true` and
`beam_agents.billed = false` where the original reached the provider. Everything
else is compared as-is. Outputs and the post-activation memory blob have no
traced counterpart, so they are reported as digests rather than diffed against a
baseline the CLI does not have.

## 4. What replays exactly — and what does not

A post-hoc snapshot observes the key's stream *after* the activation you want to
replay, so the pre-image and the committed responses pull in opposite
directions:

| Target attempt | Memory in the snapshot | Its cached responses | Replays? |
|---|---|---|---|
| **Completed / suspended** | post-image (differs from the pre-image only by its own writes) | present, until later activations evict them | exactly, as long as its requests and intent walk did not read memory it itself overwrote |
| **Failed** | **exact** — invariant 1 committed nothing | absent — its staged inserts were discarded | up to its first uncached provider call; a failure in agent logic before any call reproduces fully |
| **Pending resume** (not yet run) | exact — it is what the resume will load | not yet minted | it is a what-if run, with no traced outcome to diff |

The property that makes this acceptable is not the coverage, it is the failure
mode: `compute_cache_key` covers the complete request material, so any input
drift that could change the model path is **detected** as a loud miss (exit `3`,
naming the key), and drift on the non-LLM path surfaces in the diff (exit `1`).
The CLI can fail to reproduce an activation; it cannot fabricate a
plausible-but-wrong reproduction.

Two practical corollaries:

- **Export promptly.** The replay cache is LRU-64 with a 6-hour TTL *per key*;
  an activation older than that has had its entries purged from live state.
  The snapshot itself never expires — TTL is evaluated against the injected
  clock, and replay takes that from the trace — so freeze the blob early and
  replay from it later, not from the live key later.
- **Replay runs *your* code.** Skew between the binary that ran in the pipeline
  and the one imported locally is a real divergence source, which is why the CLI
  prints the package version and the agent import path in its header. A
  non-default `HitlPolicy` (intent TTL, approval channel, HITL timeout) also
  belongs to the pipeline's configuration, and a difference there shows up as a
  divergent `expires_at_ms`.

## 5. Schema versions

Every blob in a loaded snapshot is version-checked before use and migrated
in memory through the *same* per-blob migrations the DoFn applies lazily (see
[state-migration.md](state-migration.md)) — one implementation, two call sites,
so replay can never disagree with the pipeline about what an old blob means.
Nothing is written back.

A snapshot (or an embedded blob) stamped **newer** than the installed package is
refused, naming both versions and the remedy, and exits `2`. Guessing forward is
how silent corruption happens.
