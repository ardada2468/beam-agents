## Context

`run_activation` ([loop.py:149](../../../src/beam_agents/core/loop.py:149)) was built pure on purpose: it takes every input as an argument — agent, `memory_blob`, `cache_blob`, `event`, resume payload, `now_ms`, an injected `LLMClient` — and returns a staged `ActivationResult` without touching Beam state or a wall clock. `ActivationContext.call_model` ([context.py:469](../../../src/beam_agents/core/context.py:469)) is cache-first over the `ReplayCache`, whose keys ([replay_cache.py:73](../../../src/beam_agents/model/replay_cache.py:73)) include `(entity_key, seq)`, so the responses an activation consumed are pinned in its committed `LlmCacheBlob`. Trace identity ([traces.py:83](../../../src/beam_agents/observability/traces.py:83)) is a pure function of `(entity_key, seq, role, index)`, and every `TraceEvent` timestamp is the injected activation clock — so a re-run with the same inputs stages byte-identical trace events.

What is missing is (a) any way to get one entity's keyed-state blobs out of a running pipeline, and (b) a harness that composes the pieces. The constraints:

- **Keyed state is runner-managed and has no portable external read path.** Beam Python exposes state only inside a stateful DoFn's `process()`/timer callbacks. Flink's State Processor API reads savepoints but is JVM-only; Dataflow snapshots have no public decode API; DirectRunner state is in-process and gone when the process exits.
- **Correctness invariant 1 (atomic commit) and 4 (per-key serialization)** — any export mechanism must not mutate state, and any point-in-time claim it makes must come from the per-key serial order, the only consistency the runtime has.
- **Invariant 3 (replay cache)** is the budget for offline re-runs: a re-walk of the same path makes zero provider calls. Replay must turn "zero additional calls" into "zero calls, ever, with no network reachable".
- **Traces carry positions and identities, never payloads** (`add-trace-events` D5, and the cost/privacy stance behind it): the triggering event bytes, memory contents, and outputs are not in `.traces` and must come from elsewhere.
- Snapshots outlive pipelines, so blobs in a snapshot may carry an older `state_schema_version` than the code replaying them (`add-state-schema-migration`, C32).

## Goals / Non-Goals

**Goals:**
- An operator can obtain a consistent, versioned `StateSnapshot` of one `entity_key`'s state from a running pipeline on any supported runner, without pausing it and without mutating state.
- `beam-agents-replay` reconstructs an activation from a snapshot plus its trace, re-runs it locally via `run_activation`, and asserts the re-run reproduces the traced outcome.
- The replay provider serves only from the snapshot's replay cache and fails loudly — naming the cache key — on any miss; no code path in the replay package can open a connection.
- Older-schema snapshots migrate on load; newer-schema snapshots are refused with an actionable error.
- Divergence between traced and replayed outcomes is rendered as a structured diff with a distinct exit code.

**Non-Goals:**
- **Reading runner savepoints/checkpoints.** Rejected per-runner in D1; revisiting requires a JVM sidecar or a Dataflow API that does not exist.
- **Retro-capturing the pre-activation memory image of an already-completed activation.** D2 states exactly which activations replay exactly and which cannot; closing the gap needs armed pre-activation capture, listed under Open Questions.
- **Replaying across multiple activations** (a seq range). One activation per invocation; a range is a shell loop once single-activation replay exists.
- **A trace/snapshot *fetch* tool** (Kafka consumer, BigQuery exporter). The CLI reads local files; getting bytes out of a topic or table is existing operator tooling.
- **Fixing nondeterministic read-only tools.** Replay re-executes them (D6) and reports divergence; making them deterministic is the agent author's problem.

## Decisions

### D1. State export is an in-band `export_request` envelope, not a savepoint reader or a debug mirror

An operator exports state by publishing an `AgentEnvelope` with the new `export_request` payload variant to the events topic, keyed by the target `entity_key`. The DoFn routes it like any element: because Beam processes one element at a time per key (invariant 4), the request observes state at a well-defined point in the key's serial history — after every commit that preceded it on the stream, before every one that follows. The handler is strictly read-only: it reads `MEMORY`, `CONTINUATION`, `LLM_CACHE`, `PENDING`, and `SEQ`, builds one `StateSnapshot`, emits it on the `.snapshots` tagged output, and writes nothing — no `SEQ` increment, no timer, no blob touch. `snapshots_to` on `AgentConfig` resolves a sink for the tag exactly as `traces_to` does (serialized keyed by `entity_key` for `kafka://`/`pubsub://`).

Rejected alternatives, with the runner facts:

- **Savepoint/checkpoint reader.** Flink can materialize keyed state via the State Processor API, but it is JVM-only — unreachable from a Python package — and encodes Beam's portable state layout as an implementation detail. Dataflow snapshots are opaque with no public read API. DirectRunner has no durable state at all. A reader would be one runner, one SDK-internal format, and zero portability; the in-band request works identically on all three supported runners because it uses only the model's own primitives.
- **Continuous debug sink** (mirror every commit's blobs to an external store). Doubles the write volume of every activation, exports memory contents by default rather than on explicit request (a data-egress hazard), and still needs a consistency story for multi-blob reads. Rejected on cost and on "export should be a deliberate operator act".

Honest limitations, stated rather than papered over: export works only against a **running** pipeline — a drained or crashed pipeline's state is inside the runner and unreachable until resumed (this is precisely the savepoint-reader gap; see Open Questions). The request requires produce access to the events topic. And the snapshot lands wherever `snapshots_to` points, so operators control egress by controlling that sink; with no sink configured the `.snapshots` output is unconsumed and Beam drops it.

### D2. What a post-hoc snapshot can reproduce — and the detected-never-fabricated property

Replaying activation A at `(key, seq)` needs A's *inputs* — the pre-image memory and cache blobs — plus the provider responses A consumed, which exist in keyed state only if A **committed** them. A post-hoc in-band snapshot observes the key's stream *after* A, so the two needs pull in opposite directions:

| Target | Memory input in snapshot | A's cached responses in snapshot | Traced outcome |
|---|---|---|---|
| **Completed / suspended attempt** | post-image — differs from the pre-image only by A's own staged writes | **present** (committed at A's seq, until later activations' LRU/TTL eviction) | present |
| **Failed attempt** | **exact** — invariant 1 committed nothing | absent — A's staged cache inserts were discarded with everything else | present (`ERROR`) |
| **Pending resume** (not yet run) | exact — it is what the resume will load | not yet minted | none — a what-if run, out of scope |

Consequences, stated honestly:

- A **committed** attempt replays exactly whenever its LLM requests and intent walk do not read memory entries the attempt itself overwrote — the common event-driven shape (read event → call model → write memory last). When they do read-then-overwrite, the recomputed cache key differs and the tripwire fires.
- A **failed** attempt replays with an exact state pre-image but without its own provider-reached responses, so the re-run reproduces the walk up to its first uncached provider call. A failure in agent logic before any provider call reproduces fully, as does a failed *resume* whose calls hit entries the suspended attempt committed at the same seq; a provider-error failure replays up to the exact failing call, where the tripwire reports the traced failure position.

What makes the partial coverage acceptable is the safety property, not the coverage: `compute_cache_key` covers the complete request material, so any input drift that could change the LLM path is *detected* as a loud miss (exit `3`, reported as **irreproducible: pre-activation state not captured or entry not committed**, naming the missing key), and drift on the non-LLM path surfaces in the outcome diff (exit `1`). The CLI can fail to reproduce; it cannot fabricate a plausible-but-wrong reproduction. Closing the gap — exact pre-images for every attempt — needs armed pre-activation capture (Open Questions).

### D3. The cache-only provider is a tripwire, because the context is already cache-first

`CacheOnlyLLMClient` implements the [`LLMClient`](../../../src/beam_agents/model/client.py:54) protocol with a `complete` that unconditionally raises `ReplayCacheMissError(cache_key_material)`. No lookup logic lives in it — [`call_model`](../../../src/beam_agents/core/context.py:469) already consults the `ReplayCache` built from the injected `cache_blob` before touching the provider, so under replay every cached request is served without the client ever being invoked. The client is reached only when the cache-first path falls through: a genuine miss, or a `digest_only` entry (response too large to store — `call_model` treats it as a miss by design). Both fail the replay loudly.

Why this shape and not a client that reads the cache itself: two lookups would be two implementations of the serving path, free to drift; the tripwire keeps the production code path — context, `ReplayCache`, `compute_cache_key` — as the one and only replay path, which is exactly what "the re-run exercises the real code" requires. And the never-hits-the-network property is structural: the class holds no transport, imports no HTTP client, and takes no endpoint; there is nothing to misconfigure into making a call.

`digest_only` entries get a dedicated error message carrying the stored `response_digest`, so an operator can at least verify a provider re-fetch against the digest by hand; the CLI itself never re-fetches.

### D4. Input reconstruction: snapshot for state, trace for scope and clock, the durable bus for payloads

`run_activation`'s arguments are rebuilt from three sources, each the system of record for what it holds:

- **From the `StateSnapshot`:** `memory_blob`, `cache_blob` (both after D5 migration), and — for a resume replay — `step_index`, the adapter `snapshot` bytes, and the pending-intent set from the embedded `Continuation`.
- **From the trace stream:** the target `(entity_key, seq)` scope (defaulting to the highest seq present, `--seq` to override), and `now_ms` — recovered from the traced `ACTIVATION_START.start_ms`, which *is* the activation's injected clock (`add-trace-events` D7: every event timestamp is `now_ms`). No flag needed, no wall clock read, and the replayed events' timestamps land byte-identical.
- **From the operator, off the durable events bus:** the triggering payload, `--event <file>` holding a serialized `AgentEnvelope` (or raw bytes for a bare external event). Traces deliberately carry no payloads, and the DoFn retains no consumed envelope, but the bus does — events, results, and approvals all live on retained topics, which is where a replayed activation's input is fetched from. The CLI cross-checks the envelope's `entity_key` against the snapshot and refuses a mismatch.
- **From import paths:** `--agent module:attribute` for the agent callable and optional `--registry`/`--decode` for the tool registry and provider decoder, reusing the `load_registry` pattern from [effector/`__main__.py:44`](../../../src/beam_agents/effector/__main__.py:44). Replay runs *the operator's* agent code; version skew between the code that ran in the pipeline and the code imported locally is a real divergence source and is called out in the diff header (the CLI prints both the package version and the agent import path it ran).

### D5. Snapshots migrate on load, and version skew fails closed

Every blob in a loaded snapshot is version-checked before use. A blob whose `state_schema_version` is older than current runs through the same per-blob migration functions the `state-schema-migration` capability (C32) gives the DoFn for lazy in-pipeline migration — one implementation, two call sites, so replay can never disagree with the pipeline about what an old blob means. A blob (or the snapshot envelope itself) with a version *newer* than the installed package refuses to load with an error naming both versions and the fix ("upgrade beam-agents to replay this snapshot") — guessing forward is how silent corruption happens. Migration is applied to the in-memory copies only; the CLI never writes a snapshot back.

### D6. Read-only tools re-execute live; side effects stay staged by construction

Replay passes the imported `ToolRegistry`/`ToolRunner` into `run_activation` unchanged, so `ctx.run_tool` executes read-only tools for real, locally. This is sound for the same reason it is sound in-pipeline: invariant 5 means a `side_effect=True` tool cannot be called — `ToolRunner.run` refuses it — and side effects exist only as staged `ToolIntent`s in the returned `ActivationResult`, which replay renders and diffs but never hands to an effector. So a replay can be run any number of times without performing an effect, structurally.

The trade-off: a read-only tool that consults a changing external source (a read-only MCP lookup, a clock-adjacent helper) can return different data than it did in the pipeline, steering the agent to different LLM requests — which the cache tripwire then reports as a miss. That is the correct failure: the divergence is real and is surfaced at the first point it becomes observable, with the diff distinguishing "tool-path divergence before the miss" when the replayed `TOOL_CALL` sequence already differs. `--no-tools` (run with an empty registry) is deliberately not offered: it would replay a different agent.

### D7. The diff compares the trace-comparable surface byte-for-byte, and reports the rest as digests

The traced record of an activation is its `TraceEvent` sequence plus the intent attributes stamped into `INTENT_EMITTED` events. Everything in a replayed event is a pure function of the reconstructed inputs — identifiers (uuid5 of scope), attributes, and timestamps (`now_ms`) — so the primary comparison is exact: the replayed `ActivationResult.traces`, serialized with `SerializeToString(deterministic=True)`, against the loaded trace events for that activation attempt, in order. On top of that, structured comparisons produce the human-readable diff: activation status (`completed`/`suspended`/failed) vs the traced `ACTIVATION_END`/`ERROR`; the replayed intents' `intent_id`/`tool_name`/`kind`/`expires_at_ms` vs the traced `INTENT_EMITTED` attributes.

Outputs and the post-activation memory blob have **no traced counterpart** (traces carry no payloads), so they are not diffed against anything; the CLI prints their sha256 digests and sizes — enough to compare two replay runs against each other, and an honest refusal to invent a baseline it does not have. Exit codes make the CLI scriptable: `0` reproduced, `1` diverged (diff printed), `2` usage/configuration error, `3` irreproducible (cache miss, digest-only entry, migration refusal, missing event payload). LLM-serving trace fields that legitimately differ under replay — `beam_agents.cache_hit` is `true` on replay where the original said `false`, and usage stays decoded-from-stored-bytes either way — are normalized before comparison, and the normalization list is closed and documented (that one attribute; billed follows from it).

## Risks / Trade-offs

- **Some activations replay as "irreproducible" rather than reproduced** — committed attempts that read memory they themselves overwrote, and failed attempts past their first uncached provider call. Inherent to post-hoc snapshots (D2). Mitigated by the loud, specific report and the detected-never-fabricated property; armed pre-activation capture would close it (Open Questions).
- **Snapshots export memory contents off the pipeline.** Deliberate operator act (D1): requires produce access to the events topic *and* a configured `snapshots_to` sink; both are the operator's existing trust boundary. The proposal adds no default sink.
- **Cache eviction erodes replayability with time.** The replay cache is LRU-64 / 6h-TTL per key; an activation older than that has had its entries purged and replays as a miss. Stated in docs: export promptly, replay from the snapshot (which freezes the blob), not from the live key later. The snapshot itself never expires — `ReplayCache` TTL is evaluated against the injected `now_ms`, which replay takes from the *traced* clock, so entries live at replay time exactly as they lived at activation time.
- **The export route adds branches to `core/dofn.py`, which is under the mutation gate.** The handler is small (read five states, build one proto, one yield) and its unit tests drive `process()` with a fake handle inside the mutmut selection, per the precedent set by `add-trace-events`; `mutation-baseline.toml` is re-checked after implementation.
- **A malicious or accidental `export_request` is a state-disclosure primitive on the events topic.** True of any control message on a bus; the envelope topic already admits forged `ToolResult`s, so the trust model (producers on the events topic are trusted) is unchanged. Noted in docs; per-request authorization is out of scope for a runtime whose input topic is the trust boundary.
- **Trace-file format friction.** The CLI reads a varint-length-delimited `TraceEvent` stream (the bytes `serialize_trace_event` produces, framed); operators dumping from BigQuery have rows, not protos. Accepted for v1: the binary stream is the canonical interchange, and a row-to-proto converter is a few lines of operator tooling; building fetchers is a stated Non-Goal.

## Migration Plan

1. Land the proto edit (`StateSnapshot`, `AgentEnvelope.export_request`) and regenerate `_pb2.py` (diff-clean in CI); add golden blobs for both. Additive only: old readers decode the new oneof variant as an unknown field, and no `state_schema_version` bump is needed.
2. Land the DoFn export route and the `.snapshots` tag + `snapshots_to` resolution. At this point pipelines can export; nothing consumes the snapshots yet. A pipeline updated across this step treats in-flight `export_request`s published early as unroutable only if the DoFn predates the route — operators publish export requests after the pipeline is on the new version.
3. Land `beam_agents/replay/` (bundle loading, migration-on-load against the C32 migration functions, tripwire provider, diff) and the `beam-agents-replay` console script.
4. Land the end-to-end semantics test last: `TestPipeline` activation → export → local replay reproduces byte-identical traces and intents with zero provider calls.

Rollback is per-step with no state migration: reverting the CLI package strands nothing; reverting the DoFn route makes `export_request` envelopes dead-letter as unrecognized (they are transient control messages, not state); the proto edit reverts cleanly because nothing persists a `StateSnapshot` in keyed state.

## Open Questions

- **Armed pre-activation capture.** A per-key flag (set by a control envelope) making the DoFn emit each activation's *input* blobs on `.snapshots` before running it would give exact pre-images for completed activations — at the cost of a new state cell and a doubled-egress mode an operator must remember to disarm. Deferred until D2's exact-replay coverage (failures, pending resumes) proves insufficient in practice.
- **Drained-pipeline export.** The one case D1 cannot serve. A Flink-only escape hatch via a JVM State Processor tool would be a separate, explicitly runner-specific change if operators need it.
- **Should `beam-agents-replay` verify a `digest_only` entry when the operator supplies the response bytes out of band** (`--response-file` keyed by cache key, checked against the stored digest)? It would extend replayability past the 100 KiB blob cap without ever letting the CLI fetch; deferred until someone hits it.
