# `apache_beam.ml.agents` — a Beam-native runtime for stateful streaming agents

**Status:** draft for discussion on dev@beam.apache.org. Not yet announced —
see [Evidence and thread-readiness](#12-evidence-and-thread-readiness) for what
blocks the announcement, and the companion
[thread plan](apache-beam-ml-agents-thread-plan.md) for how it will be run.

**Audience:** Beam committers and contributors. No prior knowledge of the
implementation is assumed; every term specific to it is defined at or before
its first use.

**Describes:** `beam-agents` 0.1.0 (the version of the implementation this
document distills; recorded so a reader can check the claims against a tag).

**Asks of the community:** design feedback on the shape below; a sponsoring
committer or PMC member; and guidance on donation mechanics, which we
deliberately do not presume to know.

---

## 1. Summary

We propose contributing a runtime for **system-triggered AI agents** to Apache
Beam, as a new package `apache_beam.ml.agents`, alongside the existing
`apache_beam.ml` inference surfaces.

The unit of the proposal is one transform:

```python
outputs = events | beam.WithKeys(entity_key) | RunAgent(my_agent)
```

`RunAgent` turns an agent into a keyed, stateful, fault-tolerant Beam
transform. It is not an agent-authoring framework and does not want to be one:
agents are authored in LangGraph, Google ADK, Pydantic AI, or a plain
async-function protocol, and reach the transform through thin adapters. What
the proposed package supplies is the part those frameworks do not have —
durable keyed memory, event- and processing-time semantics, effectively-once
side effects, backpressure-aware scale-out, and runner portability.

The target workload is explicitly **not** interactive chat. It is the class of
agents an event invokes: fraud triage, anomaly response, IoT reaction,
personalization, ops automation — where decisions must be durable, replayable,
and horizontally scalable, and where the pipeline, not a request, is the unit
of execution. That is Beam's problem domain, and it is the reason we think the
runtime belongs here rather than in yet another standalone agent framework.

A working implementation exists today, Apache-2.0 licensed, outside the ASF. It
runs on DirectRunner, Dataflow and Flink, with Spark verified weekly as
best-effort. This document proposes donating it, states the behavioral contract
it would bring, records module by module what would and would not move, and
lists what we cannot answer from outside the project.

## 2. Vocabulary

The implementation's terms, defined once, in the order the rest of this
document uses them.

| Term | Meaning |
|---|---|
| **Activation** | One execution of the agent for one element, inside one `process()` call. The unit of atomicity throughout. |
| **seq** | A per-key monotonic activation counter held in keyed state. Scopes cache keys and intent IDs, so "the second activation of key K" is nameable and reproducible. |
| **Fast path** | An activation that completes within a single element, with no suspension. |
| **Intent** (`ToolIntent`) | A declarative *request* to perform a side effect — tool name, arguments, deadline, and a deterministic id — emitted instead of performing it. |
| **Effector** | An external service that consumes intents, deduplicates them by id, executes the tool, and publishes results. It imports no Beam. |
| **Continuation** | The persisted resume-state of an activation that suspended awaiting a result or an approval. |
| **Re-injection** | A result or approval re-entering the pipeline as a new element on the same key, resuming its continuation. |
| **Replay cache** | Keyed-state memoization of model calls, making bundle retries free and path-stable. |
| **HITL** | Human-in-the-loop: an approval gate on an intent, with a timer and a fail-closed fallback. |

## 3. The problem

Agent frameworks solve authoring: how a developer expresses a loop of model
calls, tool calls, and control flow. None of them solves *execution at stream
scale*, because that is not their problem:

- **Memory is a process-local dictionary or an external store with no
  transactional relationship to the work.** When the worker dies, either the
  memory is gone or it has partially applied writes for an activation that
  never finished.
- **Side effects are function calls.** A retried execution charges the card
  twice. Frameworks paper over this with in-process durable-execution engines,
  which reintroduces the coupling between effect atomicity and retry semantics
  that a distributed data processor exists to break.
- **Scaling is a deployment exercise**, not a property of the programming
  model — no keyed parallelism, no backpressure, no watermarks, no
  event-time reasoning about lateness.
- **Human-in-the-loop is an open socket**, which does not survive a
  deployment, let alone a bundle retry.

Beam already has the answers: per-key serialized user state, event- and
processing-time timers, bundle-atomic commits, watermarks, and a portable model
across runners. What has been missing is the layer that expresses an agent in
those primitives. Building that layer *outside* Beam — as we did — means
re-deriving Beam's semantics from the outside and being one release behind its
runners forever. Building it inside means agents inherit Beam's execution
guarantees by construction.

The competitive context is worth stating plainly rather than leaving implied:
Apache Flink announced Flink Agents, putting agents inside a streaming engine.
We think that is the right instinct and the wrong architecture for side effects
(§7 argues why), and we would rather Beam had a considered answer than no
answer.

## 4. Governing principle: a runtime, not a framework

**Beam owns execution semantics. Agent frameworks own authoring. Adapters join
them.**

This is the same division of labor `RunInference` already draws: Beam owns
batching, model lifetime, and pipeline integration; PyTorch, TensorFlow and
scikit-learn own the models. `apache_beam.ml.agents` would own activation,
state, timers, effect ordering, and replay; LangGraph, ADK and Pydantic AI own
graphs, prompts and control flow.

Consequently the following are **out of scope for the proposed package**, and
we would ask that they stay out of scope after donation:

- prompt templating,
- an orchestration or workflow DSL,
- agent-authoring abstractions of any kind (agent classes, node types, planner
  interfaces),
- a hosted or managed effector.

A proposal that adds any of them turns a runtime into a competing framework
and puts Beam in the business of having opinions about prompt engineering. The
implementation enforces this rule on itself today; we would carry the rule
across with the code.

## 5. Shape and the two execution paths

### 5.1 Pipeline shape

```
Kafka/PubSub events ──┐
tool-results topic ───┼─► WithKeys(entity_id) ─► Flatten ─► RunAgent
approvals topic ──────┘                                       │
   .output  (main)   ─► downstream
   .intents          ─► outbox topic ─► effector ─► results topic (re-injected)
   .traces           ─► OTLP / BigQuery
   .errors           ─► dead-letter sink
```

Everything the agent produces leaves as a tagged output of one multi-output
transform. Results and approvals return as ordinary elements on the same key.

### 5.2 The fast path

The agent runs to completion inside one `process()` call. Read-only tools
execute inline; model calls go through an async client whose responses are
memoized in keyed state. One element in, one output out, state committed with
the bundle.

### 5.3 The re-injection path

When the agent needs a side-effectful tool or a human approval, it does not
call anything. It emits an **intent**, persists a **continuation** in keyed
state, and yields. The effector executes the tool outside the pipeline and
publishes a result; the result re-enters the pipeline keyed by the same entity
and resumes the continuation from its continuation point.

### 5.4 Why loops go through the bus and never through the DAG

An agent loop with external effects is iterative and data-dependent: the number
of tool calls is not known at pipeline-construction time. There are only three
ways to express that in a dataflow system, and two of them are wrong here:

1. **A cycle in the DAG.** Beam DAGs are acyclic. Not available, and we are not
   asking for it.
2. **Unrolling to a fixed depth.** Bounds the agent's reasoning by a
   construction-time constant and multiplies the graph.
3. **Cycling through the message bus** — emit intent, suspend, resume on
   re-injection. The iteration count is unbounded, the graph is fixed and
   small, and every hop is an ordinary Beam element with the runner's
   fault-tolerance behind it.

We take the third. The cost is real and worth naming: each external tool call
is a round trip through a topic, which is why this runtime targets
system-triggered agents rather than sub-second interactive ones.

## 6. The contract: seven correctness invariants

These are the behavioral commitments the package would bring to Beam. They are
stated here at the same force the implementation's own constitution states
them, because a donated runtime whose guarantees soften in the retelling is
worse than no donation.

1. **Atomic commit with the bundle.** State mutations and emitted outputs
   commit atomically with the Beam bundle. All effects of an activation —
   memory writes, cache inserts, intents, traces, outputs — are *staged* in an
   activation context and applied only on success. A failed/timed-out
   activation mutates nothing. There is no partial activation.

2. **Deterministic intent IDs.** `intent_id = uuid5(NAMESPACE, key + seq +
   step_index)`. A replayed bundle that walks the same path produces
   byte-identical intents, and the effector dedups on intent_id. This is the
   entire effectively-once argument: the pipeline side contributes determinism,
   the effector side contributes deduplication, and neither has to know how the
   other fails.

3. **Replay cache.** Every model call is keyed by
   `sha256(model_id, canonical_json(messages), tools_schema, sampling_params,
   key, seq)` and cached in keyed state — LRU, bounded entries, bounded TTL,
   bounded blob. Bundle retries must incur ZERO additional provider calls on
   the cached path. Without this, Beam's own retry behavior would make agents
   arbitrarily expensive and non-deterministic; with it, a retry re-walks the
   same path at no provider cost.

4. **Per-key serialization.** Beam stateful DoFns process one element at a time
   per key. Memory is race-free by construction — not by locking. Cross-key
   parallelism comes from the runner. No cross-key shared mutable state exists
   anywhere in the design.

5. **Side effects only via intents.** Calling a side_effect=True tool directly
   raises; `ctx.act(...)` is the only effect path. External writes never
   execute inside the pipeline. The one documented exception is idempotent
   upserts to the long-term memory store, keyed by `(key, seq)`.

6. **Timeouts fail closed at both layers.** A HITL timer firing takes the
   configured fallback path, *and* the effector refuses expired intents by
   their `expires_at`. Late results are dropped as orphaned_result to the
   errors output rather than applied to a key that has moved on. Both layers,
   because either alone leaves a window where a decision is made twice.

7. **Protobuf-only state.** State is protobuf, never pickle. Pipeline
   `--update` compatibility follows from that: additive proto changes only;
   a breaking change requires a `state_schema_version` bump, a registered lazy
   migration, and a golden-blob compatibility test.

Invariants 1, 2, 3 and 6 are gated by a dedicated correctness tier that never
skips (§9); 7 is gated by a golden corpus offline and a real `--update` job
nightly.

## 7. Effectively-once side effects, and its honest boundary

The outbox model, end to end:

1. The agent stages an intent with a deterministic id and a deadline.
2. The intent commits with the bundle and is written to a topic, keyed by
   entity.
3. The effector claims the intent under a lease, refuses it if expired,
   executes the tool, records a durable terminal result, publishes it, and only
   then advances its offset.
4. The result re-enters the pipeline and resumes the continuation.

Each edge is chosen as a crash argument: expiry is decided before the dedup
store is touched, so an outage cannot make a deadline fail open; the result is
durable before it is published, so a crash republishes rather than re-executes;
the offset advances last, so a crash redelivers rather than loses.

**What this guarantees:** at most one *dispatch* per intent id, one agreed
terminal result per intent id, per-key execution order, and no intent lost.

**What it does not guarantee, and we will not claim:** unconditional
exactly-once *effects*. Two windows remain, and both are inherent rather than
implementation defects:

- **Crash mid-tool.** A worker killed between invoking a tool and durably
  recording its completion cannot know whether the effect landed. Redelivery
  re-executes.
- **Lease expiry under partition.** A worker alive but partitioned from the
  dedup store past its lease can have its intent re-claimed.

So the honest statement is conditional on the tool. A tool that keys its
downstream effect on the intent id — a payment `Idempotency-Key`, a `SETNX`, a
keyed upsert, an `INSERT … ON CONFLICT DO NOTHING` — gets exactly-once effects,
because the re-invocation replays the same key and the downstream collapses it.
A tool that does not is at-least-once across crash recovery: zero lost effects,
duplicates bounded to the crash window between a tool's effect and its durable
completion record, and strict exactly-once when no worker dies mid-tool. **True
exactly-once requires tools idempotent on intent_id**, and the runtime makes
that easy (a tool may declare a keyword-only parameter and receive the
executing intent's identity) rather than pretending it is unnecessary.

We think that boundary is a feature of the proposal, not an embarrassment: it
is the same boundary every exactly-once story in the ecosystem has, stated
where a user can act on it.

### Why an outbox rather than inline durable execution

The alternative — the one Flink Agents takes — is to execute side effects
inside the streaming engine under a durable-execution runtime. It buys a
simpler programming model and pays for it by coupling effect atomicity to the
engine's retry and checkpoint semantics on every runner. Beam's portability
premise makes that a bad trade specifically for Beam: a guarantee that must be
re-derived per runner is not portable. The outbox keeps the *pipeline* purely
deterministic and replayable — which is exactly what Beam is good at — and
pushes execute-once to a dedicated boundary that can be reasoned about, and
crash-tested, on its own.

## 8. Keyed state and timers, under the Beam Python SDK as it is

The runtime is built out of what the Python SDK offers today. Each constraint
below is a fact we designed around, **not** a defect this proposal asks Beam to
fix; no part of this contribution is contingent on an SDK change.

### 8.1 State specs

| Spec | Kind | Holds |
|---|---|---|
| `MEMORY` | `ReadModifyWriteState` | Working memory for the key, as one proto blob |
| `CONTINUATION` | `ReadModifyWriteState` | The suspended activation's resume-state |
| `LLM_CACHE` | `ReadModifyWriteState` | The replay cache, bounded, with explicit LRU eviction |
| `PENDING` | `BagState` | Intents emitted and not yet resolved |
| `SEQ` | `CombiningValueState` (sum) | The per-key activation counter |

### 8.2 Timers

| Timer | Domain | Purpose |
|---|---|---|
| `TTL_TIMER` | watermark | Memory garbage collection — state growth is bounded by it |
| `HITL_TIMER` | real time | Approval/result timeout, firing the fail-closed fallback |
| `FLUSH_TIMER` | real time | Adaptive batching only |

### 8.3 Constraint → design response

| Beam Python SDK reality | What the design does about it |
|---|---|
| No `MapState` or `OrderedListState` in user state | Bounded maps live *inside* single-value proto blobs, with explicit LRU eviction written by hand. This is why the cache and memory are one blob each rather than a map cell. |
| No portable async DoFn | `setup()` starts one background thread per DoFn instance with a dedicated asyncio loop and shared HTTP pools. `process()` submits the activation coroutine and blocks with a timeout; on timeout it cancels and routes to the errors output, mutating no state. |
| Stateful DoFns require KV input | The transform raises `ValueError` at pipeline-construction time on non-KV input, with an actionable message, rather than failing at runtime on the first element. Keying by entity is part of the contract, not an accident of the caller's graph. |
| Beam distributions carry no percentiles | Latency budgets are gated by a dedicated offline benchmark suite rather than by reading the pipeline's own metrics (§10). |

### 8.4 Size discipline and update compatibility

Every state blob is capped at 100 KiB and working memory has a soft cap of
1 MiB per key, with a compaction hook and TTL-driven GC. A runtime that lets
per-key state grow without bound is a runtime that fails in month three, on the
busiest key.

Keyed state outlives the binary that wrote it, so the package would arrive with
a state-compatibility discipline already in place: a `state_schema_version`
stamp, additive-only proto evolution (a field number is never retyped or
reused), registered lazy migrations for version bumps, a golden corpus that
decodes historical bytes on every pull request, and a nightly real-Dataflow
`--update` job that drives a job to hold live keyed state and replaces it in
place. The transform names and state spec ids are treated as part of the public
contract, because Dataflow's update check matches steps by name and state by
spec id and coder. Details are in
[`docs/state-compat.md`](../state-compat.md) and
[`docs/state-migration.md`](../state-migration.md).

## 9. Compatibility: the adapter conformance matrix

"Bring your own framework" is a claim that decays into marketing unless
something executes it. The implementation's answer is a conformance matrix:
**seven lifecycle scenarios × every registered adapter × runner legs.**

The scenarios are the portable definition of correct behavior:

| Scenario | Asserts |
|---|---|
| `single_shot` | Fast path: one element in, one decision out |
| `multi_tool_inline` | Several read-only tools inside one activation |
| `suspension_resume` | Intent → suspension → re-injected result → resume |
| `approval_timeout_fallback` | HITL timer fires; the fail-closed fallback runs |
| `restart_mid_suspension` | A worker dies while suspended; the continuation survives |
| `bundle_retry_cache` | A retried bundle makes zero additional provider calls |
| `ttl_expiry` | Memory GC fires on the watermark timer |

The adapter axis today is the reference protocol agent plus LangGraph, Google
ADK and Pydantic AI. Registration is explicit and guarded: shipping an adapter
subpackage without registering it on the matrix is a collection error, not a
quietly smaller matrix. A meta-test audits registry × scenario × leg against
the cells actually collected, counting declared per-leg skips, so the matrix
cannot shrink without someone writing down why.

The runner axis is DirectRunner (required on every pull request), Flink (a
required check, on a mini-cluster), and Spark (weekly, **best-effort, not
promoted** — promotion has a written evidence bar and a demotion path; see
[`docs/ci.md`](../ci.md)).

For Beam, this matrix is the interesting artifact independent of the code: it
is an executable specification of what an agent runtime must do, expressed
without reference to any one authoring framework. It is also the natural place
for a future Java or Go implementation to prove itself.

## 10. Dependency policy

Beam cannot grow a dependency on every LLM vendor's SDK, and it does not have
to. Stated as a commitment we would carry across:

1. **Required dependencies stay minimal.** Beyond Beam's own stack, the runtime
   needs `httpx` and `pydantic`. `protobuf` is already a Beam dependency.
2. **Zero LLM-provider SDKs in required dependencies, ever.** Provider access
   is plain HTTP over `httpx`. This is not a promise about future restraint —
   it is how the code is built today: the Anthropic client, the
   OpenAI-compatible client and the vLLM provider are hand-rolled over the same
   HTTP layer, and there is no vendor SDK anywhere in the dependency tree.
   Provider taxonomy and decode behavior are verified offline against a mock
   transport, so the test suite needs no vendor SDK either.
3. **Everything else is an optional extra with lazy imports.** Agent frameworks
   (`langgraph`, `pydantic-ai`, `adk`), the effector's transports (Kafka,
   Pub/Sub, Redis, Bigtable), the OTLP exporter, and the vLLM GPU sidecar are
   already extras today, each with an import-time guard so that importing the
   package never imports a framework and a missing extra surfaces as an error
   naming it. This mirrors how `apache_beam.ml` treats model frameworks.

What we propose is an `apache-beam[agents]` extra carrying `httpx` and
`pydantic`. **Whether Beam's dependency review accepts that arrangement is a
question for the community, not something we can settle** — a vendoring policy
or a different HTTP layer would change the donation's cost, and we would rather
hear it on the thread than discover it in review.

## 11. What would move upstream, and what would stay

The first question on any donation thread is "what exactly are you offering".
Answering it per-module, with rationale, is the difference between a scoping
argument and a design discussion. The dispositions below are our **opening
position**, not a demand; the ones we consider most negotiable are flagged.

| Module | Disposition | Rationale |
|---|---|---|
| `core/` — the transform, the stateful DoFn, activation context, loop driver, coders, migration registry | **moves** | This *is* the contribution: the Beam-native runtime. Everything else exists to serve it. |
| `protos/` + generated `_protos/` — wire and state schemas | **moves** | Language-neutral by design, precisely so a future Java or Go SDK can implement the same contract. Becomes the cross-SDK definition of an agent's state and effects. |
| `model/` — the client seam, the httpx providers (Anthropic, OpenAI-compatible, vLLM), the replay cache, and the scripted fake | **moves** | httpx-only, no vendor SDKs (§10). The fake provider moves as test infrastructure: it is what makes the correctness tier runnable offline. Governance of *which* providers ship becomes Beam's to set. |
| `tools/` — the tool registry, the side-effect flag, argument validation | **moves** | Inseparable from invariant 5: the side-effect flag is what makes "side effects only via intents" enforceable rather than advisory. |
| `actions/` — intent construction and the outbox sink | **moves** | The pipeline half of the effectively-once argument. Only the *execution* half is external. |
| `memory/` — the facade, compaction, and the long-term store backends (Bigtable, Redis, Firestore, SQL) | **moves, backends as extras** | Working memory is keyed state and unambiguously belongs. The long-term backends move as optional extras on the same footing as Beam's existing IO-adjacent optional dependencies. |
| `hitl.py` — approval policy, timers, fail-closed fallbacks | **moves** | Invariant 6 lives here; a fail-closed timeout is a runtime semantic, not an application concern. |
| `keys.py` — hot-key sharding helpers | **moves** | A key function plus its documented safety conditions, not an aggregation DSL. Shard assignment is a correctness input (it feeds intent ids and cache keys), so it must ship with the runtime that depends on it. |
| `_deprecation.py` — the deprecation-warning helper behind the API freeze | **stays** | Private machinery for *this repository's* compatibility policy (one-minor-release windows, `CONTRIBUTING.md`). Beam has its own deprecation conventions and its own release cadence; a donated module would arrive carrying a second, conflicting policy. Nothing in the runtime imports it. |
| `observability/` — trace events, metrics, exporters | **moves, adapted** | The trace and metric *semantics* (OpenTelemetry GenAI conventions) move as-is. The exporters should be reconciled with Beam's existing metrics and IO surfaces rather than imported wholesale — this is the entry most likely to be restructured in review, and we expect that. |
| `adapters/` — the agent protocol, the shared transport seam, and the LangGraph, ADK and Pydantic AI adapters | **moves, framework deps stay optional** | The protocol plus the conformance matrix *is* the compatibility story (§9). Each framework stays an extra, exactly as `apache_beam.ml` treats model frameworks. Negotiable: the community may prefer adapters live out-of-tree with only the protocol and matrix upstream. |
| `yaml/` — the Beam YAML provider | **moves** | It is a Beam surface by construction — a Python-typed provider exposing `RunAgent` to a YAML pipeline. Upstream it stops being a third-party provider and becomes a first-party transform. |
| `testing/` — the bundle-retry chaos harness | **moves** | Invariant 2 and invariant 3 are only credible because something forces bundle retries and checks the results are byte-identical. Donating the guarantees without the harness that enforces them would be donating prose. |
| `intent_signing.py` — HMAC signing and verification of `ToolIntent`s | **moves with the contract** | It is the authenticity half of the intent/result protobuf contract the row below proposes Beam standardize: the pipeline signs at the outbox, and any conforming effector — in any language — verifies before executing. Upstreaming the wire fields without the reference signing and verification routines would standardize a format nobody could interoperate on. Pure stdlib (`hmac`, `hashlib`), no Beam import, no provider SDK. Key *distribution* stays deployment-specific and out of scope. |
| `console/` — the local telemetry viewer (SQLite store, read API, bundled UI) | **stays external** | A developer tool that *reads* the trace, error, and snapshot records the row above proposes Beam standardize. It imports no Beam on its read path, ships an HTTP server and a JavaScript bundle, and would arrive carrying a web build toolchain a runtime SDK has no reason to own. The part worth upstreaming is already upstream-bound: the record semantics. Anyone can then write their own viewer, and this one keeps working against a donated runtime unchanged, because it only depends on the wire format. Its one pipeline-side piece — the `console://` sink — is a thin `SinkResolver` wrapper that deliberately modifies nothing in `core/`. |
| `effector/` — the reference side-effect executor service | **stays external** | It is a deployed *service* (consume → dedup → execute → publish), not a pipeline transform; it imports no Beam and never will. Beam ships transforms and SDKs, not long-running side-effect executors. What Beam would standardize is the **intent/result protobuf contract** — any conforming effector implementation then works, including ones written in other languages or embedded in an existing job runner. **Where the reference implementation lives after a donation is an open question** (§13), not something this document decides. |

Two boundaries are worth restating because they carry the design's weight. The
effector **stays external** because the effectively-once argument deliberately
splits pipeline-side determinism (Beam's job) from execution-side deduplication
(the effector's job) — a project boundary belongs exactly at that seam. And the
protobuf schemas move because they are the only part of this that a
non-Python SDK could adopt without adopting our code.

## 12. Evidence and thread-readiness

This document is a proposal, and proposals of this size get read for their
numbers. We would rather have none than have unbacked ones, so this section
cites artifacts and nothing else. **No figure below is a measurement until its
artifact is checked off.**

The standing bar the implementation holds itself to is stated in its
constitution as a release-blocking constraint: runtime overhead **p50 < 15 ms**
and **p99 < 60 ms** per activation, excluding model and tool time. That is a
threshold, not a result. What the benchmark suite measures against it is
described in [`docs/benchmarks.md`](../benchmarks.md): a no-op activation
throughput ceiling; per-activation overhead at three fake-provider latency
tiers, with the lowest tier gated and the higher ones proving latency
invariance; a suspension round trip; activation cost across committed state
sizes up to the blob cap; and a side-by-side against `RunInference` on
identical zero-latency work. Percentiles are pooled over per-activation samples
rather than per-process means, one activation per sample, so the tail the
budget exists to catch is not averaged away.

Deliberately, the benchmark baseline in this repository is **unseeded**: the
gate refuses to run against numbers measured on developer hardware, and no
CI-measured figures exist yet. Quoting any today would be exactly the invented
detail this section exists to prevent.

### Thread-ready checklist

The dev@ announcement is blocked on every box below.

- [ ] **Benchmark report vs. Apache Flink Agents.** Produced by the 0.3.0
      release (`docs/benchmarks/0.3.0-vs-flink-agents.md`): the closest-matching
      harness workload against its nearest reproducible Flink Agents
      equivalent, pinned versions, disclosed environment, full methodology
      including every dimension where the comparison is not like-for-like, and
      **all** completed runs reported — including unfavorable ones. Pending:
      0.3.0 has not shipped.
- [ ] **Conformance matrix results.** The scenarios × adapters × runner legs
      actually green at the release candidate, with the declared skip inventory
      attached. Pending: to be captured from the 0.3.0 release run.
- [ ] **Benchmark baseline seeded from CI hardware.** Required before any
      overhead figure may be quoted anywhere in this document. Pending: the
      baseline's medians table is deliberately empty.
- [ ] **Design-partner usage.** Summarized from the 0.3.0 feedback triage, with
      dispositions. Pending: 0.3.0 has not shipped.
- [ ] **Final consistency pass.** Re-diff §6 and §8 against the implementation's
      constitution and update the version this document declares it describes.

Until every box is checked, this document circulates as a design proposal whose
evidence section is explicitly incomplete — which is a fair thing to send to a
mailing list, and much better than a confident number nobody can reproduce.

## 13. Open questions

Carried into the thread as questions, not answered here.

1. **Donation mechanics.** Does a codebase authored under Apache-2.0 outside
   the ASF, donated into an existing top-level project, require formal
   Incubator IP clearance, a Software Grant, or both — and what does the Beam
   PMC specifically expect? We know a contribution of this size carries
   IP-clearance-style obligations; we do not know their exact shape for this
   case and will not guess at it. **Asking the thread.**
2. **Package path.** Is `apache_beam.ml.agents` right? It is proposed for
   discoverability next to the existing ML surfaces, but the runtime is not
   "ML" in the inference sense, and `apache_beam.agents` may be the better
   name. Whether the package should land behind an experimental annotation
   first is a related question. **Asking the thread.**
3. **Where the reference effector lives** after a donation: its current
   repository, a Beam-adjacent one, or inside apache/beam despite being a
   service. §11 fixes only that it is not part of the donated package.
4. **Dependency review.** Does Beam accept `httpx` + `pydantic` under an
   extra, or does review force alternatives? This materially affects the
   donation's cost (§10).
5. **Minimum Beam version.** The implementation pins `apache-beam>=2.60`.
   In-tree code normally tracks HEAD; whether anything here needs an SDK
   capability that is not yet released would be settled during the code-movement
   phase, which is out of scope for this document.
6. **Sponsorship.** A contribution this size stalls without a sponsoring
   committer or PMC member. Identifying one is an explicit ask and cannot be
   resolved from outside the project.

## 14. What we are asking for

1. **Design feedback** on the shape above — especially §7 (outbox vs. inline
   durable execution), §11 (what should and should not move), and §10
   (dependencies).
2. **A sponsoring committer or PMC member** willing to shepherd the
   contribution.
3. **Guidance on donation mechanics** (question 1 above), from people who have
   done this before.

We are not asking for a merge decision on a thread. We are asking whether Beam
wants an agent runtime, and if so, whether this is the right one.

Maintainership is part of the offer, not an afterthought: a donation without
sustained maintenance is a burden on the project, and the proposal includes the
people who wrote it. The specifics belong in the thread, where they can be
committed to publicly.
