# dev@beam.apache.org thread plan

The companion to [the `apache_beam.ml.agents` design
document](apache-beam-ml-agents.md). That document is the argument; this one is
the plan for the conversation it starts: the announcement email, the register of
objections we expect and what we intend to say to each, and the sequencing that
decides when the mail may be sent.

A large contribution's dev@ thread lives or dies on whether the hard questions
were anticipated. An email draft optimizes the first message; the register
optimizes the whole thread.

## 1. Sequencing — when this may be sent

**Sending is blocked on the design document's thread-ready checklist** (§12
there) being complete. Every box on it is a 0.3.0 release artifact — the
benchmark report against Apache Flink Agents, the conformance-matrix run, a
benchmark baseline seeded from CI hardware, design-partner usage, and a final
consistency pass. 0.3.0 has not shipped, so the announcement is not yet
sendable. That is the dependency working, not a delay to route around: a
proposal whose evidence section is a promise gets read as marketing.

Sending is also a **human owner's action**, outside the change that wrote these
documents. Nothing here should be interpreted as authorizing an automated send.

At send time, three mechanical steps:

1. **Mirror the markdown.** Beam's convention is a shared, commentable document
   announced on the list, so non-committers can comment inline. Export the
   then-current [design document](apache-beam-ml-agents.md) to a commentable
   shared document and link both from the mail: the shared doc for inline
   comments, the repository file as the canonical source. The markdown stays
   canonical; resolved comments come back as ordinary pull requests against it.
2. **Pin the commit.** Reference the exact commit the mirror was taken from, so
   a reader can diff the two later.
3. **Re-run the consistency pass.** Confirm §6 and §8 of the design document
   still match the implementation's constitution, and that the version the
   document declares it describes is current.

## 2. Announcement email — draft

> **To:** dev@beam.apache.org
> **Subject:** [DISCUSS] `apache_beam.ml.agents` — a Beam-native runtime for stateful streaming agents

Hi all,

We would like to propose contributing an agent *runtime* to Beam, and to get
the design in front of the list before anything resembling a patch exists.

**The problem.** A growing class of workloads is agents that events invoke
rather than users chat with: fraud triage, anomaly response, IoT reaction, ops
automation. Agent frameworks solve authoring, and none of them solves execution
at stream scale — durable per-entity memory, event- and processing-time
semantics, effectively-once side effects, backpressure, and portability across
runners. Beam already has those primitives; what is missing is the layer that
expresses an agent in them.

**The proposal.** A new package, `apache_beam.ml.agents`, whose surface is
essentially one transform — `events | RunAgent(my_agent)` — turning an agent
into a keyed, stateful, fault-tolerant Beam transform. It is explicitly a
runtime and not an authoring framework: agents are written in LangGraph, Google
ADK, Pydantic AI or a plain async protocol and reach the transform through thin
adapters, the same division of labor `RunInference` already draws between Beam
and model frameworks. The package would bring a stated behavioral contract —
bundle-atomic activation, deterministic intent ids, a replay cache that makes
bundle retries free, per-key serialized memory, side effects only as intents
executed outside the pipeline, fail-closed timeouts, and protobuf-only state
with an `--update` compatibility discipline. A working Apache-2.0 implementation
exists today outside the ASF, running on DirectRunner, Dataflow and Flink, with
Spark verified weekly as best-effort.

**Evidence.** [links: the design document; the benchmark report vs. Apache
Flink Agents, with its methodology section; the conformance-matrix results; the
repository]. The comparison report states every dimension on which it is not
like-for-like, and reports the unfavorable runs too.

**What we are asking.**

1. *Design feedback* — particularly on the outbox/effector split for side
   effects versus inline durable execution, on which modules should and should
   not move upstream, and on the dependency policy.
2. *A sponsoring committer or PMC member* willing to shepherd this.
3. *Guidance on donation mechanics.* We know a contribution of this size
   carries IP-clearance-style obligations; we do not know their exact shape for
   this case and would rather ask than assume.

Maintainership is part of the offer: a donation without sustained maintenance
is a burden on the project, and the people who wrote this intend to keep
maintaining it in-tree. Happy to say more about that specifically.

The design document is linked above as both a commentable copy and a
version-controlled file; inline comments on the former come back as pull
requests against the latter.

Thanks,
[names]

## 3. Objections register

Each entry is an objection or question we expect, with either a prepared
answer or an explicit open marker. An entry marked open is one we intend to ask
the list about — not one we forgot to think through.

### O1 — Why a stateful DoFn and not an SDF?

**Answer.** Splittable DoFns solve splittable *source* work: a large,
partitionable element of input that one bundle should be able to hand back and
resume. An agent is the other shape entirely — a keyed, event-driven, stateful
consumer. Two things it needs are exactly what stateful DoFns provide and SDFs
do not: per-key serialized user state (which is what makes agent memory
race-free by construction rather than by locking) and user timers in both the
event-time and processing-time domains (which is what makes TTL-based memory GC
and fail-closed approval timeouts expressible at all). Restriction trackers do
not model "wait for a human for up to two hours, then take the fallback path".
We do use an SDF where an SDF belongs — in test sources — and it is not the
right primitive for the runtime.

### O2 — Isn't this RunInference with extra steps?

**Answer.** `RunInference` is stateless request/response over a model handler,
and it is excellent at that. The substance of this runtime is what happens
*between* model calls: durable per-key memory that outlives the bundle,
suspension and resumption of an activation across elements and across worker
restarts, side effects that survive retries, and human approval gates driven by
timers. None of that is in `RunInference`'s problem statement, and none of
`RunInference`'s batching and model-lifetime machinery is in this one's. They
are complementary: an agent that wants a local model called per activation
should be able to use a Beam model handler underneath. If the community sees
overlap we would rather fold than duplicate — but we believe the seam is
between "one model call" and "a durable, resumable conversation with the
world".

### O3 — Why outbox-and-effector rather than inline durable execution?

**Answer.** Inline durable execution — running side-effecting tools inside the
engine under a durable-execution runtime, which is the Flink Agents approach —
buys a simpler programming model and pays for it by coupling effect atomicity
to the engine's retry and checkpoint semantics. For Beam specifically that is a
bad trade, because a guarantee that has to be re-derived for every runner is
not portable, and portability is Beam's premise. The outbox keeps the pipeline
side purely deterministic (a replayed bundle re-mints byte-identical intent
ids) and pushes execute-once to a single dedup boundary that can be crash-tested
on its own.

We state the cost rather than hiding it: duplicates are bounded to the crash
window between a tool's effect and its durable completion record, and true
exactly-once effects require tools idempotent on the intent id — which the
runtime makes easy by handing the tool its intent identity, so the id can be
used directly as a payment idempotency key or an upsert key. The end-to-end
correctness gate kills effector workers and task managers mid-flight and
asserts zero lost effects, duplicates only inside that window, and zero extra
executions on full replay.

### O4 — Python-only: is Beam taking on a single-SDK feature?

**Answer.** Yes, initially — as other `apache_beam.ml` surfaces have been. Two
things make that a starting point rather than a dead end. First, all wire and
state schemas are protobuf, deliberately, so a Java or Go implementation
defines the same messages and interoperates with the same effector and the same
topics rather than reimplementing a Python pickle format. Second, the
conformance scenarios are a framework-neutral and language-neutral definition
of correct behavior; a second SDK's implementation would prove itself against
the same seven lifecycle scenarios. We are not promising a Java implementation,
and we would rather say so than imply a roadmap we do not own.

### O5 — Dependency policy: what would Beam actually have to carry?

**Answer.** Beyond Beam's own stack: `httpx` and `pydantic`. That is the whole
required set — `protobuf` is already a Beam dependency. Zero LLM-provider SDKs,
now or ever, in required dependencies: provider access is plain HTTP, and the
Anthropic, OpenAI-compatible and vLLM clients are hand-rolled over that one
layer today, so this is an already-kept commitment rather than a promise. Every
framework adapter, effector transport, trace exporter and GPU sidecar is
already an optional extra with a lazy import and an actionable error when it is
missing. We propose an `apache-beam[agents]` extra.

**Open — asking the thread.** Whether that arrangement passes Beam's dependency
review is not ours to decide. If review prefers a different HTTP layer, or has
a vendoring policy we should follow, we want to hear it early: it materially
changes the cost of the donation.

### O6 — What happens to pipeline `--update` and state compatibility?

**Answer.** The package would arrive with the discipline already in place,
because keyed state here outlives the binary that wrote it. State is protobuf,
never pickle; schema evolution is additive-only, with a field number never
retyped or reused; a `state_schema_version` stamp plus registered lazy
migrations handle anything that cannot be additive; a golden corpus decodes
committed historical bytes on every pull request; and a nightly job on real
Dataflow drives a streaming job to hold live keyed state — a suspended
activation with a pending approval, plus populated working memory — and
replaces it in place with `--update` at head. Transform names and state spec
ids are treated as contract, since the update check matches steps by name and
state by spec id and coder. The compatibility table and the release procedure
that blocks on this gate are already written down.

### O7 — Who maintains it?

**Answer.** The people proposing it, in-tree, and we would name them on the
thread rather than in a document. This is the part of a donation that is easy
to under-promise and expensive to get wrong: a large contribution without
sustained maintainership is a net burden on a project, no matter how good the
code is. The offer is the code *and* the maintainers. We would also want to
agree explicitly on what "maintained" means here — review turnaround,
release-blocking gate ownership, and what happens if the maintainers step away
— because those expectations are cheaper to set now than to discover later.

**Open — asking the thread.** What the Beam PMC expects of maintainers for a
contribution of this size, and whether committership for the maintainers is
part of that conversation or a separate one.

### O8 — Governance and donation mechanics: how does this actually happen?

**Open — asking the thread.** This is the entry we are least able to answer,
and we would rather say so than guess. What we can state: the codebase is
Apache-2.0 licensed, was authored outside the ASF, and its provenance is
documented; we understand that a code donation of this size into an existing
top-level project carries IP-clearance-style obligations, and that contributors
would need ICLAs on file. What we do not know, and are asking: whether formal
Incubator IP clearance applies to this shape of donation, whether a Software
Grant is required in addition or instead, and what the Beam PMC specifically
expects in sequence and in artifacts. We will follow whatever process the PMC
directs; we are not proposing one.

### O9 — How does this relate to Apache Flink Agents?

**Answer.** Flink Agents put agents inside a streaming engine, which we think
is the right instinct — the disagreement is architectural, not competitive
posturing. Ours is over side effects (O3: outbox and external dedup versus
inline durable execution) and over portability: Beam's value here is that the
same agent runs on DirectRunner in a unit test, on Flink, and on a managed
Dataflow service, with the guarantees stated once rather than per engine. The
benchmark report accompanying the announcement compares the two on the closest
reproducible workloads and is explicit about every dimension where the
comparison is not like-for-like. We would rather Beam had a considered answer
than none, and we think the answer is stronger for being portable.

### O10 — Why is the effector not part of the donation?

**Answer.** Because it is a service, not a transform. It consumes intents,
deduplicates by id, executes tools, publishes results, and imports no Beam at
all. Beam ships transforms and SDKs; it does not ship long-running side-effect
executors, and we do not think it should start. What Beam would standardize is
the intent/result protobuf contract, which is the part that has to be agreed:
any conforming effector then works, including one written in another language
or embedded in an existing job runner. The reference implementation stays
available and maintained.

**Open — asking the thread.** Where the reference effector should live after a
donation — its current repository, a Beam-adjacent one, or in-tree despite
being a service. We have a preference, not a position.

### O11 — Is `apache_beam.ml.agents` the right name and the right place?

**Answer.** We propose it for discoverability next to the existing ML surfaces,
and we are not attached to it. The honest objection to our own proposal is that
this runtime is not "ML" in the inference sense — it orchestrates model calls
but its substance is state, timers and effects — so `apache_beam.agents` may be
the better home.

**Open — asking the thread.** The package path, and whether the package should
land behind an experimental annotation with a stability caveat before it makes
any API promises. We would take that constraint gladly; a new surface that
cannot evolve is worse than one that is honestly marked unstable.

### O12 — Isn't a per-tool-call round trip through a topic too slow?

**Answer.** For interactive chat, yes, and the runtime says so rather than
competing there: the target is system-triggered agents where the event, not a
waiting human, sets the latency budget. The fast path — an activation with only
read-only tools — never leaves the process, so the round trip is paid only when
an external effect is actually performed, which is the case where durability is
worth more than milliseconds. The runtime's own overhead is budgeted separately
and gated, excluding model and tool time, so the cost of the runtime is
distinguishable from the cost of the work.

### O13 — What are the known limitations?

**Answer.** Stated up front so they are not discovered adversarially:
per-key working memory is capped, so long conversation histories must be
summarized or trimmed by the agent rather than accumulated; exactly-once
effects are conditional on tool idempotency (O3); the implementation is
Python-only today (O4); Spark is best-effort and not promoted; and the
re-injection round trip makes this the wrong tool for sub-second interactive
agents (O12). The design document's own register is a design review, including
the unflattering parts.

## 4. Running the thread

- **Answer within the thread, not in a rewritten document.** Substantive
  resolutions land as pull requests against the design document afterwards, so
  the canonical file reflects what was agreed rather than what was proposed.
- **Do not defend the opening position past its usefulness.** The dispositions
  in §11 of the design document are an opening position; the failure mode to
  avoid is having no position, not having a revisable one. Say which parts are
  negotiable when asked, and they mostly are.
- **Route every process question to the PMC's answer,** not to ours. Everything
  in O8 is theirs to define.
- **If the answer is no,** the useful outcome is knowing why: scope, timing,
  maintainership, or architecture. Each of those points somewhere different,
  and only one of them means the work was wrong.
