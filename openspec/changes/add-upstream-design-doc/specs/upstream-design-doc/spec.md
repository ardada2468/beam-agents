## Purpose

The content contract for the beam-agents upstreaming artifacts: a Beam-community-style design document proposing an `apache_beam.ml.agents` package (including a module-by-module decision record of what moves and what stays), and a dev@beam.apache.org thread plan (announcement draft plus objections register). The requirements govern what these documents must contain and how their claims must be grounded — not any code movement.

## ADDED Requirements

### Requirement: The design document distills the constitution for a Beam-committer audience

The repository SHALL contain a design document at `docs/design/apache-beam-ml-agents.md` proposing an `apache_beam.ml.agents` package. The document MUST be readable standalone by a Beam committer with no prior beam-agents context: it SHALL define this repo's domain vocabulary (activation, continuation, intent, effector, re-injection, seq, fast path) on first use, SHALL state the runtime-not-framework governing principle and the division of labor it implies (Beam owns execution semantics; agent frameworks own authoring, integrated via adapters), and SHALL describe the two execution paths — fast path and re-injection path — including why iterative loops with external effects cycle through the message bus and never through the DAG.

#### Scenario: A reader without beam-agents context is never stranded

- **WHEN** the design document is read front to back by someone who has never seen this repository
- **THEN** every beam-agents-specific term is defined at or before its first use, and no section requires consulting `openspec/project.md` to be intelligible

#### Scenario: The runtime/framework boundary is stated as a scoping rule

- **WHEN** the document's principles section is read
- **THEN** it states that the proposed package is an agent runtime and not an agent-authoring framework, and names what is therefore out of scope for it (prompt templating, orchestration DSLs, authoring abstractions)

### Requirement: The design document states the correctness invariants without weakening them

The design document SHALL enumerate all seven correctness invariants from `openspec/project.md` — atomic commit with the bundle, deterministic intent IDs, the LLM replay cache, per-key serialization, side effects only via intents, fail-closed timeouts at both layers, and protobuf-only state with versioned schema evolution — as the behavioral contract the package would bring to Beam. Each invariant's statement in the doc MUST NOT be weaker than its statement in `project.md`; in particular the doc SHALL retain that replayed bundles produce byte-identical intents, that bundle retries incur zero additional provider calls on the cached path, and that late HITL results are dropped as orphaned rather than applied.

#### Scenario: All seven invariants are present and load-bearing phrases survive

- **WHEN** the design document's invariants section is compared against `openspec/project.md`'s correctness invariants
- **THEN** all seven invariants appear, and the byte-identical-intents, zero-additional-provider-calls, and fail-closed/orphaned-result properties are asserted with equivalent or stronger force

#### Scenario: The effectively-once boundary is stated honestly

- **WHEN** the document describes the intents/effector execution model
- **THEN** it states the guarantee's honest boundary — duplicate effects are bounded to the effector crash window between a tool's effect and its durable completion record, and true exactly-once requires tools idempotent on `intent_id` — rather than claiming unconditional exactly-once

### Requirement: The design document grounds the state layout in Beam SDK realities

The design document SHALL describe the keyed-state and timer layout (the `MEMORY`, `CONTINUATION`, `LLM_CACHE`, `PENDING`, and `SEQ` state specs; the `TTL_TIMER`, `HITL_TIMER`, and `FLUSH_TIMER` timers) and SHALL justify its design choices against the Beam Python SDK constraints it was built under: no MapState or OrderedListState in user state (hence bounded maps inside single-value proto blobs with explicit LRU eviction), no portable async DoFn (hence the bridge thread), and the KV-input requirement of stateful DoFns. The document SHALL also state the state-size discipline (blob and working-memory caps, TTL-based GC) and the pipeline-update compatibility story (`state_schema_version`, additive-only proto evolution, golden-blob compatibility tests).

#### Scenario: Each SDK constraint is paired with the design response

- **WHEN** the document's state-layout section is read
- **THEN** each named Beam Python SDK limitation appears alongside the specific design mechanism that accommodates it, and none is presented as a Beam defect to be fixed as part of this proposal

### Requirement: The design document presents the conformance matrix as the compatibility story and cites only real evidence

The design document SHALL present the adapter conformance matrix — the lifecycle scenarios executed for every registered adapter across the DirectRunner and Flink legs — as the verifiable definition of "bring your own framework" compatibility, and SHALL include an evidence section citing the 0.3 release artifacts: the benchmark report against Flink Agents (with the standing runtime-overhead latency budget as the bar), the conformance-matrix results, and design-partner usage. Every quantitative claim in the document MUST trace to a referenced artifact; the document SHALL NOT contain free-standing performance or adoption numbers, and until the 0.3 artifacts exist the evidence section SHALL carry an explicit thread-ready checklist marking them pending rather than placeholder figures.

#### Scenario: No number without an artifact

- **WHEN** the design document is scanned for quantitative performance or adoption claims
- **THEN** every such claim carries a reference to an existing in-repo or 0.3-produced artifact, and none is an unreferenced or placeholder figure

#### Scenario: Announcement readiness is gated on evidence

- **WHEN** the 0.3 artifacts are not yet available
- **THEN** the evidence section shows the thread-ready checklist with those items pending, and the thread plan marks the announcement as blocked on completing it

### Requirement: The design document contains a module-by-module move/stay decision record

The design document SHALL contain a decision record dispositioning every top-level runtime module (core, protos/wire schemas, model, tools, actions, memory, hitl, observability, adapters, effector) as moving into `apache_beam.ml.agents`, moving in adapted form, or staying external — each with a stated rationale. The record SHALL state that the reference effector is proposed to remain an external service outside the donated package, with the intent/result protobuf contract identified as the standardized boundary, and SHALL mark the effector's future home as an open question rather than deciding it.

#### Scenario: Every module has an explicit disposition

- **WHEN** the decision record is compared against the runtime's top-level module map
- **THEN** every module appears with exactly one disposition and a rationale, and no module is silently omitted

#### Scenario: The effector boundary is a recorded decision

- **WHEN** the decision record's effector entry is read
- **THEN** it proposes the effector stays outside the donated package, names the protobuf intent/result contract as what Beam standardizes instead, and lists the effector's post-donation home as open

### Requirement: The design document states a dependency policy compatible with Beam

The design document SHALL state the proposed package's dependency policy: required dependencies limited to the runtime's existing non-Beam needs (`httpx`, `pydantic`; `protobuf` via Beam itself), a commitment that no LLM-provider SDK ever becomes a required dependency (provider access remains httpx-based, as the existing clients demonstrate), and optional integrations (agent frameworks, effector transports, OTLP export) mapped to Beam-style optional extras with lazy imports. Whether Beam's dependency review accepts this arrangement SHALL be marked as a question for the community, not asserted as settled.

#### Scenario: The no-provider-SDK commitment is explicit and evidenced

- **WHEN** the dependency-policy section is read
- **THEN** it commits to zero provider SDKs in required dependencies and cites the existing httpx-based provider clients as proof the commitment is already kept, not a promise

### Requirement: The thread plan pairs an announcement draft with an objections register

The repository SHALL contain a dev@beam.apache.org thread plan at `docs/design/apache-beam-ml-agents-thread-plan.md` containing: (1) an announcement email draft with a problem statement, a one-paragraph summary of the proposal, links to the design document and evidence artifacts, and explicit asks (design feedback, a sponsoring committer/PMC member, guidance on donation mechanics); and (2) an objections register in which each anticipated objection is paired with either a prepared answer or an explicit "open — asking the thread" marker. The register MUST cover at minimum: why a stateful DoFn rather than an SDF; why the package is not redundant with RunInference; why outbox-based side effects rather than inline durable execution; cross-language (Python-only) scope and the protobuf contract's role; the dependency policy; pipeline-update/state compatibility; maintainership commitment; and governance/donation mechanics.

#### Scenario: Every register entry is answered or honestly open

- **WHEN** the objections register is read
- **THEN** every entry has either a substantive prepared answer or an explicit open marker, and none is left blank or answered with filler

#### Scenario: The minimum objection set is present

- **WHEN** the register's entries are checked against the required minimum topics
- **THEN** each required topic — SDF, RunInference overlap, inline durable execution, cross-language scope, dependencies, state compatibility, maintainership, governance — has at least one corresponding entry

### Requirement: ASF process uncertainty is surfaced, never invented

Wherever the design document or thread plan touches ASF or Beam governance process — IP clearance applicability, Software Grant requirements, PMC expectations, incubating/experimental status for the package — any detail not verifiable from public ASF or Beam policy SHALL be phrased as an open question or an explicit ask of the community. The documents SHALL NOT assert specific donation mechanics as settled fact, and the governance discussion SHALL honestly acknowledge that a code donation of this size carries IP-clearance-style obligations whose exact shape the thread must confirm.

#### Scenario: Governance passages contain asks, not fabricated procedure

- **WHEN** every governance or donation-mechanics passage in both documents is reviewed
- **THEN** each unverifiable process detail is framed as a question or ask, and no passage prescribes a specific ASF procedure as certain without a verifiable basis

### Requirement: A docs-consistency test keeps the documents honest in CI

The offline unit lane SHALL include a docs-consistency test covering the upstreaming documents. The test SHALL fail if the design document is missing any of the seven invariants' identifying phrases, if any relative link between the upstreaming documents and in-repo files they reference is broken, or if the evidence section states quantitative claims while its thread-ready checklist still marks the underlying 0.3 artifacts pending.

#### Scenario: Dropping an invariant from the doc fails the build

- **WHEN** an edit removes or renames one of the seven invariants in the design document's invariants section
- **THEN** the docs-consistency test fails naming the missing invariant

#### Scenario: A placeholder number cannot ride to thread-readiness

- **WHEN** the evidence section contains a quantitative claim while a referenced 0.3 artifact is still marked pending on the thread-ready checklist
- **THEN** the docs-consistency test fails, identifying the unbacked claim
