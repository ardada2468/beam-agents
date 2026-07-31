## Context

See `proposal.md` — Why. Constraints that shape the artifacts:

- **The audience is Beam committers, not beam-agents contributors.** `openspec/project.md` assumes its reader is inside this repo's workflow; the design doc must stand alone, define this repo's vocabulary (activation, continuation, intent, effector, re-injection, seq) on first use, and justify choices against *Beam's* alternatives (SDF, `RunInference`, `GroupIntoBatches`-style stateful patterns), not against a green field.
- **Beam's design-doc convention is a shared, commentable document announced on dev@beam.apache.org.** Historically these are Google Docs linked from the thread (often via an s.apache.org short link) so non-committers can comment inline. Our constitution requires artifacts to live in-repo under version control. Both constraints are real; D1 resolves them.
- **The evidence is a 0.3 deliverable.** The benchmark vs. Flink Agents, the conformance-matrix run, and design-partner usage come from `add-0-3-0-release` (pending sibling). A design doc that asserts performance or adoption claims without those artifacts in hand would be the exact kind of invented detail this change forbids itself.
- **Dependency reality is favorable and worth leading with.** The runtime's install-time dependencies are `apache-beam[gcp]>=2.60`, `httpx[http2]`, `pydantic>=2`, `protobuf` (`pyproject.toml`). There are no provider SDKs anywhere in the dependency tree — Anthropic/OpenAI-compatible clients are hand-rolled over httpx — and the effector/langgraph/otlp integrations are already isolated as optional extras with lazy imports. This maps cleanly onto Beam's optional-extra pattern and defuses the most predictable dev@ objection before it is raised.
- **ASF process knowledge inside this repo is limited.** IP clearance for code donations into existing ASF projects exists as a process (Incubator IP clearance), but its exact applicability to this donation's shape, the need for a Software Grant, and Beam-PMC-specific expectations cannot be verified from this repository. Anything not verifiable goes to Open Questions and into the thread plan as a question *for* the community — never asserted.

## Goals / Non-Goals

**Goals:**

- One design document a Beam committer can read cold and come away knowing exactly what `apache_beam.ml.agents` would be, what invariants it commits Beam to, what evidence backs it, and what is being asked of the community.
- A module-by-module decision record of what moves upstream and what stays external, each with a stated rationale — so the dev@ thread argues about decisions, not about scope ambiguity.
- A thread plan that makes the announcement a prepared conversation: email draft plus an objections register where every anticipated objection has either an answer or an honest "open."
- Traceability: every technical claim in the doc traces to `openspec/project.md`, an archived change, or a 0.3 artifact; every number traces to a 0.3 artifact.

**Non-Goals:**

- **Not upstreaming any code.** No Beam fork, no `apache_beam.ml.agents` skeleton, no PR against apache/beam. This change ends when the documents are ready; the donation itself is future work gated on the dev@ outcome.
- Not sending the dev@ email. Sending is a follow-on action requiring 0.3 evidence in hand and a human owner; the thread plan prepares it.
- Not renaming, re-licensing, or restructuring this repository in anticipation of a donation.
- Not writing a Beam Improvement Proposal for any adjacent feature (e.g. async DoFn support, MapState in Python) — gaps in the Beam SDK are *named* in the doc as constraints we designed around, not proposed for fixing here.
- Not producing marketing material. The doc's register is a design review, including the unflattering parts (duplicate-effect crash window, 1 MiB memory cap, Python-only).

## Decisions

### D1 — Venue: canonical markdown in-repo, mirrored to a commentable doc at announcement time

The canonical design document is `docs/design/apache-beam-ml-agents.md`, version-controlled in this repository, with the thread plan beside it as `docs/design/apache-beam-ml-agents-thread-plan.md`. When the announcement is actually sent (post-0.3), the then-current markdown is exported to a commentable shared document per Beam convention, and the email links both — the shared doc for inline comments, the repo file as the canonical source that incorporates resolved comments via normal PRs.

*Why:* the repo's own conventions require reviewable, versioned artifacts, and a Google Doc as source of truth would fork the content from day one. Mirroring at announcement time costs one export and keeps the canonical history in git. *Alternative rejected:* authoring directly in a shared doc (the historical Beam default) — loses versioning, review-by-PR, and the docs-consistency test; the mirror preserves the community's ability to comment inline, which is the actual point of the convention. *Alternative rejected:* proposing the doc straight into `apache/beam`'s docs tree — puts the artifact's fate inside the very approval process it is meant to initiate.

### D2 — What moves, what stays: the effector stays external; the runtime moves whole

The decision record dispositions every top-level module. The headline decisions, made here rather than left to the doc's drafting:

| Module | Disposition | Rationale sketch |
|---|---|---|
| `core/` (RunAgent, AgentConfig, `_AgentDoFn`, coders) | **moves** | This *is* the contribution — Beam-native stateful runtime. |
| `protos/` wire+state schemas | **moves** | Language-neutral by design; becomes the cross-SDK contract. |
| `model/` (LLMClient seam, LlmFacade, anthropic/openai_compat clients, FakeLLM) | **moves** | httpx-only, no provider SDKs; FakeLLM moves as test infrastructure. Provider client *growth* policy becomes Beam's to govern. |
| `tools/`, `actions/`, `memory/`, `hitl.py` | **moves** | Inseparable from the invariants (side-effects-only-via-intents, fail-closed HITL, keyed memory). |
| `observability/` | **moves, adapted** | Trace/metric semantics move; exporters get reconciled with Beam's existing metrics/IO surfaces rather than imported wholesale. |
| `adapters/` (protocol + LangGraph) | **moves, framework deps stay optional** | The adapter protocol and conformance matrix are the compatibility story; `langgraph` remains an extra, mirroring how Beam ML treats model frameworks. |
| `effector/` | **stays external** | It is a deployed *service* (consume→dedup→execute→publish), not a pipeline transform; Beam ships transforms and SDKs, not long-running side-effect executors. The intent/result protobuf contract is what Beam standardizes; any conforming effector implementation works. Where the reference implementation then lives (this repo, a Beam-adjacent repo, or elsewhere) is an Open Question for the thread. |

*Why decide this now:* "what exactly are you donating" is the first question on any donation thread; answering it per-module with rationale is the difference between a scoping discussion and a design discussion. *Why the effector boundary specifically:* project.md's own constraint list includes "no hosted effector," and the effectively-once argument deliberately splits pipeline-side determinism (Beam's job) from execution-side dedup (the effector's job) — that seam is exactly where a project boundary belongs.

### D3 — Dependency policy: no provider SDKs, extras-mapped optionals, stated as a hard commitment

The doc commits the proposed package to: (a) required deps limited to what Beam can carry — the runtime today needs only `httpx` and `pydantic` beyond Beam's own stack (`protobuf` is already a Beam dependency); (b) **zero provider SDKs, ever, in required deps** — provider access stays httpx-based, which the existing anthropic/openai_compat clients prove viable; (c) framework and transport integrations (`langgraph`, effector clients, `otlp`) expressed as optional extras with lazy imports, the pattern this repo already enforces via per-module lint/type carve-outs in `pyproject.toml`. Whether `httpx`/`pydantic` land as required deps of an `apache-beam[agents]` extra or are acceptable in some other arrangement is for Beam's dependency review — the doc proposes the extra and marks the mechanics open.

*Why elevate this to a decision:* Beam's dependency bar is the most predictable hard objection on the thread; the answer is strongest as an already-kept commitment ("this is how the code is built today") rather than a promise.

### D4 — The thread plan is an objections register, not just an email draft

The thread plan contains: (1) the announcement email draft — problem statement, one-paragraph proposal, evidence links, explicit asks (design feedback; a committer/PMC sponsor; guidance on donation mechanics); (2) an objections register: each anticipated objection with a prepared answer or an explicit "open — asking the thread." The register's minimum population, decided here so drafting can't quietly shrink it:

- **Why a stateful DoFn and not an SDF?** Agents are keyed, event-driven, stateful consumers — per-key serialized state is what makes memory race-free by construction, and TTL/HITL semantics need user timers; SDF solves splittable *source* work and offers neither per-key user state nor timers as this problem needs them.
- **Why isn't this `RunInference` with tools?** `RunInference` is stateless request/response over a model handler; this runtime's substance is what happens *between* model calls — durable keyed memory, suspension/resumption across elements, effectively-once side effects. Complementary, not overlapping.
- **Why the outbox/effector split instead of inline durable execution (the Flink Agents approach)?** Executing side effects inside the pipeline couples effect atomicity to bundle retry semantics on every runner; the outbox keeps the pipeline deterministic and replayable (byte-identical intents) and pushes execution-once to a dedicated dedup boundary. The honest cost — a bounded duplicate window on effector crash unless tools are idempotent on `intent_id` — is stated, with the e2e gate's measured behavior as evidence.
- **What about Java/Go — is Beam taking on a Python-only feature?** Yes initially, like other `apache_beam.ml` surfaces; wire/state schemas are protobuf specifically so other SDKs can implement the same contract, and the conformance scenarios are the portable definition of correct behavior.
- **Dependency policy** — per D3.
- **Pipeline `--update` and state compatibility** — `state_schema_version`, additive-only proto evolution, golden-blob compat tests: the package arrives with a state-compat discipline consistent with Beam's update story.
- **Who maintains it?** Named maintainer commitment from the beam-agents side; honest statement that donation without sustained maintainership is a burden, and that the proposal includes the people, not just the code.
- **Governance/donation mechanics** — IP provenance of the codebase, the expectation of an IP-clearance-style process and/or Software Grant for a code donation into an existing ASF project, ICLAs for contributors. Listed honestly; exact mechanics marked open per the Context constraint.

*Why:* dev@ threads on large contributions live or die on whether hard questions were anticipated. An email draft alone optimizes the first message; the register optimizes the whole thread.

### D5 — Evidence section binds to 0.3 artifacts by reference, with drafting decoupled from thread-readiness

The doc's evidence section cites the 0.3 benchmark report (vs. Flink Agents, including the p50/p99 runtime-overhead budget from project.md as the standing bar), the conformance-matrix results (scenarios × adapters × runners actually passing), and design-partner usage — each by reference to the artifact 0.3 produces, never as free-standing numbers. Distillation sections (principles, invariants, execution model, state layout, decision record, objections register) are drafted immediately; the doc carries an explicit "thread-ready" checklist whose final items are the 0.3 evidence links, and the announcement is blocked on that checklist, not on drafting.

*Why:* serializing everything behind 0.3 wastes the batch; sending before evidence lands guts credibility. The checklist makes the dependency precise instead of vibes-based. *Consistency guard:* the docs test asserts the evidence section contains no numeric performance claims unless the referenced artifacts exist in-repo — preventing "placeholder numbers" from surviving into a sent announcement.

## Risks / Trade-offs

- **The distillation drifts from the constitution** — a paraphrased invariant that is subtly weaker (e.g. dropping "byte-identical" from the intent-determinism claim) misrepresents the contract to Beam. → The docs-consistency test checks the doc names all seven invariants and key load-bearing phrases; review explicitly diffs the invariants section against `project.md` §Correctness invariants.
- **The community's design conversation invalidates decisions already recorded here** (e.g. wants the effector in-project, or a different package path than `apache_beam.ml.agents`). → Expected and fine: D2/D3 are our *opening position*, the doc frames them as proposals, and the register marks which decisions we consider negotiable. The failure mode to avoid is having no position, not having a revisable one.
- **0.3 evidence disappoints** — the benchmark shows a gap vs. Flink Agents, or design partners are thin. → The doc reports what the artifacts say; per D5 there are no free-standing claims to walk back. A weak evidence section may delay the announcement — that is the dependency working as intended, not a failure of this change.
- **ASF-process description contains an error despite the honesty policy.** → Everything not verifiable is phrased as a question; the announcement's explicit ask includes "guidance on donation mechanics," which invites correction gracefully rather than staking credibility on process trivia.
- **Staleness between authoring and sending** — the runtime keeps evolving under the doc (this batch alone touches many surfaces). → The thread-ready checklist includes a final consistency pass against `project.md` and the shipped 0.3 state; the doc records the beam-agents version it describes.
- **A public design doc informs the competitor.** Accepted: the differentiation claims (runner portability, managed Dataflow, adapter model, outbox semantics) are architectural and already public in this repo; openness is the cost of proposing into ASF and the point of doing so.

## Migration Plan

No code or state migrates; the plan is sequencing:

1. Land this change: draft the design doc's distillation sections, the decision record, the thread plan with the D4 register, and the docs-consistency test. Evidence section carries the thread-ready checklist with 0.3 items unchecked.
2. When `add-0-3-0-release` ships its artifacts, fill the evidence section by reference, complete the checklist, and run the final consistency pass against `project.md`.
3. Human owner exports the markdown to a commentable shared doc, sends the dev@ announcement per the thread plan, and works the register.
4. Thread outcomes flow back as ordinary PRs against the doc; if the community says yes, the actual donation (code movement, IP clearance, apache/beam PRs) is proposed as its own future OpenSpec change — none of it is in scope here.

Rollback: deleting the two documents and the docs test restores the previous state entirely; nothing depends on them.

## Open Questions

- **Exact ASF donation mechanics for this shape:** does a codebase authored under Apache-2.0 outside ASF, donated into an existing TLP, require formal Incubator IP clearance, a Software Grant Agreement, or both — and what does the Beam PMC specifically expect? Carried as an explicit ask in the announcement; not asserted in the doc.
- **Package path:** is `apache_beam.ml.agents` right, or does the community prefer `apache_beam.agents` (the runtime is not "ML" in the inference sense) or an incubating/experimental namespace first? The doc proposes `apache_beam.ml.agents` for discoverability next to the existing ML surfaces and marks the alternative.
- **Where does the reference effector live post-donation** if the runtime moves — this repo (continuing as the effector's home), a new Beam-adjacent repo, or inside apache/beam despite being a service? D2 fixes only that it is *not part of the donated package*.
- **Does Beam accept `httpx` + `pydantic` under an extra, or does dependency review force alternatives** (e.g. vendoring policy, urllib3-based clients)? The doc states the requirement and the fallback question; the answer materially affects donation cost.
- **Minimum Beam version / feature dependencies:** the runtime pins `apache-beam>=2.60` today; whether upstreamed code tracks Beam HEAD only (normal for in-tree code) or needs any SDK capability not yet released is settled during the (out-of-scope) code-movement phase.
- **Who is the committer/PMC sponsor?** A large contribution without a sponsoring committer stalls; identifying one is an explicit ask of the thread and cannot be resolved from inside this repo.
