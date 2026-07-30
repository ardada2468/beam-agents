## Context

`pyproject.toml` still reads `version = "0.0.0"`; C25 `add-0-1-0-release` is the change that puts the package onto a released line (0.1.0) and, with it, establishes the only release mechanics this repository has — version bump, changelog, annotated tag, publish workflow. This change is the third instantiation of that process and the first one with real gate inputs beyond "tests are green": by 0.3.0 the C33 benchmark harness exists and enforces the latency budget `openspec/project.md` states as release-blocking (runtime overhead p50 < 15 ms / p99 < 60 ms per activation, excluding LLM/tool time), and the adapter conformance matrix (`tests/conformance/`: seven lifecycle scenarios × registered adapters × DirectRunner/Flink legs, with a meta-test that stops the matrix from silently shrinking) is the cross-adapter correctness surface a release must not regress.

Two workstreams distinguish 0.3.0 from a routine version bump. First, design partners have been running the 0.1.x/0.2.x line; their feedback lands here. None of it exists at proposal time, so the deliverable is a triage process with recorded dispositions, not a fix list. Second, `openspec/project.md` names Apache Flink Agents as the direct competitor. That framework runs agents *inside* Flink with inline durable execution; beam-agents runs agents as Beam stateful DoFns with outbox-based effectively-once side effects and runner portability. 0.3.0 publishes the first measured comparison between the two, built on C33's scenarios.

The sibling M2 changes (C26–C34) are cited by roadmap ID and name throughout; their internals are owned by their own proposals and are deliberately not restated here.

## Goals / Non-Goals

**Goals:**
- Ship 0.3.0 through the C25 release process with zero process innovation — same changelog format, same tag discipline, same publish workflow.
- Make the release gate explicit and checkable: nine archived M2 changes, green benchmark regression gates, green conformance matrix, closed release-blocking feedback.
- Establish a feedback-triage rubric that survives past 0.3.0: every partner item gets a recorded disposition, and "release-blocking" has a definition narrower than "a partner wants it".
- Publish a benchmark comparison against Apache Flink Agents that a skeptical reader — including a Flink Agents maintainer — would call fair: pinned versions, disclosed methodology, disclosed non-equivalences, no cherry-picked runs.

**Non-Goals:**
- Changing the release process itself. Anything C25's spec says about versioning, changelog, tagging, or publishing is consumed verbatim; a defect found in the process during this release becomes a follow-up change against C25's capability, not an inline patch here.
- Implementing any specific partner-feedback fix. Fixes that the rubric classifies release-blocking get their own OpenSpec change folders under the normal spec-driven workflow; this change only tracks that they close before the gate.
- Adding benchmark scenarios or harness features. The workload comes from C33's existing scenario set; if no scenario matches Flink Agents' demo workloads closely enough, the gap is reported in the comparison's methodology section, not closed by growing the harness inside a release change.
- A continuous competitive-benchmarking program. This is one report for one release; whether to repeat it per-release is an open question, deliberately.

## Decisions

### D1. 0.3.0 instantiates the C25 release process; a release change never extends it

The release-milestone change contains no mechanics of its own: version bump to `0.3.0`, a changelog section, a tag, a publish — all as C25 defines them. This is why `## Capabilities` modifies nothing: the `release-0-3` capability states *what must be true for this version to ship*, and defers *how a version ships* entirely to C25's capability. The alternative — folding a process tweak ("while we're releasing, let's also automate X") into the milestone — was rejected because it makes every release a process-review and breaks the one-capability-per-change convention. If executing this release exposes a process defect, the migration plan routes it to a follow-up change.

### D2. Feedback triage: a two-bucket rubric with recorded dispositions, specced before any feedback exists

Every design-partner item is triaged into exactly one of two buckets:

- **Release-blocking fix** — the item evidences: a violation of a correctness invariant in `openspec/project.md` (atomic commit, deterministic intent IDs, replay-cache zero-extra-calls, per-key serialization, side-effects-only-via-intents, fail-closed timeouts, protobuf-only state); loss or corruption of user data or state; a break in `--update` state compatibility; or a security defect. These get their own OpenSpec change folders, and 0.3.0 does not ship until they are archived.
- **Follow-up OpenSpec change** — everything else: feature requests, ergonomics, docs gaps, performance short of the stated budget. Filed as proposed changes (or roadmap notes) targeting post-0.3.0, so the request is durably captured without holding the release hostage.

Every item — including ones that arrive as "just a question" — gets a written disposition (bucket, rationale, link to the resulting change folder or issue) in the release notes. Two failure modes motivated speccing this now, before feedback exists: an undefined blocking bar lets any partner request stall the release indefinitely, and an undefined intake lets feedback evaporate into chat history. The rubric's blocking bucket is deliberately anchored to the project's existing invariant list rather than to severity adjectives ("critical", "major") that have no testable meaning. If zero feedback items exist at release time, that fact is recorded explicitly in the release notes — an empty table is a disposition, an absent table is a process failure.

### D3. Benchmark comparison methodology: honesty is a requirement, not a tone

The comparison will be read adversarially, so the report is structured to survive that reading:

- **Workload selection.** Pick the single C33 scenario closest to a workload Apache Flink Agents can express idiomatically (an event-triggered, keyed, stateful agent with tool calls — the shared core of both systems' target use case), and implement its nearest equivalent on Flink Agents using that project's own recommended APIs, not a deliberately naive port. One primary scenario, compared deeply, beats five compared shallowly.
- **Measure runtime overhead, not model latency.** Both sides run with a scripted fake model of equal cost (C33's FakeLLM-over-HTTP style on ours; the closest achievable stub on theirs), so the numbers isolate what the runtimes add — the same "excluding LLM/tool time" definition as our own p50/p99 budget. A comparison dominated by provider latency measures the provider, not the runtimes.
- **Enumerate the apples-to-oranges dimensions explicitly.** At minimum: Flink Agents executes agents inline with durable execution inside a JVM runtime, while beam-agents is Python on the Beam portability layer with outbox-based effects — different language runtimes, different effect models (their inline execution does not pay our re-injection round-trip; our outbox buys effector-side dedup and pipeline-external effects they price differently); checkpointing/state backends differ; and the beam-agents leg runs on the Flink *runner* for maximum comparability, which is itself a portability layer Flink Agents does not pay for. Where a difference structurally favors one side, the report says which side and why.
- **No cherry-picking.** Every completed run of the final configuration is reported — percentile tables, not best-of. Unfavorable results ship. If beam-agents loses a metric, the report says so and, where the loss is the known cost of a design decision (e.g., re-injection latency bought for effectively-once effects), links the decision.
- **Reproducibility.** Pinned versions of both frameworks, Flink, Beam, and Python; committed run configurations and environment manifest; enough detail that a third party can re-run both legs.

Rejected alternative: a multi-scenario "benchmark suite shootout" published as a marketing page. It maximizes surface for methodological error, invites cherry-picking by construction, and the credibility cost of one unfair chart exceeds the value of ten favorable ones.

### D4. The report is a versioned release artifact, not a website

The report lives at `docs/benchmarks/0.3.0-vs-flink-agents.md`, versioned in the repository, named by the release, linked from the 0.3.0 changelog section and release notes. Numbers are frozen at publication: the report states the exact commits/tags measured, and is never edited to reflect later performance changes — a later release publishes a later report. This keeps every published number attributable to a reproducible configuration and keeps the comparison honest over time (no silent re-benchmarking after a favorable optimization).

### D5. The release gate is a checklist evaluated at a named commit, all-or-nothing

The gate conditions — nine M2 archives, benchmark regression gates green, conformance matrix green on both legs with no new skips, release-blocking feedback closed — are evaluated together against the release-candidate commit, and recorded (with run links) in the release notes. Partial shipping ("tag now, fix the red conformance cell in 0.3.1") is rejected: the semantics/conformance tiers are documented in `openspec/project.md` as gating every release and never getting skipped or marked flaky, and a release milestone is exactly where that documentation is either enforced or revealed to be fiction. If a gate condition cannot be met, the release slips; the gate does not bend.

## Risks / Trade-offs

- **The comparison is contestable no matter how carefully it is done.** Cross-framework benchmarks always are. Mitigation is D3's structure — disclosed methodology, disclosed non-equivalences, reproducible configs — plus framing the report around *our own* absolute budget (p50 < 15 ms / p99 < 60 ms) so the primary claim is "we meet our stated budget under this workload", with the Flink Agents numbers as context rather than a scoreboard. Residual risk accepted: a competitor rebuttal is survivable if the methodology is airtight; it is fatal only if we cherry-picked.
- **Flink Agents is a moving target.** Announced in 2025 and evolving quickly; the measured version may look stale within months. Mitigated by pinning and dating everything (D4) and never claiming more than "as of these versions". Not mitigated further on purpose — chasing their trunk is the continuous-benchmarking program we declared a non-goal.
- **Feedback volume is unknown.** Zero items makes the workstream trivially green (recorded as such, per D2); a flood of blocking-grade items slips the release, which is the correct outcome under D5 — the rubric bounds *what can block*, not *how long blocking takes*.
- **Nine upstream archives is a wide dependency fan-in.** Any M2 change slipping slips 0.3.0. Accepted: that is what a milestone release *is*. The mitigation is sequencing visibility (the gate checklist names each change and its state), not gate erosion.
- **A release-process defect discovered mid-release.** D1 forbids fixing it inline; the migration plan's escape hatch is to pause, land a follow-up change against C25's capability, then resume. Slower than patching in place, but keeps the process spec true.

## Migration Plan

1. **Gate assembly (pre-RC).** Confirm all nine M2 changes are archived; run the C33 regression gates and the full conformance matrix (offline leg via `ci`'s semantics selection, Flink leg via `make test-conformance-flink`) against the candidate commit; record run links in the gate checklist.
2. **Feedback close-out.** Triage any outstanding partner items per D2; verify every blocking-bucket change folder is archived; freeze the disposition table into the release notes (explicitly recording "none received" if empty).
3. **Comparison run.** Execute the paired benchmark legs in the dedicated environment; commit configs, environment manifest, and `docs/benchmarks/0.3.0-vs-flink-agents.md`; internal review against D3's checklist (versions pinned, non-equivalences enumerated, all runs included) before it merges.
4. **Ship.** Execute the C25 process: bump `pyproject.toml` to `0.3.0`, write the changelog section (nine M2 changes, feedback dispositions, link to the report), tag, publish.
5. **Rollback.** Before the tag: nothing to roll back — slip the date. After a bad tag/publish: follow C25's defined remediation (yank/supersede with a patch release); the benchmark report, being versioned and frozen, needs no rollback — a correction ships as an errata note in a subsequent release, never as a silent edit.

No state, wire, or API migration is involved; the only `src/`-adjacent edit is the version string.

## Open Questions

- **Which Flink Agents version to pin?** Whatever is the latest stable release when step 3 begins; if the project has no stable-release discipline yet, pin the exact commit and say so in the report. Decided at execution time, recorded in the report — nothing else in this change depends on it.
- **Does a C33 scenario match closely enough?** If the closest scenario still requires meaningful adaptation to express in Flink Agents, the adaptation is documented per D3; if *no* fair pairing exists, the report ships as a beam-agents-only budget report with a written explanation — a worse outcome than a comparison, but better than a forced one. Judged during step 3's internal review.
- **Is the comparison repeated per-release?** Deliberately open. The answer depends on what the 0.3.0 report costs to produce and what it earns; a follow-up change proposes a cadence if the answer is yes.
- **Where does the dedicated benchmark environment live?** C33 owns the harness's execution story for our own gates; the *paired* environment (both stacks, controlled hardware) is provisioned during step 3 and described in the report. If C33's environment already suffices, reuse it.
