## Context

1.0.0 closes Phase M4. Everything hard about it lives in the four sibling changes it depends on: `add-effector-security` (intent signing), `add-1-0-api-freeze` (public-surface snapshot + deprecation policy), `add-state-guarantees` (documented state compatibility + nightly `--update` compat test), and `promote-spark-runner` (a time-gated promotion decision). The release machinery — version bump, changelog automation, tag, publish workflow, versioning policy — was established by `add-0-1-0-release` and has been exercised at each milestone since, most recently `add-0-5-0-release`. What is genuinely new at 1.0 is the meaning of the number: after this tag, the deprecation policy governs all public-surface changes and the state-migration guarantees govern all wire/state changes. `pyproject.toml` still reads `version = "0.0.0"` in this worktree; the intermediate bumps belong to their own release changes.

One dependency is structurally different from the other three. Spark promotion is time-gated on four consecutive green weeks of its nightly leg — wall-clock time no amount of engineering can compress — and `openspec/project.md` already scopes 1.0 runner support as "DirectRunner, Dataflow, Flink. Spark is best-effort." The design question this change actually has to decide is how the 1.0 gate treats a Spark promotion that is legitimately still counting weeks when everything else is done.

## Goals / Non-Goals

**Goals:**
- State the 1.0 gate as a checkable contract: exactly which conditions block the tag, verifiable from CI signals and archive state, not judgment calls at release time.
- Reuse the C25 release machinery unchanged; the only 1.0-specific artifact is the gate.
- Make the post-1.0 regime explicit: which policies govern which kinds of change from the moment the tag exists.
- Resolve the lagging-Spark question with a recorded decision rather than an ambiguity discovered at release time.

**Non-Goals:**
- Building or modifying any release mechanics (workflow, changelog tooling, versioning policy) — all C25's, referenced by name.
- Restating the requirements of the four M4 changes; the gate requires their *archival*, and their own specs define what that took.
- Promising Spark support. The gate requires a recorded decision, not a particular outcome.
- Any post-1.0 roadmap content (1.1 planning, LTS policy, support windows).

## Decisions

### D1. The gate is archive-state plus named green signals — nothing softer

The tag is blocked on: (a) all four M4 changes implemented and moved to `openspec/changes/archive/`; (b) the C45 API-freeze snapshot test green; (c) the C46 documented state guarantees in place and its nightly `--update` compat test green on the latest scheduled run; (d) C44 effector intent signing shipped with its rollout complete (verification enforced, not merely available); (e) the C47 Spark decision recorded (per D2). Archive state was chosen over "merged" because in this repo archival is the moment a change's delta lands in the main specs — an unarchived change is by definition not yet part of the promised surface. Each signal is a thing a release runner can check mechanically; the checklist in `tasks.md` §2 mirrors this list one-to-one so the gate cannot drift from its executable form.

### D2. A lagging Spark promotion does not block 1.0 — but an unrecorded one does

Spark's promotion is time-gated on four consecutive green nightly weeks and may still be mid-window when (a)–(d) are done. The gate treats that as follows: **1.0.0 may ship with Spark promotion explicitly deferred, provided the deferral is recorded — the `promote-spark-runner` decision written down either way, and the roadmap noting why Spark remains best-effort.** Waiting is rejected for three reasons. First, 1.0 is an API-and-semantics promise, and Spark does not participate in it: the constitution (`openspec/project.md`, "Supported runners v1.0") already scopes Spark as best-effort at 1.0, so deferral is consistent with the promise as written, while promotion would be a bonus on top. Second, the block would be pure wall-clock — holding an API-stability promise hostage to calendar weeks of a best-effort runner's nightly leg buys no additional stability for anyone. Third, a flaky week would reset the window and could push 1.0 indefinitely on a signal unrelated to what 1.0 guarantees. The symmetric hazard — shipping with the question silently unanswered — is what the gate does prohibit: an unrecorded decision means the 1.0 announcement cannot state Spark's status truthfully, so condition (e) requires the decision *exist*, not that it be "promote". If the window completes before the tag, Spark ships promoted and the changelog says so; the gate is indifferent between the outcomes and strict about the recording.

### D3. 1.0.0 is a regime change, not a feature release

The version number's content is the pair of standing policies it activates: from `v1.0.0`, every public-surface change is governed by the C45 deprecation policy and every wire/state change by the C46 migration guarantees. The spec delta states this as a requirement rather than leaving it to the changelog, because it is the one behavior of this change that outlives release day — future proposals must be able to cite a spec, not a blog post, when a breaking change is rejected. No new enforcement is added here: the enforcement mechanisms (snapshot test, compat test, golden blobs) are C45's and C46's; this change makes the *promise* normative.

## Risks / Trade-offs

- **A dependency slips and the gate holds 1.0 indefinitely.** Intended behavior, not a failure mode — the gate exists to make that hold explicit. Mitigation is scoping: only (a)–(e) block; nothing else may be appended at release time without amending this change.
- **Spark deferral reads as a broken promise.** Mitigated by D2's recording requirement and by the constitution having scoped Spark as best-effort at 1.0 all along; the changelog states the status and the roadmap states the why.
- **The nightly `--update` compat signal is stale at tag time** (nightlies run daily; the tag is cut at an arbitrary hour). The checklist requires the *latest scheduled run* green and permits a manual re-trigger if the last run predates a state-touching merge. Cheap, and closes the only timing gap in (c).
- **Minimal proposal under-specifies.** Accepted deliberately: every mechanism this change could specify is owned by C25 or the four M4 changes, and duplicating their text here creates the archive-order conflict problem other changes in this repo have had to manage. The gate references; it does not restate.

## Migration Plan

1. Wait for gate conditions (a)–(e) of D1; track them via the `tasks.md` §2 checklist.
2. Bump `pyproject.toml` to `1.0.0`; write the changelog entry per the C25 process, headlining the stability regime and Spark's recorded status.
3. Tag `v1.0.0` and publish via the C25 release workflow, unchanged.
4. Verify the published artifact installs (`pip install beam-agents==1.0.0`) with each extra (`effector`, `langgraph`, `otlp`) resolvable.
5. Archive this change; from this point the D3 regime is normative for all future proposals.

Rollback: before publish, delete the tag and revert the bump — nothing else moved. After publish, there is no rollback of the promise; a defective 1.0.0 is followed by 1.0.1 under the now-active policies, which is the regime working as designed.

## Open Questions

- **Where exactly is the Spark decision "recorded"?** `promote-spark-runner` (C47) owns its decision record's location; this gate requires the record exist and the roadmap carry the rationale if deferred. If C47 lands without designating a location, the changelog entry plus roadmap note satisfies (e) — flagged here so the two proposals cannot each assume the other chose.
- **Does 1.0.0 ship any deprecations already in flight?** If C45's freeze marked anything deprecated pre-1.0, the changelog must list it with its removal horizon under the deprecation policy. Depends on C45's final content; resolved when it archives.
