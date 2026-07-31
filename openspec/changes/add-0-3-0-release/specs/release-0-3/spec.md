## ADDED Requirements

### Requirement: 0.3.0 ships only after the M2 release gate is fully satisfied

The 0.3.0 release SHALL NOT be tagged or published until every condition of the release gate holds simultaneously at the release-candidate commit:

- All nine M2 changes are archived: C26 `add-vllm-provider`, C27 `add-adaptive-batching`, C28 `add-token-budgets`, C29 `add-longterm-memory-stores`, C30 `add-compaction-strategies`, C31 `add-adk-adapter`, C32 `add-state-schema-migration`, C33 `add-benchmark-harness`, C34 `add-hot-key-sharding-guidance`.
- The benchmark regression gates established by C33 `add-benchmark-harness` are green: runtime overhead p50 < 15 ms and p99 < 60 ms per activation, excluding LLM/tool time.
- The adapter conformance matrix (`tests/conformance/`) is green on both the DirectRunner and Flink legs, with no cell newly skipped relative to the previous release.
- Every design-partner feedback item triaged as release-blocking has its fix change archived.

The gate SHALL be evaluated as a whole and recorded — each condition with evidence (archive state, CI/benchmark run links) — in the 0.3.0 release notes. Partial satisfaction MUST NOT be worked around by tagging first and fixing after; an unmet condition slips the release.

#### Scenario: An unarchived M2 dependency blocks the release

- **WHEN** the release gate is evaluated and at least one of the nine M2 changes is not yet archived
- **THEN** the gate fails, no 0.3.0 tag is created, and the recorded gate checklist names the unarchived change as the blocking condition

#### Scenario: A benchmark regression blocks the release

- **WHEN** the C33 regression gates report runtime overhead at or above p50 15 ms or p99 60 ms per activation (excluding LLM/tool time) at the release-candidate commit
- **THEN** the gate fails and 0.3.0 is not tagged until a candidate commit brings both percentiles back under budget

#### Scenario: A red or newly skipped conformance cell blocks the release

- **WHEN** the conformance matrix run at the release-candidate commit has a failing cell, or a cell is skipped that was not a declared skip at the previous release
- **THEN** the gate fails and the release does not proceed until the matrix is green with no new skips

#### Scenario: A fully green gate opens the release

- **WHEN** all nine M2 changes are archived, the benchmark regression gates and conformance matrix are green, and all release-blocking feedback fixes are closed at the same candidate commit
- **THEN** the gate passes, the evaluated checklist with evidence links is recorded in the release notes, and the release proceeds

### Requirement: Design-partner feedback is triaged through a documented rubric with recorded dispositions

Every design-partner feedback item received before the 0.3.0 gate evaluation SHALL be triaged into exactly one of two buckets:

- **Release-blocking fix**, if and only if the item evidences a violation of a correctness invariant documented in `openspec/project.md`, loss or corruption of user data or state, a break in pipeline-`--update` state compatibility, or a security defect. Each such item SHALL get its own OpenSpec change folder, and its fix MUST be archived before the release gate can pass.
- **Follow-up OpenSpec change**, for every other item (feature requests, ergonomics, documentation gaps, performance short of the stated budget). Each SHALL be captured as a proposed change or roadmap entry targeting a post-0.3.0 milestone.

Each item's disposition — bucket, rationale, and a link to the resulting change folder or tracking entry — SHALL be recorded in a disposition table in the 0.3.0 release notes. If no feedback items were received, the release notes SHALL state that explicitly.

#### Scenario: An invariant-violation report becomes release-blocking

- **WHEN** a design partner reports behavior that violates a documented correctness invariant, such as a replayed bundle producing non-identical intent IDs
- **THEN** the item is triaged release-blocking, a dedicated OpenSpec change is opened for the fix, and the 0.3.0 gate does not pass until that change is archived

#### Scenario: A feature request becomes a follow-up change

- **WHEN** a design partner requests new functionality that does not evidence an invariant violation, data loss, state-compat break, or security defect
- **THEN** the item is triaged as a follow-up OpenSpec change targeting a post-0.3.0 milestone, its disposition and rationale are recorded, and it does not block the release

#### Scenario: Zero feedback is recorded, not omitted

- **WHEN** the gate is evaluated and no design-partner feedback items were received during the 0.3.0 cycle
- **THEN** the release notes state explicitly that no feedback was received, rather than omitting the disposition table

### Requirement: 0.3.0 is executed through the established release process without modification

The 0.3.0 release SHALL be executed exactly as the release process established by `add-0-1-0-release` defines: the `pyproject.toml` version SHALL be bumped to `0.3.0`, the changelog SHALL gain a 0.3.0 section enumerating the nine M2 changes, the feedback dispositions, and a link to the published benchmark comparison report, and the release SHALL be tagged and published through that process's defined mechanics. This change SHALL NOT alter the release process itself; a process defect discovered during execution SHALL be fixed through a separate change against the release-process capability before the release resumes.

#### Scenario: The shipped version and changelog match the milestone

- **WHEN** the 0.3.0 tag is created
- **THEN** the tagged commit has `pyproject.toml` version `0.3.0` and a changelog section that enumerates the nine M2 changes, records the feedback dispositions, and links the benchmark comparison report

#### Scenario: A process defect pauses rather than forks the process

- **WHEN** executing the release exposes a defect in the release process itself
- **THEN** the release pauses, the fix lands as a separate change against the release-process capability, and 0.3.0 resumes under the corrected process rather than shipping through an ad-hoc variant
