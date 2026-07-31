## Why

0.3.0 is the release that closes out the M2 batch. Nine feature changes — C26 `add-vllm-provider`, C27 `add-adaptive-batching`, C28 `add-token-budgets`, C29 `add-longterm-memory-stores`, C30 `add-compaction-strategies`, C31 `add-adk-adapter`, C32 `add-state-schema-migration`, C33 `add-benchmark-harness`, C34 `add-hot-key-sharding-guidance` — land between the 0.1.x line and this milestone, and none of them reaches a user until a version is cut, tagged, and published through the release process C25 `add-0-1-0-release` established. A release milestone is lighter than a feature change, but it is not zero-spec: what may ship, when it may ship, and what must be published alongside it are behavioral requirements with failure modes (shipping past a red gate, silently dropping partner feedback, publishing a benchmark comparison that flatters us).

Beyond the mechanical ship, 0.3.0 carries two deliverables of its own:

1. **Design-partner feedback fixes.** By 0.3.0 the runtime has been in design partners' hands across the 0.1.x/0.2.x line. No concrete feedback items exist at proposal time — so this change specs the *process*, not the fixes: every item is triaged through a documented rubric into either a release-blocking fix or a follow-up OpenSpec change, and every disposition is recorded. Without a rubric, "fix partner feedback before release" degenerates into either an unbounded release blocker or a silently ignored inbox.
2. **A published benchmark report versus Apache Flink Agents.** `openspec/project.md` names Apache Flink Agents (Flink's agent framework, announced 2025) as the direct competitor and states our differentiation (runner portability, bring-your-own-framework adapters, outbox-based effectively-once vs. their inline durable execution). 0.3.0 is the first release with a benchmark harness (C33) capable of backing that claim with numbers instead of prose. The comparison must be honest about where it is apples-to-oranges — a dishonest benchmark is worse for a runtime project's credibility than no benchmark — so methodology disclosure is a spec requirement, not a courtesy.

## What Changes

- **Ship 0.3.0 through the C25 release process.** Version bump in `pyproject.toml` to `0.3.0` (the file reads `0.0.0` today; C25 owns moving it onto the 0.1.x line and defines the changelog format, tagging, and publish workflow this change reuses verbatim). Changelog section enumerating the nine M2 changes and the triaged feedback fixes; annotated tag; publish. No release-process mechanics are added or altered here.
- **A release gate that is checked, not assumed.** 0.3.0 may not ship until: all nine M2 changes above are archived (implemented, gated, merged, specs synced); the C33 benchmark regression gates are green against the release candidate — runtime overhead p50 < 15 ms / p99 < 60 ms per activation excluding LLM/tool time, the release-blocking budget `openspec/project.md` already states; and the adapter conformance matrix (`tests/conformance/`, seven scenarios × registered adapters × DirectRunner/Flink legs) is fully green with no new skips.
- **A design-partner feedback triage workstream.** Intake of all partner-reported items, each triaged through the rubric in `design.md` (D2) into *release-blocking fix* (correctness-invariant violations, data loss, state-compat breaks, security) or *follow-up OpenSpec change* (everything else), with dispositions recorded in the release notes. Release-blocking fixes follow the normal spec-driven workflow — they get their own change folders; this change tracks that they exist and are closed before the gate.
- **A published benchmark comparison report.** One document, versioned with the release under `docs/benchmarks/`, comparing beam-agents against an equivalent Apache Flink Agents scenario: the closest-matching C33 harness workload on our side, its nearest reproducible equivalent on theirs, pinned versions, disclosed environment, full methodology including every dimension where the comparison is not like-for-like, and all completed runs reported — including unfavorable ones.

## Capabilities

### New Capabilities

- `release-0-3`: the 0.3.0 milestone contract — the gate conditions that must hold before shipping, the design-partner feedback triage process, and the execution of the release through the C25-established process.
- `benchmark-comparison-report`: the published beam-agents vs. Apache Flink Agents comparison — workload selection, methodology-honesty obligations, and publication alongside the release.

### Modified Capabilities

None. The release *process* capability belongs to C25 `add-0-1-0-release` and is consumed here unchanged — this change instantiates that process for one version rather than amending it. The benchmark harness capability belongs to C33 and is likewise consumed as-is (this change runs its scenarios; it does not add any). Keeping both deltas additive means this change cannot conflict with either sibling's spec text at archive time.

## Impact

**Depends on** — the full M2 batch, all archived before the gate opens: C26 `add-vllm-provider`, C27 `add-adaptive-batching`, C28 `add-token-budgets`, C29 `add-longterm-memory-stores`, C30 `add-compaction-strategies`, C31 `add-adk-adapter`, C32 `add-state-schema-migration`, C33 `add-benchmark-harness` (supplies the scenarios and the p50/p99 regression gates this release blocks on), C34 `add-hot-key-sharding-guidance`. Also builds on C25 `add-0-1-0-release`, which defines the versioning, changelog, tag, and publish mechanics this change executes.

**New code:** none in `src/`. New artifacts: `docs/benchmarks/0.3.0-vs-flink-agents.md` (the comparison report) plus the committed run configurations/environment manifest it references; the 0.3.0 changelog section; a feedback-disposition table in the release notes. Any release-blocking partner fixes are separate change folders, not part of this one.

**Modified code:** the version bump touches three files, not one (amended — see tasks.md Revision 1): `pyproject.toml`'s version field (`→ 0.3.0`); `uv.lock`, which records the project's own version and which `docs/releasing.md` already says "always comes with" a bump; and `docs/yaml.md`'s `beam-agents==X.Y.Z` provider pin, which `tests/yaml/test_docs_example.py` asserts equals the installed version. The changelog file C25 introduces gains a section. No runtime, wire-schema, or state-schema changes originate from this change.

**CI/build:** no new workflows. The C25 release workflow runs once for the tag; the C33 benchmark regression job and the existing `ci`/`integration`/`quality` checks are consumed as gate inputs. The comparison run against Flink Agents executes outside CI (dedicated benchmark environment, documented in the report) because CI runners cannot host both stacks under controlled, comparable conditions.

**Gates:** all nine M2 changes archived; C33 benchmark regression gates green (overhead p50 < 15 ms / p99 < 60 ms per activation, excluding LLM/tool time); conformance matrix green on both legs with no new skips; all triaged release-blocking feedback fixes closed; `make lint`, `make type`, `make test-unit`, coverage ratchet, `openspec validate add-0-3-0-release --strict`.
