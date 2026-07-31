## 1. Tests (written first, must fail for the right reason)

This is a release-milestone change: its deliverables are gate evaluations, process artifacts, and a published report, so the automated-test section is deliberately thin. The regression tests that give the gate its teeth already exist upstream — C33 `add-benchmark-harness` owns the p50/p99 overhead gates and the conformance matrix predates this change — and re-implementing them here would duplicate gate ownership. What this change adds as executable checks is release-artifact consistency; the process scenarios (gate blocking, triage dispositions, methodology review) are verified by the checklist steps in sections 2–5, each traceable to a named spec scenario.

- [x] 1.1 Add a release-artifact consistency check (test or release-checklist script per the C25 mechanics) asserting the tagged commit has `pyproject.toml` version `0.3.0` and a changelog section that names all nine M2 changes and links `docs/benchmarks/0.3.0-vs-flink-agents.md` — implements *The shipped version and changelog match the milestone*; must fail while the version reads `0.0.0`/`0.2.x` and the section is absent — `tests/release/test_release_0_3_0.py` (8 tests): version, `uv.lock` agreement, the nine-change enumeration, the report link, the disposition subsection, the recorded gate checklist, and "a published fragment is not still pending". The nine names are parsed out of this change's own `proposal.md` (`m2_changes()`), not transcribed, and each is asserted to exist as a live-or-archived change folder — the C42 sourcing discipline. Verified red first: 7 of 8 failed on `version == 0.1.0` and the absent section (the ninth, the folder-existence guard, passed, which is what proves the parse was real)
- [x] 1.2 Add a report-presence/shape check asserting `docs/benchmarks/0.3.0-vs-flink-agents.md` exists, names the C33 scenario used, and contains a methodology section pinning the measured versions — implements the checkable core of *The report ships with the release* and *The methodology section enumerates non-equivalences*; must fail while the report does not exist — `tests/docs/test_benchmark_comparison_report.py` (10 tests). Beyond presence/shape it carries the honesty half: the named scenario is checked against `scripts/bench_gate.py`'s own `EXPECTED_RESULTS`/`GATED_BENCHMARK` (a renamed benchmark fails here, not silently); every results-table data row must carry the literal `pending (CI hardware)`; and no figure with a unit may appear that `openspec/project.md`, `docs/benchmarks.md`, `benchmark-baseline.toml`, or `benchmarks/_harness.py` does not already state. Verified red first (report absent), then verified to have teeth: injecting `| beam-agents … | 3.2 ms | 11.4 ms | 1000 | pass |` into a results row turns both honesty tests red

## 2. Release gate assembly

- [x] 2.1 Verify all nine M2 changes are archived — C26 `add-vllm-provider`, C27 `add-adaptive-batching`, C28 `add-token-budgets`, C29 `add-longterm-memory-stores`, C30 `add-compaction-strategies`, C31 `add-adk-adapter`, C32 `add-state-schema-migration`, C33 `add-benchmark-harness`, C34 `add-hot-key-sharding-guidance` — recording each archive reference in the gate checklist (*An unarchived M2 dependency blocks the release*) — **verified, and the answer is no**: all nine are implemented, gated, and merged, but every one is still a live folder under `openspec/changes/` (`openspec/changes/archive/` holds only the nine pre-0.1.0 changes). Recorded in the release notes' gate table as `pending (archival)`, which is the condition-not-met outcome this task exists to detect — the gate stays shut and `v0.3.0` is not tagged
- [ ] 2.2 Run the C33 benchmark regression gates at the release-candidate commit and record the run link; confirm overhead p50 < 15 ms and p99 < 60 ms per activation excluding LLM/tool time (*A benchmark regression blocks the release*) **(blocked: needs CI hardware)** — `benchmark-baseline.toml`'s `[medians_ms]` is deliberately unseeded, and `docs/benchmarks.md` forbids seeding it from developer hardware; the gate is rendered by the nightly `bench` job on a GitHub-hosted runner. Recorded as `pending (CI hardware)` in the gate table
- [ ] 2.3 Run the conformance matrix at the candidate commit — offline leg via the required `ci` semantics selection, Flink leg via `make test-conformance-flink` — and confirm every cell is green with no cell newly skipped versus the previous release (*A red or newly skipped conformance cell blocks the release*) **(blocked: needs CI run)** — the offline half is green here (`make test-semantics-offline`: 72 passed, 5 skipped — all declared, pre-existing skips); the Flink leg needs the docker compose stack and runs in the `integration` workflow. Recorded as `pending (CI run)` in the gate table
- [x] 2.4 Assemble the recorded gate checklist (condition, evidence link, pass/fail) for the release notes; if any condition fails, stop here — the release slips, per design D5 (*A fully green gate opens the release*) — assembled as the `### Release gate` table in the `0.3.0` `CHANGELOG.md` section: five conditions, each with its evidence and verdict (two `pass`, three pending). Its header states the consequence plainly — **the gate is not fully green, so `v0.3.0` is not tagged** — and D5's refusal of partial shipping is restated there rather than assumed. Pinned by `test_changelog_section_records_the_release_gate_checklist`

## 3. Design-partner feedback triage

- [x] 3.1 Collect all design-partner feedback items received during the 0.1.x/0.2.x cycle into a single intake list — **the intake list is empty, and the reason is checkable**: `add-0-1-0-release` tasks 5.1/5.2/5.4 are still unchecked (blocked on the one-time PyPI project registration and trusted-publisher binding), so no design partner has run a released build of the 0.1.x line
- [x] 3.2 Triage each item through the D2 rubric: release-blocking (invariant violation, data loss/corruption, `--update` state-compat break, security) versus follow-up OpenSpec change; record bucket and rationale per item (*An invariant-violation report becomes release-blocking*, *A feature request becomes a follow-up change*) — no items to bucket. The rubric itself is written into the release notes verbatim (both buckets, the invariant-anchored blocking bar, and the note that the bar is deliberately not a severity adjective), so the process is durable past this release rather than living only in `design.md`
- [x] 3.3 For each release-blocking item, open (or confirm) a dedicated OpenSpec change folder for the fix and track it to archive before gate evaluation — vacuous: the blocking bucket is empty. Recorded in the gate table as the one condition that genuinely passes on its merits
- [x] 3.4 For each follow-up item, file the proposed change or roadmap entry targeting a post-0.3.0 milestone and link it from the disposition — vacuous: the follow-up bucket is empty
- [x] 3.5 Freeze the disposition table into the release notes; if the intake list is empty, record "no design-partner feedback received" explicitly (*Zero feedback is recorded, not omitted*) — the `### Design-partner feedback` subsection carries the rubric plus a one-row table stating no items were received and why, closing with "An empty table is a disposition. An absent one would be a process failure." Pinned by `test_changelog_section_records_the_feedback_dispositions`

## 4. Benchmark comparison vs Apache Flink Agents

- [x] 4.1 Select the C33 scenario closest to a workload Apache Flink Agents expresses idiomatically (event-triggered, keyed, stateful agent with tool calls) and record the selection rationale (*The workload comes from the C33 scenario set*) — **`overhead_50ms`** from `benchmarks/bench_overhead_tiers.py`, with `noop_throughput` as the throughput secondary. Rationale recorded in the report's *The paired workload* section: it is the tier the release budget is actually gated on (so the comparison cannot be metric-shopped), it is an event-triggered keyed stateful activation both systems express natively, and its wall-time-minus-provider-latency definition transfers to the other side unreinterpreted. `suspension_roundtrip` is *deliberately excluded* from the pairing and reported unpaired — Flink Agents has no re-injection round trip, so a head-to-head row there would measure the presence of a feature and call it a loss. Pinned by `test_report_names_a_real_c33_scenario`, which checks the name against `bench_gate.EXPECTED_RESULTS`
- [ ] 4.2 Implement the paired workload on Apache Flink Agents using that project's recommended APIs, pinned to its latest stable release (or an exact commit, if no stable release exists — design Open Questions) **(blocked: needs paired benchmark environment)** — the Flink Agents version row in the report's pin table reads `pending — pinned at run time`, and the report states why no run configuration is committed yet: writing one against APIs that have never been executed would be a fabricated artifact, which is worse than a pending one
- [ ] 4.3 Configure both legs with a scripted fake model of equal cost so the measurement isolates runtime overhead (*Model latency is excluded from the comparison*) **(blocked: needs paired benchmark environment)** — the rule is written into the report's methodology (`FakeLLM` on our side, the closest achievable stub on theirs, equal configured cost, "excluding LLM and tool time") and is pinned by `test_report_states_the_equal_cost_fake_model_rule`; configuring theirs requires the environment 4.2 blocks on
- [ ] 4.4 Provision the paired benchmark environment (reusing C33's if sufficient), run both legs, and commit run configurations plus the environment manifest **(blocked: needs CI hardware)** — C33's environment is a GitHub-hosted runner for our leg only; the paired host carrying both stacks does not exist yet. The report describes what the manifest must capture and how the beam-agents leg is reproduced (`make bench` then `make bench-gate`, pinned sampling, no `BENCH_ARGS`)
- [x] 4.5 Write `docs/benchmarks/0.3.0-vs-flink-agents.md`: percentile latency (≥ p50/p99) and throughput for both legs, framed around beam-agents' own overhead budget, with the methodology section pinning all versions and enumerating the language-runtime, effect-model, and state-backend non-equivalences and which side each favors — written, with **every measurement cell reading `pending (CI hardware)`** and the reason stated in the report's first section rather than buried (see Revision 2). Everything that does not require a run is final: the pairing, the version pins that exist (beam-agents 0.3.0, Beam 2.72.0, Python 3.11, Flink 1.19 and the Beam Flink job server, both digest-pinned in `docker/compose.yaml`), the equal-cost fake-model rule, the no-cherry-picking rule, the freeze rule, and a five-row non-equivalence table — language runtime, effect model, state backend/checkpointing, portability layer, measurement surface — each stating which side it structurally favors, including the two where the honest answer is "Flink Agents" and the one where it is "direction unknown"
- [ ] 4.6 Include every completed run of the final configuration, unfavorable results at equal prominence; if review concludes no fair pairing exists, ship the beam-agents-only budget report with a written explanation instead (*An unfavorable result is published unfiltered*, *No fair pairing exists*) **(blocked: needs CI hardware)** — zero runs of the final configuration are complete, so there is nothing yet to include or filter. The obligation is written into the report (*No cherry-picking*: all completed runs reported, unfavorable rows at equal prominence, configuration changes restart the run set) and the *No fair pairing exists* escape hatch is recorded under *What is still open*
- [x] 4.7 Internal review of the report against the design D3 checklist (versions pinned, non-equivalences enumerated, all runs included, reproducible) before merge (*The methodology section enumerates non-equivalences*) — reviewed against D3 item by item. Versions pinned: yes for every component that exists, with the one unpinnable component marked `pending` rather than omitted. Non-equivalences enumerated with direction: yes, five dimensions. Reproducible: our leg yes (exact commands and pinned sampling); their leg deferred with 4.2. All runs included: deferred with 4.6 — there are none. Three of the four items are additionally mechanized in `tests/docs/test_benchmark_comparison_report.py`, so the review outcome cannot silently rot

## 5. Ship 0.3.0

- [x] 5.1 Bump `pyproject.toml` version to `0.3.0` per the C25 `add-0-1-0-release` process — bumped from `0.1.0`; `uv lock` refreshed (one line: `beam-agents v0.1.0 -> v0.3.0`), and `uv sync --locked --group lint --group typecheck --group test` re-confirmed green afterwards, which is the property `docs/releasing.md` says the lock refresh exists to preserve. The bump also required `docs/yaml.md`'s package pin — see Revision 1
- [x] 5.2 Write the 0.3.0 changelog section: the nine M2 changes, the feedback disposition table (or its explicit "none received"), and the link to the benchmark comparison report — `make changelog VERSION=0.3.0` assembled the eight pending fragments into a dated section (four `added`, two `docs`, consuming each exactly once) and the milestone record was curated on top of it: a *The M2 batch* table enumerating all nine changes with what each delivered, plus the `### Benchmarks`, `### Release gate`, and `### Design-partner feedback` subsections
- [x] 5.3 Confirm the section-1 release-artifact checks now pass at the candidate commit — `pytest tests/release/test_release_0_3_0.py tests/docs/test_benchmark_comparison_report.py`: 18 passed
- [ ] 5.4 Tag and publish through the C25 process; if executing it exposes a process defect, pause and land the fix as a separate change against the release-process capability before resuming (*A process defect pauses rather than forks the process*) **(blocked: needs release infra)** — and independently blocked by the gate itself: 2.1, 2.2 and 2.3 are unmet, so under D5 the release slips rather than ships. One process defect *was* exposed and is routed per D1 rather than patched inline — see Revision 1
- [ ] 5.5 Verify the published artifacts: tag present, package version `0.3.0`, changelog and report reachable from the release notes; record post-publication that `docs/benchmarks/0.3.0-vs-flink-agents.md` is frozen (*The published report is frozen*) **(blocked: needs release infra)** — requires 5.4. The freeze rule is already stated in the report itself (*This report is frozen*) and pinned by `test_report_states_the_freeze_rule`, so the post-publication record is an attestation, not the rule's only home

## 6. Gates

- [x] 6.1 `make lint` — `ruff check` clean, `ruff format --check` clean (366 files)
- [x] 6.2 `make type` — `mypy --strict`: no issues in 360 source files
- [x] 6.3 `make test-unit` — 1818 passed, 4 skipped (all environment-declared), 196 deselected; total coverage 95.44%. One genuine failure surfaced by the bump and fixed: see Revision 1
- [x] 6.4 `make coverage-ratchet` (no coverage movement expected — this change adds no `src/` code beyond the version string — but the gate runs regardless) — "branch coverage 91.18% is at baseline", exactly as predicted; no baseline edit needed
- [x] 6.5 `uv run pre-commit run --all-files` — all ten hooks pass, including `Changelog fragment required for src/ edits` (this change touches no `src/`, so no fragment is due — and adding one would have been wrong: it would publish in the *next* release, not this one)
- [x] 6.6 `openspec validate add-0-3-0-release --strict` — "Change 'add-0-3-0-release' is valid" (re-run after the Revision edits below)
- [x] 6.7 `make test-semantics-offline` (the offline half of the release gate roster `release.yml` re-runs, and the offline leg of gate condition 2.3) — 72 passed, 5 skipped

## Revisions

Numbered corrections to the planning artifacts, made because implementation
proved them wrong.

### Revision 1 — a version bump touches three files, not one; `proposal.md`'s Impact named one

`proposal.md` said, under **Modified code**: "`pyproject.toml` version field
only". That is false in two ways, and the second one is a real (small) defect in
the release process rather than in this change.

1. **`uv.lock`.** `docs/releasing.md` already states that the lock records the
   project's own version and that "a bump therefore always comes with
   `uv lock`" — so the Impact section simply contradicted the process it says it
   consumes verbatim. `uv lock` was run; the diff is one line.
2. **`docs/yaml.md`.** The Beam YAML provider block pins the installable package
   as `beam-agents==X.Y.Z`, in two places, and
   `tests/yaml/test_docs_example.py::test_the_documented_package_pin_matches_the_shipped_version`
   asserts that pin equals `importlib.metadata.version("beam-agents")`. The bump
   to `0.3.0` therefore turned `make test-unit` red until the doc was updated.
   That test is correct and was not weakened; the pin was updated.

`proposal.md`'s Impact section is amended to name all three.

The second item is also a gap in C25's release checklist: `docs/releasing.md`'s
"The release PR" steps are (1) bump, (2) `uv lock`, (3) `make changelog`,
(4) optional local build — with no step for version-coupled references
elsewhere in the tree. Per design D1, this change does **not** patch that
checklist inline: a release milestone never edits the release process. The
correction is routed to a follow-up change against C25's `release-process`
capability, which is exactly the escape hatch D1 and the migration plan describe
("pause, land a follow-up change against C25's capability, then resume"). It is
recorded here so the follow-up has a written cause, and it is why task 5.4 cites
a process defect.

### Revision 2 — the report ships methodology-complete and measurement-pending, and that is the honest reading of its own spec

The `benchmark-comparison-report` spec says the report "SHALL present percentile
latency (at minimum p50 and p99) and throughput for both legs". At this commit
it presents neither, because no admissible number exists:

- `benchmark-baseline.toml`'s `[medians_ms]` table is deliberately empty — C33
  shipped it unseeded on purpose, and an unseeded entry *fails* its gate rather
  than passing, so there is no measured figure to quote;
- `docs/benchmarks.md` states the prohibition directly — **never seed the
  baseline from developer hardware** — and the reasoning is strictly stronger
  for a published document naming another Apache project than for an internal
  ratchet. Design D3's own argument is that one unfair chart costs more
  credibility than ten favorable ones buy; a number measured on an unpinned,
  noisy developer box would be exactly that chart.

No spec text is amended, because the requirement is not yet violated: it governs
the *tagged* release, and the gate (task 2.4) is not green, so 0.3.0 is not
tagged. What is published now is the part of the report that does not depend on
a run — pairing, methodology, version pins, non-equivalences with their
direction, the no-cherry-picking rule, and the freeze rule — with every
measurement cell reading `pending (CI hardware)` and the reason stated in the
report's opening section. Populating those cells from a CI-hardware run is the
last step before tagging, tracked by tasks 2.2 and 4.2–4.6.

Two tests make this non-negotiable rather than a promise:
`test_results_tables_are_marked_pending_not_filled_with_placeholders` (every
results-table data row must carry the pending marker) and
`test_no_figure_in_the_report_is_invented` (no figure with a unit may appear
that this repository does not already state). Both were verified to fail when a
plausible-looking measured row was injected.
