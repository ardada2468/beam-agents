## 1. Tests (written first, must fail for the right reason)

This is a release-milestone change: its deliverables are gate evaluations, process artifacts, and a published report, so the automated-test section is deliberately thin. The regression tests that give the gate its teeth already exist upstream — C33 `add-benchmark-harness` owns the p50/p99 overhead gates and the conformance matrix predates this change — and re-implementing them here would duplicate gate ownership. What this change adds as executable checks is release-artifact consistency; the process scenarios (gate blocking, triage dispositions, methodology review) are verified by the checklist steps in sections 2–5, each traceable to a named spec scenario.

- [ ] 1.1 Add a release-artifact consistency check (test or release-checklist script per the C25 mechanics) asserting the tagged commit has `pyproject.toml` version `0.3.0` and a changelog section that names all nine M2 changes and links `docs/benchmarks/0.3.0-vs-flink-agents.md` — implements *The shipped version and changelog match the milestone*; must fail while the version reads `0.0.0`/`0.2.x` and the section is absent
- [ ] 1.2 Add a report-presence/shape check asserting `docs/benchmarks/0.3.0-vs-flink-agents.md` exists, names the C33 scenario used, and contains a methodology section pinning the measured versions — implements the checkable core of *The report ships with the release* and *The methodology section enumerates non-equivalences*; must fail while the report does not exist

## 2. Release gate assembly

- [ ] 2.1 Verify all nine M2 changes are archived — C26 `add-vllm-provider`, C27 `add-adaptive-batching`, C28 `add-token-budgets`, C29 `add-longterm-memory-stores`, C30 `add-compaction-strategies`, C31 `add-adk-adapter`, C32 `add-state-schema-migration`, C33 `add-benchmark-harness`, C34 `add-hot-key-sharding-guidance` — recording each archive reference in the gate checklist (*An unarchived M2 dependency blocks the release*)
- [ ] 2.2 Run the C33 benchmark regression gates at the release-candidate commit and record the run link; confirm overhead p50 < 15 ms and p99 < 60 ms per activation excluding LLM/tool time (*A benchmark regression blocks the release*)
- [ ] 2.3 Run the conformance matrix at the candidate commit — offline leg via the required `ci` semantics selection, Flink leg via `make test-conformance-flink` — and confirm every cell is green with no cell newly skipped versus the previous release (*A red or newly skipped conformance cell blocks the release*)
- [ ] 2.4 Assemble the recorded gate checklist (condition, evidence link, pass/fail) for the release notes; if any condition fails, stop here — the release slips, per design D5 (*A fully green gate opens the release*)

## 3. Design-partner feedback triage

- [ ] 3.1 Collect all design-partner feedback items received during the 0.1.x/0.2.x cycle into a single intake list
- [ ] 3.2 Triage each item through the D2 rubric: release-blocking (invariant violation, data loss/corruption, `--update` state-compat break, security) versus follow-up OpenSpec change; record bucket and rationale per item (*An invariant-violation report becomes release-blocking*, *A feature request becomes a follow-up change*)
- [ ] 3.3 For each release-blocking item, open (or confirm) a dedicated OpenSpec change folder for the fix and track it to archive before gate evaluation
- [ ] 3.4 For each follow-up item, file the proposed change or roadmap entry targeting a post-0.3.0 milestone and link it from the disposition
- [ ] 3.5 Freeze the disposition table into the release notes; if the intake list is empty, record "no design-partner feedback received" explicitly (*Zero feedback is recorded, not omitted*)

## 4. Benchmark comparison vs Apache Flink Agents

- [ ] 4.1 Select the C33 scenario closest to a workload Apache Flink Agents expresses idiomatically (event-triggered, keyed, stateful agent with tool calls) and record the selection rationale (*The workload comes from the C33 scenario set*)
- [ ] 4.2 Implement the paired workload on Apache Flink Agents using that project's recommended APIs, pinned to its latest stable release (or an exact commit, if no stable release exists — design Open Questions)
- [ ] 4.3 Configure both legs with a scripted fake model of equal cost so the measurement isolates runtime overhead (*Model latency is excluded from the comparison*)
- [ ] 4.4 Provision the paired benchmark environment (reusing C33's if sufficient), run both legs, and commit run configurations plus the environment manifest
- [ ] 4.5 Write `docs/benchmarks/0.3.0-vs-flink-agents.md`: percentile latency (≥ p50/p99) and throughput for both legs, framed around beam-agents' own overhead budget, with the methodology section pinning all versions and enumerating the language-runtime, effect-model, and state-backend non-equivalences and which side each favors
- [ ] 4.6 Include every completed run of the final configuration, unfavorable results at equal prominence; if review concludes no fair pairing exists, ship the beam-agents-only budget report with a written explanation instead (*An unfavorable result is published unfiltered*, *No fair pairing exists*)
- [ ] 4.7 Internal review of the report against the design D3 checklist (versions pinned, non-equivalences enumerated, all runs included, reproducible) before merge (*The methodology section enumerates non-equivalences*)

## 5. Ship 0.3.0

- [ ] 5.1 Bump `pyproject.toml` version to `0.3.0` per the C25 `add-0-1-0-release` process
- [ ] 5.2 Write the 0.3.0 changelog section: the nine M2 changes, the feedback disposition table (or its explicit "none received"), and the link to the benchmark comparison report
- [ ] 5.3 Confirm the section-1 release-artifact checks now pass at the candidate commit
- [ ] 5.4 Tag and publish through the C25 process; if executing it exposes a process defect, pause and land the fix as a separate change against the release-process capability before resuming (*A process defect pauses rather than forks the process*)
- [ ] 5.5 Verify the published artifacts: tag present, package version `0.3.0`, changelog and report reachable from the release notes; record post-publication that `docs/benchmarks/0.3.0-vs-flink-agents.md` is frozen (*The published report is frozen*)

## 6. Gates

- [ ] 6.1 `make lint`
- [ ] 6.2 `make type`
- [ ] 6.3 `make test-unit`
- [ ] 6.4 `make coverage-ratchet` (no coverage movement expected — this change adds no `src/` code beyond the version string — but the gate runs regardless)
- [ ] 6.5 `uv run pre-commit run --all-files`
- [ ] 6.6 `openspec validate add-0-3-0-release --strict`
