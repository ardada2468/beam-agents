## 1. Tests (written first, must fail for the right reason)

- [x] 1.1 ~~This change adds no new test code, deliberately~~ — **amended, see Revision 1.** The scenario → verification mapping below still holds for the process half of the gate, but the release-artifact half is executable and is now a test: `tests/release/test_release_0_5_0.py` (7 tests), mirroring `tests/release/test_release_0_3_0.py`. The seven dependency names are **parsed out of this change's own `proposal.md`** (`m3_changes()` reads the `**Depends on** — …:` sentence and asserts exactly seven), never transcribed, so the list cannot drift from what the release promised. Verified red first: 6 of 7 failed on the absent `0.5.0` section and the `0.3.0` version/lock; the seventh — the folder-existence guard — passed, which is what proves the proposal parse was real rather than vacuous. Scenario mapping: "A dependency change is still pending" and "All seven dependency changes are archived" → §2.1 plus `test_recorded_archival_verdict_matches_the_repository`; "Conformance matrix is green on the release commit" and "Benchmark gate is green on the release commit" → §2.2–2.3 plus `test_changelog_section_records_the_release_gate_checklist`; "Version and changelog agree at tag time" → §3.1–3.2 plus the version/lock/enumeration tests; "Published artifact reports the released version" → §3.4, unexecutable here
- [x] 1.2 *(added by Revision 3)* The archival verdict recorded in the release notes is checked **against the repository, in both directions** — `test_recorded_archival_verdict_matches_the_repository` fails if the gate table records `pending (archival)` once every M3 change is archived, and fails just as loudly if the table drops the marker while any of them is not. Verified to have teeth: flipping the archival row's verdict cell from `pending (archival)` to `pass` turns it red. Its first form (a bare substring check) had **no** teeth — the section's own prose names the marker — so the verdict is read from the table row's third cell instead

## 2. Gate verification (all on the intended release commit — blocking, in order)

- [x] 2.1 Confirm all seven M3 dependency changes are **archived** — **verified, and the answer is no.** `add-yaml-provider` (C36), `add-dataflow-flex-template` (C37), `add-replay-cli` (C38), `add-pydantic-ai-adapter` (C39), `add-slack-approval-example` (C40), `add-eval-pipeline-example` (C41) and `add-upstream-design-doc` (C42) are all implemented, gated, and merged, but every one is still a **live** folder under `openspec/changes/`; `openspec/changes/archive/` holds only the nine pre-0.1.0 changes. Under design D2 archival — not merge — is the gating state, so this condition is unmet and the release does not proceed to a tag. Recorded in the release notes' gate table as `pending (archival)`, and that recording is itself machine-checked (task 1.2), so it cannot be quietly upgraded. There are no archive paths to record, which is the finding
- [ ] 2.2 Confirm the adapter conformance matrix is green on the release commit **(blocked: needs CI run)** — the DirectRunner leg is green here (`make test-semantics-offline`: 79 passed, 5 skipped, every skip declared and pre-existing), and the registry now carries four adapters (`reference`, `langgraph`, `pydantic_ai`, `adk` — `tests/conformance/_registry.py`), so the matrix meta-test passing already means "green including Pydantic AI" with no extra wiring, exactly as design D3 predicted. The Flink leg is `make test-conformance-flink` in the `integration` workflow and needs the docker compose stack at the candidate commit. Recorded as `pending (CI run)`
- [ ] 2.3 Confirm the benchmark regression gate on the runtime-overhead latency budget is green on the release commit **(blocked: needs CI hardware)** — `benchmark-baseline.toml`'s `[medians_ms]` is still deliberately unseeded and `docs/benchmarks.md` forbids seeding it from developer hardware, so no admissible figure exists outside the nightly `bench` job on a GitHub-hosted runner. Unchanged from the 0.3.0 evaluation. Recorded as `pending (CI hardware)`
- [ ] 2.4 Confirm `ci`, `integration`, and `quality` are all green on the release commit **(blocked: needs CI run)** — the offline roster is green here (§4), but the docker-backed halves of `integration` and the `quality` mutation job have not run at this commit, and D3's rule is that the runs must be on the commit the tag points at rather than an earlier green one. Recorded as `pending (CI run)`

## 3. Release mechanics (only after §2 is fully checked)

§2 is **not** fully checked: 2.1 is unmet on its merits and 2.2–2.4 are
unrunnable here. Under design D1 that slips the tag, not the scope. The two
mechanical steps below are still performed, because they are what the release
*candidate* is — the artifact the gate is evaluated against — and because the
version/changelog requirement is a property of the commit, not of the tag.
Everything that actually publishes stays unchecked.

- [x] 3.1 Set `pyproject.toml` `version` to `0.5.0` — bumped from `0.3.0`; `uv lock` refreshed (one line: `beam-agents v0.3.0 -> v0.5.0`) and `uv sync --locked --group lint --group typecheck --group test` re-confirmed green afterwards, which is the property `docs/releasing.md` says the lock refresh exists to preserve. The bump also required `docs/yaml.md`'s two package pins — see Revision 1
- [x] 3.2 Add the `0.5.0` section to the changelog — `make changelog VERSION=0.5.0` assembled the one pending fragment (`add-effector-security.added.md`, consumed exactly once) into a dated section, and the milestone record was curated around it: a *The M3 batch* table enumerating all seven changes with what each delivered, and the `### Release gate` table. The D4 verification against the archive rather than memory found something the archive alone would have hidden: four of the seven (`add-yaml-provider`, `add-dataflow-flex-template`, `add-replay-cli`, `add-upstream-design-doc`) had their fragments consumed into the `0.3.0` section, and three (`add-pydantic-ai-adapter`, `add-slack-approval-example`, `add-eval-pipeline-example`) landed with **no fragment at all** — so mechanical assembly could never have produced a complete M3 section. The batch table is their only release-note home, and `test_changelog_section_enumerates_every_m3_change` pins it. `add-effector-security` (C44) is the one non-M3 change archived-since-window entry, and it appears in `### Added` on its own fragment
- [ ] 3.3 Merge the release PR, then tag `v0.5.0` and publish **(blocked: needs release infra)** — and independently blocked by the gate itself: 2.1 is unmet, so under D1/D5 the release slips rather than ships. No tag is created and nothing is published
- [ ] 3.4 Verify the published artifact: `pip install beam-agents==0.5.0` into a clean environment **(blocked: needs release infra)** — requires 3.3

## 4. Gates

- [x] 4.1 `make lint` — `ruff check` clean, `ruff format --check` clean (376 files)
- [x] 4.2 `make type` — `mypy --strict`: no issues found in 370 source files
- [x] 4.3 `make test-unit` — 1914 passed, 4 skipped (all environment-declared), 204 deselected; total coverage 95.63%. One genuine failure surfaced by the bump, in a *previous* milestone's test, and fixed rather than suppressed: see Revision 2
- [x] 4.4 `uv run pre-commit run --all-files` — all hooks pass, including `Changelog fragment required for src/ edits` (this change touches no `src/`, so no fragment is due — and adding one would have been wrong: it would publish in the *next* release, not this one)
- [x] 4.5 `openspec validate add-0-5-0-release --strict` — "Change 'add-0-5-0-release' is valid" (re-run after the Revision edits below)
- [x] 4.6 *(added)* `make test-semantics-offline` — 79 passed, 5 skipped. Both a standing gate and the offline leg of gate condition 2.2
- [x] 4.7 *(added)* `make coverage-ratchet` — at baseline, no movement and no baseline edit; this change adds no `src/` code beyond the version string

## Revisions

Numbered corrections to the planning artifacts, made because implementation
proved them wrong.

### Revision 1 — the version bump touches four files, not one; `proposal.md`'s Impact named two

`proposal.md` said, under **Modified code**: "`pyproject.toml` (version line
only) and the changelog file … No `src/`, `tests/`, or proto changes." Three of
those clauses are false, and the same correction was already made one milestone
ago by `add-0-3-0-release`'s Revision 1 — which is itself the finding: the
release checklist in `docs/releasing.md` still does not carry a step for
version-coupled references elsewhere in the tree, so the next release will
rediscover it a third time.

1. **`uv.lock`.** `docs/releasing.md` states that the lock records the project's
   own version and that "a bump therefore always comes with `uv lock`". The
   Impact section contradicted the process it says it consumes verbatim.
   `uv lock` was run; the diff is one line.
2. **`docs/yaml.md`.** The Beam YAML provider block pins the installable package
   as `beam-agents==X.Y.Z` in two places, and
   `tests/yaml/test_docs_example.py::test_the_documented_package_pin_matches_the_shipped_version`
   asserts every pin on the page equals `importlib.metadata.version("beam-agents")`.
   The bump to `0.5.0` turns `make test-unit` red until the doc is updated. That
   test is correct and was not weakened; both pins were updated. (Two further
   `beam-agents==0.1.0` strings exist, in `src/beam_agents/yaml/__init__.py`'s
   docstring and `src/beam_agents/yaml/providers.yaml`'s comment. They are
   deliberately **not** touched: they are illustrative prose no test governs,
   editing `src/` from a release change would demand a changelog fragment for a
   non-change, and `add-0-3-0-release` left them alone for the same reason.
   Consistency across milestones beats a drive-by edit.)
3. **`tests/`.** This change adds `tests/release/test_release_0_5_0.py` and
   amends `tests/release/test_release_0_3_0.py` (Revision 2). Task 1.1's "adds
   no new test code, deliberately" was wrong on its own terms: the milestone's
   *process* conditions are unexecutable, but its *artifact* conditions —
   version, lockfile, changelog enumeration, recorded verdict — are exactly the
   kind of claim a test should hold, and the sibling milestone already proved it
   by shipping one.

`proposal.md`'s Impact section is amended to name all four.

### Revision 2 — a milestone test that pins equality with the current version breaks at the next milestone

Bumping to `0.5.0` turned two tests in `tests/release/test_release_0_3_0.py`
red: `test_project_version_is_the_release_version` (`== "0.3.0"`) and
`test_lockfile_records_the_release_version` (`== ["0.3.0"]`). Neither is a
failure of this change — they are correct statements about the moment C35 was
written and false statements about every commit after the next bump.

The durable property those tests were reaching for is a **floor plus an
agreement**, not an equality:

- the tree never regresses below a milestone whose release notes it has already
  assembled (`version >= 0.3.0`), and each later milestone's own test raises the
  floor again, so "forgot to bump" is still caught — by the change that was
  supposed to bump;
- `uv.lock` agrees with `pyproject.toml`, which is the invariant
  `scripts/check_release.py` actually enforces at tag time and the one that
  breaks every `uv sync --locked` job when violated.

Both files were amended to that form, with the reasoning recorded in each test's
comment. This is not weakening a test to make an implementation pass: the
implementation (a version bump the release process mandates) is not in question,
the equality assertion is unsatisfiable by *any* correct future release, and the
property that gives the test its teeth is retained. `test_release_0_5_0.py`
carries the same shape one milestone up, so C43 does not hand the same defect to
C45.

### Revision 3 — the recorded gate verdict needs its own check, and the obvious form of it has no teeth

Design D2 makes the gate "mechanically checkable: seven named directories
present under `openspec/changes/archive/`", but nothing in the original tasks
connected that check to what the release notes *claim*. Since the honest outcome
here is a failed condition, the failure mode that matters is a gate table that
drifts optimistic — recording `pass` for a condition the tree contradicts.
Task 1.2 was added for it, asserting the archival row against the archive
directory in both directions.

Its first implementation was a substring test (`"pending (archival)" in
section`) and it passed even after the row's verdict was flipped to `pass` —
because the surrounding prose names the marker while explaining the check. The
check now parses the gate table's rows and reads the third cell, and was
re-verified by flipping the verdict (red) and restoring it (green). Recorded
because the lesson generalizes: an honesty check that reads the same prose it is
policing is not a check.
