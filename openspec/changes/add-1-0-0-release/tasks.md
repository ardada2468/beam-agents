## 1. Tests (written first, must fail for the right reason)

~~This section is deliberately thin: a release gate adds no runtime behavior, so
it derives no new test code.~~ — **amended, see Revision 1.** The *process* half
of the gate is unexecutable and is still verified by inspection below; the
*release-artifact* half is executable and is now a test.

- [x] 1.1 Map each spec scenario to its enforcing signal and record the mapping in the PR description: *All gate conditions hold* / *A hardening change is unarchived* → archive-state inspection (§2.1) **plus `test_recorded_archival_verdict_matches_the_repository`**; *Spark inside its window* / *Spark decision unrecorded* → decision-record inspection (§2.5) **plus `test_changelog_section_records_the_spark_promotion_decision`**; *post-1.0 public-symbol removal* → C45's API-freeze snapshot test (`tests/test_public_surface.py`), **plus `test_changelog_section_states_the_post_1_0_stability_regime`**, which pins that the release notes name `public-surface.toml` by path so a future proposal has something to be measured against; *post-1.0 state-schema change* → C46's golden-blob and nightly `--update` compat tests, plus the same regime test for `docs/state-compat.md`
  - New: `tests/release/test_release_1_0_0.py`, 9 tests, mirroring `tests/release/test_release_0_5_0.py`. The four dependency names are **parsed out of this change's own `proposal.md`** (`m4_changes()` reads the `**Depends on:** …` sentence and asserts exactly four) and never transcribed, so the list cannot drift from what the release promised, and each name is asserted to exist as a live-or-archived change folder. Verified red first: 7 of 9 failed on the absent `1.0.0` section and the `0.5.0` version; the two that passed were the lockfile-agreement test (durable by construction, see Revision 2) and the folder-existence guard — the latter passing is what proves the proposal parse was real rather than vacuous (`['add-1-0-api-freeze', 'add-effector-security', 'add-state-guarantees', 'promote-spark-runner']`)
- [x] 1.2 Confirm the §2 checklist below matches the gate requirement in `specs/release-1-0/spec.md` condition-for-condition, with nothing added and nothing dropped
  - Checked one-to-one, both directions. Spec bullets 1–5 ↔ tasks 2.1–2.5; task 2.6 is the standing-blocker re-verification the spec's closing paragraph and `openspec/project.md` already impose rather than a sixth gate condition, and the `### Release gate` table in `CHANGELOG.md` carries the same rows plus two evidence rows (offline roster, version/changelog agreement) that record *how* the checked conditions were established. Nothing in the table is a condition the spec does not state
  - The three verdict-bearing rows are additionally machine-checked, in both directions, against the tree rather than against memory: the archival row against `openspec/changes/archive/`, the Spark row against `openspec/project.md`'s support statement, and the regime row against the two policy artifacts' paths. Each was verified to have teeth by flipping it — archival verdict `pending (archival)` → `pass`, the word `deferred` → `advanced`, and `docs/state-compat.md` → prose — and confirming exactly the corresponding test went red

## 2. 1.0 release-gate checklist (all boxes required before §3 may start)

- [x] 2.1 `add-effector-security`, `add-1-0-api-freeze`, `add-state-guarantees`, and `promote-spark-runner` are all present under `openspec/changes/archive/` — **verified, and the answer is no.** All four are implemented, gated, and merged, and all four are still **live** folders under `openspec/changes/`; `openspec/changes/archive/` holds only the nine pre-0.1.0 changes. Design D1(a) chose archival over merge deliberately — archival is when a change's delta lands in the main specs, so an unarchived change is by definition not yet part of the promised surface — which makes this condition unmet on its merits, not on a technicality. Recorded in the release notes' gate table as `pending (archival)`, and that recording is itself machine-checked (task 1.2), so it cannot be quietly upgraded. There are no archive paths to record, which is the finding
  - *Addendum (2026-08-03):* **archival is now done and the condition is met.** All four M4 changes are archived via `openspec archive`, each landing `openspec validate --strict` clean: `openspec/changes/archive/2026-08-04-add-1-0-api-freeze/`, `…/2026-08-04-add-effector-security/`, `…/2026-08-04-add-state-guarantees/`, and `…/2026-08-04-promote-spark-runner/` (the last after its task 6.1 reconciliation against the freshly-landed `openspec/specs/adapter-conformance-matrix/spec.md`, which found no drift). The `### Release gate` table's archival row is flipped to `pass` with the four archive paths recorded, and `tests/release/test_release_1_0_0.py` is green (9 passed) — the machine check now holds in the met direction. The original finding above is preserved as written; it was correct when recorded
- [x] 2.2 The API-freeze snapshot test from `add-1-0-api-freeze` is green — `tests/test_public_surface.py` passes in `make test-unit` at this commit, comparing `public-surface.toml` against the tree by exact equality in both directions; the ruff `D1` docstring gate rides in `make lint`, also green. This change adds **no public name** (its only additions are a test module and documentation), so the snapshot needed no regeneration and `docs/api.md` needed no new entry — verified by the test staying green with the file untouched. Recorded as `pass`
- [ ] 2.3 The state guarantees from `add-state-guarantees` are documented and its nightly `--update` compat test is green on the latest scheduled run **(blocked: needs CI run)** — the documentation half is met: `docs/state-compat.md` ships with the full change-class table, and its offline companions (`tests/dataflow/test_update_compat_harness.py`, `tests/core/test_state_compat_doc.py`) are green here. The gate itself, `tests/dataflow/test_update_compat.py`, is `dataflow`-marked and nightly-only; it needs a GCP project, region, and temp bucket, and **no scheduled run exists at this commit to be green on**, so the design's stale-signal mitigation (re-trigger the nightly if the last run predates a state-touching merge) has nothing to re-trigger. Recorded as `pending (CI run)`
- [ ] 2.4 Effector intent signing from `add-effector-security` is shipped with rollout complete: the effector *enforces* signature verification **(blocked: rollout is an operator step this repository cannot evidence)** — the mechanism is fully shipped: the additive proto signature envelope, `IntentSigner` on `WriteIntents`, verification as the effector's first phase (ahead of even expiry refusal), dead-lettering that never publishes a `ToolResult`, and an offline semantics gate (`tests/semantics/test_effector_signing.py`, 5 cells) proving a mixed genuine/tampered/forged stream under `require`. But `EffectorConfig.verify_intents` defaults to `off` (`src/beam_agents/effector/config.py:317`), and D1(d)'s wording is deliberate: "verification enforced, not merely available". Moving a deployment through `permissive` to `require` is an operator action against a live topic whose retention holds unsigned intents; no artifact in this repository can attest that any deployment has completed it. Available is what shipped; enforced is not yet a fact anyone here can assert. Recorded as `pending (rollout)`
- [x] 2.5 The Spark decision from `promote-spark-runner` is recorded either way — **recorded: deferred.** The promotion gate is four consecutive green *scheduled* weekly runs with no skip added during the window; **zero** such runs exist. The leg has never run against a Spark job server (`promote-spark-runner` tasks 2.2 and 3.5 are both blocked on docker), three of its seven scenarios are declared structural skips with named constraints, and the other four are provisional `Run()` declarations whose evidence is the first weekly run — and that change's own Revision 2 notes the first four weekly summaries will report skip drift from the commit that introduced the leg, so the promotion clock could not even have started. Per design D2 this does **not** block 1.0: `openspec/project.md:113` has scoped Spark as best-effort at 1.0 all along, the block would be pure wall-clock on a runner that does not participate in the 1.0 promise, and a flaky week could push 1.0 indefinitely on an unrelated signal. What D2 does require is the recording, and it is made in three places that agree: the `### Release gate` table row, the **Spark: deferred, not promoted** paragraph in the release notes stating the reason, and `openspec/project.md`'s unchanged support statement. The design's open question — *where* the decision is recorded — resolves as it anticipated: C47 designated `docs/ci.md` for the promotion/demotion process and left the support statement to the stage-2 change, so the changelog entry plus the unchanged constitution is the record. Recorded as `pass`, and cross-checked against `openspec/project.md` by `test_changelog_section_records_the_spark_promotion_decision`, which fails if this section ever claims promotion while the constitution still says best-effort
- [ ] 2.6 Standing release blockers from `openspec/project.md` re-verified **(blocked: needs CI hardware)** — semantics: the offline selection is green and unskipped here (`make test-semantics-offline`: 79 passed, 5 skipped, every skip declared and pre-existing; nothing marked flaky or xfail), but the docker-backed `semantics and integration` leg needs the compose stack at this commit. Latency budget: the p50 < 15 ms / p99 < 60 ms overhead budget still has **no admissible figure** — `benchmark-baseline.toml`'s `[medians_ms]` is deliberately unseeded and `docs/benchmarks.md` forbids seeding it from developer hardware, so "no open regression" cannot be asserted, only "no measurement". Unchanged from the 0.3.0 and 0.5.0 evaluations. Recorded as `pending (CI hardware)` — *2026-08-03 addendum:* the latency half is now met. `[medians_ms]` was seeded from the scheduled nightly `bench` run 30806138398 (`ubuntu-latest`, `main` @ `e5cf356`) per the `docs/benchmarks.md` procedure; that run's absolute budget read p50 0.8517 ms / p99 0.9767 ms on 1000 pooled samples, and `scripts/bench_gate.py` over its downloaded artifact exits 0 against the seeded baseline. The box stays unchecked for the semantics half alone: the docker-backed `semantics and integration` leg still needs a run at the release commit (CI's `flink-minicluster` job on the release PR). The gate-table verdict is updated to `pending (CI run)` accordingly

## 3. Release execution (via the `add-0-1-0-release` process, unchanged)

§2 is **not** fully checked: 2.1 is unmet on its merits, 2.4 is unmet on its
merits (the dial ships at `off`), and 2.3/2.6 are unrunnable here. Under design
D1 that slips the tag; it does not shrink the release and it does not license a
waiver — "the failing condition MUST be resolved in its owning change, not
waived in this one". The two mechanical steps below are still performed,
because they are what the release *candidate* is — the artifact the gate is
evaluated against — and because the version/changelog requirement is a property
of the commit rather than of the tag. Everything that actually publishes stays
unchecked.

- [x] 3.1 Bump `version` in `pyproject.toml` to `1.0.0` — bumped from `0.5.0`; `uv lock` refreshed (one line: `beam-agents v0.5.0 -> v1.0.0`) and `uv sync --locked --group lint --group typecheck --group test` re-confirmed green afterwards, which is the property `docs/releasing.md` says the lock refresh exists to preserve. The bump also required `docs/yaml.md`'s two package pins — see Revision 1
- [x] 3.2 Write the 1.0.0 changelog entry per the C25 changelog process — `make changelog VERSION=1.0.0` assembled the one pending fragment (`add-1-0-api-freeze.breaking.md`, consumed exactly once; `check_release.py --consume-internal` reported no internal fragments) into a dated section, and the milestone record was curated around it:
  - **The M4 hardening batch** table enumerating all four changes with what each delivered. Verified against the archive rather than memory, and it found what the 0.5.0 curation found one milestone earlier: only `add-1-0-api-freeze` had a fragment pending for *this* release — `add-effector-security`'s was consumed into `0.5.0`, `add-state-guarantees`' into `0.3.0`, and `promote-spark-runner` is CI/test infrastructure that landed with no fragment at all — so mechanical assembly could never have produced a complete M4 section. The table is their only release-note home, pinned by `test_changelog_section_enumerates_every_m4_change`
  - **What the 1.0.0 number promises**, the D3 regime stated in the release notes: the public surface frozen by `public-surface.toml` under CONTRIBUTING.md's deprecation window, wire/state changes governed by `docs/state-compat.md`, both named by path so the promise is falsifiable
  - **In-flight deprecations: none** — the design's second open question, resolved against the tree rather than assumed. `public-surface.toml` carries no deprecation marker and `src/beam_agents/_deprecation.py` has no call sites anywhere in `src/`; the C45 sweep renamed its internal machinery outright (that is the release's `### Breaking changes` entry) instead of deprecating it, so there is no removal horizon to publish
  - **Spark: deferred, not promoted**, with the reason, per 2.5
  - The `### Release gate` table, recording all five D1 conditions plus the standing blockers with evidence and verdicts, and stating that `v1.0.0` is not tagged
  - The changelog's own header prose was rewritten: it opened "The project is pre-1.0 and versioned `0.MINOR.PATCH`", which the bump makes false about the tree. It now scopes that policy to the `0.x` sections below it and points at the 1.0.0 regime section. (`docs/releasing.md`'s pre-1.0 policy text is deliberately **not** touched — see Revision 3)
- [ ] 3.3 Tag `v1.0.0` and publish via the release workflow established by `add-0-1-0-release` **(blocked: needs release infra)** — and independently blocked by the gate itself: 2.1, 2.3, 2.4 and 2.6 are unmet, so under D1 the release slips rather than ships. No tag is created and nothing is published

## 4. Post-release verification

- [ ] 4.1 `pip install beam-agents==1.0.0` succeeds in a clean 3.11 and 3.12 environment; `import beam_agents` works, and each extra resolves **(blocked: needs release infra)** — requires 3.3
- [ ] 4.2 The tag, the published version, and the changelog entry agree on `1.0.0` **(blocked: needs release infra)** — requires 3.3. The two halves that exist at this commit do agree and are pinned: `pyproject.toml` and `uv.lock` both read `1.0.0` (`test_lockfile_agrees_with_the_project_version`), and the changelog section is dated `1.0.0`. Only the tag is missing, and `scripts/check_release.py` is what will prove all three at tag time
- [ ] 4.3 Archive this change; from archival, the `release-1-0` regime requirement (design D3) is normative for all future proposals **(not done: archival is the reviewer's step, and it must not precede the four M4 changes this gate is written about — archiving the gate before its dependencies would invert 2.1)**

## 5. Gates

- [x] 5.1 `make lint` — `ruff check`: all checks passed; `ruff format --check`: 380 files already formatted
- [x] 5.2 `make type` — `mypy --strict`: success, no issues found in 374 source files
- [x] 5.3 `make test-unit` — 1941 passed, 4 skipped (all environment-declared: bench group, `aiokafka`, `google.cloud.firestore`), 204 deselected; total coverage 95.68%, above the 90% floor. Includes the 9 new release tests and `tests/yaml/test_docs_example.py`'s pin assertion, which the bump would have turned red without Revision 1's doc edit
- [x] 5.4 `uv run pre-commit run --all-files` **(not run: pre-commit is not in the synced dependency groups here)** — its checks are covered individually: `ruff` / `ruff-format` via 5.1, `mypy` via 5.2, and the `openspec-change-required` / `changelog-fragment-required` hooks are no-ops for this change, which touches no `src/` file (adding a fragment would also have been wrong: it would publish in the *next* release, not this one) <!-- discharged by verify-live-infrastructure phase 0 (2026-07-31): `uv run pre-commit run --all-files` executed on the merged tree, all 10 hooks passed (ruff, ruff-format, check-yaml, check-toml, end-of-file-fixer, trailing-whitespace, mypy, protobuf-drift, openspec-change-required, changelog-fragment-required). See verification-report.md. -->
- [x] 5.5 `openspec validate add-1-0-0-release --strict` — "Change 'add-1-0-0-release' is valid" (re-run after the Revision edits below)
- [x] 5.6 *(added)* `make test-semantics-offline` — 79 passed, 5 skipped. Both a standing gate and the offline half of gate condition 2.6
- [x] 5.7 *(added)* `make coverage-ratchet` — branch coverage 91.64%, at baseline; no movement and no baseline edit, as expected for a change that adds no `src/` code beyond a version string
- [x] 5.8 *(added)* `mkdocs build --strict` — exit 0; run because `docs/yaml.md` changed (Revision 1)

## Revisions

Numbered corrections to the planning artifacts, made because implementation
proved them wrong.

### Revision 1 — "New code: none" was wrong in two directions, and the version bump touches four files

`proposal.md`'s Impact section said "**New code:** none. The only artifacts are
the version bump, the changelog entry, and the tag", and "**Modified code:**
`pyproject.toml` … and the changelog file". Both are false, and the same
correction has now been made at three consecutive milestones —
`add-0-3-0-release` Revision 1, `add-0-5-0-release` Revision 1, and this one.
That repetition is itself the finding (see Revision 3).

1. **`uv.lock`.** `docs/releasing.md` states that the lock records the project's
   own version and that "a bump therefore always comes with `uv lock`". The
   Impact section contradicted the process it says it reuses unchanged. `uv lock`
   was run; the diff is one line, and `uv sync --locked` was re-confirmed green.
2. **`docs/yaml.md`.** The Beam YAML provider block pins the installable package
   as `beam-agents==X.Y.Z` in two places, and
   `tests/yaml/test_docs_example.py::test_the_documented_package_pin_matches_the_shipped_version`
   asserts every pin on the page equals `importlib.metadata.version("beam-agents")`.
   The bump turns `make test-unit` red until the doc is updated. That test is
   correct and was not weakened; both pins were updated. (Two further
   `beam-agents==0.1.0` strings exist, in `src/beam_agents/yaml/__init__.py`'s
   docstring and `src/beam_agents/yaml/providers.yaml`'s comment. They are
   deliberately **not** touched, for the third milestone running: they are
   illustrative prose no test governs, and editing `src/` from a release change
   would demand a changelog fragment for a non-change.)
3. **`tests/`.** This change adds `tests/release/test_release_1_0_0.py`. Task
   1's "a release gate adds no new test code" was wrong on its own terms: the
   gate's *process* conditions are unexecutable, but its *artifact* conditions —
   version, lockfile, batch enumeration, recorded verdicts, the Spark decision,
   and the named regime artifacts — are exactly the kind of claim a test should
   hold, and both sibling milestones already proved it by shipping one.

`proposal.md`'s Impact section is amended to name all four.

### Revision 2 — the milestone-version assertion is a floor, not an equality

Not a correction to this change's own artifacts, but a constraint carried
forward deliberately, recorded so it is not re-litigated. `add-0-5-0-release`
Revision 2 found a real defect in `tests/release/test_release_0_3_0.py`: a
milestone test asserting `version == "<its own version>"` is true only between
its bump and the next one, and went red the moment 0.5.0 landed. The durable
properties are (a) the tree never regresses *below* a milestone whose release
notes are already assembled, and (b) `uv.lock` **agrees with** `pyproject.toml`
rather than equalling a literal — which is the invariant
`scripts/check_release.py` actually enforces at tag time.
`test_release_1_0_0.py` carries both forms and no literal-equality assertion,
and this was confirmed at the point it mattered: the lockfile test was one of
the two that passed against a `0.5.0` tree before the bump, which is precisely
the behavior the pinned form would not have had.

### Revision 3 — the release checklist still has no step for version-coupled references

`add-0-5-0-release` Revision 1 closed with: "the release checklist in
`docs/releasing.md` still does not carry a step for version-coupled references
elsewhere in the tree, so the next release will rediscover it a third time." It
did. `docs/yaml.md`'s two pins were rediscovered by a red `make test-unit`, not
by the checklist.

This change does not fix it, and the reason is scope rather than agreement.
`docs/releasing.md` is not in this change's Impact section, and — more
importantly — it is the document that would have to be *rewritten*, not
appended to, because its "Pre-1.0 versioning policy" section and its "There is
**no 1.0 API-stability commitment yet**" sentence are exactly what 1.0.0
retires. Rewriting the versioning policy from inside a gate that has recorded
itself as **not green** would announce a promise the repository has not made:
1.0.0 is not tagged and not published, so the pre-1.0 policy is still the only
one under which anything has actually shipped. That rewrite — the post-1.0
versioning policy, the semver commitment, and a checklist step for
version-coupled references — belongs to the release PR that cuts the tag, once
§2 is genuinely green. It is recorded here so that PR inherits the finding
instead of rediscovering it a fourth time.

The `CHANGELOG.md` header prose *was* updated (task 3.2), because it is this
change's own artifact and because leaving "The project is pre-1.0" directly
above a `## 1.0.0` section would have been a statement contradicted on the next
line.
