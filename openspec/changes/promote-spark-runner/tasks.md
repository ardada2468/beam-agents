## 1. Tests (written first, must fail for the right reason)

- [x] 1.1 Leg-declaration completeness test (spec scenario: *A scenario without a spark declaration cannot build a cell*): unit test in `tests/conformance/` asserting every `ScenarioSpec.legs` dict declares every entry of `LEGS` — fails first because `LEGS` has no `spark` entry and no scenario declares one
  - `tests/conformance/test_harness_unit.py::test_every_scenario_declares_every_leg` now derives from `set(LEGS)` (was a hardcoded `{"direct", "flink"}`), plus `test_leg_vocabulary_is_the_declared_legs`, `test_every_spark_skip_names_a_specific_constraint`, `test_spark_runnable_scenarios_exclude_the_declared_skips`, `test_real_time_variant_applies_to_both_portable_legs`. 13 passed.
- [x] 1.2 Meta-test expectation for the three-leg matrix (spec scenario: *The meta-test accounts for the spark leg*): with `spark` in `LEGS`, `tests/conformance/test_matrix.py` must expect `len(ADAPTERS) × len(SCENARIOS) × 3` cells — fails until `test_spark.py` cells exist and are collected
  - `test_matrix.py` needed no edit: it already computes `ADAPTERS × SCENARIOS × LEGS`. Verified it failed for the right reason with `LEGS` extended and no `test_spark.py` (`1 failed, 45 passed`, missing 28 cells), then went green once the cells landed (`50 passed` over `pytest tests/conformance -m "not integration"`). Every count derives from `len(ADAPTERS)`/`len(LEGS)`, never a literal.
- [x] 1.3 Marker-selection pinning tests (spec scenarios: *Pull-request workflows are unchanged*, *Spark cells are counted but stay out of the semantics partition*): collection-level test asserting the per-PR selections (`-m "integration and not semantics and not spark"`, `-m "semantics and not integration"`, `-m "semantics and integration"`) collect zero spark-leg cells and `-m "integration and spark"` collects exactly the declared spark cells — fails until the `spark` marker and cell markings exist
  - `tests/conformance/test_spark_selection.py` (4 tests, `--collect-only` through `scripts/check_semantics_partition.collect`, offline). Expected size is `len(ADAPTERS) * len(SCENARIOS)`, not 28.
- [x] 1.4 `scripts/spark_weekly_status.py` unit tests against offline fixtures (spec scenarios: *Consecutive green scheduled runs accumulate a streak*, *A red week resets the streak*, *A missed week breaks consecutiveness*, *Manual dispatch does not affect the promotion window*, *A skip added mid-window resets the promotion clock*): streak over scheduled-run JSON pages, final-conclusion rerun handling, >8-day cadence gap breaking the streak, `workflow_dispatch` exclusion, skip-drift detection over git-log text, and the ready/not-ready verdict lines — fail first because the script does not exist
  - `tests/scripts/test_spark_weekly_status.py`: 34 tests, one class per spec scenario, plus demotion (2-red / 1-red) and summary rendering. Confirmed collection-error-on-missing-script first. All pure — no network, no docker.
- [x] 1.5 Spark leg cell tests `tests/conformance/test_spark.py` (spec scenarios: *Spark leg runs the declared scenarios through the job server*, *A spark-inexpressible scenario is an explicit skip with a reason*, *Stack failure is not a Spark verdict*): mirror `test_flink.py`'s per-scenario assertions (terminal outputs, deterministic intents, error channel, declared-skip reporting, `InfraFailure` separation) against the spark harness — fail first because `tests/conformance/_spark/` does not exist
  - Seven cell tests × 4 adapters = 28 collected cells; four assert terminals/deterministic intents/error channel, three report their declared skip. Collected and skipped offline (no docker) — the *runs* are unverified, see 3.5.

## 2. Leg vocabulary and spike findings

- [x] 2.1 Extend `tests/conformance/_spec.py`: add `SPARK` to `LEGS`, generalize `flink_variant()` to a leg-parameterized `variant_for(leg)` (design D1) keeping the existing real-time HITL override, and add `SPARK_SCENARIOS` mirroring `FLINK_SCENARIOS`
  - `LEGS = (DIRECT, FLINK, SPARK)`; `variant_for(leg)` replaces `flink_variant()` and applies `flink_hitl_timeout_ms` on both `_REAL_TIME_LEGS`; `SPARK_SCENARIOS` + a `skip_inventory(leg)` helper the status script reads. Both `flink_variant()` call sites in `_flink/pipeline.py` updated; `RULE_BUILDERS` made public there so both portable legs script from one mapping.
- [ ] 2.2 Spike the Spark portable runner against the stack (design D6): bring up the overlay, attempt each scenario starting with `single_shot`, and record per-scenario outcomes as the initial `Run()`/`Skip("<specific feature gap or harness constraint>")` declarations; append the findings (state-spec support, WATERMARK/REAL_TIME timer behavior, SDF source behavior, restart expressibility) to this change's `design.md` as a Findings section, following the `add-adapter-conformance-matrix` precedent
  - **(blocked: needs docker)** — no Spark job server can run in this environment. The Findings section IS appended to `design.md` (F1–F8) and records what was determined: three structural skips with named constraints (F2, including the answer to the embedded-master `restart_mid_suspension` open question), the option/health/diagnosis differences (F3, F4), and that the remaining four scenarios are **provisional `Run()`** whose evidence is the first weekly run (F1). Runner state-spec / WATERMARK / REAL_TIME / SDF behavior remain open.
- [x] 2.3 If the spike shows the streaming profile is entirely unsupported, land the leg with the documented all-skip declarations and a parked promotion process — do not convert the pipeline to batch to manufacture green (design risk 1)
  - Not triggered (the spike did not run), and the escape hatch is preserved: the pipeline is `StandardOptions.streaming = True` with a checkpoint dir, and design.md F1 states explicitly that red cells become named `Skip` declarations rather than a batch-mode conversion.

## 3. Spark stack and harness

- [x] 3.1 `docker/compose.spark.yaml` overlay (design D2): Beam Spark job server image at the repo's Beam pin (`2.72.0`, digest-pinned), embedded `local[4]` master, shared `beam-artifact-staging` volume, spark-scoped SDK-harness worker-pool service namespace-bound to the job-server container; base `docker/compose.yaml` untouched
  - `apache/beam_spark3_job_server:2.72.0@sha256:91b9a02c…6b95` (digest resolved from the registry), ports `28099/28098/28097`, `beam-sdk-harness-spark` with `network_mode: service:spark-jobserver`. Statically validated with `yaml.safe_load`; asserted no service-name collision with the base file, which is unmodified.
- [x] 3.2 `make compose-up-spark` / `compose-down-spark` targets wrapping the two-file compose invocation
  - Plus `compose-logs-spark` for the weekly workflow's capture-before-teardown step (`$(COMPOSE)` cannot see the overlay's services). `$(COMPOSE_SPARK)` is deliberately never folded into `$(COMPOSE)`.
- [x] 3.3 `tests/conformance/_spark/pipeline.py`: portable-runner options against the spark job endpoint, reusing the merged provider/registry/dispatch-agent machinery from `tests/conformance/_flink/pipeline.py` by module reference (everything picklable by name, spool ingest, outbox egress)
  - Reuses `RULE_BUILDERS`, `scenario_from_key`, and the three tagged-output encoders by import; owns `merged_provider`/`MergedSparkRegistry`/`SparkDispatchAgent` scoped to `SPARK_SCENARIOS` so a declared skip never reaches the job. `__reduce__` on both classes; no closures in the DoFn graph.
- [x] 3.4 `tests/conformance/_spark/harness.py`: run-scoped topics/spool, responder/drainer/topic-watcher reuse, a `SparkStackControl` analog with health checks, freshness handling, and `InfraFailure` classification; condition-driven deadlines, never bare sleeps
  - `SparkStackControl` (freshen, worker-pool restart, socket health check on both endpoints, job-server log tail as the stall self-diagnosis), `SparkRunConfig`/`SparkLegResults`/`SparkResponder`, `InfraFailure` on submission stalls. No restart phase (that scenario is a declared skip). Every deadline is `observed(predicate, …)`-driven.
- [ ] 3.5 Bring `test_spark.py` to green-or-declared-skip for both registered adapters against the local stack; record wall-clock per adapter in `design.md` Findings
  - **(blocked: needs docker)** — no stack to run against. Offline the cells are collected and skipped, which is expected. Wall-clock unmeasured (design F6); the leg timeout is provisionally the Flink leg's 1200 s with a 420 s phase deadline, to be re-derived from the first green weekly runs. Note also that the registry now has **four** adapters, not two.

## 4. Weekly workflow, marker wiring, and status reporting

- [x] 4.1 Implement `scripts/spark_weekly_status.py` to the 1.4 tests (design D5): scheduled-runs API query, streak + cadence computation, skip-drift scan, skip-inventory print, `$GITHUB_STEP_SUMMARY` verdict; report-only by construction
  - Pure core (`parse_runs`, `scheduled_runs`, `with_current_run`, `green_streak`/`red_streak`, `added_spark_skips`, `promotion_verdict`, `demotion_verdict`, `render_summary`) with the API call, `git log -p`, and summary write confined to `main()`. Always exits 0; the inventory is read from the live `_spec.py` declarations, never a copy. `--runs-json` reads a fixture instead of the network (used by 5.3).
- [x] 4.2 Add the `spark` marker to the closed registry in `pyproject.toml`; mark spark cells `integration + spark` (no `semantics` while best-effort, design D4) plus their `conformance_cell(adapter, scenario, "spark")` markers
  - `pytestmark = [integration, spark, slow]` in `test_spark.py`; the cell marker comes from `adapter_params(spec, SPARK)`, so the inventory hook counts all 28.
- [x] 4.3 Makefile selection wiring: `test-conformance-spark` (`-m "integration and spark" tests/conformance`, no exit-5 tolerance), `test-integration` gains `and not spark`; confirm `scripts/check_semantics_partition.py` and both semantics selections are byte-identical in behavior to before
  - `check_semantics_partition.py` green: `65 offline + 29 docker = 94 total; docker lane covered by 1 tests/semantics + 28 tests/conformance` — unchanged shape, no spark cell in either selection. Restated as a test in `test_spark_selection.py` so the property is enforced, not just observed once.
- [x] 4.4 `.github/workflows/spark-weekly.yml`: weekly `schedule` cron + `workflow_dispatch`, base stack + spark overlay bring-up, `make test-conformance-spark`, always-run status job invoking `scripts/spark_weekly_status.py` with the test job's conclusion
  - `cron: "0 6 * * 1"`, no `pull_request` trigger, `cancel-in-progress: false` (a cancelled scheduled run is a lost week). Status job is `if: always()`, `needs: [spark-conformance]`, `fetch-depth: 0` for the drift scan, `actions: read` for the run history. Statically validated with `yaml.safe_load`.
- [x] 4.5 Docs: add the `spark-weekly` row to the workflow table in `docs/ci.md` and a section documenting the promotion/demotion process (gate definition, streak/cadence/skip rules, who authors the flip changes); README note that Spark is best-effort with weekly verification — the `project.md:113` support statement itself is NOT changed by this change
  - `docs/ci.md`: table row + "The weekly Spark leg" section (containment story, local commands, promotion window). `README.md`: "Runner verification" subsection + the marker registry line. `openspec/project.md` line 113 is **untouched** — verified by `git diff --stat`.

## 5. Promotion and demotion process artifacts

- [x] 5.1 Write the promotion checklist into `docs/ci.md`: four qualifying scheduled-run links, zero-skip-drift confirmation, surviving-skip enumeration with what each excludes from the supported claim, benchmark-evidence position (design Open Questions), and the list of files the stage-2 change flips (`openspec/project.md` support statement, README, `docs/ci.md`, required-weekly marking)
  - Six numbered items under "Promotion checklist (author of the stage-2 change)".
- [x] 5.2 Write the demotion checklist: two-consecutive-red confirmation from the status summaries, files to flip back, and the announcement requirement (README + release notes)
  - Four numbered items under "Demotion checklist", including that the leg keeps running and re-promotion carries no partial credit.
- [x] 5.3 Dry-run both checklists against the status script's fixtures (a synthetic 4-green window and a synthetic 2-red window) to confirm each checklist is answerable from the summaries alone
  - Ran `spark_weekly_status.py --runs-json` against both synthetic windows. The 4-green window printed `PROMOTION READY` with four run links, the full three-skip inventory, and `Skip drift: none`; the 2-red window printed `NOT READY (green streak 0/4 …)` and `DEMOTION TRIGGERED (2 consecutive red scheduled weeks)`. **One gap found and fixed:** the demotion-watch line reported only the streak *length*, so checklist item 1 ("confirm both are scheduled runs at their final conclusion") was not answerable from the summary — `render_summary` now lists the red runs' links, pinned by `test_summary_links_the_red_runs_a_demotion_would_cite`. See design.md F8.

## 6. Archive coordination

- [ ] 6.1 Before archiving, confirm `add-adapter-conformance-matrix` has archived and `openspec/specs/adapter-conformance-matrix/spec.md` exists with the base requirement headers this change's delta renames/modifies (design D8); if its text drifted, reconcile this change's delta verbatim before archiving
  - Deferred to archive time by construction (that is what the task says). Current state: `openspec/specs/adapter-conformance-matrix/` still does not exist and `openspec/changes/add-adapter-conformance-matrix/` is still pending, so this change must not archive first. `openspec validate promote-spark-runner --strict` is green against the pending delta.

## 7. Gates

- [x] 7.1 `make lint` — `ruff check`: all checks passed; `ruff format --check`: 305 files already formatted.
- [x] 7.2 `make type` — `mypy --strict`: success, no issues in 299 source files.
- [x] 7.3 `make test-unit` (includes the new status-script and selection tests; coverage may not decrease) — `1341 passed, 9 skipped, 186 deselected` (was 1299 passed before this change; +42 tests). Combined coverage 94.97%, above the 90% floor.
- [ ] 7.4 `make coverage-ratchet`
  - **Pre-existing failure on the merged base, not caused by this change.** `coverage-baseline.toml` demands `branch_rate = 0.9497`; the merged tree measures `0.8984` **both with and without this change** (verified by stashing the whole working tree and re-running the unit tier: identical `0.8984`). This change adds no `src/` code and only adds tests, so it cannot lower the branch rate. Left for whoever reconciles the baseline against the merged M2 branch.
- [x] 7.5 `uv run python scripts/check_semantics_partition.py` (unchanged selections, still green) — `semantics tier partition OK: 65 offline + 29 docker = 94 total`.
- [x] 7.6 `uv run pre-commit run --all-files` <!-- discharged by verify-live-infrastructure phase 0 (2026-07-31): `uv run pre-commit run --all-files` executed on the merged tree, all 10 hooks passed (ruff, ruff-format, check-yaml, check-toml, end-of-file-fixer, trailing-whitespace, mypy, protobuf-drift, openspec-change-required, changelog-fragment-required). See verification-report.md. -->
  - **(not run: pre-commit is not in the synced dependency groups here.)** Its four checks are covered individually: `ruff`/`ruff-format` via 7.1, `mypy` via 7.2, `check-yaml` via `yaml.safe_load` on both new YAML files, and the `openspec-change-required` / `changelog-fragment-required` hooks are no-ops for this change (they fire only on `src/` edits, and this change touches no `src/` file).
- [x] 7.7 `openspec validate promote-spark-runner --strict` — `Change 'promote-spark-runner' is valid`.

Additionally run (not in the original list): `pytest -m "semantics and not integration"` → `64 passed, 5 skipped`; `pytest tests/conformance -m "not integration"` → `50 passed, 1 skipped`.

## Revision 1 — the spike could not run, so the initial declarations are provisional

**Artifact affected:** `design.md` D6 and the Open Questions.

D6 specifies that the stage-1 spike "drives each scenario against the spark leg
and records the outcome as its declaration". The implementation environment has
no docker, so no Spark job server could be started and no scenario could be
driven. Rather than block the whole change on an environment constraint —
which would leave the leg, the overlay, the workflow, and the promotion gate
unlanded — the declarations were made a priori and split into two kinds, with
the distinction documented in the new `design.md` Findings section (F1):

* **structural skips** (`bundle_retry_cache`, `restart_mid_suspension`,
  `ttl_expiry`), whose reasons are properties of the harness and the overlay
  topology rather than of the Spark runner, so a spike could not have changed
  them — `restart_mid_suspension` in particular *answers* the design's open
  question about embedded-master restarts;
* **provisional `Run()`** for the other four, whose evidence is the first
  `workflow_dispatch` or scheduled weekly run.

This does not weaken the gate: a provisional `Run()` that turns out to be
wrong goes red in the weekly lane, the summary publishes it, the promotion
clock never starts, and converting the cell to a named `Skip` resets the window
per D6's own rule. The alternative — `Skip("unknown")` — would violate the
spec's requirement that every skip reason name a *specific* constraint.

No spec change is required: the `spark-runner-support` requirements are written
about the process, not about which scenarios pass, and the *A spark-inexpressible
scenario is an explicit skip with a reason* scenario is satisfied by the three
structural skips.

## Revision 2 — the initial weekly summaries will report skip drift from this change itself

**Artifact affected:** none (behavior clarification for the first four weekly runs).

D5's drift scan is `git log --since='28 days ago' -p -- tests/conformance/_spec.py`
looking for added spark `Skip` declarations. This change's own commit adds
three of them, so for 28 days after it merges the weekly summary will report
skip drift and a `NOT READY` verdict — correctly. The promotion clock is
supposed to start after the leg's skip set has settled, not before it exists.
No suppression was added: an exception for "the commit that introduced the leg"
would be exactly the kind of special case that makes a mechanical gate
untrustworthy.
