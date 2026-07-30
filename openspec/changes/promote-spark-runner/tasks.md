## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 Leg-declaration completeness test (spec scenario: *A scenario without a spark declaration cannot build a cell*): unit test in `tests/conformance/` asserting every `ScenarioSpec.legs` dict declares every entry of `LEGS` — fails first because `LEGS` has no `spark` entry and no scenario declares one
- [ ] 1.2 Meta-test expectation for the three-leg matrix (spec scenario: *The meta-test accounts for the spark leg*): with `spark` in `LEGS`, `tests/conformance/test_matrix.py` must expect `len(ADAPTERS) × len(SCENARIOS) × 3` cells — fails until `test_spark.py` cells exist and are collected
- [ ] 1.3 Marker-selection pinning tests (spec scenarios: *Pull-request workflows are unchanged*, *Spark cells are counted but stay out of the semantics partition*): collection-level test asserting the per-PR selections (`-m "integration and not semantics and not spark"`, `-m "semantics and not integration"`, `-m "semantics and integration"`) collect zero spark-leg cells and `-m "integration and spark"` collects exactly the declared spark cells — fails until the `spark` marker and cell markings exist
- [ ] 1.4 `scripts/spark_weekly_status.py` unit tests against offline fixtures (spec scenarios: *Consecutive green scheduled runs accumulate a streak*, *A red week resets the streak*, *A missed week breaks consecutiveness*, *Manual dispatch does not affect the promotion window*, *A skip added mid-window resets the promotion clock*): streak over scheduled-run JSON pages, final-conclusion rerun handling, >8-day cadence gap breaking the streak, `workflow_dispatch` exclusion, skip-drift detection over git-log text, and the ready/not-ready verdict lines — fail first because the script does not exist
- [ ] 1.5 Spark leg cell tests `tests/conformance/test_spark.py` (spec scenarios: *Spark leg runs the declared scenarios through the job server*, *A spark-inexpressible scenario is an explicit skip with a reason*, *Stack failure is not a Spark verdict*): mirror `test_flink.py`'s per-scenario assertions (terminal outputs, deterministic intents, error channel, declared-skip reporting, `InfraFailure` separation) against the spark harness — fail first because `tests/conformance/_spark/` does not exist

## 2. Leg vocabulary and spike findings

- [ ] 2.1 Extend `tests/conformance/_spec.py`: add `SPARK` to `LEGS`, generalize `flink_variant()` to a leg-parameterized `variant_for(leg)` (design D1) keeping the existing real-time HITL override, and add `SPARK_SCENARIOS` mirroring `FLINK_SCENARIOS`
- [ ] 2.2 Spike the Spark portable runner against the stack (design D6): bring up the overlay, attempt each scenario starting with `single_shot`, and record per-scenario outcomes as the initial `Run()`/`Skip("<specific feature gap or harness constraint>")` declarations; append the findings (state-spec support, WATERMARK/REAL_TIME timer behavior, SDF source behavior, restart expressibility) to this change's `design.md` as a Findings section, following the `add-adapter-conformance-matrix` precedent
- [ ] 2.3 If the spike shows the streaming profile is entirely unsupported, land the leg with the documented all-skip declarations and a parked promotion process — do not convert the pipeline to batch to manufacture green (design risk 1)

## 3. Spark stack and harness

- [ ] 3.1 `docker/compose.spark.yaml` overlay (design D2): Beam Spark job server image at the repo's Beam pin (`2.72.0`, digest-pinned), embedded `local[4]` master, shared `beam-artifact-staging` volume, spark-scoped SDK-harness worker-pool service namespace-bound to the job-server container; base `docker/compose.yaml` untouched
- [ ] 3.2 `make compose-up-spark` / `compose-down-spark` targets wrapping the two-file compose invocation
- [ ] 3.3 `tests/conformance/_spark/pipeline.py`: portable-runner options against the spark job endpoint, reusing the merged provider/registry/dispatch-agent machinery from `tests/conformance/_flink/pipeline.py` by module reference (everything picklable by name, spool ingest, outbox egress)
- [ ] 3.4 `tests/conformance/_spark/harness.py`: run-scoped topics/spool, responder/drainer/topic-watcher reuse, a `SparkStackControl` analog with health checks, freshness handling, and `InfraFailure` classification; condition-driven deadlines, never bare sleeps
- [ ] 3.5 Bring `test_spark.py` to green-or-declared-skip for both registered adapters against the local stack; record wall-clock per adapter in `design.md` Findings

## 4. Weekly workflow, marker wiring, and status reporting

- [ ] 4.1 Implement `scripts/spark_weekly_status.py` to the 1.4 tests (design D5): scheduled-runs API query, streak + cadence computation, skip-drift scan, skip-inventory print, `$GITHUB_STEP_SUMMARY` verdict; report-only by construction
- [ ] 4.2 Add the `spark` marker to the closed registry in `pyproject.toml`; mark spark cells `integration + spark` (no `semantics` while best-effort, design D4) plus their `conformance_cell(adapter, scenario, "spark")` markers
- [ ] 4.3 Makefile selection wiring: `test-conformance-spark` (`-m "integration and spark" tests/conformance`, no exit-5 tolerance), `test-integration` gains `and not spark`; confirm `scripts/check_semantics_partition.py` and both semantics selections are byte-identical in behavior to before
- [ ] 4.4 `.github/workflows/spark-weekly.yml`: weekly `schedule` cron + `workflow_dispatch`, base stack + spark overlay bring-up, `make test-conformance-spark`, always-run status job invoking `scripts/spark_weekly_status.py` with the test job's conclusion
- [ ] 4.5 Docs: add the `spark-weekly` row to the workflow table in `docs/ci.md` and a section documenting the promotion/demotion process (gate definition, streak/cadence/skip rules, who authors the flip changes); README note that Spark is best-effort with weekly verification — the `project.md:113` support statement itself is NOT changed by this change

## 5. Promotion and demotion process artifacts

- [ ] 5.1 Write the promotion checklist into `docs/ci.md`: four qualifying scheduled-run links, zero-skip-drift confirmation, surviving-skip enumeration with what each excludes from the supported claim, benchmark-evidence position (design Open Questions), and the list of files the stage-2 change flips (`openspec/project.md` support statement, README, `docs/ci.md`, required-weekly marking)
- [ ] 5.2 Write the demotion checklist: two-consecutive-red confirmation from the status summaries, files to flip back, and the announcement requirement (README + release notes)
- [ ] 5.3 Dry-run both checklists against the status script's fixtures (a synthetic 4-green window and a synthetic 2-red window) to confirm each checklist is answerable from the summaries alone

## 6. Archive coordination

- [ ] 6.1 Before archiving, confirm `add-adapter-conformance-matrix` has archived and `openspec/specs/adapter-conformance-matrix/spec.md` exists with the base requirement headers this change's delta renames/modifies (design D8); if its text drifted, reconcile this change's delta verbatim before archiving

## 7. Gates

- [ ] 7.1 `make lint`
- [ ] 7.2 `make type`
- [ ] 7.3 `make test-unit` (includes the new status-script and selection tests; coverage may not decrease)
- [ ] 7.4 `make coverage-ratchet`
- [ ] 7.5 `uv run python scripts/check_semantics_partition.py` (unchanged selections, still green)
- [ ] 7.6 `uv run pre-commit run --all-files`
- [ ] 7.7 `openspec validate promote-spark-runner --strict`
