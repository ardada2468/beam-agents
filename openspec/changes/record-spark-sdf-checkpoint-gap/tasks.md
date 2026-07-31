## 1. Record the finding in the leg declarations

- [x] 1.1 Convert `single_shot`, `multi_tool_inline`, `suspension_resume` and `approval_timeout_fallback` from `SPARK: Run()` to `SPARK: Skip(reason=...)` in `tests/conformance/_spec.py` (spec: *Scenarios blocked by the Spark SDF bundle-checkpoint gap are declared skips naming it*). Each reason names the missing bundle checkpoint handler, the `unbounded_per_element` spool SDF's `defer_remainder` tail poll in `tests/semantics/_e2e/spool.py`, the runner-level/adapter-independent nature of the gap, and the 2026-07-31 first-real-job-server run — worded per scenario, not four copies of one string.
  - Written in the voice of the existing `restart_mid_suspension` / `bundle_retry_cache` spark skips: each opens on the constraint, and each says what its own scenario consequently does *not* prove on Spark (fast path, inline tools, suspend/resume, REAL_TIME timers).
- [x] 1.2 Keep the `SPARK: Skip(` line shape that `scripts/spark_weekly_status.py`'s `_ADDED_SPARK_SKIP` regex matches, so the four additions are visible to the window's skip-drift scan rather than hidden by formatting (spec: *Adding the skips resets the window*).
  - All four render as `SPARK: Skip(` on its own line, matching `^\+.*\bSPARK\s*:\s*Skip\s*\(` in the diff.
- [x] 1.3 State the consequence at the declaration site (spec: *The consequence is stated where the declarations live*): a comment above `SPARK_SCENARIOS` recording that the tuple is now empty, that all 28 spark cells are collected-but-skipped, that the weekly status script's shrinkage rule makes promotion unreachable on an all-skip leg (the correct outcome), and the two ways out.

## 2. Verify the accounting is undisturbed

- [x] 2.1 `uv run pytest tests/conformance -m "not integration" -q` — the matrix meta-test's registry × scenario × leg accounting still balances (declared skips are cells), `test_every_spark_skip_names_a_specific_constraint` accepts the four new reasons, and `test_spark_runnable_scenarios_exclude_the_declared_skips` holds with an empty runnable set. **50 passed, 1 skipped.**
- [x] 2.2 `uv run pytest tests/scripts/test_spark_weekly_status.py -q` — `skip_inventory(SPARK)` is read by the status script; the seven-entry inventory and the drift/verdict rendering still behave. **34 passed.**
- [x] 2.3 `uv run ruff check .` and `uv run ruff format --check .`.
- [x] 2.4 `openspec validate record-spark-sdf-checkpoint-gap --strict`.
- [x] 2.5 Do **not** re-run the spark leg for this change: its verdict is the recorded 26m26s run of 2026-07-31, and re-running an all-skip leg submits nothing.

## 3. Follow-up (not done here)

- [ ] 3.1 Convert the four now-stale executing cell tests in `tests/conformance/test_spark.py` (`test_spark_single_shot`, `test_spark_multi_tool_inline`, `test_spark_suspension_resume`, `test_spark_approval_timeout_fallback`) into declared-skip reporters using the file's existing `_declared_skip(...)` helper, and update its module docstring (it still says three declared skips). Until then the declarations and the cell tests disagree: the next weekly run would submit a job scoped to an empty `SPARK_SCENARIOS` and report a confusing red instead of seven clean skips. Deliberately out of this change's scope, which is the declarations and this record.
- [ ] 3.2 File the finding upstream against Beam's Spark portable runner (`ActiveBundle` has no registered bundle checkpoint handler, so any `unbounded_per_element` SDF calling `defer_remainder` fails the invocation), and link the issue from this change so the first way out has a tracked owner.
- [ ] 3.3 If the upstream path stalls, evaluate a non-SDF ingest for the spark leg (pre-sealed bounded spool read, or Kafka ingest through the runner's own unbounded source) as a separate proposal — with the explicit constraint from `promote-spark-runner` design risk 1 that a batch-mode conversion is not an acceptable way to obtain green.
