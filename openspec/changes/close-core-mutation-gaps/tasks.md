# Tasks: close-core-mutation-gaps

**The standing rule, from `mutation-exclusions.toml` and `openspec/project.md`:** never weaken,
deselect or `xfail` a test to make a mutant pass. A mutant is killed by a test derived from the
owning spec scenario, or it is declared equivalent with a written reason. There is no third option.

**Write the test from the spec, not from the mutant.** A test shaped to kill `__mutmut_37`
specifically will kill that mutant and nothing else. Ask what behavior the mutated line implements,
find the scenario that requires it, and assert *that*. One good boundary test usually kills a dozen
mutants at once — which is why the 64-mutant `_flush` cluster is not 64 tasks.

Run `make mutation` to regenerate; `mutants/` is cached, so
`uv run python scripts/mutation_gate.py` alone re-reports without re-running.

## 1. `_AgentDoFn._flush` — 64 survivors, 44% of the debt

- [ ] 1.1 Read the flush path against its specs first (`add-adaptive-batching`,
  `add-longterm-memory-stores`, and the outbox/commit requirements in the state-guarantees
  capability). List the decisions `_flush` actually makes: what triggers a flush, what is included,
  ordering, what happens on partial failure, and what is committed vs. re-buffered.
- [ ] 1.2 Map the 64 survivors onto those decisions. Expect a small number of *behavioral clusters* —
  boundary comparisons (`>=` vs `>`), the empty/singleton batch, size vs. count triggers, the
  ordering of commit and emit — not 64 independent facts.
- [ ] 1.3 Write boundary tests per cluster, each naming the scenario it comes from. Prefer the
  existing fake-handle DoFn suites over pipeline tests: `test_dofn_pipeline.py`,
  `test_dofn_streaming.py` and `test_transform.py` are **deselected under mutmut** (mutmut's
  `os.wait()` reaps DirectRunner worker subprocesses), so a mutant is only killable from a
  non-pipeline test.
- [ ] 1.4 Re-run and confirm the `_flush` count drops. Record the before/after.
- [ ] 1.5 Any residual survivor that is genuinely equivalent goes to `mutation-exclusions.toml` with a
  reason describing the mutation — and re-read that file's positional-drift warning first, because an
  entry is an *index*, not an identity.

## 2. The rest of `dofn.py` — 38 survivors

- [ ] 2.1 `_flush_expiring` (9): TTL/expiry boundary behavior.
- [ ] 2.2 `_activate` (7).
- [ ] 2.3 `_buffer` (5), `_commit` (5), `_record_commit` (5) — these sit either side of the commit
  boundary, so cover them together with `_flush`'s commit/emit ordering rather than in isolation.
- [ ] 2.4 `setup` (2), `teardown` (1), `_build_store` (1), `_AgentDoFn.__init__` (1),
  `_failed_flush` (1), `_rearm_flush` (1).

## 3. `context.py` — 15 survivors

- [ ] 3.1 `ActivationContext.__init__` (11): mostly field-initialisation mutants. Several may be
  genuinely equivalent (a private counter initialised to `None` vs `0` that the first write replaces);
  judge each, and exclude with a reason rather than contriving a test that asserts a private initial
  value.
- [ ] 3.2 `_charge` (2), `call_model` (1), `AgentContext.drain` (2).
- [ ] 3.3 Note that `call_model` and both `__init__`s are exactly where `add-token-budgets` inserted
  statements and caused the two exclusion entries to drift. Verify any new entry against
  `mutants/src/beam_agents/core/context.py` before committing it.

## 4. `migration.py` — 15 survivors plus the one timeout

- [ ] 4.1 `_migrate_to_current` (4) and `_migration` (4): the registry lookup and step-application
  path. `add-state-schema-migration`'s scenarios are the source.
- [ ] 4.2 `MigrationStepError.__init__` (3) and `MissingMigrationError.__init__` (3): error-message
  construction. If a mutant only changes a message the specs do not pin, that is a candidate
  exclusion — but check whether a scenario requires the message to name the version or step, in which
  case it is a real gap.
- [ ] 4.3 **Resolve the timeout**: `x_migrate_to_current__mutmut_45` is reported `[timeout !]`, which
  is neither killed nor survived. Determine whether the mutation causes a genuine infinite loop (a
  real finding about the migration loop's termination condition) or merely exceeds the per-mutant
  budget. Record which; an indeterminate result must not be left standing.

## 5. `loop.py`, `transform.py`, `batching.py` — 12 survivors

- [ ] 5.1 `loop.py`: `_flush_longterm` (3), `run_activation` (2), `LongtermFlushFailed.__init__` (2).
- [ ] 5.2 `transform.py` `__require_positive` (3) and `batching.py` `__require_positive` (2): argument
  validation boundaries. These are small and independent — a good place to start for calibration.

## 6. The two `no tests` ratchet regressions

- [ ] 6.1 `snapshot.py` rose **0 → 2**. It was fully reached before, so this is new core code the
  selection does not cover. Cover it.
- [ ] 6.2 `transform.py` rose **409 → 474** (+65). Its pipeline suites are deselected under mutmut for
  the documented DirectRunner/`os.wait()` reason, so much of this is structural — but establish how
  much of the +65 is reachable from non-pipeline tests and cover that part. Raise the ceiling in
  `mutation-baseline.toml` only for the genuinely unreachable remainder, with the reason written down.
- [ ] 6.3 Confirm `context.py` (0) and `dofn.py` (3) remain at their baselines.

## 7. Gates

- [ ] 7.1 `make mutation` passes: no survivors outside `mutation-exclusions.toml`, no timeout, both
  `no tests` ceilings satisfied.
- [ ] 7.2 Every new exclusion entry names a real mutation and states why no test can kill it, verified
  against the generated `mutants/` tree.
- [ ] 7.3 `make lint`, `make type`, `make test-unit`, `make test-semantics-offline` all green.
- [ ] 7.4 `make coverage-ratchet` at or above baseline — new tests should raise it, never lower it.
- [ ] 7.5 Discharge the mutation items the five deferring changes left blocked, citing this run.
- [ ] 7.6 `openspec validate close-core-mutation-gaps --strict` passes.
