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

**Correction to the note above, found while working:** `mutants/` caches the generated *mutants*,
not their *results*. mutmut 3.6.0's `create_mutants_for_file` resets every recorded exit code to
`None` for any source file it finds unmodified, on every `mutmut run` — so a run always re-executes
the whole selection, and `mutation_gate.py` alone re-reports only if no `mutmut run` has happened
since. Targeted re-runs (`uv run mutmut run "beam_agents.core.migration.*"`) are the fast iteration
loop and were used throughout, but they leave every other module reading `not checked`, so the final
verdict has to come from a full `make mutation`. Always `rm -rf mutants/tests` first (as the Makefile
does): mutmut's copy only ever *adds*, so an edited test file is otherwise never re-copied and the
run silently measures the old suite.

**Result: `make mutation` passes.** 2475 core mutants — killed **1975** (was 1841), no tests **477**
(was 479), survived **23** (was 154), timeout **0** (was 1). All 23 survivors are declared equivalents
with written reasons; 12 of them are new entries from this change.

## 1. `_AgentDoFn._flush` — 64 survivors, 44% of the debt

- [x] 1.1 Read the flush path against its specs first (`add-adaptive-batching`,
  `add-longterm-memory-stores`, and the outbox/commit requirements in the state-guarantees
  capability). List the decisions `_flush` actually makes: what triggers a flush, what is included,
  ordering, what happens on partial failure, and what is committed vs. re-buffered.
- [x] 1.2 Map the 64 survivors onto those decisions. Expect a small number of *behavioral clusters* —
  boundary comparisons (`>=` vs `>`), the empty/singleton batch, size vs. count triggers, the
  ordering of commit and emit — not 64 independent facts. — **The clusters are not the ones this
  task predicted, and that is the finding.** `_flush`'s boundary decisions were already asserted;
  what was unasserted was its *failure* surface. The 64 split four ways:
  - **51 in the three exception exits.** 22 in `except ActivationTimeout`, 25 in the general
    `except Exception`, and 10 in `except ActivationFailed`. The first two branches were **never
    executed by any selected test at all** — every argument in them could be nulled, crossed with its
    neighbour, or dropped outright and nothing noticed. The third ran, but the one test covering it
    asserted a substring of the dead-letter detail and the *count* of traces, so the key on the
    record, the seq and clock on the trace, the error type, and the whole failure-position block were
    all free.
  - **6 in the two keyed-state reads** (`memory`/`llm_cache`): every batching test started from an
    empty blob, where "read the committed state" and "start from nothing" are the same picture.
  - **6 in `_activate`'s own call**, plus the batch-trigger hand-off: a dropped `batch_trigger` still
    produces a batch trace, just one that has forgotten which threshold assembled it.
  - **1** in `_failed_flush`'s dead-letter key.
- [x] 1.3 Write boundary tests per cluster, each naming the scenario it comes from. Prefer the
  existing fake-handle DoFn suites over pipeline tests: `test_dofn_pipeline.py`,
  `test_dofn_streaming.py` and `test_transform.py` are **deselected under mutmut** (mutmut's
  `os.wait()` reaps DirectRunner worker subprocesses), so a mutant is only killable from a
  non-pipeline test. — Six tests in `tests/core/test_dofn_batching.py`, all derived from
  "A failed flush dead-letters every batched event and consumes the buffer", "A retried flush bundle
  replays deterministically", and "One activation per flush …": the timeout exit (via
  `make_briefly_slow_provider`, the pattern the per-event path already uses), the general-exception
  exit (via a deliberately un-`setup()` DoFn, mirroring
  `test_activating_before_setup_is_refused_and_named`), the `ActivationFailed` exit's trace shape, a
  flush over seeded working memory, a flush served from a seeded replay cache, and the batch
  attributes on the flush's `ACTIVATION_START`. The dead-letter assertions moved from field-by-field
  to whole-`ActivationError` equality, which is what makes the key and the event time load-bearing.
- [x] 1.4 Re-run and confirm the `_flush` count drops. Record the before/after. — `_flush`: **64 → 0**.
  `dofn.py` as a whole: **102 → 2**, both of the remainder declared equivalent (1.5).
- [x] 1.5 Any residual survivor that is genuinely equivalent goes to `mutation-exclusions.toml` with a
  reason describing the mutation — and re-read that file's positional-drift warning first, because an
  entry is an *index*, not an identity. — Two: `_commit`/`_record_commit`'s `flush_trigger=""`
  default. Its only consumer is `flush_trigger == TRIGGER_SIZE`, and the default is reached only when
  a caller omits the argument, so every non-`"size"` default selects the same counter and no input
  distinguishes them. Both verified against the generated tree before the entries were written.

## 2. The rest of `dofn.py` — 38 survivors

- [x] 2.1 `_flush_expiring` (9): TTL/expiry boundary behavior. — The `store is None or bridge is None`
  guard and its message (a hook wired past `AgentConfig`'s refusal has nowhere to flush to), and the
  hook's bridge submission being bounded by `activation_timeout_s`. `tests/core/test_dofn_expire.py`.
- [x] 2.2 `_activate` (7). — The long-term store, the summarizer, and the batch trigger: three knobs
  the DoFn forwards across the seam and never reads itself, so a dropped one produces an
  ordinary-looking committed activation with the feature silently off. `tests/core/test_dofn_activation.py`
  (store + summarizer, each with its opt-out shape asserted alongside) and `test_dofn_batching.py`.
- [x] 2.3 `_buffer` (5), `_commit` (5), `_record_commit` (5) — these sit either side of the commit
  boundary, so cover them together with `_flush`'s commit/emit ordering rather than in isolation. —
  `_buffer`'s two arming mutants needed a **cross-bundle** test: within one bundle under a fixed
  clock, "armed on the first buffered event" and "armed on every later one" produce the same single
  mark, so the existing test could not tell them apart. Appending to a buffer an earlier bundle left
  behind can: zero marks against one. The `_commit`/`_record_commit` handle assertions are driven
  directly, and `COUNTER_LONGTERM_UPSERTS` needed *three* staged upserts — `incr(name)` defaults to a
  step of 1, which a single-upsert activation cannot distinguish from `incr(name, len(upserts))`.
- [x] 2.4 `setup` (2), `teardown` (1), `_build_store` (1), `_AgentDoFn.__init__` (1),
  `_failed_flush` (1), `_rearm_flush` (1). — `setup`/`teardown`/`_build_store` are covered by two
  tests in `test_dofn_longterm.py`: the parsed URI's `parts` reaching the factory (every existing test
  there used `memory://`, whose parts are *empty*, so a dropped `parts` was invisible), and both
  lifecycle submissions carrying the configured timeout rather than `None`, which would block the
  Beam thread indefinitely against a wedged backend.

## 3. `context.py` — 15 survivors

- [x] 3.1 `ActivationContext.__init__` (11): mostly field-initialisation mutants. Several may be
  genuinely equivalent (a private counter initialised to `None` vs `0` that the first write replaces);
  judge each, and exclude with a reason rather than contriving a test that asserts a private initial
  value. — **None were field initialisations and none were equivalent.** All 11 are fragments of the
  two `ValueError` messages the constructor raises: `events=[]` (6) and a budget without a decoder (5).
  Both are spec-pinned — the second by "SHALL raise `ValueError` … `ActivationContext` SHALL apply the
  same set-without-decode rejection at its own construction" — and both were asserted with a
  `match=` substring that survived every rewrite of the explanation. Now asserted whole.
- [x] 3.2 `_charge` (2), `call_model` (1), `AgentContext.drain` (2). — `drain`'s two were a real gap:
  nothing anywhere asserted that the drained result carries the staged long-term upserts, so a
  `longterm.save` could go unflushed with every other drained field looking correct. `call_model`'s
  one is the pre-existing `cache_hit=None` equivalent.
- [x] 3.3 Note that `call_model` and both `__init__`s are exactly where `add-token-budgets` inserted
  statements and caused the two exclusion entries to drift. Verify any new entry against
  `mutants/src/beam_agents/core/context.py` before committing it. — No new `context.py` entries were
  needed; the six that survive are the pre-existing ones, all still matching their reasons.

## 4. `migration.py` — 15 survivors plus the one timeout

- [x] 4.1 `_migrate_to_current` (4) and `_migration` (4): the registry lookup and step-application
  path. `add-state-schema-migration`'s scenarios are the source. — Seven are `typing.cast`
  **first-argument** flips. `cast(typ, val)` returns `val` and never inspects `typ`; it is erased at
  runtime and the only consumer that could tell them apart (mypy) never sees the generated tree. The
  eighth is `version > current` → `>=`, made unreachable by the `version == current` fast path
  immediately above it. All eight declared equivalent.
- [x] 4.2 `MigrationStepError.__init__` (3) and `MissingMigrationError.__init__` (3): error-message
  construction. If a mutant only changes a message the specs do not pin, that is a candidate
  exclusion — but check whether a scenario requires the message to name the version or step, in which
  case it is a real gap. — Checked, and they are real gaps: the spec requires "a typed
  missing-migration error naming the message type and the missing `from_version`", and both messages
  carry arithmetic (`1..current - 1`, `expected from_version + 1`) that the existing substring
  assertions left free. Killed by whole-message assertions in `test_migration.py`.
- [x] 4.3 **Resolve the timeout**: `x_migrate_to_current__mutmut_45` is reported `[timeout !]`, which
  is neither killed nor survived. Determine whether the mutation causes a genuine infinite loop (a
  real finding about the migration loop's termination condition) or merely exceeds the per-mutant
  budget. Record which; an indeterminate result must not be left standing. — **Genuine
  non-termination, not a budget overrun.** The mutation is `version += 1` → `version = 1` at the foot
  of the chain-walk loop. That increment is the loop's *only* source of progress, so with it pinned
  the walk re-fetches and re-applies the same step forever; every step still advances the blob's stamp
  by one, so the per-step verification passes on each pass and nothing else terminates it. Every test
  that walks a chain hangs, which is why the run reported a timeout rather than a kill.
  Resolved **into a kill**, not suppressed: the suite's `_step_appending` doubles now refuse a second
  application, so the first chain-walking test to run fails immediately instead of hanging, and
  `test_the_walk_advances_one_version_per_step_and_terminates` states the property directly (each
  registered step sees exactly the version it was registered for, once, in order). The mutant is now
  reported `killed`.

## 5. `loop.py`, `transform.py`, `batching.py` — 12 survivors

- [x] 5.1 `loop.py`: `_flush_longterm` (3), `run_activation` (2), `LongtermFlushFailed.__init__` (2). —
  The flush error now names the record it stopped at (attribute and message), the summarizer trigger
  is pinned at exactly `size_bytes` and `size_bytes + 1` (the two adjacent values that tell `>=` from
  `>`; every other trigger in the suite sits far enough away that both agree), and the guard message
  is driven directly.
- [x] 5.2 `transform.py` `__require_positive` (3) and `batching.py` `__require_positive` (2): argument
  validation boundaries. These are small and independent — a good place to start for calibration. —
  Used for calibration, and they surfaced a structural point worth recording. `transform.py`'s
  validator was *reached* (test_batching.py constructs `AgentConfig`) but unassertable from anywhere
  in the selection, because the scenario that owns it lives in the deselected `test_transform.py`.
  The runner-free half of that requirement — the numeric knobs' boundary and immutability — **moved**
  into a new `tests/core/test_config_validation.py` rather than being duplicated; the sink-URI half
  stayed behind, because reaching `DefaultSinkResolver.validate`/`resolve`/`_parse` from inside the
  selection is the move `mutation-baseline.toml` already records as tried and reverted.

## 6. The two `no tests` ratchet regressions

- [x] 6.1 `snapshot.py` rose **0 → 2**. It was fully reached before, so this is new core code the
  selection does not cover. Cover it. — Covered, and **not** ceilinged. `serialize_snapshot` is a pure
  function whose only caller is `_WriteSnapshots` inside a pipeline; `tests/core/test_dofn_export.py`
  now drives it directly against the "A configured snapshots sink receives serialized snapshots keyed
  by entity" scenario. The module is back to the implicit zero. Its two mutants then surfaced as
  survivors and are declared equivalent for exactly the reason the four `error_records` entries are:
  `SerializeToString(deterministic=...)` orders `map<>` fields and nothing else, and the only map in
  the whole schema is `TraceEvent.attributes`, which nothing reachable from `StateSnapshot` embeds.
- [x] 6.2 `transform.py` rose **409 → 474** (+65). Its pipeline suites are deselected under mutmut for
  the documented DirectRunner/`os.wait()` reason, so much of this is structural — but establish how
  much of the +65 is reachable from non-pipeline tests and cover that part. Raise the ceiling in
  `mutation-baseline.toml` only for the genuinely unreachable remainder, with the reason written down.
  — Established by **measurement**, not by reading the diff: mutmut's generator was re-run over the
  revision that set the 409 ceiling (`5afd49b`) and over `HEAD`, and the per-function delta is
  recorded in `mutation-baseline.toml`. All +68 new mutants are the `snapshots_to` sink
  (`_WriteSnapshots`, the `snapshots_to` arms of `validate`/`resolve`, the `.snapshots` wiring in
  `RunAgent.expand`, and the `_encoded_transform` extraction) — the same deselected-suite-only
  territory the file's earlier entries describe. Three moved the other way and are killed (5.2), so
  409 + 68 − 3 = **474**.
- [x] 6.3 Confirm `context.py` (0) and `dofn.py` (3) remain at their baselines. — Both at baseline;
  `dofn.py`'s 3 are `_SumCombineFn`'s methods, which no selected test executes.

## 7. Gates

- [x] 7.1 `make mutation` passes: no survivors outside `mutation-exclusions.toml`, no timeout, both
  `no tests` ceilings satisfied. — `2475 core mutants -- killed: 1975, no tests: 477, survived: 23`,
  `mutation gate passed`.
- [x] 7.2 Every new exclusion entry names a real mutation and states why no test can kill it, verified
  against the generated `mutants/` tree. — 12 new entries (7 `cast` first-arguments and 1 unreachable
  comparison in `migration.py`, 2 `flush_trigger` defaults in `dofn.py`, 2 `deterministic=` flips in
  `snapshot.py`), each read off `mutants/src/beam_agents/core/<module>.py` at its index before the
  reason was written. The gate's own "may exempt only live survivors" check passes, so none of them
  has drifted onto a killed mutant.
- [x] 7.3 `make lint`, `make type`, `make test-unit`, `make test-semantics-offline` all green. —
  lint clean; `mypy --strict` clean over 375 files; 1998 unit tests pass; 79 offline semantics gates
  pass (1 declared skip).
- [x] 7.4 `make coverage-ratchet` at or above baseline — new tests should raise it, never lower it. —
  Raised: branch coverage 91.64% → **91.80%**, and `coverage-baseline.toml` is moved up to lock it in.
- [x] 7.5 Discharge the mutation items the five deferring changes left blocked, citing this run. —
  Discharged in `add-adaptive-batching` (9.5), `add-compaction-strategies` (7.6), `add-replay-cli`
  (3.3, 6.7), `add-state-schema-migration` (6.5) and `add-token-budgets` (6.5), each citing this run
  and naming the survivors that were its own. `add-longterm-memory-stores` (10.4) is discharged too:
  it deferred the same gate plus the coverage ratchet, and both now pass.
- [x] 7.6 `openspec validate close-core-mutation-gaps --strict` passes.
