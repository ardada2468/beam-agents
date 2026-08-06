## Why

`make mutation` had **never executed** on this tree. `verify-live-infrastructure` found it aborting
in 7 seconds during baseline stats collection, repaired it, and ran it for the first time. The gate
now works — and fails:

```
2475 core mutants -- killed: 1841, no tests: 479, survived: 154, timeout: 1
```

Excluding the 11 justified entries in `mutation-exclusions.toml`, **144 surviving mutants** need a
test or a justification, plus one indeterminate timeout. Five changes had deferred this gate, so the
debt accumulated invisibly behind a target that could not run.

**This is not 144 scattered problems.** The survivors concentrate sharply:

| Function | Survivors | Share |
| --- | --- | --- |
| `_AgentDoFn._flush` | **64** | 44% |
| `ActivationContext.__init__` | 11 | 8% |
| `_AgentDoFn._flush_expiring` | 9 | |
| `_AgentDoFn._activate` | 7 | |
| `_AgentDoFn._buffer` / `_commit` / `_record_commit` | 15 | |
| `core/migration.py` (4 functions + 2 error `__init__`s) | 15 | |
| `_flush_longterm`, `_run_activation`, `__require_positive`, misc | 23 | |

By module: `dofn.py` 102, `context.py` 15, `migration.py` 15, `loop.py` 7, `transform.py` 3,
`batching.py` 2.

A single function — the adaptive-batching flush path — accounts for nearly half the debt. That is
the shape of a function whose behavior is exercised end-to-end but whose individual decisions are
never asserted: the tests prove flushing happens, not that it happens *for the right reasons at the
right boundaries*. Mutation testing is the only gate that distinguishes those, which is why this
matters more than the raw number suggests.

Two `no tests` ratchets also regressed, meaning core code the mutation selection cannot reach grew:

```
error: un-mutation-tested mutants in snapshot.py rose from 0 to 2.
error: un-mutation-tested mutants in transform.py rose from 409 to 474.
```

`transform.py`'s is partly structural — its pipeline suites are deselected under mutmut because
mutmut's `os.wait()` reaping collides with Beam DirectRunner worker subprocesses — but the count still
grew by 65, and `snapshot.py` went from fully covered to not covered at all.

## What Changes

- **Tests that kill the surviving mutants**, each derived from the owning spec scenario rather than
  written to the mutant. The `_flush` cluster is the priority and is treated as one body of work, not
  64 individual fixes.
- **Genuine equivalents move to `mutation-exclusions.toml`** with a stated reason, per that file's
  existing rules — never a weakened or deselected test.
- **The one `timeout` mutant** (`migration.x_migrate_to_current__mutmut_45`) is resolved into a real
  verdict; an indeterminate result is neither a kill nor a survival.
- **The two `no tests` regressions** are either covered or their ceilings raised with justification in
  `mutation-baseline.toml` — with the structural reason for `transform.py`'s deselection stated
  explicitly rather than assumed.

## Capabilities

### Modified Capabilities

None. No specified behavior changes — this adds test coverage for behavior the specs already
require. Where a surviving mutant reveals that a spec scenario is *unasserted*, the test is derived
from that scenario.

## Impact

- **Modified:** `tests/core/` (new and strengthened tests), `mutation-exclusions.toml` (justified
  equivalents), possibly `mutation-baseline.toml` (the two `no tests` ceilings, with justification).
- **No `src/` change expected.** If a surviving mutant reveals an actual behavioral defect rather than
  a test gap, that is a separate filed change — this one closes coverage.
- **Gates:** `make mutation` currently fails. This change is what makes it pass, which in turn is what
  lets the five changes that deferred it be honestly discharged.
- **Effort:** substantial and concentrated. The `_flush` cluster is the bulk; the `migration.py` and
  `context.py` clusters are small and self-contained and can land independently.
- **Not in scope:** the Spark leg (`record-spark-sdf-checkpoint-gap`) and the Dataflow tier
  (`complete-dataflow-verification`).
