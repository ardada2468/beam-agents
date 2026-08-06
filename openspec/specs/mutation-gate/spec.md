# mutation-gate Specification

## Purpose
TBD - created by archiving change enforce-mutation-gate-on-core. Update Purpose after archive.
## Requirements
### Requirement: Mutation scope is limited to `core/`

Mutant generation SHALL be restricted to `src/beam_agents/core/`. `[tool.mutmut]` MUST set `only_mutate` to a glob covering that directory and no other, and MUST retain `do_not_mutate` for the generated protobuf bindings. Modules outside `core/` MUST NOT be mutated, because the killing test selection is `tests/core` and mutants elsewhere can only be reported as uncovered or survived regardless of their real test quality.

`source_paths` MUST remain at its default (`src/`) so that the whole package is copied into the mutant tree and imports resolve; scope MUST be narrowed with `only_mutate` rather than by narrowing `source_paths`.

#### Scenario: Non-core modules produce no mutants

- **WHEN** mutant generation runs against the repository
- **THEN** no mutant is generated for any file under `src/beam_agents/model/`, `tools/`, `memory/`, `actions/`, or `testing/`

#### Scenario: Generated protobuf bindings are never mutated

- **WHEN** mutant generation runs against the repository
- **THEN** no mutant is generated for any file under `src/beam_agents/_protos/`

#### Scenario: Core tests still import non-core packages under mutation

- **WHEN** the mutation run executes `tests/core` inside the generated `mutants/` tree
- **THEN** modules such as `beam_agents.memory.facade` import successfully, because the full `src/` tree was copied even though only `core/` was mutated

### Requirement: Surviving mutants fail the build

`mutmut run` exits `0` regardless of results and MUST NOT be used as the pass/fail signal. A gate script SHALL read mutmut's recorded per-mutant statuses and exit non-zero when any mutant has a failing status.

The following statuses MUST fail: `survived`, `timeout`, `suspicious`, `segfault`, `not checked`, and `check was interrupted by user`. A `timeout` MUST be reported distinctly from a `survived`, because it indicates the mutant was neither confidently killed nor confidently survived. The statuses `killed`, `skipped`, and `caught by type check` MUST pass.

The gate MUST print each failing mutant's name and source location so the failure is actionable without re-running mutation locally.

#### Scenario: Survivor fails the gate

- **WHEN** the gate evaluates a run in which at least one mutant has status `survived`
- **THEN** the gate exits non-zero and prints the surviving mutant's name and source location

#### Scenario: Clean run passes the gate

- **WHEN** the gate evaluates a complete run with no undeclared survivor or indeterminate status and every module's `no tests` count is at or below its baseline
- **THEN** the gate exits zero

#### Scenario: Timed-out mutant fails the gate

- **WHEN** the gate evaluates a run in which a mutant has status `timeout`
- **THEN** the gate exits non-zero and reports it distinctly from a survivor

#### Scenario: Incomplete run fails the gate

- **WHEN** the gate evaluates a run in which a mutant has status `not checked`
- **THEN** the gate exits non-zero rather than treating the absent result as a pass

### Requirement: The un-mutation-tested surface is ratcheted, not ignored

Mutants on `core/` lines that the selected test suite never executes are reported by mutmut as `no tests`. They cannot be killed without re-enabling the Beam pipeline test suites, which is blocked upstream: mutmut reaps children with `os.wait()`, which also reaps DirectRunner worker subprocesses. Failing on them is therefore unsatisfiable, and silently discarding them would hide how much of `core/` is not mutation-tested.

The gate SHALL compare each module's `no tests` count against a committed per-module baseline and exit non-zero when any module **exceeds** its own baseline. A module absent from the baseline MUST have an implicit baseline of zero. A decrease in one module MUST NOT offset an increase in another. When a module's count falls below its baseline, the gate MUST pass if no other condition fails and MUST report that the baseline for that module can be tightened.

The baseline file MUST be committed so that any increase is visible and reviewable in the diff.

#### Scenario: Growing one module's uncovered surface fails the gate

- **WHEN** a change raises one `core/` module's `no tests` count above that module's committed baseline
- **THEN** the gate exits non-zero and reports the module's new count against its baseline

#### Scenario: Improvement in another module cannot mask a regression

- **WHEN** one module's `no tests` count falls while another module's count rises above its baseline
- **THEN** the gate exits non-zero for the regressing module

#### Scenario: Newly uncovered module has a zero baseline

- **WHEN** a new `core/` module produces at least one `no tests` mutant and has no committed baseline entry
- **THEN** the gate exits non-zero and reports that module against an implicit baseline of zero

#### Scenario: Uncovered surface at baseline passes

- **WHEN** every module's `no tests` count equals its committed baseline
- **THEN** the gate exits zero

#### Scenario: Shrinking the uncovered surface passes and invites a tighter baseline

- **WHEN** a change brings previously-uncovered `core/` lines under test, lowering one module's `no tests` count below its baseline without causing another failure
- **THEN** the gate exits zero and reports that the baseline for that module can be lowered

### Requirement: Mutants excluded from killing are declared with a reason

Some mutants cannot be killed by any test because they are equivalent to the original code — a mutation with no observable behavioural difference.

Every such mutant SHALL be recorded in a committed exclusion file naming the mutant and a mandatory reason. The gate MUST exempt a listed mutant only when its live status is `survived`; an exclusion MUST NOT suppress `timeout`, `suspicious`, `segfault`, `not checked`, or `check was interrupted by user`. The gate MUST fail if an exclusion names a mutant that no longer exists or no longer has status `survived`, so the list cannot accumulate stale or unnecessary entries.

Deselecting or weakening a test to make a mutant pass is forbidden; the exclusion file is the only sanctioned escape hatch, and every entry is reviewable in the diff.

#### Scenario: Declared equivalent mutant does not fail the gate

- **WHEN** a surviving mutant is listed in the exclusion file with a reason
- **THEN** the gate does not count it as a failure

#### Scenario: Stale exclusion entry fails the gate

- **WHEN** the exclusion file names a mutant that mutant generation no longer produces
- **THEN** the gate exits non-zero and identifies the stale entry

#### Scenario: Exclusion cannot suppress an indeterminate result

- **WHEN** an excluded mutant has status `timeout`, `suspicious`, `segfault`, `not checked`, or `check was interrupted by user`
- **THEN** the gate exits non-zero and reports that status

#### Scenario: Exclusion is removed after the mutant is killed

- **WHEN** an excluded mutant now has status `killed`
- **THEN** the gate exits non-zero and identifies the exclusion as unnecessary

#### Scenario: Undeclared survivor still fails

- **WHEN** a mutant survives and is absent from the exclusion file
- **THEN** the gate exits non-zero regardless of the file's other contents

### Requirement: The full `core/` sweep gates pull requests and runs nightly

The complete `core/` mutant set SHALL be run and gated on every pull request that changes `src/beam_agents/core/**` **or** `tests/core/**`. Filtering on source changes alone is insufficient: a change that only weakens a core test would otherwise skip the gate entirely.

A nightly run SHALL execute the same sweep **unconditionally**, because a change to a module outside `core/` that `core/` imports — for example `beam_agents.memory.facade`, imported by `core/context.py` — can cause a core mutant to survive without touching either filtered path.

Mutant selection MUST NOT be narrowed to the changed lines of a pull request. A diff-scoped run cannot observe a mutant that stops being killed because its only killing test was weakened elsewhere, which is a defect class mutation testing exists to catch.

#### Scenario: Pull request touching core sources is gated

- **WHEN** a pull request modifies a file under `src/beam_agents/core/`
- **THEN** the full `core/` sweep runs and its gate result is required for merge

#### Scenario: Pull request touching only core tests is gated

- **WHEN** a pull request modifies only files under `tests/core/` and no source file
- **THEN** the full `core/` sweep still runs and its gate result is required for merge

#### Scenario: Pull request touching neither path skips the sweep

- **WHEN** a pull request changes only documentation
- **THEN** the mutation sweep does not run and the check succeeds

#### Scenario: Weakened test in an unfiltered module is caught nightly

- **WHEN** a merged change to `src/beam_agents/memory/` causes a mutant in `core/context.py` to survive
- **THEN** the pull-request filter does not trigger the sweep and the next unconditional nightly run fails

#### Scenario: Nightly sweep needs no cloud credentials

- **WHEN** the nightly workflow runs in a repository with no `GCP_PROJECT_ID`, `ANTHROPIC_API_KEY`, or `OPENAI_API_KEY` configured
- **THEN** the mutation job still executes to completion

### Requirement: Mutation runs execute mutants in parallel

The mutation target SHALL pass an explicit child-process count to mutmut rather than relying on its default. The count MUST be derived from the host's available CPUs so that a GitHub-hosted runner and a developer machine each use their own capacity, and MUST be computed portably rather than with a Linux-only utility.

#### Scenario: Mutation target sets child count

- **WHEN** the mutation target's command line is inspected
- **THEN** it passes an explicit `--max-children` value derived from the host's CPU count

#### Scenario: Mutation target runs on macOS

- **WHEN** a contributor runs the mutation target on macOS
- **THEN** the child count is computed successfully without depending on `nproc`
