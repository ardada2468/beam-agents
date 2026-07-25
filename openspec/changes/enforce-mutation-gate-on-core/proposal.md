## Why

The `quality` workflow's mutation step is the slowest required check on any PR that touches `core/`, and it is not buying the quality it costs. Two measured facts drive this change:

1. **It mutated far more than it tests.** `[tool.mutmut]` set only `do_not_mutate = ["*/_protos/*"]`, so mutmut generated **2,219 mutants across 27 files** — the whole `src/beam_agents` tree — while `pytest_add_cli_args_test_selection` restricts the killing test suite to `tests/core` alone. **299 of the 418 survivors (72%) sat in modules `tests/core` could not possibly kill mutants in**, e.g. `beam_agents.tools.runner.xǁToolRunnerǁrun__mutmut_1: survived`. Those were configuration artifacts, not test gaps, and each cost a fork. The run did not finish in **over 25 minutes locally at 4-way parallelism**. The stated policy in `docs/ci.md` and `openspec/specs/repo-scaffolding/spec.md` is mutation "on touched `core/`"; the configuration did not implement it.

2. **The gate cannot fail.** `mutmut run` returns `None` and exits 0 regardless of survivors (`mutmut/__main__.py::_run`). `make mutation` is therefore a very long check that reports green with **154 surviving mutants in `core/`** — 91 of them in `context.py`, the module implementing the atomic-commit staging invariant. The required check protecting our load-bearing correctness invariants asserts nothing.

So the check was slow *and* vacuous. Speeding it up alone would just make it fail faster at nothing; making it strict alone would make an already-slow check block on unrelated noise. Both had to move together.

Scoping mutation to `core/` (already applied and measured) resolves the cost problem outright: **911 mutants, ~2.8 min wall at 10-way parallelism**, down from 2,219 and >25 min. That makes the full `core/` sweep affordable as a required check on every PR, so this change is about making the gate *enforce* rather than about making it selective.

## What Changes

- **Scope mutation to `core/`.** `only_mutate = ["src/beam_agents/core/*"]`. Verified: 8 files mutated instead of 27, 911 mutants instead of 2,219, zero result entries outside `beam_agents.core.*`.
- **Make the gate enforce.** New `scripts/mutation_gate.py` reads mutmut's per-mutant results and exits non-zero on `survived`, `timeout`, `suspicious`, `segfault`, `not checked`, or an interrupted run. mutmut's own exit code is unusable for this.
- **Ratchet the un-mutation-tested surface per module.** 453 `core/` mutants have no covering test — `dofn.py` (263), `transform.py` (189), and `context.py` (1). Their test suites are partly deselected because mutmut's `os.wait()` reaping breaks Beam DirectRunner subprocesses. The gate fails if any module rises above its committed baseline; a reduction in one module cannot hide a regression in another, and a newly uncovered module has an implicit baseline of zero.
- **Resolve all 154 existing survivors** with scenario-derived tests, `context.py` first. Tests trace to spec scenarios per the project's TDD rule; a mutant no scenario justifies killing is a spec gap or a genuinely equivalent mutant, and is recorded in a reviewed exclusion file rather than papered over. Exclusions apply only to survivors, never to timeouts or incomplete checks. Deselecting a test to make a mutant pass stays forbidden.
- **Run the full `core/` sweep on every PR that changes its implementation or killing suite**, gated on `src/beam_agents/core/**` *or* `tests/core/**` changing — the existing filter, broadened so that weakening a core test cannot skip the gate.
- **Add an unconditional nightly sweep** as the backstop for cross-module drift: `core/context.py` imports `beam_agents.memory.facade`, so a change outside `core/` and outside `tests/core/` can make a core mutant survive without tripping the PR filter.
- **Run mutants in parallel.** `--max-children` was unset; it is now derived from the host CPU count.

Not changing: the `mutation` target name, the `semantics`/`integration`/`unit` tiers, the coverage ratchet, or mutmut as the tool.

**Rejected during implementation:** `mutate_only_covered_lines = true` (which would have removed the 453 `no tests` mutants at generation time) makes mutmut run pytest in-process under `coverage.collect()`, and apache-beam's numpy import then fails with `ImportError: cannot load module more than once per process`. Upstream incompatibility, no config workaround. The ratchet above is the replacement, and is more honest — it keeps the gap in the report instead of deleting it.

**Also rejected:** diff-scoped mutant selection (mapping changed lines to enclosing functions and running only those mutants). It was the original plan, justified by a >25 min sweep. At 2.8 min the full sweep is affordable, and it is strictly stronger — a diff-scoped gate cannot see a mutant that stops being killed because its only killing test was weakened elsewhere. Dropping it also removes the most failure-prone new code in the change, whose bugs would have silently shrunk the gate's scope toward nothing.

## Capabilities

### New Capabilities

- `mutation-gate`: What mutation testing asserts, at what scope, and what makes it fail — the `core/`-only mutation scope, the zero-undeclared-survivor bar, the per-module `no tests` ratchet, the status-to-verdict mapping, and how equivalent mutants are declared and excluded.

### Modified Capabilities

- `repo-scaffolding`: The GitHub-Actions-workflows requirement states that `quality.yml` runs `make mutation`; it must now also state that the step enforces (fails on survivors), when it runs, and that `nightly.yml` runs an unconditional sweep. The `Makefile`-targets requirement is unchanged — `mutation` keeps its name.

## Impact

- **Config:** `pyproject.toml` `[tool.mutmut]` — `only_mutate` added, comment block rewritten (**applied**).
- **Build:** `Makefile` — `mutation` gains `--max-children` and the gate invocation.
- **CI:** `.github/workflows/quality.yml` — broaden the change filter to core sources *and* core tests. `.github/workflows/nightly.yml` — new unconditional mutation job, no credentials required.
- **New:** `scripts/mutation_gate.py` (sibling to `scripts/coverage_ratchet.py`), `mutation-exclusions.toml`, `mutation-baseline.toml`.
- **Tests:** the bulk of the work — tests under `tests/core/` killing 154 survivors (`context.py` 91, `loop.py` 33, `bridge.py` 28, `coders.py` 1, `agent.py` 1), plus unit tests for the gate script.
- **Docs:** `docs/ci.md` workflow table, plus an explicit statement of which `core/` modules sit outside the mutation-tested surface.
- **No runtime impact.** Nothing under `src/beam_agents` changes behavior; `mutmut` stays in the `test` group.
