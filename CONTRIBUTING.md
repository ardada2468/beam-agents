# Contributing

## OpenSpec workflow

No commit under `src/` is accepted without an active OpenSpec change backing
it. Before writing code:

1. Propose the change under `openspec/changes/<name>/` (proposal, design,
   specs, tasks).
2. Implement against the task list, checking items off as you go.
3. Archive the change once merged, promoting its delta specs into
   `openspec/specs/`.

A local pre-commit hook (`openspec-change-required`) enforces this: it
blocks any commit touching `src/` if no `openspec/changes/*/proposal.md`
exists in the tree. If you're doing exploratory work that genuinely doesn't
warrant a change yet, set `BEAM_AGENTS_ALLOW_NO_CHANGE=1` to bypass it —
sparingly, and expect reviewers to ask why.

## Makefile is the CI/local contract

Every CI workflow step is a `make <target>` call. Whatever you run locally
with `make lint`, `make type`, `make test-unit`, etc. is exactly what CI
runs — there's no separate CI-only path to keep in sync. If a `make` target
doesn't cover something you need, add the target rather than special-casing
a workflow file.

## `pytest` marker registry is closed

`pyproject.toml` registers exactly four markers: `integration`, `semantics`,
`dataflow`, `slow`. `--strict-markers` means an unregistered or misspelled
marker fails the test session outright rather than silently collecting zero
tests. Add new markers to `[tool.pytest.ini_options]` if you need one.

## Code quality gates

- `ruff` (including `ASYNC` rules — never block the async bridge event loop)
- `mypy --strict` on `src/` (Beam modules get `ignore_missing_imports`, your
  code does not)
- `mutmut` mutation testing on `core/`, run in the `quality` CI job
- Coverage may never regress vs. `main` (enforced by
  `make coverage-ratchet`)

`mutation-baseline.toml` contains independent per-module ceilings for mutants
that run no tests. Lower a ceiling when coverage improves; never raise one
without an explicit review of why the mutation-tested surface must shrink.

`mutation-exclusions.toml` is only for live mutants proven behaviorally
equivalent to the original implementation. Every entry must name the exact
mutant and give a mandatory technical reason. Missing, stale, killed, or
indeterminate exclusions fail the gate. Weakening or deselecting a test is
never an alternative to killing an observable mutant.

Run `make bootstrap && pre-commit run --all-files` before pushing to catch
all of the above locally.
