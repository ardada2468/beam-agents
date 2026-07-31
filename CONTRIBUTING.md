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

## Changelog fragments

Every change touching `src/` also carries a changelog fragment:

```
changelog.d/<openspec-change-name>.<type>.md
```

named after the OpenSpec change folder backing the commit, holding one or two
sentences in **user voice** — what someone installing the next release can now
do, or must now do differently. The point is that the release note is written
by the person who made the change, reviewed in the same diff as the code, and
never reconstructed from commit archaeology at tag time.

The type registry is closed: `breaking`, `added`, `changed` (each requires a
MINOR release), `fixed`, `docs` (PATCH-compatible), and `internal` (satisfies
the requirement, renders nowhere — use it for refactors with no user-visible
effect). An unregistered type fails the hook and `make changelog` alike.

A local pre-commit hook (`changelog-fragment-required`) enforces this the same
way `openspec-change-required` does, with its own escape hatch:
`BEAM_AGENTS_ALLOW_NO_FRAGMENT=1`. The two hooks are independent — bypassing
one does not bypass the other.

Preview what the next release will say with `make changelog-draft` (writes
nothing). See [`changelog.d/README.md`](changelog.d/README.md) for the format
and [`docs/releasing.md`](docs/releasing.md) for the versioning policy the
types map onto.

## Releasing

The project is pre-1.0 (`0.MINOR.PATCH`): a MINOR release may add features and
may break the documented compatibility surface; a PATCH release carries fixes
and docs only. The version is a single static string in `pyproject.toml` and
must agree with both the release tag and `uv.lock` — which is why a version
bump is the one routine reason to run `uv lock`.

Releases are cut by pushing an annotated `vX.Y.Z` tag, which is the only human
action that publishes; `.github/workflows/release.yml` builds, verifies, gates,
and publishes to PyPI via trusted publishing (no API token exists in secrets).
Full policy, compatibility surface, and checklist: [`docs/releasing.md`](docs/releasing.md).

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
