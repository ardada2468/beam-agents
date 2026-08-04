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

The project is versioned `MAJOR.MINOR.PATCH` — semver over the frozen public
surface (`public-surface.toml`): a MINOR release may add features and open or
close deprecation windows; a PATCH release carries fixes and docs only; a
frozen name is never removed without the deprecation window below. The
version is a single static string in `pyproject.toml` and
must agree with both the release tag and `uv.lock` — which is why a version
bump is the one routine reason to run `uv lock`.

Releases are cut by pushing an annotated `vX.Y.Z` tag, which is the only human
action that publishes; `.github/workflows/release.yml` builds, verifies, gates,
and publishes to PyPI via trusted publishing (no API token exists in secrets).
Full policy, compatibility surface, and checklist: [`docs/releasing.md`](docs/releasing.md).

## Public API surface and the deprecation policy

`public-surface.toml` is the frozen public API: one entry per public module
holding the public top-level names it declares and its `__all__`. It is
**generated, never hand-edited**:

```
uv run python tests/test_public_surface.py
```

`tests/test_public_surface.py` re-derives the surface from the sources with
`ast` and compares by exact equality in *both* directions, so an unreviewed
addition and an unreviewed removal both fail `make test-unit`. Regenerating is
not a way to make the gate pass — the resulting diff is the API review, and it
must appear in the same PR as the code that moved a name. Every public name a
public module declares must also be in that module's `__all__`; anything else
carries a leading underscore. Modules under a `_`-prefixed package, or with a
`_`-prefixed name, are internal machinery by their path and are outside the
surface entirely. Every frozen name also needs a line on
[`docs/api.md`](docs/api.md), which a drift test enforces.

`ruff`'s `D1` rules (pydocstyle *completeness* — not style) ride `make lint`, so
a new public function, class, method, module, or package without a docstring
fails the build. `D105` (magic methods) and `D107` (`__init__`) are ignored:
dunder semantics come from the language protocol, and constructor contracts
belong in the class docstring.

**The deprecation window.** From 1.0 on, a frozen public name may not be removed
or renamed without a deprecation window of **at least one minor release**. During
the window the old name MUST keep working and MUST emit a `DeprecationWarning`
naming the replacement (or stating there is none) and the first release that may
remove it. Serve it from a module-level `__getattr__` — the PEP 562 pattern
`beam_agents/__init__.py` already uses for lazy adapter classes — calling the
single helper in `beam_agents/_deprecation.py`:

```python
def __getattr__(name: str) -> object:
    if name == "old_name":
        return deprecated_attribute(
            name, replacement="beam_agents.pkg.new_name",
            removed_in="1.1.0", value=new_name, module=__name__,
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
```

Serving it that way keeps the deprecated name out of the module namespace, so
**both edges of the window are snapshot diffs**: opening it removes a declared
name (reviewers check that the `__getattr__` shim and the warning exist), and
closing it one minor later removes the shim. Neither can happen silently — CI
cannot know whether a window elapsed, but it guarantees no removal is invisible.

**The one historical exemption.** The bulk privatization performed by
`add-1-0-api-freeze` itself was exempt: it landed in the 0.x line, before the
surface was declared frozen, and pruning accidental names cheaply was exactly
what pre-1.0 semantics were for. From 1.0 on, no further exemptions.

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
