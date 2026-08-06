# Releasing

Cutting a release of `beam-agents` is one deliberate human act — pushing an
annotated `vX.Y.Z` tag — and nothing else. Everything between that tag and
PyPI is automated by [`.github/workflows/release.yml`](https://github.com/ardada2468/beam-agents/blob/main/.github/workflows/release.yml),
verified, and fails closed.

## Versioning policy

From `1.0.0` the project is versioned `MAJOR.MINOR.PATCH`, semver over a
frozen public surface:

| release | may contain                                                              |
| ------- | ------------------------------------------------------------------------ |
| `MINOR` (`X.Y.0`) | new features and new deprecations; the removal of a name whose deprecation window has elapsed — every break carries a `breaking` changelog entry naming the migration |
| `PATCH` (`X.Y.Z`) | defect fixes and documentation only; the compatibility surface does not change (machine-checked — see the fragment table below) |

The frozen surface is [`public-surface.toml`](https://github.com/ardada2468/beam-agents/blob/main/public-surface.toml):
one entry per public module, holding the public top-level names it declares
and its `__all__`. It is generated, never hand-edited, and
`tests/test_public_surface.py` re-derives it from the sources on every
`make test-unit` run and compares by exact equality in *both* directions — an
unreviewed addition and an unreviewed removal both fail. Regenerating the
snapshot is the API review: the diff must land in the same PR as the code
that moved a name.

**The deprecation window.** A frozen public name may not be removed or renamed
without a window of **at least one minor release**:

- During the window the old name MUST keep working and MUST emit a
  `DeprecationWarning` naming the replacement (or stating there is none) and
  the first release that may remove it.
- The deprecated name is served from a module-level `__getattr__` calling
  `beam_agents._deprecation.deprecated_attribute` — the PEP 562 pattern
  `beam_agents/__init__.py` already uses for lazy adapter classes — which
  keeps it out of the module namespace. Both edges of the window are therefore
  `public-surface.toml` snapshot diffs: opening it removes a declared name
  (reviewers check that the shim and the warning exist), and closing it one
  minor later removes the shim. Neither can happen silently.
- The removal release carries a `breaking` fragment naming the migration.

The mechanics, with the shim code to copy, are in
[`CONTRIBUTING.md`](https://github.com/ardada2468/beam-agents/blob/main/CONTRIBUTING.md).

### The 0.x line (history)

Before 1.0 the project was versioned `0.MINOR.PATCH` under a deliberately
weaker rule: a `0.MINOR.0` release could break the compatibility surface
without any deprecation window, as long as the break carried a `breaking`
changelog entry naming the migration, and a `0.x.PATCH` release carried fixes
and documentation only. That regime ended at `1.0.0`; it is recorded here
because the `0.x` sections of `CHANGELOG.md` were written against it.

### The compatibility surface

These, and only these, are under the policy:

1. **The public API** — exactly the names `public-surface.toml` records, for
   every public module. At the root that is the sixteen names
   `beam_agents/__init__.py` re-exports: `AdkAgent`, `AgentConfig`, `Deny`,
   `Drop`, `Escalate`, `FallbackContext`, `HitlPolicy`, `LangGraphAgent`,
   `PydanticAIAgent`, `RunAgent`, `RunAgentOutputs`, `ShardKeys`,
   `StreamAgent`, `shard_key`, `tool`, `unshard_key`. Everything
   underscore-prefixed, and everything outside the snapshot, is private.
2. **The wire and state protobuf schemas.** Changes are additive-only within a
   MINOR line. A `state_schema_version` bump is by definition MINOR-requiring:
   it ships with its lazy migration and a `breaking` entry in the same release,
   because an in-flight pipeline's `--update` compatibility depends on it.
3. **The `beam-agents-effector` CLI** — its flags and its exit behaviour.
4. **The published extras and the console-script names** — the nine extras
   (`effector`, `langgraph`, `pydantic-ai`, `adk`, `otlp`, `vllm`,
   `memory-stores`, `console`, `console-ingest`) and the three scripts
   (`beam-agents-effector`, `beam-agents-replay`, `beam-agents-console`).
   Adding an extra is `added`; renaming or removing one is `breaking`.
5. **The supported Python range** (`requires-python`) and the supported-runner
   list (DirectRunner, Dataflow, Flink supported; Spark best-effort).
   Narrowing either is `breaking`.

Explicitly **out of contract at any version**: underscore-prefixed modules,
`tests/`, `scripts/`, Make targets, CI workflow shape, and anything reachable
only by importing a private submodule.

### Fragment type → version component

`scripts/check_release.py` machine-checks this table at release time: a PATCH
tag (`vX.Y.Z` with `Z > 0`) whose pending fragments include any MINOR-requiring
type fails verification before anything is built.

| fragment type | rendered heading  | version component      |
| ------------- | ----------------- | ---------------------- |
| `breaking`    | Breaking changes  | requires MINOR         |
| `added`       | Added             | requires MINOR         |
| `changed`     | Changed           | requires MINOR         |
| `fixed`       | Fixed             | PATCH-compatible       |
| `docs`        | Documentation     | PATCH-compatible       |
| `internal`    | *not rendered*    | PATCH-compatible       |

The registry is closed. An unregistered type (`.feature.md`) fails the
`changelog-fragment-required` pre-commit hook and `make changelog` alike, so it
can never be silently dropped from the notes. See
[`changelog.d/README.md`](https://github.com/ardada2468/beam-agents/blob/main/changelog.d/README.md)
for how to write one.

## Where the version lives

The version is a single static string: `[project].version` in
`pyproject.toml`. It is **never** derived from git state — `uv.lock` records
the project's own version and every CI job installs with `uv sync --locked`, so
a tag-derived version would make the lockfile disagree with the build on every
commit.

Three things must agree, and `scripts/check_release.py` proves it before
anything is built for publishing:

```
git tag  vX.Y.Z   ==   pyproject.toml [project].version   ==   uv.lock beam-agents version
```

A bump therefore always comes with `uv lock`. This is the one routine reason
`uv.lock` changes without a dependency change.

## Release checklist

Before opening the release PR:

- [ ] The required checks (`ci`, `integration`, `quality`, `docs`) are green on
      the commit you intend to release from, and it is on `main`.
- [ ] The latest `nightly` run is green — the `dataflow` and `smoke` tiers have
      no required-check enforcement to inherit, so this is a human check.
- [ ] No open latency-budget regression. The runtime overhead budget (p50
      < 15 ms, p99 < 60 ms per activation, excluding LLM and tool time) is a
      release blocker, machine-checked twice over: `make bench-gate`
      (`scripts/bench_gate.py`) enforces the absolute budget plus the
      per-benchmark ratchet against `benchmark-baseline.toml` — seeded
      2026-08-03 from scheduled nightly run 30806138398 (`ubuntu-latest`) —
      and the release workflow itself refuses to publish without a recent
      green `bench` run (see "What the tag triggers"). What remains a human
      check is only that the *latest* nightly bench is the one you expect.
- [ ] `changelog.d/` reads like release notes for a user, not a work log.

The release PR:

1. Bump `[project].version` in `pyproject.toml`.
2. Sweep the version-coupled references — this exact gap has been rediscovered
   at three consecutive milestones, so it is a checklist step, not tribal
   knowledge. `grep -rn` for the outgoing version and update:
   - `docs/yaml.md` — the `beam-agents==X.Y.Z` provider pins (test-guarded);
   - `src/beam_agents/yaml/providers.yaml` and
     `src/beam_agents/yaml/__init__.py` — the same pin in the provider
     listing's comment and the module docstring;
   - `docs/replay.md` — the version the transcript shows
     `beam-agents-replay` printing (the CLI prints the installed
     `importlib.metadata` version);
   - `website/lib/site.ts` — `PACKAGE_VERSION`;
   - `uv.lock` — handled by step 3, listed here so the sweep is complete.
3. `uv lock` — then confirm `uv sync --locked` still succeeds.
4. `make changelog VERSION=X.Y.Z` — assembles the pending fragments into a
   dated `CHANGELOG.md` section and deletes the fragments it consumed. Preview
   first with `make changelog-draft`, which writes nothing.
5. `make build && uv run python scripts/check_wheel.py dist` locally if you
      want to eyeball the artifact. Local builds are for inspection only;
      nothing outside `release.yml` uploads.
6. Open, review, squash-merge.

Then tag the squash-merge commit:

```sh
git checkout main && git pull
git tag -a vX.Y.Z -m "beam-agents X.Y.Z"
git push origin vX.Y.Z
```

## What the tag triggers

`release.yml` runs two jobs, and the second is unreachable unless the first
fully passes:

1. **`build-and-verify`** (no publish permissions, no OIDC token):
   `uv sync --locked` → `make build` → `scripts/check_release.py` (tag ==
   version == lock, tagged commit is an ancestor of `main`, fragment policy) →
   `scripts/check_wheel.py` (the artifact actually contains `py.typed`, the
   generated `_protos` bindings, the console script, and the right metadata,
   and contains no test/docker/CI content) → the offline gate roster
   (`make lint type test-unit test-semantics-offline` plus the semantics-tier
   partition check) re-run on the exact tagged ref → upload `dist/`.
2. **`publish`** (`environment: pypi`, `id-token: write`): before publishing
   anything, locate the most recent nightly run on `main` whose **`bench` job**
   concluded green — read via the jobs API, not the run conclusion, since the
   sibling `dataflow`/`smoke` jobs can be red independently — and download its
   `benchmark-report` artifact. **No green bench run in the last 30 nightly
   runs fails the release here**, before anything reaches PyPI: the latency
   budget is a release blocker, and a green bench job is exactly a run
   `scripts/bench_gate.py` passed against the seeded
   `benchmark-baseline.toml`. Then download the build artifact, publish via
   PyPI **trusted publishing**, and create the GitHub Release for the tag with
   that version's `CHANGELOG.md` section as its body and `bench-report.md`
   plus `bench-results.zip` attached.

The docker-backed gates (the effectively-once e2e gate, the adapter conformance
matrix's Flink leg) are deliberately not re-run here: they are required checks
on `main`, and `check_release.py` has just proved the tagged commit is an
ancestor of `main`. Re-running the compose-backed suites on an already-gated
commit costs latency and flake surface and produces no new information.

## Trusted publishing

There is **no PyPI API token anywhere in this repository's secrets**.
Publishing authenticates via OIDC, the same no-long-lived-credential stance
`nightly.yml` takes with Workload Identity Federation for GCP.

The bindings are configured once on the PyPI side. Record them here verbatim —
renaming the workflow file or the environment breaks publishing with an OIDC
error, and this table is what makes the fix mechanical:

| index      | owner       | repository    | workflow       | environment |
| ---------- | ----------- | ------------- | -------------- | ----------- |
| PyPI       | `ardada2468` | `beam-agents` | `release.yml`  | `pypi`      |
| TestPyPI   | `ardada2468` | `beam-agents` | `release.yml`  | `testpypi`  |

The `pypi` environment can additionally be configured to require a reviewer
approval between verification and publish; the two-job shape supports that with
no workflow change.

## Rehearsing on TestPyPI

PyPI filenames are immutable: a botched first upload of a version can be
yanked, never replaced. Before the first real tag, rehearse via
`workflow_dispatch` on `release.yml` (leave `rehearse` checked), which runs the
identical build-and-verify job and publishes to TestPyPI instead. Then, from a
clean environment, verify:

```sh
pip install --index-url https://test.pypi.org/simple/ \
            --extra-index-url https://pypi.org/simple/ beam-agents
python -c "import beam_agents; print(beam_agents.RunAgent)"
beam-agents-effector --help
```

…and repeat for each of the nine extras (`beam-agents[effector]`,
`[langgraph]`, `[pydantic-ai]`, `[adk]`, `[otlp]`, `[vllm]`,
`[memory-stores]`, `[console]`, `[console-ingest]`), plus
`beam-agents-replay --help` and `beam-agents-console --help` for the other
two console scripts.

## If a release goes wrong

Published artifacts are **yanked, never deleted and re-uploaded** — the version
number is spent. Land the fix on `main` and release `X.Y.Z+1` per the policy.
If publishing fails *after* tagging but before anything reached PyPI, delete or
supersede the tag and re-push once the fix is merged.
