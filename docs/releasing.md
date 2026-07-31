# Releasing

Cutting a release of `beam-agents` is one deliberate human act — pushing an
annotated `vX.Y.Z` tag — and nothing else. Everything between that tag and
PyPI is automated by [`.github/workflows/release.yml`](https://github.com/ardada2468/beam-agents/blob/main/.github/workflows/release.yml),
verified, and fails closed.

## Pre-1.0 versioning policy

The project is versioned `0.MINOR.PATCH`. Plain semver leaves "what may break
when" undefined before 1.0, so this is the rule:

| release | may contain                                                              |
| ------- | ------------------------------------------------------------------------ |
| `0.MINOR.0` | new features, and breaking changes to the compatibility surface — every break carries a `breaking` changelog entry naming the migration |
| `0.x.PATCH` | defect fixes and documentation only; the compatibility surface does not change |

There is **no 1.0 API-stability commitment yet**. `0.x` deliberately promises
less than a stable release: a MINOR bump is allowed to break you, as long as
the changelog tells you how to migrate.

### The compatibility surface

These, and only these, are under the policy:

1. **The public API** — exactly what `beam_agents/__init__.py` re-exports
   (`RunAgent`, `AgentConfig`, `RunAgentOutputs`, `HitlPolicy`, `Deny`, `Drop`,
   `Escalate`, `FallbackContext`, the adapter classes). Everything else is
   private.
2. **The wire and state protobuf schemas.** Changes are additive-only within a
   MINOR line. A `state_schema_version` bump is by definition MINOR-requiring:
   it ships with its lazy migration and a `breaking` entry in the same release,
   because an in-flight pipeline's `--update` compatibility depends on it.
3. **The `beam-agents-effector` CLI** — its flags and its exit behaviour.
4. **The published extras and the console-script name** — `effector`,
   `langgraph`, `otlp`, `vllm`, and `beam-agents-effector`. Adding an extra is
   `added`; renaming or removing one is `breaking`.
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
      release blocker. This is not yet machine-checked; it needs the bench
      harness to produce a stable baseline number first.
- [ ] `changelog.d/` reads like release notes for a user, not a work log.

The release PR:

1. Bump `[project].version` in `pyproject.toml`.
2. `uv lock` — then confirm `uv sync --locked` still succeeds.
3. `make changelog VERSION=X.Y.Z` — assembles the pending fragments into a
   dated `CHANGELOG.md` section and deletes the fragments it consumed. Preview
   first with `make changelog-draft`, which writes nothing.
4. `make build && uv run python scripts/check_wheel.py dist` locally if you
      want to eyeball the artifact. Local builds are for inspection only;
      nothing outside `release.yml` uploads.
5. Open, review, squash-merge.

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
2. **`publish`** (`environment: pypi`, `id-token: write`): download the
   artifact, publish via PyPI **trusted publishing**, and create the GitHub
   Release for the tag with that version's `CHANGELOG.md` section as its body.

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

…and repeat for each extra (`beam-agents[effector]`, `[langgraph]`, `[otlp]`).

## If a release goes wrong

Published artifacts are **yanked, never deleted and re-uploaded** — the version
number is spent. Land the fix on `main` and release `X.Y.Z+1` per the policy.
If publishing fails *after* tagging but before anything reached PyPI, delete or
supersede the tag and re-push once the fix is merged.
