## Context

See `proposal.md` — Why. Constraints that shape the approach:

- Packaging already works; only the process around it is missing. Hatchling builds a wheel from `src/beam_agents` wholesale ([pyproject.toml:112–117](../../../pyproject.toml:112)), which ships `py.typed`, the committed `_protos/*_pb2.py(i)` bindings, and the `beam-agents-effector` console script. The release process must *verify* that stays true per release, not re-engineer it.
- `uv.lock` is committed and CI installs with `uv sync --locked` everywhere ([ci.yml:34](../../../.github/workflows/ci.yml:34)); the lock's own `beam-agents` entry records the project version ([uv.lock:277](../../../uv.lock:277)). Any versioning scheme must keep `--locked` deterministic — a version that changes without a lock refresh fails every CI job.
- "Makefile is the CI/local contract" ([CONTRIBUTING.md:20](../../../CONTRIBUTING.md:20)): every primary workflow step is a `make` target. The release workflow must follow suit or it becomes the first CI-only path in the repo.
- The repo already has a strong per-change discipline to hang a changelog on: squash-merge, one merged commit = one archived OpenSpec change, commit messages reference the change folder ([project.md:92](../../project.md)). Commit messages are *not* conventional-commit formatted (`Implement add-adapter-conformance-matrix: 7 scenarios x adapters x 2 runners`), and retrofitting that format repo-wide is a contribution-contract change this proposal should not smuggle in.
- Release gating is already specified in prose: the semantics tier "gate[s] every release" ([project.md:75](../../project.md)) and latency-budget regressions are "release blockers" ([project.md:111](../../project.md)). The `ci`/`integration`/`quality` checks are required on `main`, so any commit on `main` has passed the PR-facing gates; nightly (`dataflow`, `smoke`) and the bench budget have no required-check enforcement to inherit.
- Precedent for credential hygiene: nightly authenticates to GCP via Workload Identity Federation with no long-lived key ([nightly.yml:53](../../../.github/workflows/nightly.yml:53)). PyPI publishing should meet the same bar.
- Precedent for enforcement mechanics: the `openspec-change-required` local pre-commit hook ([.pre-commit-config.yaml:33](../../../.pre-commit-config.yaml:33), [scripts/check_openspec_change.sh](../../../scripts/check_openspec_change.sh)) blocks `src/` commits lacking a change folder, with a documented escape hatch. The fragment check copies this shape.

## Goals / Non-Goals

**Goals:**

- A tag push is the *only* human action that publishes; everything between tag and PyPI is automated, verified, and fails closed.
- One authoritative version string, and a release process that cannot publish an artifact whose version disagrees with its tag or its lockfile.
- A versioning policy precise enough to be machine-checked at release time (fragment types → allowed version component), not just prose.
- Changelog entries written at change time by the change author, reviewed with the code, and assembled mechanically at release time.
- Zero new required PR checks and zero new PR-time friction beyond one small fragment file per user-facing change.

**Non-Goals:**

- No 1.0 API-stability commitment — this change defines what 0.x promises, which is deliberately weaker (breaking changes allowed at MINOR).
- No conventional-commit adoption, no commit-message linting.
- No signed artifacts / SLSA provenance beyond what PyPI trusted publishing provides out of the box; revisit before 1.0.
- No automated *scheduling* of releases (no release-please-style bot PRs); cutting a release stays a deliberate human act.
- No conda-forge, no docker image publishing for the effector — the wheel + sdist on PyPI is the 0.1.0 distribution story.
- No mechanization of the nightly/bench release blockers beyond a checklist assertion (see D5 and Open Questions).

## Decisions

### D1. Static single-source version in `pyproject.toml`, bumped by a release PR — not VCS-derived versioning

The version stays a literal string at [pyproject.toml:3](../../../pyproject.toml:3). A release is a reviewed PR ("release PR") that bumps it, refreshes `uv.lock`, and assembles the changelog; the `vX.Y.Z` tag is pushed on that squash-merged commit, and `release.yml` hard-fails if `tag != version` before anything is built or published.

The alternative — `hatch-vcs`/`uv-dynamic-versioning` deriving the version from git tags — was rejected on repo-specific grounds: (a) the committed lockfile records the project's own version ([uv.lock:277](../../../uv.lock:277)), and a tag-derived version makes every local build's version depend on git state while the lock says something else, undermining the `--locked` determinism CI relies on ([ci.yml:34](../../../.github/workflows/ci.yml:34)); (b) dynamic versioning makes builds from sdists, shallow CI checkouts, and exported trees produce fallback versions unless carefully configured — a class of silent wrongness with no compensating benefit here; (c) the repo's unit of history is the OpenSpec change, not the commit, so "every commit gets a unique dev version" buys nothing. The known cost of static versioning — someone tags without bumping, or bumps without tagging — is exactly what the tag==version check converts from a published mistake into a failed workflow run.

### D2. Tag-triggered `release.yml` with PyPI trusted publishing and a gated two-job shape

`release.yml` triggers on `push: tags: ["v*"]` and has two jobs:

1. **build-and-verify** (no publish permissions): `uv sync --locked` → `make build` (`uv build`, producing sdist + wheel into `dist/`) → `uv run python scripts/check_release.py` (tag == `[project].version` == the lock's `beam-agents` version; the tagged commit is an ancestor of `main`; fragment-type-vs-bump policy, D4) → `uv run python scripts/check_wheel.py dist/*.whl` (content verification, D3) → `make lint type test-unit test-semantics-offline` — the same offline gate roster as the required `ci` job, re-run on the exact tagged ref — → upload `dist/` as a job artifact.
2. **publish** (`needs: build-and-verify`, `environment: pypi`, `permissions: id-token: write`): download the artifact, publish via `pypa/gh-action-pypi-publish` using trusted publishing (OIDC — the PyPI project is bound one-time to this repo + workflow + environment; no API token in secrets, matching the WIF stance of [nightly.yml:53](../../../.github/workflows/nightly.yml:53)), then create the GitHub Release for the tag with the version's `CHANGELOG.md` section as its body.

Splitting publish into its own environment-guarded job keeps the OIDC token away from the job that executes the test suite, lets the `pypi` environment optionally require a reviewer approval later without touching the workflow, and makes "verified but not published" a visible intermediate state on failure. Primary steps are `make` targets per the contract; the publish step is a marketplace *action*, not a shell command, which the Makefile contract's own scenario language ("the step's shell command") does not cover — and wrapping OIDC exchange in `make` would be strictly worse. The docker-backed gates are deliberately *not* re-run here: they are required checks on `main`, `check_release.py` proves the tag is an ancestor of `main`, and repeating the long compose-backed run (the `integration` job is budgeted at 60 minutes, [integration.yml:20](../../../.github/workflows/integration.yml:20)) on an already-gated commit adds latency, flake surface, and no new information.

### D3. Distribution verification is a script with its own unit tests, not ad-hoc workflow shell

`scripts/check_wheel.py` inspects the built artifacts and fails on: missing `beam_agents/py.typed`; missing `_protos/*_pb2.py` bindings (a wheel built from a tree where gen output was stripped would import-fail at runtime only); any `tests/` or `docker/` content leaking into the wheel; wrong or missing `beam-agents-effector` entry point; metadata drift on `Requires-Python` (`>=3.11,<3.13`) or the three extras (`effector`, `langgraph`, `otlp`). It reads the wheel as a zip and the sdist as a tarball — no installation, no network — so its tests run as plain offline unit tests against synthetic archives (build a minimal zip with/without each required member, assert the specific failure message), keeping the verification logic itself under `make test-unit` and the coverage ratchet rather than being testable only by cutting a release. Same rationale as the existing standalone gate scripts (`coverage_ratchet.py`, `mutation_gate.py`, `check_semantics_partition.py`): release-critical logic lives where the unit lane can exercise it.

### D4. Changelog mechanism: towncrier fragments keyed to OpenSpec change names — not conventional-commit generation

Chosen mechanism: [towncrier](https://towncrier.readthedocs.io/) fragments in `changelog.d/`, one file per user-facing change, named `<openspec-change-name>.<type>.md` (e.g. `add-runtime-metrics.added.md`), assembled by `make changelog` (`towncrier build --version X.Y.Z`) into a Keep-a-Changelog-style `CHANGELOG.md` section at release time. towncrier ships in a new `release` dependency group.

Why fragments beat commit-message generation *here*: the repo's changelog-shaped unit already exists and it is not the commit — it is the OpenSpec change. Squash-merge discipline means one commit per change ([project.md:92](../../project.md)), but commit subjects are implementation-voiced (`Implement add-adapter-conformance-matrix: 7 scenarios x adapters x 2 runners`) and were never written for users; conventional-commit tools (git-cliff, commitizen) would require either rewriting the contribution contract around `feat:`/`fix:` prefixes or generating notes from text not authored as release notes. A fragment is authored in user voice, in the same PR, reviewed by the same reviewer, and its filename ties it to the change folder — extending the existing traceability chain (scenario → test → code) with change → fragment → changelog entry. It also degrades gracefully: a change with no user-facing effect writes an `internal` fragment (rendered nowhere) and the enforcement stays uniform.

Fragment types form a closed registry configured in `[tool.towncrier]`, mirroring the closed pytest-marker registry philosophy: `breaking`, `added`, `changed` (MINOR-requiring); `fixed`, `docs` (PATCH-compatible); `internal` (satisfies enforcement, not rendered). `scripts/check_release.py` enforces the mapping: a `vX.Y.(Z>0)` tag with any MINOR-requiring fragment pending fails verification — the machine-checked core of the pre-1.0 policy. Enforcement at PR time is a local pre-commit hook (`changelog-fragment-required`, shape copied from [scripts/check_openspec_change.sh](../../../scripts/check_openspec_change.sh), escape hatch `BEAM_AGENTS_ALLOW_NO_FRAGMENT=1`) that fires when `src/` is touched; it also runs repo-wide in the `quality` workflow's existing `pre-commit run --all-files` step ([quality.yml:33](../../../.github/workflows/quality.yml:33)), so no new required check is introduced.

### D5. Pre-1.0 policy: 0.MINOR = features and/or breaks, 0.x.PATCH = fixes only, with an enumerated compatibility surface

Pre-1.0 semver leaves "what may break when" undefined, so `docs/releasing.md` defines it: MINOR releases may add features and may break the compatibility surface, but every break carries a `breaking` fragment naming the migration; PATCH releases change behavior only to fix defects and may not carry `breaking`/`added`/`changed` fragments (machine-checked, D4). The compatibility surface is enumerated, not implied: (1) the public API re-exported by `beam_agents/__init__.py` (the project already scopes "public" this way, [project.md:86](../../project.md)); (2) wire/state protos — additive-only within a MINOR line, and any `state_schema_version` bump is by definition MINOR-requiring with its lazy migration in the same release; (3) the `beam-agents-effector` CLI's flags and exit behavior; (4) extras names and the console-script name; (5) `requires-python` and the supported-runner list. Everything not enumerated (underscore modules, `tests/`, `scripts/`, Make targets, CI shape) is explicitly out of contract at any version. The prose release blockers that cannot be machine-checked from the repo alone — latest nightly green, no open latency-budget regression ([project.md:111](../../project.md)) — live in the `docs/releasing.md` checklist that the release PR template asserts, rather than being half-automated badly.

### D6. 0.1.0 bootstrap: curated backfill, then the machinery takes over

There are nine archived OpenSpec changes and some twenty more in flight with no fragments, because fragments didn't exist. Rather than pretending the automation covers history, `CHANGELOG.md` is seeded with a hand-curated `0.1.0` section summarizing the shipped capability set (sourced from `openspec/changes/archive/` and the merged changes at tag time), and towncrier's rendered output begins at 0.2.0. The fragment-required hook activates when this change lands, so every change merged between this change and the 0.1.0 tag contributes fragments that fold into the curated section. A TestPyPI rehearsal (manual `workflow_dispatch` input on `release.yml` publishing to TestPyPI via a second trusted-publisher binding) runs once before the real tag to validate the OIDC bindings and artifact rendering end-to-end without burning the `0.1.0` version number on PyPI — necessary because PyPI file names are immutable and a botched first upload of `0.1.0` cannot be replaced, only yanked.

## Risks / Trade-offs

- **Tag pushed off a bad or non-`main` commit** → `check_release.py` requires the tagged commit be an ancestor of `main` (so it has passed the required `ci`/`integration`/`quality` checks) and re-runs the offline gates in-workflow; the docker gates are trusted from the required checks rather than re-run (D2 trade-off, accepted).
- **Fragment fatigue / drive-by `internal` mislabeling** → reviewer surface is small and colocated (the fragment is in the diff); the closed type registry keeps the choice binary ("does a user observe this?"); mislabeled entries cost a changelog line, not correctness.
- **Trusted-publisher binding drifts** (workflow renamed, environment renamed) → publishing fails closed with an OIDC error; `docs/releasing.md` records the exact binding tuple (repo, workflow file, environment) so the fix is mechanical.
- **`uv build` output differs from what CI tested** (stale local `dist/`, dirty tree) → releases are built only in `release.yml` from a clean tag checkout; `make build` cleans `dist/` first; local builds are for inspection, never upload (no `uv publish` path outside the workflow).
- **Version bump PR races another merge** → the tag is pushed on the release PR's squash-merge commit specifically, and tag==version==lock is checked; a race produces a failed verification, not a wrong artifact.
- **towncrier as a new dependency** → confined to the `release` group (never installed by `ci`/`integration` lanes), and only its CLI is used — no runtime import surface.
- **First release exercises the workflow's untested path** → mitigated by the TestPyPI rehearsal (D6) and by the check scripts being unit-tested offline (D3).

## Migration Plan

1. Land this change's tooling (towncrier config, `changelog.d/`, hooks, scripts, `make build`/`make changelog`, `release.yml`, `docs/releasing.md`) with `version` still `0.0.0`. Nothing publishes; PR-time behavior changes only by the fragment hook activating.
2. One-time PyPI setup: register/reserve the `beam-agents` project, configure trusted publishers for PyPI and TestPyPI bound to `release.yml` + the `pypi`/`testpypi` environments.
3. Rehearse: `workflow_dispatch` publish of a `0.1.0rc1`-versioned build to TestPyPI; verify install, extras resolution, console script, and rendered metadata from a clean environment.
4. Cut 0.1.0: release PR (bump `version` to `0.1.0`, refresh `uv.lock`, curated `CHANGELOG.md` 0.1.0 section folding in any pending fragments) → squash-merge → push annotated `v0.1.0` tag → `release.yml` publishes and creates the GitHub Release.
5. Steady state: every user-facing change carries a fragment; releases repeat step 4 with `make changelog` doing the assembly.

No rollback concerns for users (there are none yet). If 0.1.0 publishing fails after tagging, the fix lands on `main`, the bad tag is deleted or superseded, and `0.1.1`/`0.1.0.post` follows per the policy — published-but-broken artifacts are yanked, never deleted-and-reused.

## Open Questions

- Should the `pypi` GitHub environment require a manual reviewer approval between verification and publish? The two-job shape supports it with zero workflow changes; 0.1.0 can start without it and tighten later.
- Whether the sdist should include `protos/*.proto` sources (useful to downstream regenerators, slightly larger artifact). Leaning yes; decided during implementation by what `uv build`'s default sdist collection already includes.
- Whether GitHub Release assets should mirror the wheel/sdist or link to PyPI only. PyPI-only is the lean default; mirroring is one upload step if wanted.
- How the latency-budget release blocker ([project.md:111](../../project.md)) eventually becomes machine-checked — a bench baseline artifact compared in `release.yml` is the obvious shape, but it needs the bench harness to produce a stable number first; out of scope here, tracked in the checklist until then.
