## Purpose

How a version of `beam-agents` gets built, verified, gated, tagged, and published: a single-source static version, a tag-triggered release workflow that publishes to PyPI via trusted publishing only after artifact verification and offline gates pass, and a machine-checked pre-1.0 versioning policy defining which changes may ship in which version component.

## ADDED Requirements

### Requirement: The project version is single-sourced and consistent across tag, metadata, and lockfile

The project version SHALL be defined exactly once, as the static `[project].version` string in `pyproject.toml`. The release process SHALL NOT derive versions from git state. A release SHALL be prepared as a reviewed pull request that bumps `[project].version`, refreshes `uv.lock` so its `beam-agents` entry records the same version, and assembles the changelog; the release tag `vX.Y.Z` SHALL be pushed on that merged commit. Release verification MUST fail, before any artifact is published, if the tag's version, `[project].version`, and the lockfile's recorded version do not all agree, or if the tagged commit is not an ancestor of `main`.

#### Scenario: Tag and version disagree

- **WHEN** a `v0.2.0` tag is pushed while `pyproject.toml` still declares `version = "0.1.0"`
- **THEN** the release workflow fails at the verification step naming both values, and no artifact is built for publishing and nothing reaches PyPI

#### Scenario: Lockfile lags the version bump

- **WHEN** a release PR bumps `[project].version` without refreshing `uv.lock`
- **THEN** release verification fails reporting the lockfile/metadata version mismatch

#### Scenario: Tag on a commit not on main

- **WHEN** a release tag is pushed on a commit that is not an ancestor of `main`
- **THEN** release verification fails, reporting that the tagged commit has not passed the required merge gates

### Requirement: Pushing a release tag builds, verifies, gates, and publishes via trusted publishing

The repository SHALL provide a release workflow triggered by pushing a `v*` tag. The workflow SHALL, in order: build the sdist and wheel via a `make` target; run release verification (version consistency, distribution-content checks, versioning-policy check); re-run the offline gate roster (lint, type check, unit tests, offline semantics gates) on the tagged ref; and only if all of these pass, publish the built artifacts to PyPI using trusted publishing (OIDC) from a dedicated deployment environment, and create a GitHub Release for the tag whose body is the released version's changelog section. Publishing MUST NOT be reachable when any prior step fails. No long-lived PyPI credential (API token or password) SHALL be stored in repository or organization secrets. Primary build and test steps in the workflow SHALL invoke `make` targets, preserving the Makefile local/CI contract.

#### Scenario: Green tag publishes

- **WHEN** a version-consistent `vX.Y.Z` tag is pushed and build, verification, and offline gates all pass
- **THEN** the sdist and wheel are published to PyPI via OIDC trusted publishing and a GitHub Release for `vX.Y.Z` is created carrying that version's changelog section

#### Scenario: Failing gate blocks publish

- **WHEN** any verification or gate step in the release workflow fails for a pushed tag
- **THEN** the publish job does not run, nothing reaches PyPI, and the workflow run shows which verification failed

#### Scenario: No static PyPI credential exists

- **WHEN** the release workflow's configuration and the repository's secrets are inspected
- **THEN** publishing authenticates via the OIDC trusted-publisher binding and no long-lived PyPI token appears in secrets

### Requirement: Distribution contents are verified before publishing

Release verification SHALL inspect the built wheel and sdist without installing them and MUST fail if any of the following does not hold: the wheel contains `beam_agents/py.typed`; the wheel contains the generated `_protos` bindings; the wheel contains no test, docker, or CI content; the `beam-agents-effector` console-script entry point is declared; the metadata declares `Requires-Python >=3.11,<3.13`; and the metadata declares exactly the published extras (`effector`, `langgraph`, `otlp`). The verification logic SHALL live in a standalone script exercised by offline unit tests, so a defect in the verification itself is caught in the unit lane rather than at release time.

#### Scenario: Wheel missing the typing marker fails verification

- **WHEN** release verification inspects a wheel that does not contain `beam_agents/py.typed`
- **THEN** verification exits non-zero naming the missing member, and publishing is blocked

#### Scenario: Wheel missing generated proto bindings fails verification

- **WHEN** release verification inspects a wheel with no `_protos` `*_pb2.py` members
- **THEN** verification exits non-zero naming the missing bindings, and publishing is blocked

#### Scenario: Metadata drift fails verification

- **WHEN** the built wheel's metadata omits one of the published extras or declares a different `Requires-Python` range
- **THEN** verification exits non-zero reporting the drifted field

#### Scenario: Verification logic is unit-tested offline

- **WHEN** `make test-unit` runs with no docker and no network
- **THEN** the distribution-verification script's checks are each exercised against synthetic archives, passing on a compliant archive and failing with the specific message on each non-compliant one

### Requirement: Pre-1.0 versioning policy is documented and machine-checked at release time

The repository SHALL document a pre-1.0 versioning policy: versions are `0.MINOR.PATCH`; MINOR releases MAY add features and MAY break the documented compatibility surface provided each break carries a breaking-type changelog entry; PATCH releases SHALL contain only fixes and documentation and SHALL NOT change the compatibility surface. The documented compatibility surface MUST enumerate at least: the public API re-exported by `beam_agents/__init__.py`, the wire/state protobuf schemas and `state_schema_version` discipline, the `beam-agents-effector` CLI, the published extras and console-script names, and the supported Python range. Release verification MUST fail a PATCH tag (`vX.Y.Z` with `Z > 0`) when any pending changelog fragment carries a MINOR-requiring type (breaking, added, or changed).

#### Scenario: Patch tag with a breaking fragment is rejected

- **WHEN** a `v0.1.1` tag is pushed while `changelog.d/` contains a `breaking`-type fragment
- **THEN** release verification fails, reporting that the pending fragment types require a MINOR release

#### Scenario: Minor tag accepts feature and breaking fragments

- **WHEN** a `v0.2.0` tag is pushed with pending `added` and `breaking` fragments
- **THEN** the versioning-policy check passes and the assembled changelog section lists the breaking change under its own heading

#### Scenario: State schema bump rides a MINOR release

- **WHEN** a change bumps `state_schema_version` in the wire/state protos
- **THEN** the policy requires its release to be a MINOR release carrying a breaking-type changelog entry that names the migration
