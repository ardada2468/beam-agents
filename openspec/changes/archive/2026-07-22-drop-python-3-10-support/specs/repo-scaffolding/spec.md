## MODIFIED Requirements

### Requirement: Python and Beam version floors match project constraints

`pyproject.toml` SHALL declare `requires-python = ">=3.11,<3.13"` and MUST include `apache-beam[gcp]>=2.60`, `httpx[http2]`, `pydantic>=2`, and `protobuf` in `[project.dependencies]`. Attempting to install on an unsupported interpreter MUST fail with a clear resolver error.

#### Scenario: Install rejects Python 3.13

- **WHEN** a contributor attempts `uv sync` under Python 3.13
- **THEN** `uv` refuses to resolve the environment and reports the `requires-python` constraint

#### Scenario: Install rejects Python 3.10

- **WHEN** a contributor attempts `uv sync` under Python 3.10
- **THEN** `uv` refuses to resolve the environment and reports the `requires-python` constraint

#### Scenario: Runtime dependencies present after minimal install

- **WHEN** a contributor runs `uv sync` with no group flags
- **THEN** `apache_beam`, `httpx`, `pydantic`, and `google.protobuf` are all importable in the resulting environment

### Requirement: GitHub Actions workflows mirror the testing tiers

The repository SHALL define four workflows under `.github/workflows/`: `ci.yml`, `integration.yml`, `quality.yml`, and `nightly.yml`. `ci.yml` MUST run a matrix of Python `3.11`, `3.12` on `ubuntu-latest` and `macos-latest`, executing `make lint type test-unit`. `integration.yml` MUST run `make compose-up test-integration test-semantics` on `ubuntu-latest`. `quality.yml` MUST run `make mutation` plus a coverage-ratchet check against `main`. `nightly.yml` MUST run on a `0 7 * * *` schedule and on `workflow_dispatch`, authenticating to GCP via Workload Identity Federation and running `-m dataflow` tests. `ci`, `integration`, and `quality` MUST be marked required for merge into `main`.

#### Scenario: CI workflow runs on pull request

- **WHEN** a pull request targeting `main` is opened
- **THEN** `ci.yml`, `integration.yml`, and `quality.yml` all trigger and their success is required before merge

#### Scenario: CI matrix excludes Python 3.10

- **WHEN** `ci.yml`'s `python-version` matrix is inspected
- **THEN** it contains exactly `3.11` and `3.12`, and does not contain `3.10`

#### Scenario: Nightly workflow uses Workload Identity Federation

- **WHEN** `nightly.yml` executes
- **THEN** authentication to GCP happens via `google-github-actions/auth@v2` with `workload_identity_provider` and no long-lived service-account JSON key appears in secrets

#### Scenario: Nightly workflow no-ops without configured GCP project

- **WHEN** `nightly.yml` runs and `vars.GCP_PROJECT_ID` is unset
- **THEN** the dataflow job is skipped with a clear log message and the workflow exits successfully
