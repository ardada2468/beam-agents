## MODIFIED Requirements

### Requirement: GitHub Actions workflows mirror the testing tiers

The repository SHALL define four workflows under `.github/workflows/`: `ci.yml`, `integration.yml`, `quality.yml`, and `nightly.yml`. `ci.yml` MUST run a matrix of Python `3.11`, `3.12` on `ubuntu-latest` only, executing `make lint type test-unit`. No workflow MAY schedule a job on a macOS runner. `integration.yml` MUST run `make compose-up test-integration test-semantics` on `ubuntu-latest`. `quality.yml` MUST run `make mutation` plus a coverage-ratchet check against `main`. `nightly.yml` MUST run on a `0 7 * * *` schedule and on `workflow_dispatch`, authenticating to GCP via Workload Identity Federation and running `-m dataflow` tests. `ci`, `integration`, and `quality` MUST be marked required for merge into `main`.

#### Scenario: CI workflow runs on pull request

- **WHEN** a pull request targeting `main` is opened
- **THEN** `ci.yml`, `integration.yml`, and `quality.yml` all trigger and their success is required before merge

#### Scenario: CI matrix excludes Python 3.10

- **WHEN** `ci.yml`'s `python-version` matrix is inspected
- **THEN** it contains exactly `3.11` and `3.12`, and does not contain `3.10`

#### Scenario: CI matrix runs on Linux only

- **WHEN** `ci.yml`'s `os` matrix is inspected
- **THEN** it contains exactly `ubuntu-latest`, and the workflow expands to two legs (`3.11` and `3.12`) rather than four

#### Scenario: No workflow uses a macOS runner

- **WHEN** every `runs-on` value across all four workflow files is inspected, including matrix-expanded values
- **THEN** none of them resolves to a `macos-*` runner label

#### Scenario: Documented workflow map matches the implemented matrix

- **WHEN** the `ci.yml` row of the workflow table in `docs/ci.md` is compared against `ci.yml`'s matrix
- **THEN** the documented tier states Python `3.11`–`3.12` on ubuntu and makes no reference to macOS

#### Scenario: Required status checks all resolve after the matrix shrinks

- **WHEN** a pull request targeting `main` is opened after the macOS legs are removed
- **THEN** every status check required by `main`'s branch-protection rule is reported by a job that actually runs, and no required context stays permanently pending

#### Scenario: Nightly workflow uses Workload Identity Federation

- **WHEN** `nightly.yml` executes
- **THEN** authentication to GCP happens via `google-github-actions/auth@v2` with `workload_identity_provider` and no long-lived service-account JSON key appears in secrets

#### Scenario: Nightly workflow no-ops without configured GCP project

- **WHEN** `nightly.yml` runs and `vars.GCP_PROJECT_ID` is unset
- **THEN** the dataflow job is skipped with a clear log message and the workflow exits successfully
