## MODIFIED Requirements

### Requirement: Project layout follows `uv`-managed `src/` convention

The repository SHALL be organised as a `uv`-managed Python project with a `src/beam_agents/` package, a top-level `tests/` directory, a `protos/` directory for `.proto` sources with generated `_pb2.py` files co-located, a `docker/` directory for compose assets, an `openspec/` directory for spec-driven change artifacts, and a `website/` directory holding the documentation site. A `.python-version` file MUST pin the default interpreter to a supported version. The `website/` directory MUST be self-contained: no Python package or test outside `tests/docs/` may import from it, and no module under `src/` may depend on it.

#### Scenario: Bootstrap on a clean checkout

- **WHEN** a contributor runs `uv sync --all-groups` on a clean checkout
- **THEN** the command completes successfully, creates `.venv/`, installs every declared dependency group, and `python -c "import beam_agents"` exits 0 with no output

#### Scenario: Required top-level directories exist

- **WHEN** the repository is inspected after this change lands
- **THEN** `src/beam_agents/__init__.py`, `tests/conftest.py`, `protos/`, `docker/compose.yaml`, `openspec/`, `website/package.json`, `pyproject.toml`, `uv.lock`, and `.python-version` all exist

#### Scenario: Node artifacts are not tracked

- **WHEN** a contributor installs the site's dependencies and builds it
- **THEN** `git status --porcelain` reports no untracked `website/node_modules/` or `website/.next/` entries, and the site's lockfile is tracked

### Requirement: GitHub Actions workflows mirror the testing tiers

The repository SHALL define five workflows under `.github/workflows/`: `ci.yml`, `integration.yml`, `quality.yml`, `nightly.yml`, and `website.yml`. `ci.yml` MUST run a matrix of Python `3.11`, `3.12` on `ubuntu-latest` and `macos-latest`, executing `make lint type test-unit`. `integration.yml` MUST run `make compose-up test-integration test-semantics` on `ubuntu-latest`. `quality.yml` MUST run `make mutation` plus a coverage-ratchet check against `main`. `nightly.yml` MUST run on a `0 7 * * *` schedule and on `workflow_dispatch`, authenticating to GCP via Workload Identity Federation and running `-m dataflow` tests. `website.yml` MUST run `make site-check` on `ubuntu-latest`, triggered by changes to `website/**`, `src/**`, `docs/**`, `openspec/specs/**`, or its own file, with a pinned Node version and a cached dependency install. `ci`, `integration`, and `quality` MUST be marked required for merge into `main`; whether `website` is required is a repository setting outside this requirement.

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

#### Scenario: Website workflow triggers on a runtime change

- **WHEN** a pull request modifies a file under `src/beam_agents/`
- **THEN** `website.yml` triggers and runs `make site-check`

#### Scenario: Website workflow does not run for unrelated changes

- **WHEN** a pull request modifies only files under `docker/`
- **THEN** `website.yml` does not trigger

### Requirement: `Makefile` is the single contract between local and CI

A top-level `Makefile` SHALL expose targets `bootstrap`, `fmt`, `lint`, `type`, `test-unit`, `test-integration`, `test-semantics`, `mutation`, `compose-up`, `compose-down`, `proto`, `site-dev`, `site-build`, and `site-check`. CI workflows MUST invoke only `make <target>` for their primary steps (setup steps such as `uv sync` or a Node setup action are exempt). A contributor running `make <target>` locally MUST get the same behaviour as CI for that target. The `site-*` targets MUST be the only targets requiring a Node toolchain, and `site-check` is the only site target that may additionally require the `uv` environment.

#### Scenario: CI step invokes a make target

- **WHEN** any CI workflow's primary build/test step is inspected
- **THEN** the step's shell command is of the form `make <target>` (setup steps excepted)

#### Scenario: Local lint matches CI lint

- **WHEN** a contributor runs `make lint` on their machine and pushes the same commit
- **THEN** the `ci-lint` job reports the same pass/fail result for that commit

#### Scenario: Python targets run without Node

- **WHEN** a contributor with no Node toolchain runs `make bootstrap lint type test-unit`
- **THEN** every target completes normally and none reports a missing Node or package-manager binary

#### Scenario: Site build runs without the Python environment

- **WHEN** a contributor with no `.venv/` runs `make site-build`
- **THEN** the target completes successfully
