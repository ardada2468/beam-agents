# repo-scaffolding

## Purpose

Provides the reproducible developer environment, dependency management, code-quality gates, local service topology, and CI workflow contracts that every future `beam-agents` capability relies on. This capability delivers scaffolding only — no runtime application code.
## Requirements
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

### Requirement: Dependency groups partition dev tooling from runtime

`pyproject.toml` SHALL define dependency groups `dev`, `test`, `lint`, `typecheck`, `integration`, `bench`, and `docs`. Runtime dependencies MUST NOT appear inside any group. Heavy or optional tooling (`testcontainers`, `mutmut`, `hypothesis`, benchmark rigs) MUST live in a group and MUST NOT be pulled by the base install.

#### Scenario: Base install omits integration tooling

- **WHEN** a contributor runs `uv sync` with no group flags
- **THEN** `testcontainers` and `mutmut` are NOT installed

#### Scenario: Lint job installs only lint group

- **WHEN** CI runs `uv sync --frozen --group lint`
- **THEN** `ruff` is installed and `pytest` is NOT installed

### Requirement: `ruff` enforces lint and format across `src/` and `tests/`

`ruff` configuration SHALL live in `[tool.ruff]` inside `pyproject.toml`. Enabled rule selectors MUST include `E`, `F`, `I`, `B`, `UP`, `SIM`, `ASYNC`, `PL`, and `RUF`. Line length MUST be 100. `ruff format` MUST be the sole formatter. The `lint` make target MUST run both `ruff check` and `ruff format --check`.

#### Scenario: Blocking call in async function fails lint

- **WHEN** a contributor commits an `async def` function that calls `time.sleep(1)`
- **THEN** `make lint` exits non-zero with an `ASYNC` rule violation

#### Scenario: Formatter drift fails CI

- **WHEN** a pull request contains unformatted code
- **THEN** the `ci-lint` job fails at the `ruff format --check` step

### Requirement: `mypy --strict` gates the source tree

`mypy` MUST run with `strict = true` against `src/`. Per-module overrides MAY set `ignore_missing_imports = true` for `apache_beam.*` only. `Any` MUST NOT appear in the public signatures of `beam_agents.__init__`. The `type` make target MUST run the full strict pass.

#### Scenario: Missing type hint fails typecheck

- **WHEN** a contributor adds a public function to `src/beam_agents/` without a return-type annotation
- **THEN** `make type` exits non-zero with a strict-mode error

#### Scenario: Beam stub gaps do not fail typecheck

- **WHEN** `src/` imports `apache_beam.transforms` and calls its API
- **THEN** `make type` succeeds without stub warnings for Beam modules

### Requirement: `pytest` uses a closed marker registry with strict mode

`pytest` configuration SHALL live in `[tool.pytest.ini_options]` and MUST set `addopts` to include `--strict-markers` and `--strict-config`, `asyncio_mode = "auto"`, and a default `timeout`. The registered markers MUST include exactly `integration`, `semantics`, `dataflow`, and `slow`. Using an unregistered marker MUST fail the test session.

#### Scenario: Unregistered marker is an error

- **WHEN** a contributor decorates a test with `@pytest.mark.integrtaion` (typo)
- **THEN** `pytest` exits non-zero reporting the unknown marker

#### Scenario: Default run excludes integration and dataflow tiers

- **WHEN** a contributor runs `make test-unit` with docker down
- **THEN** the run completes successfully and no `integration`, `semantics`, or `dataflow`-marked test executes

### Requirement: `pre-commit` enforces gates locally before push

A `.pre-commit-config.yaml` SHALL wire hooks for `ruff check --fix`, `ruff format`, `mypy` on staged files, protobuf generation drift, and a local hook that blocks commits touching `src/` when no `openspec/changes/<name>/` directory is present in the working tree or referenced by the commit message. An escape-hatch environment variable `BEAM_AGENTS_ALLOW_NO_CHANGE=1` MUST bypass the OpenSpec hook only.

#### Scenario: Committing to src/ without a change fails

- **WHEN** a contributor stages a change under `src/beam_agents/` with no `openspec/changes/*/` directory present
- **THEN** `git commit` is rejected by the pre-commit hook with a message pointing to the OpenSpec workflow

#### Scenario: Protobuf drift blocks commit

- **WHEN** a contributor edits a `.proto` file but forgets to regenerate the `_pb2.py`
- **THEN** the pre-commit protobuf hook exits non-zero

### Requirement: Docker compose provides Redpanda, Redis, and Flink for integration tests

`docker/compose.yaml` SHALL define services `redpanda`, `redis`, and `flink` (jobmanager + taskmanager), each pinned by image digest. Ports MUST be namespaced away from defaults: Kafka on `19092`, Redis on `16379`, Flink JobManager on `18081`. Each service MUST declare a healthcheck. `make compose-up` MUST bring the stack up and `make compose-down` MUST tear it down cleanly.

#### Scenario: Compose stack starts healthy

- **WHEN** a contributor runs `make compose-up`
- **THEN** within 60 seconds all three services report healthy via `docker compose ps`

#### Scenario: Unit tests pass with compose down

- **WHEN** a contributor runs `make test-unit` with no docker services running
- **THEN** the run completes successfully with no skipped tests attributable to missing services

### Requirement: GitHub Actions workflows mirror the testing tiers

The repository SHALL define five workflows under `.github/workflows/`: `ci.yml`, `integration.yml`, `quality.yml`, `nightly.yml`, and `website.yml`. `ci.yml` MUST run a matrix of Python `3.11`, `3.12` on `ubuntu-latest` and `macos-latest`, executing `make lint type test-unit`. `integration.yml` MUST run `make compose-up test-integration test-semantics` on `ubuntu-latest`. `quality.yml` MUST run `make mutation` plus a coverage-ratchet check against `main`, where the mutation step runs when `src/beam_agents/core/**` or `tests/core/**` changed and MUST fail the workflow on a surviving mutant. `nightly.yml` MUST run on a `0 7 * * *` schedule and on `workflow_dispatch`, authenticating to GCP via Workload Identity Federation and running `-m dataflow` tests, and MUST additionally run `make mutation` in a job that requires no cloud credentials and is not conditioned on repository variables or secrets. `website.yml` MUST run `make site-check` on `ubuntu-latest`, triggered by changes to `website/**`, `src/**`, `docs/**`, `openspec/specs/**`, or its own file, with a pinned Node version and a cached dependency install. `ci`, `integration`, and `quality` MUST be marked required for merge into `main`; whether `website` is required is a repository setting outside this requirement.

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

#### Scenario: Quality workflow mutation step triggers on core test changes

- **WHEN** a pull request changes only files under `tests/core/`
- **THEN** `quality.yml`'s change detection selects the mutation step rather than skipping it

#### Scenario: Quality workflow fails on a surviving mutant

- **WHEN** `quality.yml`'s mutation step completes with a surviving mutant in `core/`
- **THEN** the step exits non-zero and the required `quality` check fails

#### Scenario: Nightly workflow runs the mutation sweep unconditionally

- **WHEN** `nightly.yml` is inspected
- **THEN** it contains a job invoking `make mutation` whose execution is not conditioned on `vars.GCP_PROJECT_ID` or on provider API-key secrets

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

### Requirement: Public API surface is typed and governed by the 1.0 freeze

`src/beam_agents/__init__.py` SHALL exist and `mypy --strict` MUST pass on the module. The surface started empty at scaffolding time and SHALL grow only through OpenSpec changes; as of the 1.0 API freeze, the names it exposes are governed by the `public-api` capability and its committed surface snapshot rather than by this requirement.

Importing the package MUST NOT perform I/O, spawn threads, mutate global state, or import optional dependencies — adapter frameworks, effector transport clients, and exporter protos MUST stay out of the import graph until their feature is used. The single sanctioned piece of import-time indirection is the module-level `__getattr__` that lazily resolves optional-extra adapter classes.

#### Scenario: Fresh import is side-effect free

- **WHEN** a contributor imports `beam_agents` in a clean interpreter with no optional extras installed
- **THEN** the import succeeds without network or filesystem access, no threads are spawned, and no optional-extra module (LangGraph, effector transports, opentelemetry-proto) appears in `sys.modules`

#### Scenario: Public surface matches the committed snapshot

- **WHEN** the surface test derives the root module's `__all__` and public names from source
- **THEN** they equal the `public-api` snapshot's record for `beam_agents/__init__.py`, and any deviation fails the offline `ci` lane
