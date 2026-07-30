## Purpose

The CI contract for the Flink mini-cluster lane: a dedicated job in the integration workflow that runs the docker-backed `semantics`/`integration` marker selections apart from the base integration tier, with layer-cached SDK-harness builds, failure-time upload of Flink logs and diagnostics before teardown, and an offline check guaranteeing that no docker-backed semantics test can escape the lane's path-scoped make targets.

## ADDED Requirements

### Requirement: The Flink mini-cluster gates run in a dedicated CI job

The integration workflow SHALL run the docker-backed semantics selections in a job dedicated to the Flink mini-cluster, named `flink-minicluster`, separate from the base `integration` job. The `flink-minicluster` job SHALL bring up the full compose stack and execute `make test-semantics` and `make test-conformance-flink` as two distinct steps (preserving the existing distinguishability between an e2e-gate failure and a conformance failure), with its own `timeout-minutes` budget and its own teardown. The base `integration` job SHALL keep its existing job name (its status-check context is required on `main`) and SHALL run `make test-integration` only. The two jobs SHALL NOT depend on each other, so they execute in parallel and can be re-run independently. Both jobs SHALL trigger on the same events the workflow triggers on today (push to `main`, pull request).

#### Scenario: A conformance failure does not disturb the base integration lane

- **WHEN** `make test-conformance-flink` fails in the `flink-minicluster` job while the base `integration` job's tests pass
- **THEN** the `integration` job reports success, the `flink-minicluster` job reports failure with the failing step identifying the conformance leg (not the e2e gate), and re-running the failed job re-executes only the Flink lane

#### Scenario: The two jobs run in parallel

- **WHEN** a pull request triggers the integration workflow
- **THEN** the `integration` and `flink-minicluster` jobs start without either waiting on the other, and the workflow's wall-clock is bounded by the slower job rather than the sum of both

#### Scenario: Gate selections are unchanged by the split

- **WHEN** the steps of the `flink-minicluster` job are inspected
- **THEN** `make test-semantics` and `make test-conformance-flink` appear as separate steps with their pytest marker expressions and path scopes identical to the pre-split workflow

### Requirement: The base integration job runs without the Flink services

The base `integration` job SHALL provision only the services its selection needs — Redpanda, Redis, and the GCP emulators — via a `make compose-up-core` target that names those services explicitly, and SHALL NOT start the Flink JobManager, TaskManager, jobserver, or SDK-harness containers, nor build the SDK-harness image. `docker/compose.yaml`'s service definitions SHALL be unchanged, and `make compose-up` SHALL retain its current full-stack, `--wait --build` behavior for local use and for the `flink-minicluster` job.

#### Scenario: Core bring-up starts no Flink container

- **WHEN** `make compose-up-core` completes
- **THEN** Redpanda, Redis, and the emulator services report healthy, no `flink-*` or `beam-sdk-harness` container exists, and no image build was performed

#### Scenario: The integration selection passes against the core stack

- **WHEN** `make test-integration` runs with only the core services up
- **THEN** the `integration and not semantics` selection passes with no test failing or erroring for want of a Flink service

### Requirement: Flink logs and diagnostics are uploaded on failure, before teardown

When any test step of a docker-backed job fails, the job SHALL capture diagnostics via a `make compose-logs` target and upload them as a CI artifact **before** the stack is torn down. For the `flink-minicluster` job the capture SHALL include, at minimum: per-service `docker compose logs` for the Flink JobManager, TaskManager, jobserver, SDK harness, Redpanda, and Redis; any TaskManager thread-dump files the harness wrote under the spool diagnostics directory (spool segment files excluded); and best-effort snapshots of the Flink REST `/jobs/overview` and `/taskmanagers` endpoints, where a snapshot failure records an error note rather than failing the capture. Teardown (`make compose-down`) SHALL continue to run unconditionally (`if: always()`), and a successful job SHALL upload no artifact.

#### Scenario: A red Flink run leaves a downloadable artifact

- **WHEN** `make test-semantics` fails in the `flink-minicluster` job
- **THEN** the workflow run exposes a downloadable artifact containing the six services' logs and any harness thread dumps, captured from the still-running containers before `make compose-down` removed them

#### Scenario: A green run uploads nothing

- **WHEN** all steps of the `flink-minicluster` job succeed
- **THEN** no diagnostics artifact is produced for that job, and teardown still runs

#### Scenario: Teardown survives a failed capture

- **WHEN** the diagnostics capture itself errors (e.g. the Flink REST API is unreachable)
- **THEN** the capture records what it could, the artifact upload proceeds with the partial contents, and `make compose-down` still executes

### Requirement: The SDK-harness image build reuses cached layers without ever running stale

The SDK-harness Dockerfile SHALL order its layers so that third-party dependency installation precedes the copy of repository sources, making the network-bound install layer cacheable across source-only changes, while producing the same final image contents and retaining the build-time import self-check. The `flink-minicluster` job SHALL build the image through a layer cache persisted across CI runs (restore on build, save on completion) and SHALL start the compose stack against the image built in that same job from the same checkout — the stack MUST NOT run an image from a previous run or a previous commit. A cache miss or cache-backend outage SHALL degrade to a full build, never to a stale or failed image.

#### Scenario: A source-only change reuses the dependency layer

- **WHEN** a CI run builds the harness image after a commit that modifies `src/` but neither the Dockerfile nor the third-party dependency set
- **THEN** the third-party installation layer is restored from cache and only the source-copy and source-install layers rebuild

#### Scenario: The stack runs the image built from the current checkout

- **WHEN** the `flink-minicluster` job reaches its compose bring-up step
- **THEN** the SDK-harness image it starts was produced by a build step earlier in the same job from the same checkout, and the build's import self-check passed

#### Scenario: Cache unavailability degrades to a full build

- **WHEN** the layer cache is empty, evicted, or the cache backend is unreachable
- **THEN** the image builds from scratch, the job proceeds normally, and no step fails on account of the cache

### Requirement: No docker-backed semantics test can escape the Flink lane's path-scoped targets

The semantics-partition check SHALL verify, in addition to its existing marker-partition assertions, that the path-scoped selections actually executed by the docker-lane make targets — `-m "semantics and integration"` over `tests/semantics` and over `tests/conformance` — are each non-empty, mutually disjoint, and together equal to the repo-wide `-m "semantics and integration"` selection. A `semantics and integration` test collected outside both directories SHALL fail the check, naming the escaped test. The check's set logic SHALL be exposed as a pure function with unit tests, and the check SHALL continue to run as part of the existing required `ci` workflow step, offline, with no docker services.

#### Scenario: An escaped docker-semantics test fails the required check

- **WHEN** a test carrying both `semantics` and `integration` markers is added under a directory other than `tests/semantics` or `tests/conformance`
- **THEN** the partition check exits non-zero and its output names the escaped test's nodeid

#### Scenario: The current layout passes

- **WHEN** the extended check runs against a layout where every docker-backed semantics test lives under `tests/semantics` or `tests/conformance`
- **THEN** the check passes, reporting the offline, docker-semantics, and docker-conformance selection sizes

#### Scenario: An emptied path-scoped selection fails

- **WHEN** either path-scoped docker selection collects zero tests (e.g. a marker or directory rename deselects a gate)
- **THEN** the check exits non-zero identifying the empty selection rather than passing on a smaller lane
