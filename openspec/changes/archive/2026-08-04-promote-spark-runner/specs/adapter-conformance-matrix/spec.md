## RENAMED Requirements

- FROM: `### Requirement: The matrix runs on DirectRunner and Flink`
- TO: `### Requirement: The matrix runs on DirectRunner, Flink, and a weekly Spark leg`

## MODIFIED Requirements

### Requirement: The matrix runs on DirectRunner, Flink, and a weekly Spark leg

The conformance matrix SHALL execute on three legs. The DirectRunner leg SHALL be offline (FakeLLM, scripted watermark/processing-time advances, no docker), marked so it is selected by the required offline semantics CI selection. The Flink leg SHALL run the suite against the local Flink mini-cluster through the portable job server, marked so it is selected by the docker-backed semantics selection in the integration workflow, and SHALL exercise restart-mid-suspension via a real TaskManager restart. The Spark leg SHALL run the suite against a Spark job server through the portable runner in a weekly scheduled workflow only — never per pull request — under the cadence, marker, and promotion rules of the `spark-runner-support` capability. Any scenario not runnable on a leg SHALL be declared per-scenario with a recorded reason (an explicit skip naming the constraint), never dropped silently.

#### Scenario: Offline leg needs no docker

- **WHEN** the offline semantics selection runs in an environment with no docker services
- **THEN** every DirectRunner conformance cell executes (or skips only for a missing optional framework package) and none requires Kafka, Redis, or Flink

#### Scenario: Flink leg survives a TaskManager restart mid-suspension

- **WHEN** the Flink leg runs the restart-mid-suspension scenario and the TaskManager is restarted between the suspend commit and the result delivery
- **THEN** the resumed activation's terminal output is observed on the output topic after recovery, with the deterministically-expected intent observed exactly once by the assertion consumer

#### Scenario: Spark leg runs weekly, not per pull request

- **WHEN** a pull request triggers the required workflows
- **THEN** no Spark-leg conformance cell is selected, and the Spark leg's cells execute only in the weekly scheduled workflow's spark conformance selection

#### Scenario: A leg-inexpressible scenario is an explicit skip

- **WHEN** a scenario is declared not runnable on one leg
- **THEN** that cell reports as skipped with the declared reason, and the meta-test's expected cell accounting includes it as a declared skip rather than a missing cell

### Requirement: The matrix cannot silently lose cells

A meta-test SHALL compute the expected cell count from the registered adapters, the scenario list, and the per-scenario runner declarations, and SHALL fail if collection produces a different number of cells than expected. The DirectRunner and Flink conformance cells SHALL carry the semantics marker (and the integration marker on the Flink leg only) so the existing semantics-partition check covers them. Spark-leg cells SHALL carry the integration marker and a dedicated spark marker — and SHALL NOT carry the semantics marker while Spark's status is best-effort — so the spark leg is excluded from both semantics-partition selections while remaining fully counted by the meta-test's cell accounting.

#### Scenario: Wiring regression is caught by the meta-test

- **WHEN** a refactor accidentally de-parameterizes a scenario or drops an adapter from the registry without touching the expected-cell declaration
- **THEN** the meta-test fails with the expected-versus-collected cell difference

#### Scenario: Spark cells are counted but stay out of the semantics partition

- **WHEN** the semantics-partition check evaluates its offline and docker-backed selections after the spark leg is added
- **THEN** neither selection contains a spark-leg cell, both selections are unchanged from before the spark leg existed, and the matrix meta-test still counts every spark cell in its expected-versus-collected accounting
