# spark-runner-support Specification

## Purpose
TBD - created by archiving change promote-spark-runner. Update Purpose after archive.
## Requirements
### Requirement: Every conformance scenario declares the spark leg explicitly

The conformance leg vocabulary SHALL include a `spark` leg alongside `direct` and `flink`, and every conformance scenario SHALL declare the spark leg per-scenario as either runnable (`Run`) or not runnable (`Skip` with a recorded reason), using the existing leg-declaration vocabulary. Each spark `Skip` reason MUST name the specific Spark portable-runner feature gap or harness constraint that prevents the scenario from running — a scenario MUST NOT be omitted from the spark leg silently. The matrix meta-test's expected-cell accounting SHALL include the spark leg, so the expected cell count equals registered adapters × scenarios × three legs, with declared skips counted as cells.

#### Scenario: A scenario without a spark declaration cannot build a cell

- **WHEN** the conformance suite is collected with a scenario whose leg declarations omit the spark leg
- **THEN** the suite fails with an error naming the undeclared scenario and leg, rather than collecting a smaller matrix

#### Scenario: A spark-inexpressible scenario is an explicit skip with a reason

- **WHEN** a scenario is declared not runnable on the spark leg
- **THEN** its spark cell is collected and reported as skipped carrying the declared reason, and the reason names the concrete missing runner feature or harness constraint

#### Scenario: The meta-test accounts for the spark leg

- **WHEN** the matrix meta-test computes expected cells after the spark leg is added
- **THEN** the expected set is registered adapters × scenarios × the three legs, and removing the spark cell tests (or dropping the leg from the vocabulary without removing its cells) fails the meta-test with the exact cell difference

### Requirement: The spark leg runs against a Spark job server through the portable runner

The spark conformance leg SHALL drive the spark-runnable scenarios through the Beam portable runner against a Spark job server, using a harness parallel to the Flink leg's: one multiplexed job per registered adapter, scenario isolation by per-scenario key prefix, ingress/egress over the same Kafka-shaped topics, and deterministic responder answering of tool intents and approval requests. The Spark services SHALL be defined in a dedicated compose overlay file, not in the base compose file, so that per-PR integration jobs incur no Spark container cost. Infrastructure failures (stack bring-up, job submission stalls, worker-pool death) SHALL be classified separately from scenario verdicts, so a broken stack is never reported as a Spark conformance failure.

#### Scenario: Spark leg runs the declared scenarios through the job server

- **WHEN** the spark conformance selection runs with the base stack and the spark overlay up
- **THEN** every spark-`Run` scenario executes for every registered adapter through the Spark job server, asserting the same terminal outputs, deterministic intents, and error-channel expectations as the corresponding Flink cells

#### Scenario: Base compose stack is unchanged

- **WHEN** the base compose file is brought up without the spark overlay
- **THEN** no Spark service starts, and every existing per-PR integration and semantics selection runs exactly as before

#### Scenario: Stack failure is not a Spark verdict

- **WHEN** the spark job server or its worker pool fails to come up or the job never starts processing
- **THEN** the leg reports an infrastructure failure distinct from any scenario assertion, and no conformance cell reports a pass or fail verdict for that run

### Requirement: The spark leg runs on a weekly schedule, never per pull request

The spark conformance leg SHALL run in a dedicated scheduled workflow with a weekly cadence, plus manual dispatch for investigation. It SHALL NOT be triggered by pull requests, SHALL NOT be a required check for merging, and its cells SHALL be excluded from every per-PR test selection (unit, integration, semantics offline, semantics docker-backed, and the Flink conformance selection) via a dedicated marker. While Spark's status is best-effort, spark cells SHALL NOT carry the semantics marker, so the semantics-partition guarantee over the direct and flink legs is undisturbed.

#### Scenario: Weekly scheduled run executes the spark leg

- **WHEN** the weekly schedule fires
- **THEN** the workflow brings up the base stack plus the spark overlay, runs the spark conformance selection for every registered adapter, and records the run's conclusion as a scheduled run

#### Scenario: Pull-request workflows are unchanged

- **WHEN** a pull request is opened
- **THEN** no spark-leg cell is selected by any workflow the pull request triggers, and the set of required status checks is identical to before the spark leg existed

#### Scenario: Manual dispatch does not affect the promotion window

- **WHEN** the weekly workflow is run via manual dispatch
- **THEN** the spark leg executes normally, and the run is excluded from the promotion window's streak accounting

### Requirement: Weekly runs report the promotion window mechanically

Each weekly run SHALL end with a status step that mechanically assesses the promotion window: it SHALL query the workflow's recent scheduled-run conclusions via the GitHub API, compute the current consecutive-green streak over scheduled runs only (a run's final conclusion counts, so an infrastructure-failure rerun that lands green before the next scheduled run counts as green), treat a gap of more than eight days between adjacent scheduled runs as breaking the streak, scan the trailing four-week window for added spark skip declarations, and publish the streak length, the current spark skip inventory with reasons, and a promotion-readiness verdict to the run summary. The status step SHALL only report; it SHALL NOT modify support statements, specs, or repository content.

#### Scenario: Consecutive green scheduled runs accumulate a streak

- **WHEN** the status step runs after the fourth consecutive green scheduled weekly run with no skip added in the window
- **THEN** the run summary reports a streak of four and a promotion-ready verdict

#### Scenario: A red week resets the streak

- **WHEN** a scheduled weekly run's final conclusion is failure
- **THEN** the next status assessment reports a streak restarted from zero, and the promotion verdict is not ready

#### Scenario: A missed week breaks consecutiveness

- **WHEN** more than eight days elapse between adjacent scheduled runs (for any reason, including the schedule being disabled)
- **THEN** the status step reports the cadence gap as breaking the streak rather than silently bridging it

#### Scenario: A skip added mid-window resets the promotion clock

- **WHEN** a spark scenario declaration changes from run to skip (or a new spark skip is added) within the trailing four-week window
- **THEN** the status step reports the skip addition and the promotion verdict is not ready, regardless of run conclusions

### Requirement: Spark is promoted to supported only through the four-week gate

Spark's status SHALL flip from best-effort to supported only after four consecutive green scheduled weekly runs with zero spark skip declarations added during that window, and only via a reviewed follow-up change. That promotion change SHALL update the runner-support statement in `openspec/project.md`, the README, and the CI documentation; SHALL mark the weekly spark leg required, meaning a red weekly run becomes a release blocker while the cadence remains weekly and not per-PR; and SHALL enumerate the surviving spark skips together with what they exclude from the supported claim. No change SHALL flip the support statement without citing the four qualifying scheduled runs.

#### Scenario: Gate satisfied flips the status

- **WHEN** the promotion window shows four consecutive green scheduled weekly runs with zero skips added, and the promotion change citing those runs is reviewed and merged
- **THEN** the project support statement lists Spark as supported, the README and CI documentation reflect the weekly-verified status, and the weekly spark leg is documented as required with red runs treated as release blockers

#### Scenario: Gate not satisfied leaves best-effort in place

- **WHEN** any run in the window is red, the cadence is broken, or a spark skip was added
- **THEN** the support statement remains best-effort and the window restarts; no partial credit is carried across a reset

#### Scenario: Surviving skips bound the supported claim

- **WHEN** the promotion change is authored while some spark scenarios remain declared skips
- **THEN** the promotion change lists each surviving skip and its reason and states what runtime behavior the supported claim consequently does not cover on Spark

### Requirement: Sustained red demotes Spark back to best-effort

After promotion, two consecutive red scheduled weekly runs (final conclusions, same cadence rules as the promotion window) SHALL trigger a demotion change that returns Spark's status to best-effort in the project support statement and README, with the demotion announced in the release notes. A single red scheduled run SHALL NOT demote; it SHALL open an investigation instead. The weekly spark leg SHALL continue running after demotion, and re-promotion SHALL use the same four-week gate with no shortcuts.

#### Scenario: Two consecutive red weeks demote

- **WHEN** two consecutive scheduled weekly runs end with final conclusion failure after Spark has been promoted
- **THEN** a demotion change returns the support statement and README to best-effort and the demotion is announced in the release notes

#### Scenario: One red week does not demote

- **WHEN** a single scheduled weekly run is red after promotion
- **THEN** the status remains supported, an investigation is opened, and the demotion clock advances only if the next scheduled run is also red

#### Scenario: Re-promotion repeats the full gate

- **WHEN** the weekly leg turns green again after a demotion
- **THEN** promotion requires a fresh four-consecutive-green-week window with zero skips added, identical to the first promotion
