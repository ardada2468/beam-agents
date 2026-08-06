# state-guarantees Specification

## Purpose
TBD - created by archiving change add-state-guarantees. Update Purpose after archive.
## Requirements
### Requirement: A published compatibility policy states the cross-release state promise and classifies every change class

The project SHALL publish a state-compatibility document (`docs/state-compat.md`) stating, in RFC-2119 terms, the cross-release promise: state blobs written by release N SHALL be readable by release N+1 (directly for additive changes, via the registered lazy migration for `state_schema_version`-bumped changes), the keyed-state wire format SHALL remain exactly the deterministic protobuf serialization of the schema messages with no additional framing, and Dataflow `--update` from release N to N+1 SHALL succeed unless the release notes declare a migration release.

The document SHALL explicitly classify what is NOT promised: skip-level updates (N to N+k) are best-effort, downgrades are unsupported, cross-version byte-identity of encodings is not promised (the promise is semantic decodability), and Flink savepoint compatibility and cross-runner state portability are out of scope.

The document SHALL contain a compatibility table with one row per state-affecting change class — at minimum: additive proto field, new enum value, new state spec, field removal/renumber/retype, coder encoding change, transform or state-spec-id rename, state-cell coder-type change, and versioned migration — each row stating whether old state remains readable, whether `--update` remains safe, and the action required of the author. The table SHALL name graph shape as part of the compatibility surface: `RunAgent`-internal transform names and state spec ids participate in Dataflow's update compatibility check.

#### Scenario: The promise separates guaranteed from best-effort

- **WHEN** an operator consults the document to plan an upgrade of a running Dataflow job from release N to N+1, and separately from N to N+3
- **THEN** the document states the N-to-N+1 path as a SHALL-grade promise backed by the nightly gate, and states the skip-level path as best-effort with the recommendation to step through releases

#### Scenario: The table classifies a safe change and a breaking change differently

- **WHEN** an author consults the table before adding an optional field to `Continuation`, and before changing `DeterministicProtoCoder` to length-prefix its output
- **THEN** the additive-field row states old state readable and `--update` safe with a golden-fixture obligation, and the coder-encoding row states the change is forbidden because the raw-proto wire format is the contract

#### Scenario: A graph-shape change is classified even though no bytes change

- **WHEN** an author consults the table before renaming a transform inside `RunAgent`'s expansion or a state spec id in the DoFn
- **THEN** the table states that stored bytes remain readable but `--update` step matching fails, and prescribes the required action

### Requirement: A nightly gate proves --update on real Dataflow preserves live keyed state across versions

The repository SHALL contain a `dataflow`-marked nightly test that launches a streaming pipeline on Dataflow at the previous released version of beam-agents (installed from PyPI), drives it to hold live keyed state — at minimum one key suspended mid-activation (persisted `Continuation` with a pending intent and an unexpired HITL deadline) and one key with populated working memory (`MemoryBlob`) — and then replaces the job in place via `--update` with the current head (built from the checkout). Both job graphs SHALL be constructed from identical launcher source, differing only in the installed library versions.

After the update takes effect, the gate SHALL assert from outside the pipeline (via the pipeline's outputs) that: the suspended key resumes and completes when its approval is injected post-update, producing output derivable only from the pre-update continuation; the memory key's next activation reads back the value written pre-update; and a previously unseen key completes normally on the updated job.

A replacement job refused by Dataflow's compatibility check (the new job fails while the prior job keeps running) SHALL be reported as a compatibility failure — the gate's primary defect class — distinct from infrastructure failures (quota, provisioning, network), and the report SHALL include both resolved version strings.

#### Scenario: A suspension survives the update

- **WHEN** the previous-release job has a key suspended awaiting approval, the job is updated to head, and the approval is then injected on the same key
- **THEN** the suspended activation resumes on the updated job and its terminal output reflects the pre-update suspension state, rather than the key restarting or dead-lettering

#### Scenario: Working memory survives the update

- **WHEN** a key's working memory was written by the previous-release job and a post-update event asks the agent to echo it
- **THEN** the updated job's output contains the pre-update marker value, proving the `MemoryBlob` bytes written by release N were decoded by head

#### Scenario: A refused compatibility check is reported as the defect it is

- **WHEN** the head job graph is incompatible with the running previous-release job and Dataflow refuses the replacement
- **THEN** the gate fails with a compatibility-failure classification naming both versions and the service's stated reason, and does not report the refusal as an infrastructure error

#### Scenario: Before any PyPI release exists, the gate runs a labelled self-update leg

- **WHEN** the nightly gate runs and no released version of beam-agents exists on PyPI
- **THEN** the gate launches head and updates to head through the same phases and assertions, and its report is prominently labelled as a bootstrap self-update run so it is never mistaken for cross-version evidence

### Requirement: A failed compatibility gate blocks release

The `--update` compatibility gate SHALL be release-blocking: the documented release procedure SHALL require the most recent nightly `dataflow` run to be green before a release is tagged, and a red gate SHALL be resolved by fixing the incompatibility or by shipping the documented migration path — never by weakening the gate. The gate SHALL NOT carry `xfail`, flake-tolerant skips, or automatic retries, and the `dataflow` make target SHALL fail on an empty test selection rather than tolerating it, so the gate cannot be silently deselected. Absent GCP configuration the gate SHALL skip visibly (a reported skip), not deselect.

#### Scenario: A red gate stops the tag

- **WHEN** the nightly `--update` gate failed with a compatibility or state-loss classification and a release is about to be tagged
- **THEN** the release procedure requires the failure to be resolved and a green nightly run to exist before tagging proceeds

#### Scenario: Deselecting the gate fails the build

- **WHEN** the `dataflow`-marked selection collects zero tests (for example, the gate module was renamed or its marker removed)
- **THEN** the `dataflow` make target exits non-zero instead of treating the empty selection as success

### Requirement: The gate is bounded in cost and cleans up unconditionally

The gate SHALL run at most one streaming worker per job, cap workers at one, use uniquely named per-run resources (job name, topics, subscriptions, temp prefix) carrying an identifying label, and complete within a stated wall-clock budget enforced by deadlines on every wait — never unbounded polling or sleep-based correctness. Teardown SHALL force-cancel every launched job and delete every provisioned resource on success, failure, and timeout alike, and each run SHALL first sweep resources leaked by prior crashed runs, identified by label and age.

#### Scenario: A failing run leaves nothing behind

- **WHEN** the gate fails an assertion or hits its timeout mid-phase
- **THEN** both Dataflow jobs are force-cancelled and the run's topics, subscriptions, and temp objects are deleted before the test reports its failure

#### Scenario: A crashed run is bounded to one night

- **WHEN** a previous nightly run crashed without executing teardown and left a labelled streaming job running
- **THEN** the next run's sweeper force-cancels the stale labelled job before provisioning its own resources
