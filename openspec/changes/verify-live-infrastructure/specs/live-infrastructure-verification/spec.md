## Purpose

A single ordered verification run against real infrastructure — Docker locally, then Dataflow —
that executes every test tier this environment could not reach, distinguishes an infrastructure
failure from a product defect, seeds the two baselines that are unseedable without real hardware,
and records a committed report the release gates can cite.

## ADDED Requirements

### Requirement: The verification run covers every tier the project defines

A verification run SHALL attempt every marked test tier — `integration`, `semantics` (docker half),
`spark`, `dataflow`, `smoke` — plus the quality gates that require a full lane (`make mutation`,
`make coverage-ratchet` over offline + integration, `make bench` / `make bench-gate`). A tier that
is not attempted SHALL be recorded as `blocked` with the missing prerequisite named. A tier SHALL
NOT be silently omitted.

#### Scenario: An unattempted tier is visible in the report

- **WHEN** a verification run finishes without a GCP project configured
- **THEN** the report contains a row for the Dataflow phase with verdict `blocked` and the missing
  prerequisite named, rather than the phase being absent from the report

#### Scenario: Every marked tier has a row

- **WHEN** a verification run completes
- **THEN** the report contains at least one row for each of `integration`, `semantics` (docker),
  `spark`, `dataflow`, `smoke`, mutation, coverage, and benchmarks

### Requirement: Infrastructure failure is recorded distinctly from product defect

Each phase SHALL record exactly one of four verdicts: `pass`, `fail (defect)`, `fail (infra)`, or
`blocked`. A failure whose cause is the environment failing to provide the conditions under test —
unhealthy containers, lost Flink slots, a dead SDK worker pool, a refused broker connection, an
emulator that never became ready, exhausted memory or disk — SHALL be recorded as `fail (infra)`
and SHALL NOT be recorded as either a pass or a product defect. A failure of a behavioral assertion
against a healthy stack SHALL be recorded as `fail (defect)`.

#### Scenario: A dead TaskManager is not a correctness verdict

- **WHEN** the effectively-once gate fails and the harness reports `InfraFailure` or the run names
  dead workers
- **THEN** the phase is recorded `fail (infra)`, the remediation applied is recorded, and the phase
  is re-run — and the report does not claim the effectively-once invariant either held or was violated

#### Scenario: A duplicate side effect against a healthy stack is a defect

- **WHEN** the effectively-once gate fails with a plain assertion about a duplicated or lost side
  effect while every container is healthy
- **THEN** the phase is recorded `fail (defect)`, and a defect is filed as its own OpenSpec change

### Requirement: No test, threshold, or gate is weakened to obtain a green run

A verification run SHALL NOT add `skip` or `xfail` to a failing test, widen a numeric tolerance,
lower the latency budget, raise a mutation ceiling, add a coverage `omit` entry, or convert a
conformance leg's `Run()` declaration to `Skip()` in order to make a phase pass. Any such change
SHALL instead be proposed as its own OpenSpec change with a rationale and reviewed separately.
Seeding an empty baseline from a measured value, and raising a coverage baseline to a measured
value, are explicitly permitted — they record reality rather than relaxing a bar.

#### Scenario: A failing Spark cell is not skipped inline

- **WHEN** a Spark conformance cell fails because the portable runner cannot express the scenario
- **THEN** the cell's `Run()` declaration is left untouched in this change, the finding is recorded
  in the report, and converting it to `Skip(reason=...)` is filed as a separate change

#### Scenario: Seeding an empty baseline is permitted

- **WHEN** the benchmark suite runs to completion on a host qualified to seed it
- **THEN** `benchmark-baseline.toml`'s `[medians_ms]` is populated from the measured medians and the
  seeding host is recorded, and this is not treated as weakening a gate

### Requirement: Benchmark medians are seeded only from a qualified host

`[medians_ms]` SHALL be seeded only from a run on a quiet machine whose host characteristics are
recorded in the report. If no qualified host is available, `[medians_ms]` SHALL be left empty and
only the absolute latency budget verdict (p50 < 15 ms, p99 < 60 ms) SHALL be recorded.

#### Scenario: A busy laptop does not seed the medians

- **WHEN** the benchmark phase runs on a machine the operator judges too noisy to seed from
- **THEN** `benchmark-baseline.toml` is left unmodified, and the report records the budget verdict
  plus the reason the medians were not seeded

### Requirement: The run records a committed report naming the host and the commit verified

A verification run SHALL produce `verification-report.md`, committed to the repository, recording:
the commit SHA verified, the host (OS, CPU count, memory, Docker version), the compose image
identifiers, and one row per phase carrying the command run, the verdict, the evidence (counts,
durations, or error signature), and any defect filed.

#### Scenario: The report identifies what was actually verified

- **WHEN** a reader opens `verification-report.md` after a run
- **THEN** they can determine which commit was verified, on what hardware, and the per-phase verdict
  without consulting console output that no longer exists

### Requirement: Blocked task items are discharged with evidence or converted to defects

For each of the previously blocked task items across the implemented changes, a verification run
SHALL either check the item off with the evidence produced by the run, or record why it remains
blocked, or file a defect. An item SHALL NOT be checked off without evidence from an executed phase.

#### Scenario: A discharged item cites its evidence

- **WHEN** the Flink conformance phase passes
- **THEN** the corresponding blocked items in the adapter-conformance, ADK-adapter and
  Pydantic-AI-adapter change folders are checked off, each citing the phase's recorded evidence

#### Scenario: A still-blocked item keeps its blocker

- **WHEN** the run completes without GPU hardware
- **THEN** the vLLM real-engine item remains unchecked with its blocker note intact

### Requirement: Cloud resources are labelled and torn down unconditionally

The Dataflow phase SHALL label every created resource with the run identifier and SHALL tear down
unconditionally, including on failure, using the existing ledger and sweeper. The report SHALL
record the sweeper's result as evidence that no streaming job was left running.

#### Scenario: A failed cloud phase still cleans up

- **WHEN** the Dataflow phase fails partway
- **THEN** the sweeper runs, its output is recorded in the report, and no job created by the run
  remains in a running state
