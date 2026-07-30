## ADDED Requirements

### Requirement: The 0.5.0 release ships only after the M3 adoption-surface batch is archived

The `v0.5.0` tag SHALL NOT be cut until all seven M3 dependency changes are implemented and archived under `openspec/changes/archive/`: `add-yaml-provider`, `add-dataflow-flex-template`, `add-replay-cli`, `add-pydantic-ai-adapter`, `add-slack-approval-example`, `add-eval-pipeline-example`, and `add-upstream-design-doc`. Archival — not merge — is the gating state. No subset of the batch MAY ship as 0.5.0; if any dependency change cannot archive, the release date slips and the scope does not.

#### Scenario: A dependency change is still pending

- **WHEN** the release checklist is run and at least one of the seven named changes has no directory under `openspec/changes/archive/`
- **THEN** the gate fails, no `v0.5.0` tag is created, and no artifact is published

#### Scenario: All seven dependency changes are archived

- **WHEN** the release checklist is run and every one of the seven named changes has an archived directory under `openspec/changes/archive/`
- **THEN** the archival gate is satisfied and release preparation may proceed to the quality gates

### Requirement: Release-blocking quality gates are green on the release commit

Before the tag is cut, the following MUST be green on the exact commit the `v0.5.0` tag will point at — not on an earlier commit: the adapter conformance matrix (all seven lifecycle scenarios × every registered adapter × both the DirectRunner and Flink legs, with the matrix meta-test passing; by 0.5.0 the registered-adapter set includes the Pydantic AI adapter added by `add-pydantic-ai-adapter`), the benchmark regression gate on the runtime-overhead latency budget (p50 < 15 ms / p99 < 60 ms per activation, excluding LLM/tool time), and the required `ci`, `integration`, and `quality` checks.

#### Scenario: Conformance matrix is green on the release commit

- **WHEN** the release commit's `ci` and `integration` runs are inspected
- **THEN** the DirectRunner conformance leg passed in the offline semantics selection, the Flink leg passed via `make test-conformance-flink`, and the matrix meta-test confirms every registered adapter — including Pydantic AI — is covered on both legs

#### Scenario: Benchmark gate is green on the release commit

- **WHEN** the benchmark regression gate is evaluated on the release commit
- **THEN** runtime overhead per activation (excluding LLM/tool time) is within p50 < 15 ms and p99 < 60 ms, and any regression against the budget blocks the tag

### Requirement: The released artifact carries version 0.5.0 and a changelog covering all M3 changes

The release commit SHALL set `pyproject.toml` `version` to `0.5.0`, and the changelog established by `add-0-1-0-release` SHALL contain a `0.5.0` section with one entry for every change archived since the previous release tag — including all seven M3 dependency changes — verified against the archive directory rather than recalled from memory. The tag and publish SHALL be performed via the release process established by `add-0-1-0-release`, with no modification to that process in this change.

#### Scenario: Version and changelog agree at tag time

- **WHEN** the `v0.5.0` tag is about to be cut
- **THEN** `pyproject.toml` reads `version = "0.5.0"` and the changelog's `0.5.0` section names every change archived since the previous release tag, with the seven M3 changes present

#### Scenario: Published artifact reports the released version

- **WHEN** `beam-agents==0.5.0` is installed from the published index into a clean environment
- **THEN** the install succeeds and package metadata reports version `0.5.0`
