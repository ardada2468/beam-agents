## ADDED Requirements

### Requirement: 0.3.0 publishes a benchmark report comparing beam-agents against an equivalent Apache Flink Agents scenario

The 0.3.0 release SHALL publish a benchmark comparison report at `docs/benchmarks/0.3.0-vs-flink-agents.md`, versioned in the repository and linked from the 0.3.0 changelog section and release notes. The comparison SHALL use the scenario from the C33 `add-benchmark-harness` scenario set that most closely matches a workload Apache Flink Agents can express idiomatically, paired with an implementation of that workload on Flink Agents built with that project's recommended APIs. Both legs SHALL run against a scripted fake model of equal cost so the reported numbers isolate runtime overhead rather than model latency, and the report SHALL present percentile latency (at minimum p50 and p99) and throughput for both legs. The report SHALL frame beam-agents' own release budget — overhead p50 < 15 ms / p99 < 60 ms per activation excluding LLM/tool time — as the primary claim, with the Flink Agents figures presented as measured context.

#### Scenario: The report ships with the release

- **WHEN** the 0.3.0 release is tagged
- **THEN** `docs/benchmarks/0.3.0-vs-flink-agents.md` exists at the tagged commit, is linked from the 0.3.0 changelog section, and reports percentile latency and throughput for a beam-agents leg and an Apache Flink Agents leg of the paired workload

#### Scenario: The workload comes from the C33 scenario set

- **WHEN** the report's beam-agents leg is inspected
- **THEN** it runs a scenario from the C33 benchmark harness, identified by name in the report, rather than a workload invented for the comparison

#### Scenario: Model latency is excluded from the comparison

- **WHEN** either leg of the comparison executes
- **THEN** model calls are served by a scripted fake of equal configured cost on both legs, and the report states this so the numbers are read as runtime overhead, not provider latency

### Requirement: The comparison methodology discloses its limits and is reproducible

The report SHALL contain a methodology section that: pins the exact versions (or commits) of beam-agents, Apache Flink Agents, Apache Flink, Apache Beam, and Python measured; describes the execution environment; references committed run configurations sufficient for a third party to re-run both legs; and explicitly enumerates every dimension where the comparison is not like-for-like — at minimum the language-runtime difference (JVM-inline versus Python on the Beam portability layer), the effect-model difference (inline durable execution versus outbox/effector re-injection), and state-backend/checkpointing differences — stating for each which side it structurally favors. All completed runs of the final configuration SHALL be reported; results MUST NOT be filtered by outcome, and a metric where beam-agents performs worse SHALL be published with the same prominence as favorable metrics. The published report SHALL be frozen at release: later performance changes are reported in a subsequent release's report, never by editing the published one.

#### Scenario: The methodology section enumerates non-equivalences

- **WHEN** the report's methodology section is reviewed before merge
- **THEN** it pins all measured versions, references the committed run configurations and environment description, and lists the language-runtime, effect-model, and state-backend non-equivalences with a statement of which side each favors

#### Scenario: An unfavorable result is published unfiltered

- **WHEN** the final configuration's runs show beam-agents worse than Flink Agents on some reported metric
- **THEN** the report publishes that result with the same prominence as favorable results, alongside all completed runs of the final configuration, rather than omitting or downplaying it

#### Scenario: The published report is frozen

- **WHEN** a performance improvement lands after 0.3.0 is tagged
- **THEN** `docs/benchmarks/0.3.0-vs-flink-agents.md` is not edited to reflect it; updated numbers appear only in a later release's report

#### Scenario: No fair pairing exists

- **WHEN** review during the comparison run concludes that no Flink Agents implementation can fairly pair with the closest C33 scenario
- **THEN** the report ships as a beam-agents-only budget report with a written explanation of why the paired comparison was withheld, rather than publishing a forced or misleading pairing
