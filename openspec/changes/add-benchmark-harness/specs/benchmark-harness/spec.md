## ADDED Requirements

### Requirement: The repository provides an offline pyperf benchmark harness

The repository SHALL provide a top-level `benchmarks/` package of pyperf-driven benchmark modules, runnable end to end via `make bench` on a clean checkout with only committed dependency groups installed. The harness MUST run fully offline: no docker services, no network access, and no real model provider — every model interaction goes through `FakeLLM`. Each benchmark module SHALL write its results as pyperf JSON to a gitignored results directory, and the harness SHALL be type-checked under the same `mypy --strict` configuration as `src/` and `tests/`.

#### Scenario: The full suite runs offline

- **WHEN** `make bench` is invoked on a machine with no docker daemon and no network access
- **THEN** every benchmark module completes and writes one pyperf JSON result file, with no test skipped or degraded for lack of infrastructure

#### Scenario: Benchmark modules cannot rot silently

- **WHEN** the offline unit tier (`make test-unit`) runs
- **THEN** every benchmark module is imported and its timed function executed for at least one iteration, so a runtime refactor that breaks a benchmark fails the required `ci` lane rather than a later nightly run

### Requirement: No-op throughput establishes the runtime's ceiling

The harness SHALL measure the per-activation cost of the `_AgentDoFn` element path with a no-op agent — no model call, no tool, no memory write — driven with in-memory state and timer handles. The published figure SHALL include the async bridge submission and the full staged commit, and the report SHALL present it both as time per activation and as derived activations/sec, labeled as the runtime ceiling with zero agent work.

#### Scenario: A no-op activation is timed through the full element path

- **WHEN** the no-op throughput benchmark runs
- **THEN** each recorded value covers one complete `process()` drain — bridge submission, activation, and commit included — and the result reports time per activation with activations/sec derivable from the median

### Requirement: Runtime overhead is measured per activation under FakeLLM latency tiers

The harness SHALL measure per-activation runtime overhead with `FakeLLM` configured at latency tiers of 50 ms, 500 ms, and 2000 ms, where overhead is defined as the activation's end-to-end wall time minus the configured provider latency — the same subtraction the runtime's `overhead_ms` distribution publishes, so the benchmark and the dashboard measure one quantity. The tiers MUST use `FakeLLM`'s existing behavior-level `latency_ms` with its real-sleep default delay, so the bridge's genuine wait path is exercised; no separate latency mechanism may be introduced for benchmarking.

Overhead values SHALL be recorded one activation per sample (inner loop of one), and p50/p99 SHALL be computed over the pooled per-activation samples across all pyperf worker processes — never over per-process means. The 50 ms tier SHALL be sampled densely enough to support a p99 (at least 1000 samples); the 500 ms and 2000 ms tiers MAY use fewer samples and SHALL be evaluated on medians.

#### Scenario: Overhead subtracts the configured tier latency

- **WHEN** the overhead benchmark runs a tier whose `FakeLLM` behavior is configured with `latency_ms=500`
- **THEN** each recorded overhead value is that activation's wall time minus 500 ms, and event-loop scheduling slop above the nominal sleep is charged to the runtime rather than excluded

#### Scenario: Percentiles are computed over per-activation samples

- **WHEN** the gate evaluates the 50 ms tier's results
- **THEN** p50 and p99 are computed across at least 1000 individual per-activation overhead samples pooled from all worker processes, not across per-process aggregates

#### Scenario: Overhead is invariant to provider latency

- **WHEN** the 50 ms, 500 ms, and 2000 ms tiers have all run
- **THEN** the median overhead of the 500 ms and 2000 ms tiers is within the stated tolerance of the 50 ms tier's median, and a tier whose overhead grows with provider latency fails the gate as a runtime defect (wait-scaled machinery), not a measurement artifact

### Requirement: Suspension round-trip latency is measured runtime-side

The harness SHALL measure the suspension round-trip on one key: an activation that stages a side-effect intent and returns `Suspend` (continuation committed, pending intent staged), followed by re-injection of the matching `ToolResult` and resumption to completion. The recorded value SHALL be the summed wall time of both element hops over shared state handles. The effector and message transport SHALL be excluded from the measurement, and the report MUST label the figure as runtime-only cost, not an end-to-end SLA.

#### Scenario: A suspend-resume pair is timed as one round trip

- **WHEN** the suspension benchmark runs
- **THEN** each recorded value covers exactly one Suspend-committing activation plus one successfully admitted resume to completion, with the continuation persisted and cleared through the real commit path, and no effector or broker in the loop

### Requirement: State-commit cost is measured as a function of MemoryBlob size

The harness SHALL measure activation cost with committed working-memory payloads of at least four sizes spanning 1 KiB to the 100 KiB blob cap, and SHALL additionally measure `DeterministicProtoCoder.encode` (deterministic protobuf serialization) alone over `MemoryBlob`s of the same sizes, so the size-dependent cost curve is attributable to the coder or to the surrounding staging. The report SHALL present cost against size and MUST note that runner-side state-backend write cost is outside the measurement.

#### Scenario: Commit cost is reported across blob sizes up to the cap

- **WHEN** the state-commit benchmark runs
- **THEN** results exist for each configured size including 100 KiB, for both the full-activation and the encode-only measurement, and the report presents the per-size medians side by side

### Requirement: The harness quantifies RunAgent against plain RunInference

The harness SHALL run the same `FakeLLM`-backed, zero-latency model work through two DirectRunner pipelines — `RunAgent`, and `apache_beam.ml.inference`'s `RunInference` with a minimal `ModelHandler` over the identical `FakeLLM` script — over identical inputs, and report the per-element delta and ratio: what the agent runtime (durable keyed memory, replay cache, deterministic intents, staged atomic commit) costs over raw model invocation. The absolute per-element figures from this comparison SHALL NOT be gated against the overhead budget (different measurement surface); the regression baseline SHALL track the delta.

#### Scenario: The comparison isolates the runtime's cost over raw inference

- **WHEN** the comparison benchmark runs both pipelines over the same input volume with zero-latency `FakeLLM` behaviors
- **THEN** the report states per-element cost for each pipeline plus their delta and ratio, and the delta (not the absolute figures) is what the baseline comparison evaluates

### Requirement: A regression gate enforces the latency budget and a committed baseline

The repository SHALL provide a gate script, run via `make bench-gate`, that renders two independent judgements over the pyperf results:

1. **Absolute budget:** the gated overhead tier's p50 MUST be under 15 ms and its p99 under 60 ms; a breach fails the gate unconditionally.
2. **Baseline ratchet:** each benchmark's median SHALL be compared against a committed `benchmark-baseline.toml`; a median regressing beyond the file's stated tolerance band fails the gate, and a median improving beyond the band SHALL cause the gate to print an instruction to lower the committed baseline by hand, so improvements are locked in deliberately.

The gate SHALL load results through pyperf's own API rather than hand-parsed JSON, and MUST fail loudly (not pass) when results are missing, unreadable, or contain fewer samples than the percentile computation requires. The gate SHALL NOT be marked flaky-tolerant, retried automatically, or skipped when results exist.

#### Scenario: A budget breach fails the gate

- **WHEN** the gated tier's pooled samples yield p50 of 15 ms or more, or p99 of 60 ms or more
- **THEN** `make bench-gate` exits non-zero naming the breached threshold and the measured value

#### Scenario: A regression beyond tolerance fails the gate

- **WHEN** a benchmark's median exceeds its committed baseline by more than the tolerance band
- **THEN** the gate exits non-zero naming the benchmark, the baseline, the tolerance, and the measured median

#### Scenario: An improvement prompts a deliberate baseline update

- **WHEN** a benchmark's median improves beyond the tolerance band
- **THEN** the gate passes and prints the new value to commit to `benchmark-baseline.toml`, mirroring the coverage ratchet's lock-in-the-gain instruction

#### Scenario: Missing results are a failure, not a pass

- **WHEN** the gate runs and a declared benchmark's result file is absent or holds too few samples for its required statistic
- **THEN** the gate exits non-zero rather than silently gating on the benchmarks that did run

### Requirement: The gate runs in the nightly lane and publishes a per-release report artifact

The benchmark suite and gate SHALL run as a job in the nightly workflow (on schedule and via manual dispatch), not as a required per-PR check — pyperf's methodology assumes quiet machines, and the gate is release-blocking, matching `project.md`'s "benchmark regressions on this are release blockers". The job SHALL upload the pyperf JSON results together with a generated human-readable markdown report (per-benchmark medians and percentiles, tier-invariance table, `RunInference` delta, gate verdicts, and environment metadata) as a single, stably named workflow artifact. Each release SHALL attach the most recent green benchmark report; the attach step belongs to the release process, which consumes this artifact by its stable name.

#### Scenario: Nightly produces the gated report artifact

- **WHEN** the nightly bench job completes
- **THEN** `make bench` and `make bench-gate` have both run, and one artifact containing the JSON results and the markdown report is uploaded under the documented stable name

#### Scenario: A red gate blocks the release, not the merge

- **WHEN** the nightly gate fails its budget or baseline judgement
- **THEN** the nightly workflow reports failure and the release process refuses to tag until a green run exists, while PR merges remain ungated by the benchmark lane
