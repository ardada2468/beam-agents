## Context

The latency budget in `project.md` — runtime overhead p50 < 15 ms / p99 < 60 ms per activation, excluding LLM/tool time — is a release blocker with no measuring instrument. The in-pipeline `overhead_ms` distribution publishes the right quantity but Beam distributions carry no percentiles, and `docs/metrics.md` explicitly defers the p99 check to a benchmark suite that does not exist. Meanwhile the `bench` dependency group (`pyperf`) is committed and unused, and there is no `benchmarks/` directory.

Four properties of the existing code shape the harness:

**The overhead subtraction is already defined.** `_AgentDoFn._activate` brackets the bounded bridge submission with an injected monotonic clock, and `_record_commit` publishes `max(0, activation_ms − Σ llm_ms − Σ tool_ms)` as `overhead_ms`. The benchmark must measure the same quantity by construction, or the dashboard and the gate will disagree about what "overhead" means.

**`FakeLLM` already simulates provider latency.** Behaviors carry `latency_ms` (`respond_with(payload, latency_ms=500)`), served through an injectable `delay` callable whose default is a real `asyncio.sleep`. The tiers need zero runtime changes — and the *real* sleep is the right default here, because the point of the tiered benchmark is to exercise the bridge's genuine wait path, not to fake it out.

**The DoFn element path is drivable without a runner.** `add-runtime-metrics` established the fake state/timer handle pattern (`tests/core/_dofn_fakes.py`) that drives `process()`/`_start`/`_resume`/`_commit` directly. That is the measurement surface for per-activation benchmarks: it includes everything the runtime owns (bridge submission, activation loop, replay cache, staging, coder encode at state write, commit ordering) and excludes everything it does not (runner scheduling, bundle formation, shuffle).

**pyperf's methodology needs quiet machines.** It spawns worker processes, warms up, and calibrates; its own documentation is blunt that results on busy or virtualized machines are noisy. GitHub-hosted runners are 2-core shared VMs. Any design that gates per-PR on wall-clock numbers from those machines will either cry wolf or be tuned so loose it catches nothing.

## Goals / Non-Goals

**Goals:**
- Five benchmark dimensions, each a runnable module producing pyperf JSON: no-op throughput, tiered overhead p50/p99, suspension round-trip, state-commit vs `MemoryBlob` size, and the `RunInference` comparison.
- A regression gate enforcing the absolute budget (p50 < 15 ms, p99 < 60 ms) and a baseline ratchet for everything else, with CI-noise handling that makes a red mean something.
- Fully offline: no docker, no network, `FakeLLM` only — runnable on a laptop and on any CI runner.
- A per-run report artifact (JSON + markdown) stable enough for the release process to attach.
- Benchmarks that break loudly in PR CI when the runtime surface they drive changes.

**Non-Goals:**
- Benchmarking on Dataflow or Flink. Runner-level performance is a different question with different machinery (the nightly dataflow lane already installs the bench group; extending there is an open question, not this change).
- Micro-optimizing the runtime. This change measures; any fix it motivates is its own change.
- Provider-latency realism beyond fixed tiers (no jitter distributions, no token streaming simulation).
- Gating memory footprint or allocation counts. Wall time only, this change.
- Continuous benchmark tracking infrastructure (dashboards, historical databases). The committed baseline file and release artifacts are the history.

## Decisions

### D1. Benchmarks are pyperf scripts in a top-level `benchmarks/` package, not pytest tests

Each dimension is one module with a `pyperf.Runner` entry point; `make bench` runs them all and writes one JSON file per benchmark under `bench-results/` (gitignored). They are not collected by pytest: pyperf owns `argv` and forks calibrated worker processes, which fights pytest's collection model, and a 3-minute timed run has no business inside a unit tier with a global 30 s timeout.

Two consequences are handled explicitly. First, code that pytest never imports rots, so `tests/benchmarks/test_bench_smoke.py` imports every benchmark module and executes one iteration of each timed function inside the offline unit tier — a runtime refactor that breaks a benchmark fails the required `ci` lane, not a nightly three days later. Second, `benchmarks/` joins `[tool.mypy].files` so the harness meets the same `--strict` bar as `src/` and `tests/` (with the same Beam-untyped-API relaxations the DoFn-driving test modules already carry).

The package ships its own ~60-line in-memory state/timer handles in `benchmarks/_harness.py` rather than importing `tests.core._dofn_fakes`: the test tree is not a public surface, benchmarks must not depend on its internals, and the duplication is small, deliberate, and kept honest by the smoke tests driving both.

### D2. Overhead is measured per activation on the DoFn element path, as end-to-end minus configured latency

`bench_overhead_tiers` drives `_AgentDoFn.process()` with fake handles and a single-model-call agent, `FakeLLM` scripted at the tier's `latency_ms`, and records for each activation `wall_time − tier_ms`. The subtraction mirrors `_record_commit`'s `overhead_ms` exactly, with the nominal tier standing in for the measured `llm_ms` — so the gate figure and the dashboard figure are the same quantity, and any event-loop scheduling slop above the nominal sleep is *charged to the runtime*, which is correct: the bridge's event loop is runtime code.

Percentiles are only meaningful over per-activation samples, so the timed function runs exactly one activation per value (`--loops 1`, defensible because a 50 ms floor dwarfs timer resolution) and the gate computes p50/p99 across all values from all pyperf worker processes via `pyperf.Benchmark.percentile`. Averaging first and taking percentiles of means — pyperf's default framing — would systematically hide the tail the p99 budget exists to catch.

The three tiers divide the work: the **50 ms tier is the gated one**, sampled densely (~1000 values across ~20 worker processes ≈ 1 minute of sleeping) so its p99 rests on real data. The 500 ms and 2000 ms tiers exist to prove **latency invariance** — median overhead within a stated tolerance of the 50 ms tier's — with far fewer samples, because a full-density 2000 ms tier would cost half an hour of wall clock to learn nothing new. Invariance is the actual claim the tiers test: overhead that grows with provider latency means some runtime mechanism (bridge polling, timeout machinery) scales with wait time, which is precisely the defect class end-to-end timing alone cannot see.

### D3. The gate runs nightly (plus on demand), never per-PR

pyperf on a shared 2-core VM produces medians stable enough to gate and tails noisy enough to lie; a per-PR gate would either block merges on scheduler noise or be widened until it catches nothing. The gate therefore runs in `nightly.yml` as a new `bench` job — the same lane as the unconditional mutation run, and the same release-blocking (not merge-blocking) semantics: `project.md` says benchmark regressions are *release* blockers, and the sibling `add-0-1-0-release` checklist requires a green bench run before tagging. `workflow_dispatch` covers the on-demand case ("does this PR cost latency?") without making noise a merge veto.

The alternative — a per-PR informational (non-required) bench run — was rejected: an advisory check that is usually noise trains everyone to ignore it, and then it is worse than absent. Promotion to per-PR on quieter (larger or self-hosted) runners stays an open question, to be decided on variance data the nightly runs will accumulate.

### D4. Two-layer verdict: absolute budget hard-gates overhead; a committed baseline ratchets everything else

`scripts/bench_gate.py` renders two independent judgements, following the split `mutation_gate.py` already models (hard failures vs ratcheted counts):

1. **Absolute budget.** Overhead p50 ≥ 15 ms or p99 ≥ 60 ms fails, full stop. Absolute thresholds are the noise-robust layer: expected overhead is low-single-digit milliseconds, so the budget carries roughly an order of magnitude of headroom over runner jitter, and the threshold needs no reference machine to be meaningful — it is the number `project.md` promises users.
2. **Baseline ratchet.** `benchmark-baseline.toml` (repo root, sibling to `coverage-baseline.toml` and `mutation-baseline.toml`) commits a per-benchmark median. A run whose median regresses beyond a tolerance band (25 %, stated in the file) fails; a run that improves beyond the band prints the `coverage_ratchet.py`-style instruction to lower the baseline by hand, so gains are locked in deliberately and a slow drift cannot hide inside the band forever. The band absorbs run-to-run and runner-generation variance that absolute thresholds cannot, and hand-updates keep a lucky fast run from silently tightening the gate.

Noise handling is layered rather than clever: medians (not means) everywhere a relative comparison is made; pyperf's multi-process/warmup orchestration left on; percentile gates only where sample counts support them (the 50 ms tier); the tolerance band on relative checks; and the quietest lane we have (a dedicated nightly job running nothing else). `pyperf system tune` is deliberately not used — it needs capabilities hosted runners don't grant, and a gate that requires root to be honest isn't portable.

### D5. Suspension round-trip is measured as two element hops, effector excluded

`bench_suspension_roundtrip` scripts one key through the full continuation machinery: element one activates an agent that stages a `side_effect` intent and returns `Suspend` (continuation written, `PENDING` populated, HITL timer armed); element two is an `AgentEnvelope` carrying the matching `ToolResult`, admitted and resumed to completion. The value is the summed wall time of both `process()` drains over the same fake handles — the runtime's cost of suspending and resuming, which is the price `re-injection path` users pay per side effect on top of plain activation cost.

The effector and the message bus are deliberately outside the measurement: their latency belongs to deployment (broker RTT, effector load), not to the runtime, and the docker-backed e2e gate already exercises that loop for correctness. Reporting runtime-only cost keeps the number stable and attributable; the benchmark's report labels it explicitly as excluding transport so nobody reads 2 ms as an end-to-end SLA.

### D6. State-commit cost is measured at committed sizes up to the blob cap, with a coder micro-benchmark to attribute it

`bench_state_commit` parameterizes an agent that writes a working-memory payload of S ∈ {1 KiB, 16 KiB, 64 KiB, 100 KiB} — the last being the documented blob cap — and times full activations, isolating the size-dependent component by difference from the no-op case. Alongside it, a pure micro-benchmark times `DeterministicProtoCoder.encode` (`SerializeToString(deterministic=True)`) over `MemoryBlob`s of the same sizes, because deterministic map-field ordering is the plausibly super-linear part and attributing the curve ("it's the coder" vs "it's staging copies") is the difference between a useful report and a mystery. With fake handles the runner's actual state write is a no-op, so what this dimension honestly measures is the runtime-side cost (staging, proto mutation, deterministic encode); the report says so.

### D7. The `RunInference` comparison holds everything constant except the runtime

`bench_runinference_compare` runs two DirectRunner pipelines over identical inputs: `RunAgent` with a single-call `FakeLLM` agent, and `apache_beam.ml.inference.base.RunInference` with a minimal `ModelHandler` whose `run_inference` invokes the same `FakeLLM` script (via a private event loop, since the handler API is synchronous). Both use zero-latency behaviors — provider wait time would only dilute the difference being measured. The reported quantity is the per-element delta and ratio: what keyed durable memory, the replay cache, deterministic intent derivation, and the staged atomic commit cost over raw model invocation.

This is the one benchmark that runs whole pipelines, because its question is inherently comparative and the DirectRunner's own overhead appears on both sides and largely cancels in the difference. The absolute per-element numbers from this benchmark are *not* gated and not comparable to D2's (different measurement surface); the baseline ratchet tracks the delta only.

### D8. One report artifact, produced nightly, consumed by the release process

`bench_gate.py` doubles as the report generator (single reader of the JSON, no drift between what is gated and what is reported): after gating it renders `bench-report.md` — per-benchmark medians and percentiles, tier-invariance table, the `RunInference` delta, gate verdicts, and the runner/environment metadata pyperf captures. The nightly job uploads `bench-results/*.json` + `bench-report.md` as one artifact under a stable name. Attaching the latest green report to a GitHub release is `add-0-1-0-release`'s step; this change's contract is that the artifact exists, is deterministic in shape, and is named stably.

## Risks / Trade-offs

- **Shared-runner noise still trips the ratchet occasionally** → the 25 % band is a guess until real variance data exists. Mitigation: the band is a single named constant in `benchmark-baseline.toml`; the first weeks of nightly runs calibrate it, and a false red costs a nightly re-run, never a blocked merge.
- **Absolute budget is too loose to catch small regressions** → a 3 ms → 6 ms overhead doubling passes the 15 ms gate. Mitigation: that is exactly what the baseline ratchet layer is for; the two layers fail independently.
- **The 50 ms tier's sleep floor dominates run time** (~1–2 min of deliberate sleeping for ~1000 samples) → accepted; it is the price of a real p99, bounded by running dense sampling on one tier only (D2). Total suite budget ≤ 10 minutes in the nightly job.
- **Fake-handle measurements understate production cost** (no real state backend write, no runner overhead) → deliberate and documented: the budget in `project.md` is about the runtime's own code, and runner/state-backend costs vary per deployment. The `RunInference` comparison and the (non-gated) pipeline benchmarks give the cross-check; extending to real runners is an open question.
- **Benchmark drift from the runtime surface** → benchmarks drive private surfaces (`_AgentDoFn`, fake handles), which refactors will break. Mitigation: the unit-tier smoke tests make that a PR-time failure; the harness is small on purpose.
- **pyperf JSON schema or API movement** → `bench_gate.py` imports pyperf's own `Benchmark` loader rather than parsing JSON by hand — the same import-the-authority stance `mutation_gate.py` takes with mutmut's status table, with the same loud-ImportError failure mode.
- **A hand-updated baseline goes stale in the other direction** (nobody lowers it after real improvements) → the gate prints the ratchet instruction on every improving run, mirroring `coverage_ratchet.py`; review convention treats an ignored instruction like an ignored coverage gain.

## Migration Plan

Purely additive: no wire, state, or `src/` change; no effect on pipeline `--update`, golden blobs, or any existing gate. Landing order inside the change: harness + benchmarks first, then one manual full run on a GitHub-hosted runner to seed `benchmark-baseline.toml` with measured medians (committed in the same PR, clearly labeled with the runner generation), then the gate and nightly wiring. Rollback is deleting the `bench` job and targets; nothing else references them. If the seeded baseline proves mis-calibrated for CI hardware, the recovery is a baseline update commit, not a code change.

## Open Questions

- Should the bench job be promoted to per-PR (required or informational) once nightly variance data shows medians are stable on hosted runners — or pinned to a larger/self-hosted runner class first?
- Should the dataflow nightly lane (which already installs the bench group) gain a runner-level benchmark leg measuring overhead on real Dataflow, giving the budget a production-shaped cross-check?
- Should the harness also track allocation counts or peak RSS per activation (`tracemalloc`), so memory regressions get the same ratchet treatment as latency?
- Should `bench_gate.py` compare against the previous nightly's artifact (trend detection) in addition to the committed baseline, so a slow drift inside the tolerance band is surfaced before it accumulates?
