## Why

[`project.md:111`](../../project.md) states a release-blocking constraint: "runtime overhead p50 < 15 ms / p99 < 60 ms per activation (excluding LLM/tool time); benchmark regressions on this are release blockers." Nothing in the repository can currently render a verdict on it. `add-runtime-metrics` shipped the in-pipeline instrument — the `overhead_ms` distribution ([metrics.py:69](../../../src/beam_agents/observability/metrics.py:69)) — but Beam distributions report only sum/count/min/max, and [`docs/metrics.md:63`](../../../docs/metrics.md:63) explicitly punts: "the p99 check itself still belongs to the benchmark suite." That suite does not exist. The `bench` dependency group with `pyperf` has been sitting in [`pyproject.toml:98`](../../../pyproject.toml:98) unused since the packaging change, there is no `benchmarks/` directory, and the nightly dataflow job even installs the group already ([nightly.yml:57](../../../.github/workflows/nightly.yml:57)) with nothing to run.

The gap is wider than the one gated number. Nobody can answer, with data: what is the runtime's ceiling with no agent work at all; what does a suspension round-trip cost; how does state-commit cost grow with `MemoryBlob` size as it approaches the 100 KiB blob cap; and what does the agent runtime cost *over* a plain `RunInference` model invocation — the number a prospective user comparing "Beam ML inference" to "Beam agents" actually wants. Every one of these is answerable offline with machinery that already exists: `FakeLLM` already supports scripted per-behavior latency with a real `asyncio.sleep` default ([fake.py:69](../../../src/beam_agents/model/fake.py:69), [fake.py:124](../../../src/beam_agents/model/fake.py:124)), so a provider that "takes 500 ms" needs no new runtime code at all.

## What Changes

- **A new top-level `benchmarks/` package** of pyperf-driven, offline (no docker, no network, `FakeLLM` only) benchmark modules, one per dimension, runnable end to end via a new `make bench`:

  | Benchmark | Measures |
  |---|---|
  | `bench_noop_throughput` | Activations/sec through the `_AgentDoFn` element path with a no-op agent — the runtime's ceiling with zero agent work |
  | `bench_overhead_tiers` | Per-activation runtime overhead (end-to-end wall time minus configured provider latency) with `FakeLLM` at latency tiers **50 ms / 500 ms / 2000 ms**; p50/p99 computed over per-activation samples |
  | `bench_suspension_roundtrip` | The suspension round-trip: `Suspend` → continuation committed → `ToolResult` re-injection → resume to completion, both element hops timed, effector excluded |
  | `bench_state_commit` | State-commit cost as a function of `MemoryBlob` size (1/16/64/100 KiB — the blob cap), plus a pure [`DeterministicProtoCoder.encode`](../../../src/beam_agents/core/coders.py:72) micro-benchmark to attribute the curve |
  | `bench_runinference_compare` | `RunAgent` vs plain `apache_beam.ml.inference` `RunInference` over the same `FakeLLM`-backed work on the DirectRunner — what durable memory, replay cache, and staged commit cost over raw model invocation |

- **A regression gate**, `scripts/bench_gate.py`, in the ratchet style of [`coverage_ratchet.py`](../../../scripts/coverage_ratchet.py) and [`mutation_gate.py`](../../../scripts/mutation_gate.py): it loads the pyperf JSON results and fails when the overhead benchmark's **p50 ≥ 15 ms or p99 ≥ 60 ms** (the absolute budget from `project.md`), or when any benchmark's median regresses beyond a tolerance band against a committed `benchmark-baseline.toml` (updated by hand when a run improves, so gains are locked in deliberately — the same mechanism as `coverage-baseline.toml`). Run via a new `make bench-gate`.

- **CI wiring in the nightly lane, not per-PR.** `nightly.yml` gains a `bench` job (schedule + `workflow_dispatch`) that runs `make bench` and `make bench-gate` and uploads the JSON results plus a generated markdown report as a workflow artifact. pyperf's methodology assumes a quiet machine; per-PR shared runners would produce false reds (design D3 covers the noise handling in detail).

- **Published per release.** The nightly artifact (pyperf JSON + markdown summary) is the benchmark report the release process attaches to each GitHub release — the attach step itself belongs to the sibling `add-0-1-0-release` change; this change produces the artifact it consumes and names it stably.

- **Unit-tier smoke coverage so benchmarks cannot rot silently.** `tests/benchmarks/` runs every benchmark module for one iteration inside the offline unit tier and unit-tests `bench_gate.py`'s percentile extraction, budget enforcement, and baseline comparison. A runtime refactor that breaks a benchmark then fails PR CI immediately instead of failing nightly three days later.

- **No changes under `src/beam_agents/`.** `FakeLLM`'s latency injection ([fake.py:141](../../../src/beam_agents/model/fake.py:141)) already covers the tier requirement; the benchmarks drive [`_AgentDoFn`](../../../src/beam_agents/core/dofn.py:196) and [`run_activation`](../../../src/beam_agents/core/loop.py:149) exactly as shipped.

## Capabilities

### New Capabilities

- `benchmark-harness`: the offline benchmark suite — the five measured dimensions and their methodology, the per-activation sampling rules that make p50/p99 meaningful, the absolute-budget and baseline-ratchet regression gates, the nightly CI wiring, and the per-release report artifact.

### Modified Capabilities

None. The runtime, model, and observability contracts are consumed exactly as specified — the harness is a pure measurer of existing behavior. The one number it gates (`project.md`'s latency budget) was always stated as release-blocking; this change adds the enforcement mechanism, not a new obligation.

## Impact

- **Depends on** `add-effectively-once-e2e-gate` (C16): that change established the release-gating discipline this harness follows — a gate that is never `xfail`/`skipif`/flaky-tolerant, with volume/iteration knobs tunable by env var locally but pinned in CI — and the two checks together form the release-blocking set the sibling `add-0-1-0-release` change requires green before tagging. Also builds directly on `add-runtime-metrics`: the harness's overhead definition (wall time minus provider/tool time) is deliberately the same subtraction as [`overhead_ms`](../../../src/beam_agents/observability/metrics.py:69), so the dashboard figure and the gated benchmark figure are the same quantity measured two ways.
- **New code:** `benchmarks/__init__.py`, `benchmarks/_harness.py` (shared no-op/suspending/scripted agents, in-memory state/timer handles, tier constants), the five `benchmarks/bench_*.py` modules, `scripts/bench_gate.py`, `benchmark-baseline.toml`, `tests/benchmarks/` (gate unit tests + one-iteration smoke tests), `docs/benchmarks.md`.
- **Modified code:** [`Makefile`](../../../Makefile) (`bench`, `bench-gate` targets), [`.github/workflows/nightly.yml`](../../../.github/workflows/nightly.yml) (the `bench` job + artifact upload), `pyproject.toml` (add `benchmarks` to `[tool.mypy].files` with the same Beam relaxations the DoFn-driving test modules get), `docs/ci.md` (workflow map row), `docs/metrics.md` (point the "belongs to the benchmark suite" note at the suite that now exists). **Nothing under `src/beam_agents/` moves.**
- **CI/build:** the nightly `bench` job is release-blocking in the same sense as the mutation job — a red nightly bench blocks tagging a release, not merging a PR. `workflow_dispatch` allows an on-demand run when a PR is suspected of costing latency. The unit-tier smoke tests ride the existing required `ci` lane with no new workflow.
- **Gates:** `make bench-gate` is the new gate (absolute budget + baseline ratchet, nightly). Coverage ratchet is unaffected in principle — `benchmarks/` and `scripts/` are outside the coverage source (`source = ["beam_agents"]`) — but is re-measured and locked in per the ratchet procedure. No mutation-gate movement: nothing in `core/` changes.
