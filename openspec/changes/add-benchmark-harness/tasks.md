## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 `tests/benchmarks/test_bench_gate.py`: the gate's two judgements over synthetic pyperf result files — a p50 ≥ 15 ms or p99 ≥ 60 ms overhead result exits non-zero naming the breached threshold and measured value ("A budget breach fails the gate"); a median beyond the baseline tolerance exits non-zero naming benchmark/baseline/tolerance/median ("A regression beyond tolerance fails the gate"); an improving median passes and prints the lower-the-baseline instruction ("An improvement prompts a deliberate baseline update"); an absent result file or one with too few samples for p99 exits non-zero ("Missing results are a failure, not a pass"); percentiles are computed over pooled per-activation values, not per-process aggregates ("Percentiles are computed over per-activation samples").
- [ ] 1.2 `tests/benchmarks/test_bench_gate.py`: the tier-invariance judgement — a synthetic result set whose 2000 ms tier median overhead exceeds the 50 ms tier's by more than the tolerance fails, and one within tolerance passes ("Overhead is invariant to provider latency").
- [ ] 1.3 `tests/benchmarks/test_bench_smoke.py`: import every `benchmarks/bench_*.py` module and execute each timed function for one iteration offline with no docker ("Benchmark modules cannot rot silently", "The full suite runs offline") — including one suspend-resume pair asserting the continuation was written then cleared through the real commit path ("A suspend-resume pair is timed as one round trip"), and the state-commit setup producing blobs at every configured size including 100 KiB ("Commit cost is reported across blob sizes up to the cap").
- [ ] 1.4 `tests/benchmarks/test_harness.py`: the overhead arithmetic — with a scripted clock, the recorded value equals wall time minus the configured tier latency, and scheduling slop above the nominal sleep stays in the value ("Overhead subtracts the configured tier latency"); the `RunInference` comparison's report computes delta and ratio from the two per-element figures and marks the delta as the baseline-tracked quantity ("The comparison isolates the runtime's cost over raw inference").
- [ ] 1.5 Confirm all of the above fail for the right reason before implementation: missing modules/`bench_gate.py`, not assertion typos.

## 2. Harness skeleton

- [ ] 2.1 Create `benchmarks/__init__.py` and `benchmarks/_harness.py`: the no-op, single-call, suspending, and memory-writing module-level agents; `FakeLLM` scripts per tier (`respond_with(payload, latency_ms=tier)` with the default real-sleep delay); in-memory state/timer handles (deliberately duplicated from `tests/core/_dofn_fakes.py` — benchmarks must not import the test tree; design D1); tier and size constants; and the `_AgentDoFn` construction helper (fake handles, `FakeLLM` provider factory).
- [ ] 2.2 Add `benchmarks` to `[tool.mypy].files` in `pyproject.toml` with the same Beam-untyped-API relaxations the DoFn-driving test modules carry; add `bench-results/` to `.gitignore`.

## 3. The five benchmarks

- [ ] 3.1 `benchmarks/bench_noop_throughput.py`: one full `process()` drain per value with the no-op agent; report time per activation (activations/sec derived in the report).
- [ ] 3.2 `benchmarks/bench_overhead_tiers.py`: one activation per value (`--loops 1`), value = wall time minus nominal tier latency; dense sampling (≥ 1000 values across ~20 worker processes) on the 50 ms tier, reduced sampling on 500/2000 ms; one pyperf benchmark per tier in one JSON file.
- [ ] 3.3 `benchmarks/bench_suspension_roundtrip.py`: per value, one Suspend-committing activation plus one admitted `ToolResult` resume over shared handles; effector and transport excluded.
- [ ] 3.4 `benchmarks/bench_state_commit.py`: full activations committing working memory at 1/16/64/100 KiB, plus the encode-only micro-benchmark over `DeterministicProtoCoder.encode` at the same sizes.
- [ ] 3.5 `benchmarks/bench_runinference_compare.py`: the two DirectRunner pipelines (`RunAgent` vs `RunInference` with the minimal `FakeLLM` `ModelHandler`), identical inputs, zero-latency behaviors; report per-element cost, delta, and ratio.

## 4. Gate and baseline

- [ ] 4.1 `scripts/bench_gate.py`: load results via pyperf's `Benchmark` API (import-the-authority, loud ImportError — the `mutation_gate.py` stance); judgement 1: absolute overhead budget p50 < 15 ms / p99 < 60 ms on the 50 ms tier's pooled values; judgement 2: per-benchmark median vs `benchmark-baseline.toml` with the file's tolerance band, including the tier-invariance check and the `RunInference` delta; fail on missing/short results; print the ratchet instruction on improvement.
- [ ] 4.2 `benchmark-baseline.toml` at the repo root (sibling to `coverage-baseline.toml`/`mutation-baseline.toml`): per-benchmark median baselines seeded from a manual full run on a GitHub-hosted runner (runner generation noted in the file's comment), plus the named tolerance constant.
- [ ] 4.3 Report generation in `bench_gate.py` (single reader of the JSON): render `bench-report.md` with per-benchmark medians/percentiles, the tier-invariance table, the `RunInference` delta, gate verdicts, and pyperf's environment metadata.

## 5. CI, Makefile, docs

- [ ] 5.1 `Makefile`: `bench` (run all benchmark modules, JSON into `bench-results/`) and `bench-gate` (`uv run python scripts/bench_gate.py`) targets with `## ` help lines.
- [ ] 5.2 `.github/workflows/nightly.yml`: a `bench` job (schedule + `workflow_dispatch`) that syncs the `test` and `bench` groups, runs `make bench` then `make bench-gate`, and uploads `bench-results/*.json` + `bench-report.md` as one stably named artifact; budget ≤ 15 `timeout-minutes`.
- [ ] 5.3 `docs/benchmarks.md`: what each dimension measures and deliberately excludes (fake handles vs runners, effector out of the round-trip), how to run locally, how to read the report, and the baseline-update procedure; add the bench row to `docs/ci.md`'s workflow map and point `docs/metrics.md`'s "belongs to the benchmark suite" note at the suite.
- [ ] 5.4 Note the artifact's stable name where the sibling `add-0-1-0-release` change consumes it (release attaches the most recent green report).
- [ ] 5.5 Verify the gate carries no `xfail`/retry/skip-when-red tolerance, and that CI pins sampling parameters (env knobs are for local iteration only) — the discipline the e2e gate established.

## 6. Gates

- [ ] 6.1 `make lint` and `make type` clean (`benchmarks/` now inside the mypy file set).
- [ ] 6.2 `make test-unit` passes offline, smoke tests included, within the unit tier's timeout.
- [ ] 6.3 `make coverage-ratchet` at or above baseline (`benchmarks/` and `scripts/` sit outside the coverage source; re-measure and lock in any movement per the ratchet procedure).
- [ ] 6.4 One full `make bench && make bench-gate` run green locally and one on a hosted runner via `workflow_dispatch`, with the seeded baseline committed.
- [ ] 6.5 `uv run pre-commit run --all-files` clean.
- [ ] 6.6 `openspec validate add-benchmark-harness --strict` passes.
