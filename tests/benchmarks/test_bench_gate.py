"""The bench gate's two judgements, driven over synthetic pyperf result files.

Mirrors ``tests/core/test_mutation_gate.py``: every test writes real pyperf
JSON (via pyperf's own ``BenchmarkSuite.dump``, the import-the-authority
stance) plus a ``benchmark-baseline.toml`` into a tmp cwd and runs
``bench_gate.main()`` for a verdict. No benchmark actually runs here — these
are the pure-judgement tests for the release gate.
"""

from __future__ import annotations

from collections.abc import Collection
from pathlib import Path

import pyperf
import pytest
from scripts import bench_gate

# Flat per-benchmark default values in seconds, comfortably inside both the
# absolute budget and the default baselines below.
_DEFAULT_S = {
    "noop_throughput": 0.0005,
    "overhead_50ms": 0.004,
    "overhead_500ms": 0.004,
    "overhead_2000ms": 0.004,
    "suspension_roundtrip": 0.002,
    "state_commit_1kib": 0.001,
    "state_commit_16kib": 0.001,
    "state_commit_64kib": 0.001,
    "state_commit_100kib": 0.001,
    "encode_1kib": 0.0001,
    "encode_16kib": 0.0001,
    "encode_64kib": 0.0001,
    "encode_100kib": 0.0001,
    "runagent_per_element": 0.010,
    "runinference_per_element": 0.006,
}

# Baseline medians (ms) matching the defaults exactly, so the green path is
# "at baseline" everywhere. The RunInference absolutes are not tracked; their
# delta (10ms - 6ms) is.
_DEFAULT_BASELINE_MS = {
    "noop_throughput": 0.5,
    "overhead_50ms": 4.0,
    "overhead_500ms": 4.0,
    "overhead_2000ms": 4.0,
    "suspension_roundtrip": 2.0,
    "state_commit_1kib": 1.0,
    "state_commit_16kib": 1.0,
    "state_commit_64kib": 1.0,
    "state_commit_100kib": 1.0,
    "encode_1kib": 0.1,
    "encode_16kib": 0.1,
    "encode_64kib": 0.1,
    "encode_100kib": 0.1,
    "runinference_delta": 4.0,
}


def _bench(name: str, runs: list[list[float]]) -> pyperf.Benchmark:
    return pyperf.Benchmark(
        [pyperf.Run(values, metadata={"name": name}, collect_metadata=False) for values in runs]
    )


def _default_runs(name: str) -> list[list[float]]:
    value = _DEFAULT_S[name]
    # The gated tier needs enough pooled samples for a p99; two worker
    # processes' worth proves pooling is across processes.
    n = 600 if name == bench_gate.GATED_BENCHMARK else 10
    return [[value] * n, [value] * n]


def write_results(
    root: Path,
    *,
    values: dict[str, list[float]] | None = None,
    runs: dict[str, list[list[float]]] | None = None,
    skip_files: Collection[str] = (),
    skip_benchmarks: Collection[str] = (),
) -> None:
    results_dir = root / "bench-results"
    results_dir.mkdir(exist_ok=True)
    for filename, names in bench_gate.EXPECTED_RESULTS.items():
        if filename in skip_files:
            continue
        benchmarks = []
        for name in names:
            if name in skip_benchmarks:
                continue
            if runs is not None and name in runs:
                bench_runs = runs[name]
            elif values is not None and name in values:
                flat = values[name]
                half = len(flat) // 2
                bench_runs = [flat[:half], flat[half:]]
            else:
                bench_runs = _default_runs(name)
            benchmarks.append(_bench(name, bench_runs))
        pyperf.BenchmarkSuite(benchmarks).dump(str(results_dir / filename))


def write_baseline(
    root: Path,
    *,
    medians_ms: dict[str, float] | None = None,
    tolerance: float = 0.25,
    invariance_tolerance_ms: float = 10.0,
) -> None:
    medians = _DEFAULT_BASELINE_MS if medians_ms is None else medians_ms
    lines = [
        f"tolerance = {tolerance}",
        f"invariance_tolerance_ms = {invariance_tolerance_ms}",
        "",
        "[medians_ms]",
        *(f'"{name}" = {value}' for name, value in medians.items()),
    ]
    (root / "benchmark-baseline.toml").write_text("\n".join(lines), encoding="utf-8")


def _baseline_with(**overrides: float) -> dict[str, float]:
    return {**_DEFAULT_BASELINE_MS, **overrides}


@pytest.fixture
def gate_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    return tmp_path


# --- Scenario: A budget breach fails the gate ---------------------------------


def test_a_p50_budget_breach_fails_the_gate(
    gate_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # 20 ms of overhead at every sample: p50 is 20 ms, over the 15 ms budget.
    # The baseline is set to match so only the absolute budget can fail.
    write_results(gate_root, values={"overhead_50ms": [0.020] * 1200})
    write_baseline(gate_root, medians_ms=_baseline_with(overhead_50ms=20.0))

    assert bench_gate.main() == 1
    err = capsys.readouterr().err
    assert "overhead_50ms" in err
    assert "p50" in err
    assert "20.0000" in err
    assert "15" in err


def test_a_p99_budget_breach_fails_the_gate(
    gate_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # A 2% tail at 70 ms: the median stays comfortably under budget, the p99
    # does not. This is the tail the per-activation sampling rule exists for.
    write_results(gate_root, values={"overhead_50ms": [0.005] * 1176 + [0.070] * 24})
    write_baseline(gate_root, medians_ms=_baseline_with(overhead_50ms=5.0))

    assert bench_gate.main() == 1
    err = capsys.readouterr().err
    assert "p99" in err
    assert "60" in err


# --- Scenario: Percentiles are computed over per-activation samples -----------


def test_percentiles_are_computed_over_pooled_per_activation_samples(
    gate_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # The slow tail lives entirely inside one worker process. Any per-process
    # aggregation (means of 5 ms and ~7.6 ms) stays far under the 60 ms budget;
    # only pooling the raw per-activation values exposes the 70 ms p99.
    run_a = [0.005] * 600
    run_b = [0.005] * 576 + [0.070] * 24
    write_results(gate_root, runs={"overhead_50ms": [run_a, run_b]})
    write_baseline(gate_root, medians_ms=_baseline_with(overhead_50ms=5.0))

    assert bench_gate.main() == 1
    err = capsys.readouterr().err
    assert "p99" in err

    bench = _bench("overhead_50ms", [run_a, run_b])
    assert len(bench_gate.pooled_values_ms(bench)) == 1200


# --- Scenario: A regression beyond tolerance fails the gate -------------------


def test_a_regression_beyond_tolerance_fails_the_gate(
    gate_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # 2.0 ms against a 0.5 ms baseline with a 25% band: a 4x regression.
    write_results(gate_root, values={"noop_throughput": [0.002] * 20})
    write_baseline(gate_root)

    assert bench_gate.main() == 1
    err = capsys.readouterr().err
    assert "noop_throughput" in err
    assert "regressed" in err
    assert "0.5000" in err  # the committed baseline
    assert "2.0000" in err  # the measured median
    assert "25%" in err  # the tolerance band


# --- Scenario: An improvement prompts a deliberate baseline update ------------


def test_an_improvement_prompts_a_deliberate_baseline_update(
    gate_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    write_results(gate_root, values={"noop_throughput": [0.0002] * 20})
    write_baseline(gate_root)

    assert bench_gate.main() == 0
    out = capsys.readouterr().out
    assert "lower" in out
    assert "0.2000" in out
    assert "benchmark-baseline.toml" in out


# --- Scenario: Missing results are a failure, not a pass ----------------------


def test_an_absent_result_file_fails_the_gate(
    gate_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    write_results(gate_root, skip_files={"bench_noop_throughput.json"})
    write_baseline(gate_root)

    assert bench_gate.main() == 1
    assert "bench_noop_throughput.json" in capsys.readouterr().err


def test_a_result_file_missing_a_declared_benchmark_fails_the_gate(
    gate_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    write_results(gate_root, skip_benchmarks={"overhead_500ms"})
    write_baseline(gate_root)

    assert bench_gate.main() == 1
    assert "overhead_500ms" in capsys.readouterr().err


def test_too_few_samples_for_the_p99_fail_the_gate(
    gate_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    write_results(gate_root, values={"overhead_50ms": [0.004] * 200})
    write_baseline(gate_root)

    assert bench_gate.main() == 1
    err = capsys.readouterr().err
    assert "samples" in err
    assert "1000" in err


def test_a_missing_baseline_file_fails_the_gate(
    gate_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    write_results(gate_root)

    assert bench_gate.main() == 1
    assert "benchmark-baseline.toml" in capsys.readouterr().err


def test_an_unseeded_baseline_entry_fails_the_gate_with_a_seed_instruction(
    gate_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # A benchmark with no committed median is a gate that has silently stopped
    # gating; it fails loudly with the value to commit.
    medians = dict(_DEFAULT_BASELINE_MS)
    del medians["noop_throughput"]
    write_results(gate_root)
    write_baseline(gate_root, medians_ms=medians)

    assert bench_gate.main() == 1
    err = capsys.readouterr().err
    assert "no committed baseline" in err
    assert "noop_throughput" in err
    assert "0.5000" in err  # the measured median to seed


# --- Scenario: Overhead is invariant to provider latency ----------------------


def test_overhead_growing_with_provider_latency_fails_the_gate(
    gate_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # 16 ms of median overhead at the 2000 ms tier against 4 ms at the 50 ms
    # tier: past the 10 ms invariance tolerance, so some runtime mechanism is
    # scaling with wait time. The tier's own baseline matches so only the
    # invariance judgement can fail.
    write_results(gate_root, values={"overhead_2000ms": [0.016] * 20})
    write_baseline(gate_root, medians_ms=_baseline_with(overhead_2000ms=16.0))

    assert bench_gate.main() == 1
    err = capsys.readouterr().err
    assert "latency-invariant" in err
    assert "2000" in err


def test_overhead_within_the_invariance_tolerance_passes(
    gate_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    write_results(gate_root, values={"overhead_2000ms": [0.012] * 20})
    write_baseline(gate_root, medians_ms=_baseline_with(overhead_2000ms=12.0))

    assert bench_gate.main() == 0
    assert "bench gate passed" in capsys.readouterr().out


# --- The green path and the report --------------------------------------------


def test_a_green_run_passes_and_renders_the_report(
    gate_root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    write_results(gate_root)
    write_baseline(gate_root)

    assert bench_gate.main() == 0
    assert "bench gate passed" in capsys.readouterr().out

    report = (gate_root / "bench-report.md").read_text(encoding="utf-8")
    # Per-benchmark medians, the tier-invariance table, the RunInference
    # delta/ratio, and the verdicts all render from the one JSON reader.
    assert "noop_throughput" in report
    assert "overhead_2000ms" in report
    assert "delta" in report
    assert "ratio" in report
    assert "p99" in report
    # The two labels that keep the numbers honest: the round trip excludes the
    # effector/transport, and state-commit excludes the runner's backend write.
    assert "effector" in report
    assert "state-backend" in report


def test_a_red_run_still_renders_the_report_with_the_failing_verdict(
    gate_root: Path,
) -> None:
    write_results(gate_root, values={"overhead_50ms": [0.020] * 1200})
    write_baseline(gate_root, medians_ms=_baseline_with(overhead_50ms=20.0))

    assert bench_gate.main() == 1
    report = (gate_root / "bench-report.md").read_text(encoding="utf-8")
    assert "FAIL" in report
