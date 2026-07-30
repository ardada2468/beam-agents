#!/usr/bin/env python3
"""Gate the benchmark suite: absolute latency budget + committed-baseline ratchet.

Two independent judgements over the pyperf JSON in ``bench-results/``
(following the hard-failures-vs-ratchet split of ``mutation_gate.py`` and the
lock-in-the-gain instruction of ``coverage_ratchet.py``):

1. **Absolute budget.** The gated overhead tier's (50 ms FakeLLM latency)
   pooled per-activation p50 must be under 15 ms and its p99 under 60 ms —
   the release-blocking budget from ``openspec/project.md``. Percentiles are
   computed over the pooled per-activation values from every worker process,
   never over per-process aggregates.
2. **Baseline ratchet.** Every benchmark's median is compared against
   ``benchmark-baseline.toml``: a median regressing beyond the file's
   tolerance band fails; one improving beyond it prints the instruction to
   lower the committed baseline by hand. Includes the tier-invariance check
   (overhead must not grow with provider latency) and the RunInference delta
   (the absolutes are a different measurement surface and are not tracked).

Missing result files, missing declared benchmarks, unreadable results, too
few samples for the p99, or an unseeded baseline entry all fail loudly: a
gate that passes on a missing run has silently stopped gating.

Doubles as the report generator (single reader of the JSON, so what is gated
and what is reported cannot drift): renders ``bench-report.md``, which the
nightly workflow uploads with the JSON as the ``benchmark-report`` artifact.
"""

from __future__ import annotations

import math
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

try:
    # Import-the-authority (the `mutation_gate.py` stance): pyperf's own
    # Benchmark loader and percentile math rather than hand-parsed JSON. A
    # loud failure if pyperf moves is the right failure mode for a gate.
    import pyperf
except ImportError as exc:  # pragma: no cover - environment error, not logic
    # Deferred to `main()` rather than raised here: the required `ci` unit lane
    # syncs lint/typecheck/test but NOT the `bench` group, and
    # tests/benchmarks/test_bench_smoke.py imports this module for
    # EXPECTED_RESULTS alone (no pyperf) to keep the benchmark-rot check in that
    # lane. Failing at import would abort collection instead.
    _PYPERF_IMPORT_ERROR: ImportError | None = exc
else:
    _PYPERF_IMPORT_ERROR = None

RESULTS_DIR = Path("bench-results")
BASELINE_PATH = Path("benchmark-baseline.toml")
REPORT_PATH = Path("bench-report.md")

# Every result file `make bench` writes, and the benchmarks each must hold.
# A file or name missing from a run fails the gate.
EXPECTED_RESULTS: dict[str, tuple[str, ...]] = {
    "bench_noop_throughput.json": ("noop_throughput",),
    "bench_overhead_tiers.json": ("overhead_50ms", "overhead_500ms", "overhead_2000ms"),
    "bench_suspension_roundtrip.json": ("suspension_roundtrip",),
    "bench_state_commit.json": (
        "state_commit_1kib",
        "state_commit_16kib",
        "state_commit_64kib",
        "state_commit_100kib",
        "encode_1kib",
        "encode_16kib",
        "encode_64kib",
        "encode_100kib",
    ),
    "bench_runinference_compare.json": ("runagent_per_element", "runinference_per_element"),
}

# The absolute budget (openspec/project.md): runtime overhead per activation,
# excluding LLM/tool time, gated on the densely sampled 50 ms tier.
GATED_BENCHMARK = "overhead_50ms"
P50_BUDGET_MS = 15.0
P99_BUDGET_MS = 60.0
MIN_GATED_SAMPLES = 1000

INVARIANCE_TIERS = ("overhead_500ms", "overhead_2000ms")

# The delta is what the ratchet tracks for the RunInference comparison; the
# absolute per-element figures ride whole DirectRunner pipelines (a different
# measurement surface) and are neither budget-gated nor baseline-tracked.
DELTA_NAME = "runinference_delta"
_UNTRACKED = {"runagent_per_element", "runinference_per_element"}
BASELINE_TRACKED: tuple[str, ...] = (
    *(name for names in EXPECTED_RESULTS.values() for name in names if name not in _UNTRACKED),
    DELTA_NAME,
)


class GateError(Exception):
    """A condition that fails the gate before any judgement is rendered."""


@dataclass(frozen=True)
class Baseline:
    """The committed baseline: per-benchmark medians and the tolerance bands."""

    tolerance: float
    invariance_tolerance_ms: float
    medians_ms: dict[str, float]


@dataclass(frozen=True)
class Comparison:
    """The RunInference comparison summary: what the runtime costs over raw
    inference. ``tracked`` names the one quantity the baseline ratchet
    evaluates.
    """

    runagent_ms: float
    runinference_ms: float
    delta_ms: float
    ratio: float
    tracked: str = DELTA_NAME


def comparison_summary(runagent_ms: float, runinference_ms: float) -> Comparison:
    return Comparison(
        runagent_ms=runagent_ms,
        runinference_ms=runinference_ms,
        delta_ms=runagent_ms - runinference_ms,
        ratio=runagent_ms / runinference_ms if runinference_ms else float("inf"),
    )


def pooled_values_ms(bench: pyperf.Benchmark) -> list[float]:
    """Every per-activation value from every worker process, in milliseconds.

    ``Benchmark.get_values()`` pools the raw values across all runs (worker
    processes), warmups excluded — the pooling the p99 budget requires.
    """
    return [value * 1000.0 for value in bench.get_values()]


def load_results() -> dict[str, pyperf.Benchmark]:
    results: dict[str, pyperf.Benchmark] = {}
    for filename, names in EXPECTED_RESULTS.items():
        path = RESULTS_DIR / filename
        if not path.exists():
            raise GateError(
                f"{path} not found -- run `make bench` first; "
                "the gate never passes on a missing run"
            )
        try:
            suite = pyperf.BenchmarkSuite.load(str(path))
        except Exception as exc:
            raise GateError(f"cannot load {path}: {exc}") from exc
        in_file = {bench.get_name(): bench for bench in suite}
        for name in names:
            if name not in in_file:
                raise GateError(f"{path} is missing benchmark {name!r}")
            if in_file[name].get_nvalue() < 1:
                raise GateError(f"{path} benchmark {name!r} holds no values")
            results[name] = in_file[name]
    return results


def load_baseline() -> Baseline:
    if not BASELINE_PATH.exists():
        raise GateError(
            f"{BASELINE_PATH} not found -- it records the per-benchmark medians "
            "to beat and must be committed."
        )
    try:
        data = tomllib.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise GateError(f"cannot read {BASELINE_PATH}: {exc}") from exc

    def _number(key: str) -> float:
        value = data.get(key)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise GateError(f"{BASELINE_PATH} must set a numeric {key}")
        return float(value)

    medians = data.get("medians_ms", {})
    if not isinstance(medians, dict):
        raise GateError(f"{BASELINE_PATH} [medians_ms] must be a table")
    invalid = sorted(
        str(name)
        for name, value in medians.items()
        if not isinstance(value, (int, float)) or isinstance(value, bool)
    )
    if invalid:
        raise GateError(f"{BASELINE_PATH} [medians_ms] entries must be numeric: {invalid}")
    return Baseline(
        tolerance=_number("tolerance"),
        invariance_tolerance_ms=_number("invariance_tolerance_ms"),
        medians_ms={name: float(value) for name, value in medians.items()},
    )


def budget_verdict(bench: pyperf.Benchmark) -> tuple[list[str], float, float]:
    """Judgement 1: the absolute overhead budget over pooled samples.

    Returns ``(failures, p50_ms, p99_ms)``; the percentiles come from
    pyperf's own math over the pooled per-activation values.
    """
    n = bench.get_nvalue()
    if n < MIN_GATED_SAMPLES:
        return (
            [
                f"{GATED_BENCHMARK} has {n} pooled samples; the p99 budget needs "
                f"at least {MIN_GATED_SAMPLES}"
            ],
            float("nan"),
            float("nan"),
        )
    p50 = bench.percentile(50) * 1000.0
    p99 = bench.percentile(99) * 1000.0
    failures = []
    if p50 >= P50_BUDGET_MS:
        failures.append(
            f"{GATED_BENCHMARK} p50 {p50:.4f} ms breaches the < {P50_BUDGET_MS:g} ms budget"
        )
    if p99 >= P99_BUDGET_MS:
        failures.append(
            f"{GATED_BENCHMARK} p99 {p99:.4f} ms breaches the < {P99_BUDGET_MS:g} ms budget"
        )
    return failures, p50, p99


def ratchet_verdict(name: str, median_ms: float, baseline: Baseline) -> tuple[list[str], list[str]]:
    """Judgement 2 for one benchmark: ``(failures, informational notes)``."""
    if name not in baseline.medians_ms:
        return (
            [
                f"no committed baseline for {name}; seed medians_ms.{name} = "
                f"{median_ms:.4f} in {BASELINE_PATH} from a quiet-hardware (CI) run"
            ],
            [],
        )
    base = baseline.medians_ms[name]
    band = abs(base) * baseline.tolerance
    if median_ms > base + band:
        return (
            [
                f"{name} median {median_ms:.4f} ms regressed beyond its baseline "
                f"{base:.4f} ms + {baseline.tolerance:.0%} tolerance"
            ],
            [],
        )
    if median_ms < base - band:
        return (
            [],
            [
                f"{name} median {median_ms:.4f} ms improved beyond its baseline "
                f"{base:.4f} ms - {baseline.tolerance:.0%} tolerance; lower "
                f"medians_ms.{name} to {median_ms:.4f} in {BASELINE_PATH} "
                "to lock in the gain"
            ],
        )
    return [], [f"{name} median {median_ms:.4f} ms is within tolerance of its baseline"]


def invariance_verdict(medians_ms: dict[str, float], tolerance_ms: float) -> list[str]:
    """Overhead must not grow with provider latency: a tier whose median
    overhead exceeds the gated tier's by more than the stated tolerance is a
    runtime defect (wait-scaled machinery), not a measurement artifact.
    """
    base = medians_ms[GATED_BENCHMARK]
    failures = []
    for name in INVARIANCE_TIERS:
        median = medians_ms[name]
        if median > base + tolerance_ms:
            tier = name.removeprefix("overhead_").removesuffix("ms")
            failures.append(
                f"overhead is not latency-invariant: the {tier} ms tier's median "
                f"{median:.4f} ms exceeds the 50 ms tier's {base:.4f} ms by more "
                f"than {tolerance_ms:g} ms"
            )
    return failures


def _percentile_cell(value_ms: float) -> str:
    """A percentile the run had too few samples to compute reads as such,
    never as a number: ``nan`` in a released report invites being read as zero.
    """
    if math.isnan(value_ms):  # budget_verdict refused to compute it
        return f"not computed (fewer than {MIN_GATED_SAMPLES} pooled samples)"
    return f"{value_ms:.4f} ms"


def _environment_lines(bench: pyperf.Benchmark) -> list[str]:
    metadata = bench.get_metadata()
    keys = (
        "hostname",
        "platform",
        "cpu_count",
        "cpu_model_name",
        "python_version",
        "perf_version",
        "date",
    )
    lines = [f"- {key}: {metadata[key]}" for key in keys if key in metadata]
    return lines or ["- (no environment metadata captured)"]


def render_report(
    results: dict[str, pyperf.Benchmark],
    medians_ms: dict[str, float],
    p50_ms: float,
    p99_ms: float,
    comparison: Comparison,
    failures: list[str],
    notes: list[str],
) -> str:
    verdict = "PASS" if not failures else "FAIL"
    lines = [
        "# Benchmark report",
        "",
        f"Gate verdict: **{verdict}**",
        "",
        "Offline pyperf suite over the `_AgentDoFn` element path with in-memory",
        "state handles and `FakeLLM` (see `docs/benchmarks.md`): runner",
        "scheduling and state-backend writes are outside every figure below.",
        "",
        "## Absolute budget (overhead_50ms, pooled per-activation samples)",
        "",
        f"- p50: {_percentile_cell(p50_ms)} (budget < {P50_BUDGET_MS:g} ms)",
        f"- p99: {_percentile_cell(p99_ms)} (budget < {P99_BUDGET_MS:g} ms)",
        f"- samples: {results[GATED_BENCHMARK].get_nvalue()}",
        "",
        "## Per-benchmark medians",
        "",
        "| benchmark | median (ms) |",
        "|---|---|",
    ]
    for name in sorted(medians_ms):
        lines.append(f"| {name} | {medians_ms[name]:.4f} |")
    throughput = 1000.0 / medians_ms["noop_throughput"] if medians_ms["noop_throughput"] else 0.0
    lines += [
        "",
        f"- `noop_throughput` derives to ~{throughput:,.0f} activations/sec per key",
        "  (the runtime ceiling with zero agent work).",
        "- `suspension_roundtrip` is runtime-only cost: the effector and message",
        "  transport are excluded, so this is not an end-to-end SLA.",
        "- `state_commit_*` measures runtime-side cost (staging, proto mutation,",
        "  deterministic encode); the runner's state-backend write is excluded.",
        "",
        "## Tier invariance (overhead must not grow with provider latency)",
        "",
        "| tier | median overhead (ms) |",
        "|---|---|",
        f"| 50 ms (gated) | {medians_ms['overhead_50ms']:.4f} |",
        f"| 500 ms | {medians_ms['overhead_500ms']:.4f} |",
        f"| 2000 ms | {medians_ms['overhead_2000ms']:.4f} |",
        "",
        "## RunAgent vs RunInference (per element, zero-latency FakeLLM)",
        "",
        f"- RunAgent: {comparison.runagent_ms:.4f} ms",
        f"- RunInference: {comparison.runinference_ms:.4f} ms",
        f"- delta: {comparison.delta_ms:.4f} ms (the baseline-tracked quantity)",
        f"- ratio: {comparison.ratio:.2f}x",
        "",
        "Whole-pipeline DirectRunner figures; not comparable to the fake-handle",
        "benchmarks and not gated against the overhead budget.",
        "",
        "## Gate messages",
        "",
    ]
    lines += [f"- FAIL: {failure}" for failure in failures]
    lines += [f"- {note}" for note in notes]
    lines += [
        "",
        "## Environment",
        "",
        *_environment_lines(results[GATED_BENCHMARK]),
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    if _PYPERF_IMPORT_ERROR is not None:
        print(
            f"error: cannot import pyperf ({_PYPERF_IMPORT_ERROR}). "
            "Install the bench dependency group: uv sync --group bench",
            file=sys.stderr,
        )
        return 1

    try:
        results = load_results()
        baseline = load_baseline()
    except GateError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    medians_ms = {name: bench.median() * 1000.0 for name, bench in results.items()}
    comparison = comparison_summary(
        medians_ms["runagent_per_element"], medians_ms["runinference_per_element"]
    )

    failures: list[str] = []
    notes: list[str] = []

    # Judgement 1: the absolute budget, over pooled per-activation samples.
    budget_failures, p50_ms, p99_ms = budget_verdict(results[GATED_BENCHMARK])
    failures += budget_failures

    # Judgement 2: the baseline ratchet, medians only.
    ratchet_medians = {name: medians_ms[name] for name in medians_ms if name not in _UNTRACKED}
    ratchet_medians[DELTA_NAME] = comparison.delta_ms
    for name in BASELINE_TRACKED:
        entry_failures, entry_notes = ratchet_verdict(name, ratchet_medians[name], baseline)
        failures += entry_failures
        notes += entry_notes

    failures += invariance_verdict(medians_ms, baseline.invariance_tolerance_ms)

    report = render_report(results, medians_ms, p50_ms, p99_ms, comparison, failures, notes)
    REPORT_PATH.write_text(report, encoding="utf-8")

    for note in notes:
        print(note)
    if failures:
        for failure in failures:
            print(f"error: {failure}", file=sys.stderr)
        print(f"\nbench gate failed ({len(failures)} failure(s)); see {REPORT_PATH}")
        return 1
    print(f"bench gate passed; report at {REPORT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
