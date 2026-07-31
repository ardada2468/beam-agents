"""The harness arithmetic the benchmark numbers rest on.

Two pure seams: the overhead subtraction (design D2 — the same subtraction
``overhead_ms`` publishes, with the nominal tier standing in for the measured
call time) and the RunInference comparison summary (design D7 — the delta, not
the absolutes, is the baseline-tracked quantity).
"""

from __future__ import annotations

import pytest

from benchmarks.bench_overhead_tiers import overhead_sample
from scripts import bench_gate


def _scripted_clock(*readings_s: float) -> object:
    remaining = list(readings_s)

    def read() -> float:
        return remaining.pop(0)

    return read


# --- Scenario: Overhead subtracts the configured tier latency -----------------


def test_the_recorded_value_is_wall_time_minus_the_configured_tier_latency() -> None:
    # Wall time 507 ms at the 500 ms tier: the recorded overhead is 7 ms.
    clock = _scripted_clock(1.0, 1.507)

    value = overhead_sample(500, activate=lambda: None, clock=clock)  # type: ignore[arg-type]

    assert value == pytest.approx(0.007)


def test_scheduling_slop_above_the_nominal_sleep_stays_in_the_value() -> None:
    # The event loop woke 12 ms late from a 50 ms sleep. That slop is runtime
    # code (the bridge's event loop), so it is charged to the runtime, not
    # excluded as provider time.
    clock = _scripted_clock(0.0, 0.062)

    value = overhead_sample(50, activate=lambda: None, clock=clock)  # type: ignore[arg-type]

    assert value == pytest.approx(0.012)


# --- Scenario: The comparison isolates the runtime's cost over raw inference --


def test_the_comparison_computes_delta_and_ratio_from_the_per_element_figures() -> None:
    summary = bench_gate.comparison_summary(10.0, 8.0)

    assert summary.delta_ms == pytest.approx(2.0)
    assert summary.ratio == pytest.approx(1.25)


def test_the_delta_is_the_baseline_tracked_quantity_not_the_absolutes() -> None:
    summary = bench_gate.comparison_summary(10.0, 8.0)

    assert summary.tracked == "runinference_delta"
    assert "runinference_delta" in bench_gate.BASELINE_TRACKED
    # The absolute per-element figures ride a different measurement surface
    # (whole DirectRunner pipelines) and are neither budget-gated nor
    # baseline-tracked.
    assert "runagent_per_element" not in bench_gate.BASELINE_TRACKED
    assert "runinference_per_element" not in bench_gate.BASELINE_TRACKED
