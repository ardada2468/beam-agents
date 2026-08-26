"""One-iteration smoke execution of every benchmark module, in the unit tier.

The benchmarks are pyperf scripts, never collected by pytest (design D1), so
code pytest never imports would rot: a runtime refactor that breaks a
benchmark would fail nightly, days after the PR that broke it. These tests
import every ``benchmarks/bench_*.py`` module and run each timed function for
one iteration, fully offline — so the breakage fails the required ``ci`` lane
instead.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from benchmarks import _harness, bench_state_commit, bench_suspension_roundtrip
from scripts import bench_gate

_BENCHMARKS_DIR = Path(_harness.__file__).resolve().parent
_REPO_ROOT = _BENCHMARKS_DIR.parent
_MODULE_NAMES = sorted(path.stem for path in _BENCHMARKS_DIR.glob("bench_*.py"))


# --- Scenario: Benchmark modules cannot rot silently / The full suite runs offline


def test_every_benchmark_module_on_disk_is_smoke_covered() -> None:
    # Discovery is from the filesystem, so a new bench_*.py module cannot be
    # added without landing in this smoke selection.
    assert _MODULE_NAMES == [
        "bench_noop_throughput",
        "bench_overhead_tiers",
        "bench_runinference_compare",
        "bench_state_commit",
        "bench_suspension_roundtrip",
    ]


# The pipeline-running comparison module gets its own slow-marked test below;
# every other module rides this fast parametrization.
@pytest.mark.parametrize(
    "module_name", [name for name in _MODULE_NAMES if name != "bench_runinference_compare"]
)
def test_every_timed_function_executes_one_iteration_offline(module_name: str) -> None:
    module = importlib.import_module(f"benchmarks.{module_name}")

    assert module.TIMED, f"{module_name} declares no timed functions"
    for name, func in module.TIMED:
        elapsed = func(1)
        assert elapsed > 0, f"{module_name}:{name} measured no time"


@pytest.mark.slow
@pytest.mark.timeout(240)
def test_the_runinference_comparison_runs_both_pipelines_offline() -> None:
    module = importlib.import_module("benchmarks.bench_runinference_compare")

    assert module.TIMED
    for name, func in module.TIMED:
        elapsed = func(1)
        assert elapsed > 0, f"bench_runinference_compare:{name} measured no time"


def test_the_timed_functions_cover_exactly_what_the_gate_expects() -> None:
    # The gate's declared benchmark set and the modules' timed functions are
    # two spellings of one surface; a drift means the gate would fail on a
    # missing result (or silently not gate a new benchmark).
    declared = {name for names in bench_gate.EXPECTED_RESULTS.values() for name in names}
    timed = {
        name
        for module_name in _MODULE_NAMES
        for name, _ in importlib.import_module(f"benchmarks.{module_name}").TIMED
    }
    assert timed == declared


# --- Scenario: A suspend-resume pair is timed as one round trip ---------------


def test_the_round_trip_persists_then_clears_the_continuation() -> None:
    dofn = _harness.make_dofn(_harness.suspending_agent)
    try:
        handles = _harness.fresh_handles()

        intent_id = bench_suspension_roundtrip.activate_suspend(dofn, handles)

        # The suspend hop went through the real commit path: continuation
        # written, the staged intent pending, seq advanced.
        assert handles.continuation.read() is not None
        assert [i.intent_id for i in handles.pending.read()] == [intent_id]
        assert handles.seq.read() == 1

        bench_suspension_roundtrip.activate_resume(dofn, handles, intent_id)

        # The resume hop was admitted and committed: continuation cleared,
        # pending drained, seq advanced again. No effector, no broker.
        assert handles.continuation.read() is None
        assert handles.pending.read() == []
        assert handles.seq.read() == 2
    finally:
        dofn.teardown()


# --- Scenario: Commit cost is reported across blob sizes up to the cap --------


def test_state_commit_covers_every_configured_size_including_the_cap() -> None:
    assert bench_state_commit.BLOB_SIZES_KIB == (1, 16, 64, 100)
    assert 100 in bench_state_commit.BLOB_SIZES_KIB  # the documented blob cap

    for kib in bench_state_commit.BLOB_SIZES_KIB:
        blob = bench_state_commit.blob_for(kib)
        # The committed payload is the configured size (plus small proto/key
        # framing), so the size axis of the report is real.
        assert blob.total_value_bytes >= kib * 1024
        assert len(blob.SerializeToString(deterministic=True)) >= kib * 1024


# --- Scenario: The committed baseline is coherent before a runner reads it ----
#
# `tests/benchmarks/test_bench_gate.py` judges the gate's logic and needs the
# `bench` group; these two read the committed file itself, need no pyperf, and
# so ride the required offline lane. A baseline is hand-copied from a nightly
# log, which is exactly where a noisy number gets frozen into a gate.


def _committed_baseline(monkeypatch: pytest.MonkeyPatch) -> bench_gate.Baseline:
    # The gate resolves its paths against the cwd; read the real file the
    # nightly reads, validation included.
    monkeypatch.chdir(_REPO_ROOT)
    return bench_gate.load_baseline()


def test_the_committed_baseline_seeds_every_benchmark_the_gate_tracks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = _committed_baseline(monkeypatch)

    assert set(baseline.medians_ms) == set(bench_gate.BASELINE_TRACKED)


def test_the_committed_encode_baseline_grows_with_blob_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # `encode_*` times one `SerializeToString` call, so the size axis *is* the
    # measurement: a larger blob is strictly more work, and a seeded curve that
    # dips means that run mismeasured. The 2026-08-03 seed did exactly that
    # (64 KiB recorded dearer than 100 KiB) and the inversion sat in the file
    # until a later night tripped the ratchet on it.
    #
    # `state_commit_*` is deliberately not asserted this way: there the
    # size-dependent term is a few percent of a fixed ~0.2 ms activation cost,
    # well inside runner noise, so monotonicity is not a property of a good run.
    baseline = _committed_baseline(monkeypatch)
    seeded = [baseline.medians_ms[f"encode_{kib}kib"] for kib in bench_state_commit.BLOB_SIZES_KIB]

    assert seeded == sorted(seeded), (
        f"the committed encode_* medians dip with blob size ({seeded}); a larger "
        "blob cannot encode faster, so that entry was seeded from a mismeasured run"
    )
