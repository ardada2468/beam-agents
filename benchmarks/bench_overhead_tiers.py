"""Per-activation runtime overhead under FakeLLM latency tiers (50/500/2000 ms).

Overhead is the activation's end-to-end wall time minus the configured
provider latency — the same subtraction ``overhead_ms`` publishes
(``beam_agents/observability/metrics.py``), with the nominal tier standing in
for the measured call time, so the dashboard and this gate measure one
quantity. Event-loop scheduling slop above the nominal sleep is charged to
the runtime: the bridge's event loop is runtime code (design D2).

Values are recorded one activation per sample (``--loops 1``), and the gate
computes p50/p99 over the pooled per-activation values from all worker
processes — never over per-process means. The 50 ms tier is the gated one,
sampled densely (~1000 values); 500/2000 ms prove latency invariance on far
fewer samples.

Per-tier sampling density needs per-tier worker settings, which one pyperf
invocation cannot express, so running this module without ``--tier``
orchestrates one pyperf run per tier, appending all three benchmarks into the
single JSON named by ``--output``.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

from benchmarks._harness import (
    TIERS_MS,
    drain,
    event_envelope,
    fresh_handles,
    make_dofn,
    single_call_agent,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from beam_agents.core.dofn import _AgentDoFn

_MODULE = "benchmarks.bench_overhead_tiers"

# Pinned per-tier sampling: (processes, values, warmups), always with
# --loops 1 so every value is one activation. CI runs exactly these; the
# --fast flag (local iteration only) scales them down.
TIER_SETTINGS: dict[int, tuple[int, int, int]] = {
    50: (20, 50, 3),  # 1000 pooled samples: the gated p50/p99 rest on these
    500: (5, 8, 1),  # medians only: latency invariance
    2000: (3, 4, 1),
}

_DOFNS: dict[int, _AgentDoFn] = {}


def _tier_dofn(tier_ms: int) -> _AgentDoFn:
    if tier_ms not in _DOFNS:
        _DOFNS[tier_ms] = make_dofn(single_call_agent, latency_ms=tier_ms)
    return _DOFNS[tier_ms]


def overhead_sample(
    tier_ms: int,
    *,
    activate: Callable[[], object],
    clock: Callable[[], float] = time.perf_counter,
) -> float:
    """One overhead value: the activation's wall time minus the nominal tier.

    Scheduling slop above the nominal sleep stays in the value — it is the
    runtime's own event loop waking late, which is exactly the defect class
    this benchmark exists to see.
    """
    started = clock()
    activate()
    return (clock() - started) - tier_ms / 1000.0


def time_overhead(loops: int, tier_ms: int) -> float:
    """``loops`` is pinned to 1 in real runs, so each value is one activation.

    Fresh handles per activation keep the replay cache empty, so the provider
    (and its tier sleep) is genuinely reached every time; handle setup stays
    outside the clocked window.
    """
    dofn = _tier_dofn(tier_ms)
    total = 0.0
    for _ in range(loops):
        handles = fresh_handles()
        activate = partial(drain, dofn, event_envelope(), handles)
        total += overhead_sample(tier_ms, activate=activate)
    return total


TIMED: tuple[tuple[str, Callable[[int], float]], ...] = tuple(
    (f"overhead_{tier}ms", partial(time_overhead, tier_ms=tier)) for tier in TIERS_MS
)


def _run_one_tier() -> None:
    """pyperf entry point for one tier (``--tier`` present: manager or worker)."""
    import pyperf

    def add_cmdline_args(cmd: list[str], args: argparse.Namespace) -> None:
        cmd.extend(["--tier", str(args.tier)])

    runner = pyperf.Runner(
        program_args=("-m", _MODULE),
        add_cmdline_args=add_cmdline_args,
    )
    runner.argparser.add_argument("--tier", type=int, choices=TIERS_MS, required=True)
    args = runner.parse_args()
    tier: int = args.tier
    runner.metadata["description"] = (
        f"per-activation overhead: wall time minus the nominal {tier} ms FakeLLM latency"
    )
    runner.bench_time_func(f"overhead_{tier}ms", time_overhead, tier)


def _orchestrate() -> None:
    """Run pyperf once per tier with its pinned settings, appending into one JSON."""
    parser = argparse.ArgumentParser(prog=_MODULE)
    parser.add_argument("-o", "--output", required=True, help="pyperf JSON output path")
    parser.add_argument(
        "--fast",
        action="store_true",
        help="scale sampling down for local iteration (never used in CI)",
    )
    args = parser.parse_args()
    output = Path(args.output)
    output.unlink(missing_ok=True)

    for tier in TIERS_MS:
        processes, values, warmups = TIER_SETTINGS[tier]
        if args.fast:
            processes = max(2, processes // 5)
            values = max(3, values // 5)
            warmups = 1
        cmd = [
            sys.executable,
            "-m",
            _MODULE,
            "--tier",
            str(tier),
            "--processes",
            str(processes),
            "--values",
            str(values),
            "--warmups",
            str(warmups),
            "--loops",
            "1",
            "--append",
            str(output),
        ]
        subprocess.run(cmd, check=True)


def main() -> None:
    if any(arg.startswith("--tier") for arg in sys.argv[1:]):
        _run_one_tier()
    else:
        _orchestrate()


if __name__ == "__main__":
    main()
