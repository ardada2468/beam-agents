"""No-op throughput: the runtime's ceiling with zero agent work.

Each value covers full ``process()`` drains — bridge submission, activation,
and staged commit included — with an agent that makes no model call, runs no
tool, and writes no memory. The report presents the median both as time per
activation and as derived activations/sec, labeled as the runtime ceiling.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from benchmarks._harness import drain, event_envelope, fresh_handles, make_dofn, noop_agent

if TYPE_CHECKING:
    from collections.abc import Callable

    from beam_agents.core.dofn import _AgentDoFn
    from benchmarks._harness import Handles

# Pinned sampling (CI runs these defaults; override on the CLI for local
# iteration only): the per-activation cost is well under a millisecond, so
# pyperf's loop calibration against its default min_time does the batching.
PROCESSES = 8
VALUES = 5
WARMUPS = 1

_STATE: list[tuple[_AgentDoFn, Handles]] = []


def _state() -> tuple[_AgentDoFn, Handles]:
    if not _STATE:
        _STATE.append((make_dofn(noop_agent), fresh_handles()))
    return _STATE[0]


def time_noop(loops: int) -> float:
    """Time ``loops`` complete no-op activations; pyperf divides by ``loops``."""
    dofn, handles = _state()
    envelope = event_envelope()
    t0 = time.perf_counter()
    for _ in range(loops):
        drain(dofn, envelope, handles)
    return time.perf_counter() - t0


# (benchmark name, timed function) — executed one iteration each by the
# unit-tier smoke tests so this module cannot rot silently.
TIMED: tuple[tuple[str, Callable[[int], float]], ...] = (("noop_throughput", time_noop),)


def main() -> None:
    import pyperf

    runner = pyperf.Runner(
        processes=PROCESSES,
        values=VALUES,
        warmups=WARMUPS,
        metadata={"description": "full process() drain with a no-op agent"},
        program_args=("-m", "benchmarks.bench_noop_throughput"),
    )
    runner.bench_time_func("noop_throughput", time_noop)


if __name__ == "__main__":
    main()
