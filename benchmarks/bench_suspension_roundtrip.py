"""Suspension round-trip: Suspend commit plus admitted ToolResult resume.

One key walks the full continuation machinery: element one stages a
side-effect intent and returns ``Suspend`` (continuation written, ``PENDING``
populated, HITL timer armed); element two carries the matching ``ToolResult``,
is admitted, and resumes to completion. The value is the summed wall time of
both ``process()`` drains over shared handles — the runtime-only price of the
re-injection path per side effect. The effector and the message bus are
deliberately outside the measurement (design D5); the report labels the
figure accordingly, so nobody reads it as an end-to-end SLA.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from beam_agents.core.agent import intent_id_for
from benchmarks._harness import (
    KEY,
    drain,
    event_envelope,
    fresh_handles,
    make_dofn,
    suspending_agent,
    tool_result_envelope,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from beam_agents.core.dofn import _AgentDoFn
    from benchmarks._harness import Handles

# Pinned sampling (CI defaults; CLI overrides are local iteration only).
PROCESSES = 8
VALUES = 5
WARMUPS = 1

_STATE: list[tuple[_AgentDoFn, Handles]] = []


def _state() -> tuple[_AgentDoFn, Handles]:
    if not _STATE:
        _STATE.append((make_dofn(suspending_agent), fresh_handles()))
    return _STATE[0]


def activate_suspend(dofn: _AgentDoFn, handles: Handles) -> str:
    """Drain the Suspend-committing element; return the staged intent's ID.

    The ID is a pure function of ``(key, seq, step_index)``, read off the SEQ
    handle before the activation, so the resume envelope can be built without
    an effector in the loop.
    """
    intent_id = intent_id_for(KEY, handles.seq.read(), 0)
    drain(dofn, event_envelope(), handles)
    return intent_id


def activate_resume(dofn: _AgentDoFn, handles: Handles, intent_id: str) -> None:
    """Drain the admitted ``ToolResult`` element, resuming to completion."""
    drain(dofn, tool_result_envelope(intent_id), handles)


def time_roundtrip(loops: int) -> float:
    """Sum both element hops' wall time, per round trip; envelope construction
    and intent-ID derivation stay outside the clocked windows.
    """
    dofn, handles = _state()
    total = 0.0
    for _ in range(loops):
        intent_id = intent_id_for(KEY, handles.seq.read(), 0)
        suspend_envelope = event_envelope()
        t0 = time.perf_counter()
        drain(dofn, suspend_envelope, handles)
        t1 = time.perf_counter()
        resume_envelope = tool_result_envelope(intent_id)
        t2 = time.perf_counter()
        drain(dofn, resume_envelope, handles)
        t3 = time.perf_counter()
        total += (t1 - t0) + (t3 - t2)
    return total


TIMED: tuple[tuple[str, Callable[[int], float]], ...] = (("suspension_roundtrip", time_roundtrip),)


def main() -> None:
    import pyperf

    runner = pyperf.Runner(
        processes=PROCESSES,
        values=VALUES,
        warmups=WARMUPS,
        metadata={
            "description": (
                "Suspend commit + admitted ToolResult resume, both process() "
                "drains summed; effector and transport excluded"
            )
        },
        program_args=("-m", "benchmarks.bench_suspension_roundtrip"),
    )
    runner.bench_time_func("suspension_roundtrip", time_roundtrip)


if __name__ == "__main__":
    main()
