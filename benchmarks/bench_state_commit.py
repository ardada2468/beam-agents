"""State-commit cost as a function of committed MemoryBlob size.

Full activations committing working-memory payloads of 1/16/64/100 KiB (the
last is the documented blob cap), plus an encode-only micro-benchmark over
``DeterministicProtoCoder.encode`` at the same sizes so the size-dependent
curve is attributable — "it's the deterministic encode" vs "it's the
surrounding staging" (design D6). With fake handles the runner's actual
state-backend write is a no-op, so what this honestly measures is the
runtime-side cost; the report says so.
"""

from __future__ import annotations

import time
from functools import partial
from typing import TYPE_CHECKING

from beam_agents._protos import MemoryBlob
from beam_agents.core.coders import DeterministicProtoCoder
from beam_agents.memory.facade import Memory
from benchmarks._harness import (
    BLOB_SIZES_KIB,
    drain,
    event_envelope,
    fresh_handles,
    make_dofn,
    memory_write_agent,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from beam_agents.core.dofn import _AgentDoFn

# Pinned sampling (CI defaults; CLI overrides are local iteration only).
PROCESSES = 5
VALUES = 5
WARMUPS = 1

_CODER = DeterministicProtoCoder(MemoryBlob)
_DOFN: list[_AgentDoFn] = []


def _dofn() -> _AgentDoFn:
    if not _DOFN:
        _DOFN.append(make_dofn(memory_write_agent))
    return _DOFN[0]


def payload(kib: int) -> bytes:
    return b"x" * (kib * 1024)


def blob_for(kib: int) -> MemoryBlob:
    """A ``MemoryBlob`` holding one ``kib``-KiB entry, built through the same
    facade the agent writes through.
    """
    memory = Memory(None, now_ms=0)
    memory.set("payload", payload(kib))
    return memory.to_blob()


def time_commit(loops: int, kib: int) -> float:
    """Full activations whose commit writes a ``kib``-KiB working-memory blob.

    Fresh handles per activation so every drain commits the same-size blob;
    handle and envelope setup stay outside the clocked window.
    """
    dofn = _dofn()
    envelope = event_envelope(payload(kib))
    total = 0.0
    for _ in range(loops):
        handles = fresh_handles()
        t0 = time.perf_counter()
        drain(dofn, envelope, handles)
        total += time.perf_counter() - t0
    return total


def time_encode(loops: int, kib: int) -> float:
    """``DeterministicProtoCoder.encode`` alone over a ``kib``-KiB blob."""
    blob = blob_for(kib)
    t0 = time.perf_counter()
    for _ in range(loops):
        _CODER.encode(blob)
    return time.perf_counter() - t0


TIMED: tuple[tuple[str, Callable[[int], float]], ...] = tuple(
    (f"state_commit_{kib}kib", partial(time_commit, kib=kib)) for kib in BLOB_SIZES_KIB
) + tuple((f"encode_{kib}kib", partial(time_encode, kib=kib)) for kib in BLOB_SIZES_KIB)


# `BLOB_SIZES_KIB` is the harness's shared constant, re-exported here because
# the size axis is this benchmark's contract (the smoke test asserts the cap is
# covered) rather than an incidental import.
__all__ = ["BLOB_SIZES_KIB", "TIMED", "blob_for", "main", "time_commit", "time_encode"]


def main() -> None:
    import pyperf

    runner = pyperf.Runner(
        processes=PROCESSES,
        values=VALUES,
        warmups=WARMUPS,
        metadata={
            "description": (
                "activation cost vs committed MemoryBlob size, plus encode-only "
                "attribution; runner-side state-backend writes excluded"
            )
        },
        program_args=("-m", "benchmarks.bench_state_commit"),
    )
    for kib in BLOB_SIZES_KIB:
        runner.bench_time_func(f"state_commit_{kib}kib", time_commit, kib)
    for kib in BLOB_SIZES_KIB:
        runner.bench_time_func(f"encode_{kib}kib", time_encode, kib)


if __name__ == "__main__":
    main()
