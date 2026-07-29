"""Seeded, progress-driven kill schedules — replayable, volume-independent.

Kills are triggered by *run progress* (the fraction of expected tool
executions recorded in the ledger), not by wall clock: a time-based schedule
either misses a fast run entirely or bunches into a slow run's tail, and the
gate's own "all scheduled kills executed" assertion would turn honest timing
variance into flakes. Thresholds are evenly spaced with seeded jitter and
victims are seeded, so a failing run is exactly reproducible from its seed.

The TaskManager kill is not scheduled here: it is phase B's opening move,
orchestrated by the gate (kill → restart → cancel → resubmit-with-replay,
design D10).
"""

from __future__ import annotations

import logging
import random
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

_LOG = logging.getLogger("beam_agents.e2e.chaos")

TM_CONTAINER = "docker-flink-taskmanager-1"


@dataclass(frozen=True)
class KillAction:
    at_progress: float  # fraction of expected executions, in (0, 1)
    victim: int  # seeded selector into the live worker list


def build_schedule(seed: int, *, effector_kills: int) -> list[KillAction]:
    """Evenly spaced progress thresholds with seeded jitter, seeded victims."""
    rng = random.Random(seed)
    actions = []
    for i in range(effector_kills):
        base = (i + 1) / (effector_kills + 1)
        jitter = (rng.random() - 0.5) * 0.5 / (effector_kills + 1)
        actions.append(
            KillAction(
                at_progress=min(0.95, max(0.05, base + jitter)),
                victim=rng.randrange(1_000_000),
            )
        )
    schedule = sorted(actions, key=lambda a: a.at_progress)
    _LOG.info("chaos schedule (seed=%d): %s", seed, schedule)
    return schedule


def kill_taskmanager() -> None:
    subprocess.run(["docker", "kill", TM_CONTAINER], check=True, capture_output=True)


class ChaosExecutor:
    """Fires each action as ``progress()`` crosses its threshold.

    ``progress`` returns the completed fraction in [0, 1]. Poll cadence is
    fast (250 ms) so a kill lands close to its threshold even on a quick run.
    ``join`` before asserting anything about executed kills.
    """

    def __init__(
        self,
        schedule: list[KillAction],
        *,
        progress: Callable[[], float],
        on_kill_effector: Callable[[int], None],
    ) -> None:
        self._schedule = list(schedule)
        self._progress = progress
        self._on_kill_effector = on_kill_effector
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="chaos", daemon=True)
        self.executed: list[KillAction] = []

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        pending = list(self._schedule)
        while pending and not self._stop.is_set():
            try:
                current = self._progress()
            except Exception:
                _LOG.exception("chaos progress probe failed; retrying")
                current = 0.0
            while pending and current >= pending[0].at_progress:
                action = pending.pop(0)
                try:
                    self._on_kill_effector(action.victim)
                    self.executed.append(action)
                    _LOG.info("chaos executed at progress %.2f: %s", current, action)
                except Exception:
                    _LOG.exception("chaos action failed: %s", action)
                    return
            time.sleep(0.25)

    def stop(self) -> None:
        self._stop.set()

    def join(self, timeout_s: float) -> None:
        self._thread.join(timeout_s)
