"""The effector worker supervisor: real processes, really killed.

Workers are `beam-agents-effector` OS processes (spawned as
``python -m beam_agents.effector``) sharing one consumer group against the
compose Redis. A "kill" is ``SIGKILL`` — no shutdown handler runs, no claim is
handed back; recovery is lease expiry plus consumer-group rebalance, exactly
the production story. The supervisor keeps the pool at size by spawning a
replacement for every kill, and teardown terminates every child it ever
started, whether the test passed, failed, or timed out.

Lease/tool-timeout pacing: ``EffectorConfig`` requires ``lease_ms >
tool_timeout_ms``, and recovery from a kill cannot be faster than the lease,
so both are kept small — the gate's wall clock is bounded below by
``LEASE_MS`` per unlucky kill.
"""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from tests.semantics._e2e.stack import HOST_BROKERS, REDIS_URL, REPO_ROOT, RunConfig

LEASE_MS = 15_000
TOOL_TIMEOUT_MS = 5_000
REGISTRY_PATH = "tests.semantics._e2e.agent:LEDGER_TOOLS"

_LOG_DIR = REPO_ROOT / "docker" / "e2e-spool"  # gitignored, survives the run for triage


@dataclass
class EffectorPool:
    config: RunConfig
    size: int
    _procs: list[subprocess.Popen[bytes]] = field(default_factory=list)
    _spawned: int = 0
    _logs: list[Path] = field(default_factory=list)

    def _spawn_one(self) -> subprocess.Popen[bytes]:
        env = {
            **os.environ,
            "BEAM_AGENTS_E2E_RUN_ID": self.config.run_id,
            "BEAM_AGENTS_E2E_REDIS_URL": REDIS_URL,
            "PYTHONPATH": str(REPO_ROOT),
        }
        log_path = _LOG_DIR / f"{self.config.run_id}-effector-{self._spawned}.log"
        self._logs.append(log_path)
        log = log_path.open("wb")
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "beam_agents.effector",
                "--registry",
                REGISTRY_PATH,
                "--intents-from",
                f"kafka://{HOST_BROKERS}/{self.config.intents_topic}",
                "--results-to",
                f"kafka://{HOST_BROKERS}/{self.config.results_topic}",
                "--approvals-to",
                f"kafka://{HOST_BROKERS}/{self.config.approval_requests_topic}",
                "--dedup",
                REDIS_URL,
                "--consumer-group",
                self.config.effector_group,
                "--lease-ms",
                str(LEASE_MS),
                "--tool-timeout-ms",
                str(TOOL_TIMEOUT_MS),
            ],
            cwd=REPO_ROOT,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        self._spawned += 1
        return proc

    def start(self) -> None:
        for _ in range(self.size):
            self._procs.append(self._spawn_one())

    def kill_one(self, index: int) -> int:
        """SIGKILL the ``index % alive``-th live worker; spawn a replacement.

        Returns the killed pid. The replacement joins the same consumer group;
        the group rebalance plus lease expiry is the recovery under test.
        """
        alive = [p for p in self._procs if p.poll() is None]
        if not alive:
            raise RuntimeError("no live effector workers left to kill")
        victim = alive[index % len(alive)]
        os.kill(victim.pid, signal.SIGKILL)
        victim.wait(timeout=10)
        self._procs.append(self._spawn_one())
        return victim.pid

    def alive_count(self) -> int:
        return sum(1 for p in self._procs if p.poll() is None)

    def check_healthy(self) -> None:
        """Raise if the pool has silently collapsed (infra, not invariant)."""
        if self.alive_count() == 0:
            tails = []
            for log_path in self._logs[-3:]:
                with contextlib.suppress(OSError):
                    tails.append(f"--- {log_path.name} ---\n" + log_path.read_text()[-2000:])
            raise RuntimeError(
                "every effector worker is dead — infrastructure failure, "
                "not an invariant violation.\n" + "\n".join(tails)
            )

    def terminate_all(self, grace_s: float = 5.0) -> None:
        for proc in self._procs:
            if proc.poll() is None:
                proc.terminate()
        deadline = time.monotonic() + grace_s
        for proc in self._procs:
            if proc.poll() is None:
                try:
                    proc.wait(timeout=max(0.1, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
