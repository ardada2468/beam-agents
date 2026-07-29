"""Per-run provisioning and teardown for the e2e gate.

Isolation is by run id, and the run id is *load-bearing*: it is embedded in
every entity key (so ``intent_id = uuid5(key, seq, step)`` can never collide
with a previous run's ids in a shared dedup store), in every topic name, in
the consumer group, in the ledger namespace, and in the spool directory.

Freshness is equally load-bearing (design F8): the SDK worker pool fails
permanently after a handful of worker exits, the TaskManager accumulates
classloader leaks per submission, and a degraded stack is indistinguishable
from a correctness stall from the outside. Every run therefore restarts the
Flink-side services and health-checks them before submitting anything, so an
infrastructure failure surfaces as `InfraFailure` — never as a red invariant.
"""

from __future__ import annotations

import shutil
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parents[3]
COMPOSE = ("docker", "compose", "-f", str(REPO_ROOT / "docker" / "compose.yaml"))

HOST_BROKERS = "localhost:19092"
INTERNAL_BROKERS = "redpanda:9092"
REDIS_URL = "redis://localhost:16379"
FLINK_REST = "http://localhost:18081"
JOB_ENDPOINT = "localhost:18099"
ARTIFACT_ENDPOINT = "localhost:18098"

HOST_SPOOL_ROOT = REPO_ROOT / "docker" / "e2e-spool"
CONTAINER_SPOOL_ROOT = "/var/beam-agents/spool"


class InfraFailure(RuntimeError):
    """The stack, not the invariant: raised when infrastructure is unhealthy.

    A gate failure wrapped in this type means "fix the environment / rerun",
    never "the runtime broke effectively-once". Assertions about the
    invariants themselves raise plain AssertionError instead.
    """


@dataclass(frozen=True)
class RunConfig:
    """Everything a single gate run needs to name its world."""

    run_id: str
    seed: int
    events: int

    @property
    def events_topic(self) -> str:
        return f"e2e-{self.run_id}-events"

    @property
    def intents_topic(self) -> str:
        return f"e2e-{self.run_id}-intents"

    @property
    def results_topic(self) -> str:
        return f"e2e-{self.run_id}-results"

    @property
    def approval_requests_topic(self) -> str:
        return f"e2e-{self.run_id}-approval-req"

    @property
    def decisions_topic(self) -> str:
        return f"e2e-{self.run_id}-decisions"

    @property
    def output_topic(self) -> str:
        return f"e2e-{self.run_id}-output"

    @property
    def errors_topic(self) -> str:
        return f"e2e-{self.run_id}-errors"

    @property
    def effector_group(self) -> str:
        return f"e2e-{self.run_id}-effector"

    @property
    def drainer_group(self) -> str:
        return f"e2e-{self.run_id}-drainer"

    @property
    def host_spool(self) -> Path:
        return HOST_SPOOL_ROOT / self.run_id

    @property
    def container_spool(self) -> str:
        return f"{CONTAINER_SPOOL_ROOT}/{self.run_id}"

    def entity_key(self, prefix: bytes, index: int) -> bytes:
        return prefix + self.run_id.encode() + b"-" + f"{index:05d}".encode()

    @property
    def all_topics(self) -> tuple[str, ...]:
        return (
            self.events_topic,
            self.intents_topic,
            self.results_topic,
            self.approval_requests_topic,
            self.decisions_topic,
            self.output_topic,
            self.errors_topic,
        )


def new_run(seed: int, events: int) -> RunConfig:
    return RunConfig(run_id=uuid.uuid4().hex[:12], seed=seed, events=events)


@dataclass
class Stack:
    """Owns the run's external resources; teardown is idempotent and total."""

    config: RunConfig
    _submitted_jobs: list[str] = field(default_factory=list)

    # -- freshness (F8) --------------------------------------------------------

    def freshen_flink(self) -> None:
        """Restart the Flink-side services so no prior run's degradation leaks in."""
        self._cancel_all_jobs()
        subprocess.run(
            [*COMPOSE, "restart", "flink-taskmanager", "flink-jobserver"],
            check=True,
            capture_output=True,
        )
        # The harness borrows the TaskManager's network namespace; after a TM
        # restart the old namespace is dead and only a forced restart
        # re-attaches (verified empirically — compose `up -d` is a no-op here).
        subprocess.run([*COMPOSE, "restart", "beam-sdk-harness"], check=True, capture_output=True)
        self._await_flink_healthy()

    def recover_taskmanager(self) -> None:
        """Bring the killed TaskManager back and wait for its slots.

        Deliberately does NOT touch the harness yet: the JobManager will
        redeploy the checkpoint-restored job onto the fresh TaskManager, and
        that job's environment churn (deploy, then our cancel) must land on
        the OLD worker pool, not the one the replay job will use — the pool
        fails permanently after a handful of worker exits (design F8), and
        burning its budget on the doomed restored job was exactly the ~50%
        phase-B stall observed across seeds.
        """
        subprocess.run([*COMPOSE, "start", "flink-taskmanager"], check=True, capture_output=True)
        self._await_flink_healthy()

    def fresh_harness(self) -> None:
        """A factory-fresh worker pool for the replay job, once jobs are gone."""
        subprocess.run([*COMPOSE, "restart", "beam-sdk-harness"], check=True, capture_output=True)
        self._await_flink_healthy()
        self._await_single_taskmanager()

    def await_no_running_jobs(self, deadline_s: float = 90.0) -> None:
        deadline = time.monotonic() + deadline_s
        while time.monotonic() < deadline:
            if not self.running_jobs():
                return
            for job in self.running_jobs():
                self.cancel_job(str(job["jid"]))
            time.sleep(2)
        raise InfraFailure(
            f"jobs still running after {deadline_s:.0f}s of cancellation: "
            f"{[j['name'] for j in self.running_jobs()]}"
        )

    def _await_single_taskmanager(self, deadline_s: float = 90.0) -> None:
        """Wait until the SIGKILLed TaskManager's phantom registration is evicted.

        A SIGKILL never deregisters: for a few seconds the JobManager still
        lists the dead TaskManager, and slots allocated on the phantom hang
        their job until failover. Resubmitting only after exactly one live
        registration removes that race from the gate's phase B entirely.
        """
        deadline = time.monotonic() + deadline_s
        while time.monotonic() < deadline:
            try:
                tms = httpx.get(f"{FLINK_REST}/taskmanagers", timeout=5).json()["taskmanagers"]
                if len(tms) == 1:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(1)
        raise InfraFailure(
            "the killed TaskManager's registration was never evicted — the "
            "JobManager still lists more than one TaskManager"
        )

    def _await_flink_healthy(self, deadline_s: float = 120.0) -> None:
        deadline = time.monotonic() + deadline_s
        while time.monotonic() < deadline:
            try:
                overview = httpx.get(f"{FLINK_REST}/overview", timeout=5).json()
                if overview.get("slots-total", 0) >= 2:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(2)
        raise InfraFailure(
            "Flink did not become healthy (no registered TaskManager slots) — "
            "check `docker compose ps` and the flink-taskmanager logs"
        )

    # -- jobs -------------------------------------------------------------------

    def note_job(self, name_fragment: str) -> None:
        self._submitted_jobs.append(name_fragment)

    def running_jobs(self) -> list[dict[str, object]]:
        try:
            jobs = httpx.get(f"{FLINK_REST}/jobs/overview", timeout=10).json()["jobs"]
        except httpx.HTTPError as exc:
            raise InfraFailure(f"Flink REST API unreachable: {exc}") from exc
        return [j for j in jobs if j["state"] in ("RUNNING", "RESTARTING", "CREATED")]

    def job_vertex_summary(self, name: str) -> str:
        """Per-vertex read/write counters for the named running job — the
        self-diagnosis attached to a phase-B stall so a red CI run says where
        records stopped instead of just 'no intents'."""
        try:
            for job in self.running_jobs():
                if str(job["name"]) == name:
                    detail = httpx.get(f"{FLINK_REST}/jobs/{job['jid']}", timeout=10).json()
                    return " | ".join(
                        f"{v['name'][:40]}: in={v['metrics']['read-records']} "
                        f"out={v['metrics']['write-records']} ({v['status']})"
                        for v in detail["vertices"]
                    )
            return f"job {name!r} not in running jobs"
        except (httpx.HTTPError, KeyError) as exc:
            return f"(vertex summary unavailable: {exc})"

    def capture_tm_thread_dump(self, label: str) -> str:
        """Snapshot the TaskManager JVM's threads for the upstream stall report.

        Best-effort: the dump is diagnostic treasure when the F12 submission
        stall reproduces, but its absence must never fail the gate.
        """
        target = HOST_SPOOL_ROOT / f"{label}-tm-threads.txt"
        try:
            result = subprocess.run(
                ["docker", "exec", "docker-flink-taskmanager-1", "jcmd", "1", "Thread.print"],
                capture_output=True,
                timeout=30,
                check=False,
            )
            target.write_bytes(result.stdout or result.stderr)
            return str(target)
        except (OSError, subprocess.TimeoutExpired) as exc:
            return f"(thread dump failed: {exc})"

    def cancel_job(self, jid: str) -> None:
        with httpx.Client(timeout=10) as client:
            client.patch(f"{FLINK_REST}/jobs/{jid}", params={"mode": "cancel"})

    def _cancel_all_jobs(self) -> None:
        try:
            for job in self.running_jobs():
                self.cancel_job(str(job["jid"]))
        except InfraFailure:
            pass  # nothing to cancel if Flink is not up yet

    # -- topics ------------------------------------------------------------------

    async def create_topics(self) -> None:
        from aiokafka.admin import AIOKafkaAdminClient, NewTopic

        admin = AIOKafkaAdminClient(bootstrap_servers=HOST_BROKERS)
        await admin.start()
        try:
            await admin.create_topics(
                [
                    NewTopic(t, num_partitions=4, replication_factor=1)
                    for t in self.config.all_topics
                ]
            )
        finally:
            await admin.close()

    async def delete_topics(self) -> None:
        from aiokafka.admin import AIOKafkaAdminClient

        admin = AIOKafkaAdminClient(bootstrap_servers=HOST_BROKERS)
        await admin.start()
        try:
            await admin.delete_topics(list(self.config.all_topics))
        except Exception:
            pass
        finally:
            await admin.close()

    # -- lifecycle ----------------------------------------------------------------

    def provision_spool(self) -> None:
        shutil.rmtree(self.config.host_spool, ignore_errors=True)
        self.config.host_spool.mkdir(parents=True, exist_ok=True)

    async def teardown(self) -> None:
        """Total cleanup: jobs, topics, spool. Ledger/dedup keys expire with Redis."""
        self._cancel_all_jobs()
        await self.delete_topics()
        shutil.rmtree(self.config.host_spool, ignore_errors=True)
