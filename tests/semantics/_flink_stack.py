"""Shared Flink-stack machinery for docker-backed semantics suites.

Move-only extraction from ``tests/semantics/_e2e/stack.py`` (the adapter
conformance matrix's Flink leg needs the same compose control, freshness,
health checks, ``InfraFailure`` separation, and run-id topic naming without
importing the effectively-once gate's ledger/effector/spool apparatus). The
e2e-specific pieces — ``RunConfig``'s topic/population layout, spool
provisioning, teardown — stay in ``_e2e/stack.py``, which re-exports
everything here so its importers are unchanged.

Freshness is load-bearing (e2e design F8): the SDK worker pool fails
permanently after a handful of worker exits, the TaskManager accumulates
classloader leaks per submission, and a degraded stack is indistinguishable
from a correctness stall from the outside. Every run therefore restarts the
Flink-side services and health-checks them before submitting anything, so an
infrastructure failure surfaces as ``InfraFailure`` — never as a red
invariant.
"""

from __future__ import annotations

import subprocess
import time
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ("docker", "compose", "-f", str(REPO_ROOT / "docker" / "compose.yaml"))

HOST_BROKERS = "localhost:19092"
INTERNAL_BROKERS = "redpanda:9092"
REDIS_URL = "redis://localhost:16379"
FLINK_REST = "http://localhost:18081"
JOB_ENDPOINT = "localhost:18099"
ARTIFACT_ENDPOINT = "localhost:18098"

# The ingest-spool bind mount (docker/compose.yaml): the host-side writer's
# root and the path the beam-sdk-harness container sees it at. Shared because
# every suite that reads events through the spool SDF must agree with the
# mount, not because the spool mechanism itself lives here.
HOST_SPOOL_ROOT = REPO_ROOT / "docker" / "e2e-spool"
CONTAINER_SPOOL_ROOT = "/var/beam-agents/spool"


class InfraFailure(RuntimeError):
    """The stack, not the invariant: raised when infrastructure is unhealthy.

    A gate failure wrapped in this type means "fix the environment / rerun",
    never "the runtime broke its contract". Assertions about the invariants
    themselves raise plain AssertionError instead.
    """


class FlinkStackControl:
    """Compose control, freshness, health checks, and job/topic helpers for
    one docker-backed run. Owns no run-specific layout; subclasses (the e2e
    gate's ``Stack``) add their topic/spool wiring on top."""

    # -- freshness (e2e design F8) ----------------------------------------------

    def freshen_flink(self) -> None:
        """Restart the Flink-side services so no prior run's degradation leaks in.

        The JobManager is in the restart set, and was not until 2026-07-31.
        Leaving it out made freshness a half-measure: it is the one Flink-side
        service that outlives every submission of a whole CI job — the e2e
        gate's retries plus one leg per conformance adapter — and its blob
        server is where the accumulated degradation actually surfaced. The
        failure looked like `uploadUserJars` dying with `PUT operation failed:
        Broken pipe`, or a job accepted and then never leaving submission with
        its source stuck at in=0/out=0. Restarting the TaskManager under a
        JobManager that has been up for forty minutes does not clear that.
        """
        self._cancel_all_jobs()
        subprocess.run(
            [*COMPOSE, "restart", "flink-jobmanager", "flink-taskmanager", "flink-jobserver"],
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

    def restart_taskmanager(self) -> None:
        """Restart the TaskManager mid-run (the conformance restart cell's
        chaos): the running job fails over and recovers from its last
        checkpoint once the fresh slots register. The harness must follow —
        it lives in the TaskManager's network namespace."""
        subprocess.run([*COMPOSE, "restart", "flink-taskmanager"], check=True, capture_output=True)
        subprocess.run([*COMPOSE, "restart", "beam-sdk-harness"], check=True, capture_output=True)
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
        registration removes that race entirely.
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

    def running_jobs(self) -> list[dict[str, object]]:
        try:
            jobs = httpx.get(f"{FLINK_REST}/jobs/overview", timeout=10).json()["jobs"]
        except httpx.HTTPError as exc:
            raise InfraFailure(f"Flink REST API unreachable: {exc}") from exc
        return [j for j in jobs if j["state"] in ("RUNNING", "RESTARTING", "CREATED")]

    def job_vertex_summary(self, name: str) -> str:
        """Per-vertex read/write counters for the named running job — the
        self-diagnosis attached to a stall so a red CI run says where records
        stopped instead of just 'no intents'."""
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

    def capture_tm_thread_dump(self, label: str, target_dir: Path = HOST_SPOOL_ROOT) -> str:
        """Snapshot the TaskManager JVM's threads for an upstream stall report.

        Best-effort: the dump is diagnostic treasure when a submission stall
        reproduces, but its absence must never fail a gate.
        """
        target = target_dir / f"{label}-tm-threads.txt"
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

    async def create_topics(self, topics: tuple[str, ...]) -> None:
        from aiokafka.admin import AIOKafkaAdminClient, NewTopic

        admin = AIOKafkaAdminClient(bootstrap_servers=HOST_BROKERS)
        await admin.start()
        try:
            await admin.create_topics(
                [NewTopic(t, num_partitions=4, replication_factor=1) for t in topics]
            )
        finally:
            await admin.close()

    async def delete_topics(self, topics: tuple[str, ...]) -> None:
        from aiokafka.admin import AIOKafkaAdminClient

        admin = AIOKafkaAdminClient(bootstrap_servers=HOST_BROKERS)
        await admin.start()
        try:
            await admin.delete_topics(list(topics))
        except Exception:
            pass
        finally:
            await admin.close()


def run_topic(prefix: str, run_id: str, kind: str) -> str:
    """Run-id topic naming: isolation is by run id, and the run id is
    load-bearing (embedded in topic names and consumer groups so no prior
    run's records can leak into this one's assertions)."""
    return f"{prefix}-{run_id}-{kind}"
