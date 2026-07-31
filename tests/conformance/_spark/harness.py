"""Host-side orchestration for the conformance Spark leg.

The Flink leg's harness, minus the pieces the spark stack cannot express and
plus the ones it needs. Per adapter: freshen the spark overlay's services,
provision run-scoped topics and a spool directory, publish one event per
spark-runnable scenario, submit the multiplexed job to the **Spark** job
server, and drive it to a fully-observed matrix.

Two structural differences from ``tests/conformance/_flink/harness.py``, both
following from declared skips rather than from taste:

* **No restart phase.** ``restart_mid_suspension`` is a declared spark skip
  (the overlay runs the job server's embedded ``local[4]`` master, so there is
  no separate worker container to restart), so this harness never parks a
  result for a restart and never touches the executor topology mid-run.
* **Health is a socket, not a REST API.** The Beam Spark job server exposes no
  Flink-style ``/jobs/overview``; liveness is the job and artifact endpoints
  accepting connections, and a stall's self-diagnosis is the job server's own
  recent log tail rather than per-vertex counters.

What is unchanged and deliberately so: the run-scoped topic naming, the
drainer/spool ingest (cross-language Kafka IO cannot run on this stack), the
at-least-once topic readers that collapse duplicates by identity, the
responder that answers intents deterministically by key prefix with the late
approval gated on the *observed* fail-closed terminal, and the
``InfraFailure`` separation so stack problems never read as a Spark verdict.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
import subprocess
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from beam_agents._protos import AgentEnvelope, ToolIntent, ToolResult
from tests.conformance._flink.pipeline import scenario_key
from tests.conformance._spark.pipeline import (
    SPARK_ARTIFACT_ENDPOINT,
    SPARK_JOB_ENDPOINT,
    run_conformance_pipeline,
)
from tests.conformance._spec import APPROVAL_TIMEOUT_FALLBACK, SPARK_SCENARIOS
from tests.semantics._e2e.assertions import read_topic_all, topic_end_offsets
from tests.semantics._e2e.drainer import Drainer
from tests.semantics._e2e.spool import SpoolWriter
from tests.semantics._flink_stack import (
    CONTAINER_SPOOL_ROOT,
    HOST_BROKERS,
    HOST_SPOOL_ROOT,
    REPO_ROOT,
    InfraFailure,
    run_topic,
)

_LOG = logging.getLogger("beam_agents.conformance.spark")

#: The two-file invocation `make compose-up-spark` uses: the base stack plus
#: the spark overlay. The base file alone must never start a Spark service.
COMPOSE_SPARK = (
    "docker",
    "compose",
    "-f",
    str(REPO_ROOT / "docker" / "compose.yaml"),
    "-f",
    str(REPO_ROOT / "docker" / "compose.spark.yaml"),
)

JOBSERVER_SERVICE = "spark-jobserver"
WORKER_POOL_SERVICE = "beam-sdk-harness-spark"

# Condition-driven deadlines, never bare sleeps. The leg per adapter is
# submission + quick scenarios + a real-time 30s HITL deadline. Budgeted above
# the Flink leg's per-phase window because this stack has none of the Flink
# stack's hardening (design D3.3) and a first-run stall must surface as a
# diagnosable InfraFailure, not as a pytest timeout.
PHASE_DEADLINE_S = 420.0
SUBMIT_STALL_WINDOW_S = 180.0
SUBMIT_ATTEMPTS = 2
_POLL_S = 2.0


class SparkStackControl:
    """Compose control, freshness, and health checks for the spark overlay.

    The analog of ``FlinkStackControl`` — not a subclass of it: every method
    there is scoped to a Flink service (``flink-taskmanager``,
    ``flink-jobserver``, the Flink REST API), and inheriting them would offer
    the spark leg operations that silently act on the wrong stack.
    """

    def freshen_spark(self) -> None:
        """Restart the spark-side services so no prior run's degradation leaks in.

        Same freshness rationale as the Flink leg (e2e design F8): the stock
        Beam SDK worker pool fails permanently after a handful of worker exits,
        and a degraded stack is indistinguishable from a correctness stall from
        the outside. The worker pool is restarted *after* the job server
        because it borrows the job server's network namespace — restarting the
        job server destroys the namespace the pool is attached to.
        """
        self._compose("restart", JOBSERVER_SERVICE)
        self._compose("restart", WORKER_POOL_SERVICE)
        self.await_healthy()

    def fresh_worker_pool(self) -> None:
        """A factory-fresh worker pool, without disturbing the job server."""
        self._compose("restart", WORKER_POOL_SERVICE)
        self.await_healthy()

    def await_healthy(self, deadline_s: float = 120.0) -> None:
        """Both job-server endpoints accepting connections.

        The Beam Spark job server has no REST surface to interrogate, so
        liveness is the gRPC job and artifact ports being connectable — the
        same property the overlay's healthcheck uses, checked from the host so
        a compose-level healthy report and an unreachable published port
        cannot disagree silently.
        """
        deadline = time.monotonic() + deadline_s
        unreachable = ""
        while time.monotonic() < deadline:
            unreachable = ""
            for endpoint in (SPARK_JOB_ENDPOINT, SPARK_ARTIFACT_ENDPOINT):
                if not _port_open(endpoint):
                    unreachable = endpoint
                    break
            if not unreachable:
                return
            time.sleep(2)
        raise InfraFailure(
            f"the Spark job server never became reachable at {unreachable} — check "
            f"`{' '.join(COMPOSE_SPARK)} ps` and the {JOBSERVER_SERVICE} logs"
        )

    def jobserver_tail(self, lines: int = 40) -> str:
        """Recent job-server log lines: the stall self-diagnosis this stack can
        offer in place of Flink's per-vertex read/write counters. Best-effort —
        a diagnostic must never be the thing that fails a run."""
        try:
            result = subprocess.run(
                [*COMPOSE_SPARK, "logs", "--no-color", "--tail", str(lines), JOBSERVER_SERVICE],
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            return result.stdout or result.stderr
        except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - diagnostics
            return f"(job server log tail unavailable: {exc})"

    def _compose(self, *args: str) -> None:
        subprocess.run([*COMPOSE_SPARK, *args], check=True, capture_output=True)


def _port_open(endpoint: str, timeout_s: float = 3.0) -> bool:
    host, _, port = endpoint.rpartition(":")
    try:
        with socket.create_connection((host, int(port)), timeout=timeout_s):
            return True
    except OSError:
        return False


# -- broker admin ------------------------------------------------------------------
#
# The broker half of what `FlinkStackControl` also offers. Duplicated here on
# purpose: that class is Flink-service-scoped (see SparkStackControl's
# docstring), and these two coroutines are pure Kafka admin against the shared
# Redpanda in the base compose file.


async def create_topics(topics: tuple[str, ...]) -> None:
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic

    admin = AIOKafkaAdminClient(bootstrap_servers=HOST_BROKERS)
    await admin.start()
    try:
        await admin.create_topics(
            [NewTopic(t, num_partitions=4, replication_factor=1) for t in topics]
        )
    finally:
        await admin.close()


async def delete_topics(topics: tuple[str, ...]) -> None:
    from aiokafka.admin import AIOKafkaAdminClient

    admin = AIOKafkaAdminClient(bootstrap_servers=HOST_BROKERS)
    await admin.start()
    try:
        await admin.delete_topics(list(topics))
    except Exception:  # teardown is best-effort; the run id already isolates us
        pass
    finally:
        await admin.close()


# -- the run's world ---------------------------------------------------------------


@dataclass(frozen=True)
class SparkRunConfig:
    """One adapter-leg run's world, named by run id (isolation is by run id)."""

    run_id: str
    adapter: str

    @property
    def events_topic(self) -> str:
        return run_topic("sconf", self.run_id, "events")

    @property
    def intents_topic(self) -> str:
        return run_topic("sconf", self.run_id, "intents")

    @property
    def results_topic(self) -> str:
        return run_topic("sconf", self.run_id, "results")

    @property
    def decisions_topic(self) -> str:
        return run_topic("sconf", self.run_id, "decisions")

    @property
    def output_topic(self) -> str:
        return run_topic("sconf", self.run_id, "output")

    @property
    def errors_topic(self) -> str:
        return run_topic("sconf", self.run_id, "errors")

    @property
    def all_topics(self) -> tuple[str, ...]:
        return (
            self.events_topic,
            self.intents_topic,
            self.results_topic,
            self.decisions_topic,
            self.output_topic,
            self.errors_topic,
        )

    @property
    def host_spool(self) -> Path:
        return HOST_SPOOL_ROOT / self.run_id

    @property
    def container_spool(self) -> str:
        return f"{CONTAINER_SPOOL_ROOT}/{self.run_id}"

    def key(self, scenario_name: str) -> bytes:
        return scenario_key(scenario_name, self.run_id)


@dataclass
class SparkLegResults:
    """Everything the per-scenario cells assert against, captured post-run."""

    adapter: str
    keys: dict[str, bytes]
    #: Distinct terminal output values (at-least-once collapsed by identity).
    outputs: set[bytes]
    #: Distinct serialized intents per entity key.
    intents_by_key: dict[bytes, set[bytes]] = field(default_factory=dict)
    #: Distinct ``reason|detail`` error values per entity key.
    errors_by_key: dict[bytes, set[bytes]] = field(default_factory=dict)

    def intents_for(self, scenario_name: str) -> set[bytes]:
        return self.intents_by_key.get(self.keys[scenario_name], set())

    def errors_for(self, scenario_name: str) -> set[bytes]:
        return self.errors_by_key.get(self.keys[scenario_name], set())


class _TopicWatcher:
    """Incrementally accumulates ``(key, value)`` records from one topic."""

    def __init__(self, topic: str) -> None:
        self._topic = topic
        self._consumer: Any = None
        self.by_key: dict[bytes, set[bytes]] = defaultdict(set)

    async def start(self) -> None:
        from aiokafka import AIOKafkaConsumer

        self._consumer = AIOKafkaConsumer(
            self._topic, bootstrap_servers=HOST_BROKERS, auto_offset_reset="earliest"
        )
        await self._consumer.start()

    async def poll(self) -> None:
        assert self._consumer is not None
        batches = await self._consumer.getmany(timeout_ms=500)
        for _tp, messages in batches.items():
            for message in messages:
                self.by_key[message.key or b""].add(message.value)

    def values(self) -> set[bytes]:
        return {value for values in self.by_key.values() for value in values}

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()


class SparkResponder:
    """Answers the multiplexed job's intents deterministically by key prefix.

    - Tool intents are answered promptly with ``OK / b"ack"``. Unlike the Flink
      leg there is nothing to park: the restart scenario is a declared spark
      skip, so no intent waits on a topology change.
    - Approval intents are parked until ``late_ready(key)`` confirms the
      fail-closed timeout terminal was *observed*, then answered — the late
      decision must be provably late, not wall-clock-probably late.

    Duplicate intents (at-least-once publisher, checkpoint-recovery replays)
    are answered idempotently: one decision per intent_id, byte-identical.
    """

    def __init__(self, config: SparkRunConfig, late_ready: Any) -> None:
        self._config = config
        self._late_ready = late_ready
        self._stopping = asyncio.Event()
        self._answered: set[str] = set()
        self._parked_approvals: list[ToolIntent] = []
        self.observed_intents: dict[bytes, set[str]] = defaultdict(set)

    def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

        consumer = AIOKafkaConsumer(
            self._config.intents_topic,
            bootstrap_servers=HOST_BROKERS,
            group_id=f"sconf-{self._config.run_id}-responder",
            auto_offset_reset="earliest",
        )
        producer = AIOKafkaProducer(bootstrap_servers=HOST_BROKERS)
        await consumer.start()
        await producer.start()
        try:
            while not self._stopping.is_set():
                batches = await consumer.getmany(timeout_ms=200)
                for _tp, messages in batches.items():
                    for message in messages:
                        intent = ToolIntent.FromString(message.value)
                        self.observed_intents[intent.entity_key].add(intent.intent_id)
                        if intent.kind == ToolIntent.APPROVAL:
                            self._parked_approvals.append(intent)
                        else:
                            await self._answer_tool(producer, intent)
                due = [i for i in self._parked_approvals if self._late_ready(i.entity_key)]
                self._parked_approvals = [
                    i for i in self._parked_approvals if not self._late_ready(i.entity_key)
                ]
                for intent in due:
                    await self._answer_approval(producer, intent)
        finally:
            with contextlib.suppress(Exception):
                await consumer.stop()
            with contextlib.suppress(Exception):
                await producer.stop()

    async def _answer_tool(self, producer: Any, intent: ToolIntent) -> None:
        result = ToolResult(
            intent_id=intent.intent_id,
            entity_key=intent.entity_key,
            seq=intent.seq,
            payload=b"ack",
            status=ToolResult.OK,
            # Deterministic per intent: duplicate requests get byte-identical
            # results, so at-least-once delivery can never diverge.
            completed_at_ms=intent.created_at_ms + 1,
        )
        await producer.send_and_wait(
            self._config.results_topic,
            key=intent.entity_key,
            value=result.SerializeToString(deterministic=True),
        )
        self._answered.add(intent.intent_id)

    async def _answer_approval(self, producer: Any, intent: ToolIntent) -> None:
        decision = AgentEnvelope.Approval(
            intent_id=intent.intent_id,
            approved=True,
            approver="conformance-responder",
            decided_at_ms=intent.created_at_ms + 1,
        )
        await producer.send_and_wait(
            self._config.decisions_topic,
            key=intent.entity_key,
            value=decision.SerializeToString(deterministic=True),
        )
        self._answered.add(intent.intent_id)


def _now_ms() -> int:
    return int(time.time() * 1000)


async def _publish_events(config: SparkRunConfig) -> None:
    from aiokafka import AIOKafkaProducer

    producer = AIOKafkaProducer(bootstrap_servers=HOST_BROKERS)
    await producer.start()
    try:
        for spec in SPARK_SCENARIOS:
            key = config.key(spec.name)
            envelope = AgentEnvelope(entity_key=key, event_time_ms=_now_ms(), external_event=b"go")
            await producer.send(config.events_topic, key=key, value=envelope.SerializeToString())
        await producer.flush()
    finally:
        await producer.stop()


def _start_pipeline_thread(config: SparkRunConfig, job_name: str) -> threading.Thread:
    def target() -> None:
        try:
            run_conformance_pipeline(
                config.adapter,
                container_spool=config.container_spool,
                intents_topic=config.intents_topic,
                output_topic=config.output_topic,
                errors_topic=config.errors_topic,
                job_name=job_name,
            )
        except Exception:  # the harness ends the run; the client raising is expected
            _LOG.info("pipeline client for %s ended", job_name, exc_info=True)

    thread = threading.Thread(target=target, name=f"spark-pipeline-{job_name}", daemon=True)
    thread.start()
    return thread


async def _submit_with_stall_retry(stack: SparkStackControl, config: SparkRunConfig) -> str:
    """Submit until the job demonstrably starts processing; bounded, and
    ``InfraFailure`` on exhaustion — never a red conformance verdict."""
    for attempt in range(1, SUBMIT_ATTEMPTS + 1):
        job_name = f"sconf-{config.run_id}-{config.adapter}-{attempt}"
        _start_pipeline_thread(config, job_name)
        submitted = time.monotonic()
        while time.monotonic() - submitted < SUBMIT_STALL_WINDOW_S:
            offsets = await topic_end_offsets(config.output_topic)
            intents = await topic_end_offsets(config.intents_topic)
            if sum(offsets.values()) + sum(intents.values()) > 0:
                return job_name
            await asyncio.sleep(_POLL_S)
        _LOG.warning(
            "spark submission %s made no progress; job server tail:\n%s",
            job_name,
            stack.jobserver_tail(),
        )
        stack.freshen_spark()
    raise InfraFailure(
        f"spark conformance job for adapter {config.adapter!r} never started processing "
        f"in {SUBMIT_ATTEMPTS} submissions (job-server or executor submission stall); "
        f"job server tail:\n{stack.jobserver_tail()}"
    )


async def run_adapter_leg(adapter_name: str) -> SparkLegResults:
    """Drive one adapter's full Spark conformance job to a captured matrix."""
    import uuid

    config = SparkRunConfig(run_id=uuid.uuid4().hex[:12], adapter=adapter_name)
    stack = SparkStackControl()
    keys = {spec.name: config.key(spec.name) for spec in SPARK_SCENARIOS}
    at_key = keys[APPROVAL_TIMEOUT_FALLBACK.name]
    timeout_terminal = APPROVAL_TIMEOUT_FALLBACK.expected_outputs[0]

    stack.freshen_spark()
    await create_topics(config.all_topics)
    config.host_spool.mkdir(parents=True, exist_ok=True)

    outputs = _TopicWatcher(config.output_topic)
    errors = _TopicWatcher(config.errors_topic)
    responder = SparkResponder(
        config,
        # The late decision releases on the OBSERVED fail-closed terminal,
        # never a wall clock (the e2e gate's pattern).
        late_ready=lambda key: key == at_key and timeout_terminal in outputs.values(),
    )
    drainer = Drainer(
        SpoolWriter(config.host_spool),
        brokers=HOST_BROKERS,
        events_topic=config.events_topic,
        results_topic=config.results_topic,
        decisions_topic=config.decisions_topic,
        group=f"sconf-{config.run_id}-drainer",
    )
    drainer_task = asyncio.create_task(drainer.run())
    responder_task = asyncio.create_task(responder.run())

    try:
        await outputs.start()
        await errors.start()
        await _publish_events(config)
        await _submit_with_stall_retry(stack, config)

        async def observed(predicate: Any, describe: str) -> None:
            start = time.monotonic()
            while time.monotonic() - start < PHASE_DEADLINE_S:
                await outputs.poll()
                await errors.poll()
                if predicate():
                    return
                await asyncio.sleep(_POLL_S)
            raise AssertionError(
                f"[adapter {adapter_name}] '{describe}' still unobserved after "
                f"{PHASE_DEADLINE_S:.0f}s; outputs so far: {sorted(outputs.values())!r}; "
                f"job server tail:\n{stack.jobserver_tail()}"
            )

        # Phase 1 — every scenario but the timeout one reaches its terminal.
        quick_terminals = {
            spec.expected_outputs[0]
            for spec in SPARK_SCENARIOS
            if spec.name != APPROVAL_TIMEOUT_FALLBACK.name
        }
        await observed(lambda: quick_terminals <= outputs.values(), "quick scenarios' terminals")

        # Phase 2 — the real-time HITL deadline fires the fail-closed fallback;
        # the gated late decision then surfaces as orphaned_result.
        await observed(
            lambda: timeout_terminal in outputs.values(),
            f"fail-closed timeout terminal for key prefix {APPROVAL_TIMEOUT_FALLBACK.name!r}",
        )
        await observed(
            lambda: any(v.startswith(b"orphaned_result") for v in errors.by_key.get(at_key, ())),
            f"orphaned late decision for key prefix {APPROVAL_TIMEOUT_FALLBACK.name!r}",
        )

        # Capture everything before teardown deletes the topics.
        intents_raw = await read_topic_all(config.intents_topic)
        intents_by_key: dict[bytes, set[bytes]] = defaultdict(set)
        for key, value in intents_raw:
            intents_by_key[key].add(value)
        errors_raw = await read_topic_all(config.errors_topic)
        errors_by_key: dict[bytes, set[bytes]] = defaultdict(set)
        for key, value in errors_raw:
            errors_by_key[key].add(value)
        outputs_all = {v for _k, v in await read_topic_all(config.output_topic)}
        return SparkLegResults(
            adapter=adapter_name,
            keys=keys,
            outputs=outputs_all,
            intents_by_key=dict(intents_by_key),
            errors_by_key=dict(errors_by_key),
        )
    finally:
        responder.stop()
        drainer.stop()
        for task in (drainer_task, responder_task):
            task.cancel()
        await asyncio.gather(drainer_task, responder_task, return_exceptions=True)
        await outputs.stop()
        await errors.stop()
        await delete_topics(config.all_topics)
        import shutil

        shutil.rmtree(config.host_spool, ignore_errors=True)
