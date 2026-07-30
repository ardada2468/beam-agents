"""Host-side orchestration for the conformance Flink leg.

Per adapter: freshen the stack, provision run-scoped topics and a spool
directory, publish one event per Flink-runnable scenario, submit the
multiplexed job, and drive it to a fully-observed matrix — the responder
answers tool intents and approval requests deterministically by key prefix
(the late decision gated on the *observed* fail-closed terminal, per the e2e
gate's pattern), and the restart scenario's result is released only after the
TaskManager has been restarted between the observed intent and the publish.

Reuses the e2e harness's proven pieces wholesale: the drainer/spool ingest
(cross-language Kafka IO cannot run on this stack), the at-least-once topic
readers that collapse duplicates by identity, and the ``InfraFailure``
separation so stack problems never read as adapter failures.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from beam_agents._protos import AgentEnvelope, ToolIntent, ToolResult
from tests.conformance._flink.pipeline import run_conformance_pipeline, scenario_key
from tests.conformance._spec import (
    APPROVAL_TIMEOUT_FALLBACK,
    FLINK_SCENARIOS,
    RESTART_MID_SUSPENSION,
)
from tests.semantics._e2e.assertions import read_topic_all, topic_end_offsets
from tests.semantics._e2e.drainer import Drainer
from tests.semantics._e2e.spool import SpoolWriter
from tests.semantics._flink_stack import (
    CONTAINER_SPOOL_ROOT,
    HOST_BROKERS,
    HOST_SPOOL_ROOT,
    FlinkStackControl,
    InfraFailure,
    run_topic,
)

_LOG = logging.getLogger("beam_agents.conformance.flink")

# Condition-driven deadlines, never bare sleeps. The full leg per adapter is
# submission (~60s under load) + quick scenarios + a real-time 30s HITL
# deadline + a TaskManager restart and checkpoint recovery.
PHASE_DEADLINE_S = 420.0
SUBMIT_STALL_WINDOW_S = 150.0
SUBMIT_ATTEMPTS = 2
_POLL_S = 2.0


@dataclass(frozen=True)
class ConformanceRunConfig:
    """One adapter-leg run's world, named by run id (isolation is by run id)."""

    run_id: str
    adapter: str

    @property
    def events_topic(self) -> str:
        return run_topic("conf", self.run_id, "events")

    @property
    def intents_topic(self) -> str:
        return run_topic("conf", self.run_id, "intents")

    @property
    def results_topic(self) -> str:
        return run_topic("conf", self.run_id, "results")

    @property
    def decisions_topic(self) -> str:
        return run_topic("conf", self.run_id, "decisions")

    @property
    def output_topic(self) -> str:
        return run_topic("conf", self.run_id, "output")

    @property
    def errors_topic(self) -> str:
        return run_topic("conf", self.run_id, "errors")

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
class FlinkLegResults:
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


class ConformanceResponder:
    """Answers the multiplexed job's intents deterministically by key prefix.

    - Tool intents are answered promptly with ``OK / b"ack"`` — except the
      restart scenario's, which is parked until :meth:`release_restart` (the
      test restarts the TaskManager between the observed intent and the
      publish, per design D3).
    - Approval intents are parked until ``late_ready(key)`` confirms the
      fail-closed timeout terminal was *observed*, then answered — the late
      decision must be provably late, not wall-clock-probably late.

    Duplicate intents (at-least-once publisher, checkpoint-recovery replays)
    are answered idempotently: one decision per intent_id, byte-identical.
    """

    def __init__(self, config: ConformanceRunConfig, late_ready: Any) -> None:
        self._config = config
        self._late_ready = late_ready
        self._stopping = asyncio.Event()
        self._restart_released = asyncio.Event()
        self._answered: set[str] = set()
        self._parked_restart: list[ToolIntent] = []
        self._parked_approvals: list[ToolIntent] = []
        self.observed_intents: dict[bytes, set[str]] = defaultdict(set)

    def release_restart(self) -> None:
        self._restart_released.set()

    def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

        restart_key = self._config.key(RESTART_MID_SUSPENSION.name)
        consumer = AIOKafkaConsumer(
            self._config.intents_topic,
            bootstrap_servers=HOST_BROKERS,
            group_id=f"conf-{self._config.run_id}-responder",
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
                        elif intent.entity_key == restart_key:
                            self._parked_restart.append(intent)
                        else:
                            await self._answer_tool(producer, intent)
                if self._restart_released.is_set():
                    parked, self._parked_restart = self._parked_restart, []
                    for intent in parked:
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


async def _publish_events(config: ConformanceRunConfig) -> None:
    from aiokafka import AIOKafkaProducer

    producer = AIOKafkaProducer(bootstrap_servers=HOST_BROKERS)
    await producer.start()
    try:
        for spec in FLINK_SCENARIOS:
            key = config.key(spec.name)
            envelope = AgentEnvelope(entity_key=key, event_time_ms=_now_ms(), external_event=b"go")
            await producer.send(config.events_topic, key=key, value=envelope.SerializeToString())
        await producer.flush()
    finally:
        await producer.stop()


def _start_pipeline_thread(config: ConformanceRunConfig, job_name: str) -> threading.Thread:
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
        except Exception:  # the harness cancels the job; the client raising is expected
            _LOG.info("pipeline client for %s ended", job_name, exc_info=True)

    thread = threading.Thread(target=target, name=f"pipeline-{job_name}", daemon=True)
    thread.start()
    return thread


async def _submit_with_stall_retry(stack: FlinkStackControl, config: ConformanceRunConfig) -> str:
    """Submit until the job demonstrably starts processing (the F12 submission
    stall can hit any submission on this stack); bounded, InfraFailure on
    exhaustion — never a red conformance verdict."""
    for attempt in range(1, SUBMIT_ATTEMPTS + 1):
        job_name = f"conf-{config.run_id}-{config.adapter}-{attempt}"
        _start_pipeline_thread(config, job_name)
        submitted = time.monotonic()
        while time.monotonic() - submitted < SUBMIT_STALL_WINDOW_S:
            offsets = await topic_end_offsets(config.output_topic)
            intents = await topic_end_offsets(config.intents_topic)
            if sum(offsets.values()) + sum(intents.values()) > 0:
                return job_name
            await asyncio.sleep(_POLL_S)
        vertices = stack.job_vertex_summary(job_name)
        _LOG.warning(
            "submission %s stalled with zero-progress vertices (%s) — cancelling and resubmitting",
            job_name,
            vertices,
        )
        stack.await_no_running_jobs()
        stack.fresh_harness()
    raise InfraFailure(
        f"conformance job for adapter {config.adapter!r} never started processing "
        f"in {SUBMIT_ATTEMPTS} submissions (runner-level submission stall)"
    )


async def run_adapter_leg(adapter_name: str) -> FlinkLegResults:
    """Drive one adapter's full Flink conformance job to a captured matrix."""
    import uuid

    config = ConformanceRunConfig(run_id=uuid.uuid4().hex[:12], adapter=adapter_name)
    stack = FlinkStackControl()
    keys = {spec.name: config.key(spec.name) for spec in FLINK_SCENARIOS}
    at_key = keys[APPROVAL_TIMEOUT_FALLBACK.name]
    rs_key = keys[RESTART_MID_SUSPENSION.name]
    timeout_terminal = APPROVAL_TIMEOUT_FALLBACK.expected_outputs[0]

    stack.freshen_flink()
    await stack.create_topics(config.all_topics)
    config.host_spool.mkdir(parents=True, exist_ok=True)

    outputs = _TopicWatcher(config.output_topic)
    errors = _TopicWatcher(config.errors_topic)
    responder = ConformanceResponder(
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
        group=f"conf-{config.run_id}-drainer",
    )
    drainer_task = asyncio.create_task(drainer.run())
    responder_task = asyncio.create_task(responder.run())

    try:
        await outputs.start()
        await errors.start()
        await _publish_events(config)
        job_name = await _submit_with_stall_retry(stack, config)

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
                f"{PHASE_DEADLINE_S:.0f}s; job vertices: "
                f"{stack.job_vertex_summary(job_name)}; outputs so far: "
                f"{sorted(outputs.values())!r}"
            )

        # Phase 1 — the un-chaosed scenarios reach terminal, and the restart
        # scenario's suspend commit is proven by its observed intent.
        quick_terminals = {
            spec.expected_outputs[0]
            for spec in FLINK_SCENARIOS
            if spec.name not in (RESTART_MID_SUSPENSION.name, APPROVAL_TIMEOUT_FALLBACK.name)
        }
        await observed(lambda: quick_terminals <= outputs.values(), "quick scenarios' terminals")
        await observed(
            lambda: bool(responder.observed_intents.get(rs_key)),
            f"suspend intent for key prefix {RESTART_MID_SUSPENSION.name!r}",
        )

        # Phase 2 — the restart: tear the TaskManager down between the suspend
        # commit and the result delivery; the job recovers from its last
        # checkpoint, and only then is the parked result released.
        _LOG.info("[%s] restarting the TaskManager mid-suspension", adapter_name)
        stack.restart_taskmanager()
        responder.release_restart()
        await observed(
            lambda: RESTART_MID_SUSPENSION.expected_outputs[0] in outputs.values(),
            f"post-restart terminal for key prefix {RESTART_MID_SUSPENSION.name!r}",
        )

        # Phase 3 — the real-time HITL deadline fires the fail-closed
        # fallback; the gated late decision then surfaces as orphaned_result.
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
        return FlinkLegResults(
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
        stack._cancel_all_jobs()
        await stack.delete_topics(config.all_topics)
        import shutil

        shutil.rmtree(config.host_spool, ignore_errors=True)
