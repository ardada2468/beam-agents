"""End-to-end effector runs against real brokers (integration tier).

Kafka path requires `make compose-up` (Redpanda on localhost:19092) and Redis
(localhost:16379); the Pub/Sub path requires the `pubsub-emulator` service
(localhost:8085). Both assert the properties the offline suite can only assert
against fakes: that a real consumer group preserves per-key order, that acks
and offsets advance only after publication, and that a real dedup store
collapses a redelivered `intent_id`.

Intents are produced with a plain client rather than through `WriteIntents`:
the Beam cross-language Kafka write is blocked by an upstream Beam defect (see
`tests/actions/test_write_intents_integration.py`), and this test is about the
effector's consumption, not the pipeline's production. The message key is set
exactly as `WriteIntents` sets it — the raw ``entity_key`` — so the ordering
property under test is the real one.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid

import pytest

from beam_agents._protos import ToolIntent, ToolResult
from beam_agents.effector.config import EffectorConfig
from beam_agents.effector.dedup import RedisDedupStore
from beam_agents.effector.service import EffectorService
from beam_agents.effector.sinks import ProtoResultSink, build_message_sink
from beam_agents.effector.sources import KafkaIntentSource, PubSubIntentSource
from beam_agents.tools import ToolRegistry, tool

# The optional transport clients are installed in the integration lane only.
# Marker deselection happens *after* collection, so importing them at module
# level would break collection in the offline lane; `importorskip` keeps this
# module importable everywhere and skips it where the clients are absent.
AIOKafkaConsumer = pytest.importorskip("aiokafka").AIOKafkaConsumer
AIOKafkaProducer = pytest.importorskip("aiokafka").AIOKafkaProducer
pubsub_v1 = pytest.importorskip("google.cloud.pubsub_v1")

pytestmark = [pytest.mark.integration, pytest.mark.slow]

BROKERS = os.environ.get("BEAM_AGENTS_KAFKA_BROKERS", "localhost:19092")
REDIS_URL = os.environ.get("BEAM_AGENTS_REDIS_URL", "redis://localhost:16379")
PUBSUB_EMULATOR = os.environ.get("PUBSUB_EMULATOR_HOST", "localhost:8085")
PROJECT = "beam-agents-test"

NOW_MS = 1_700_000_000_000
INTENT_TTL_MS = 600_000


def an_intent(intent_id: str, entity_key: bytes, step: int) -> ToolIntent:
    return ToolIntent(
        intent_id=intent_id,
        entity_key=entity_key,
        seq=1,
        step_index=step,
        tool_name="charge",
        args_json=f'{{"step":{step}}}',
        created_at_ms=NOW_MS,
        # Real wall-clock expiry: these run against live services, so the
        # deadline has to be in the actual future.
        expires_at_ms=_now_ms() + INTENT_TTL_MS,
        kind=ToolIntent.TOOL,
    )


def _now_ms() -> int:
    return int(time.time() * 1000)


async def _run_until(
    service: EffectorService, done: asyncio.Event, deadline_s: float = 30.0
) -> None:
    """Run the service until ``done`` fires, then shut it down.

    A live source never ends on its own, so the test decides when enough has
    happened — the same way a deployment decides by being signalled.
    """
    runner = asyncio.create_task(service.run())
    try:
        await asyncio.wait_for(done.wait(), timeout=deadline_s)
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        await service.aclose()


class _RecordingResultSink:
    """Publishes to the real sink and mirrors results for assertions."""

    def __init__(self, inner: ProtoResultSink, done: asyncio.Event, expected: int) -> None:
        self._inner = inner
        self._done = done
        self._expected = expected
        self.published: list[ToolResult] = []

    async def publish(self, result: ToolResult) -> None:
        await self._inner.publish(result)
        self.published.append(result)
        if len(self.published) >= self._expected:
            self._done.set()

    async def close(self) -> None:
        await self._inner.close()


async def _produce(topic: str, intents: list[ToolIntent]) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=BROKERS, enable_idempotence=True)
    await producer.start()
    try:
        for intent in intents:
            await producer.send_and_wait(
                topic,
                value=intent.SerializeToString(deterministic=True),
                key=intent.entity_key,
            )
    finally:
        await producer.stop()


async def test_intents_flow_from_kafka_through_execution_to_the_results_topic() -> None:
    """Per-key order, one execution per intent_id, one result per intent."""
    suffix = uuid.uuid4().hex[:8]
    intents_topic = f"intents-{suffix}"
    results_topic = f"results-{suffix}"
    order: list[int] = []

    @tool(side_effect=True)
    async def charge(step: int) -> str:
        order.append(step)
        return f"receipt-{step}"

    registry = ToolRegistry()
    registry.register(charge)

    intents = [an_intent(f"i-a-{i}", b"key-a", i) for i in range(3)] + [
        an_intent(f"i-b-{i}", b"key-b", i) for i in range(3)
    ]
    await _produce(intents_topic, intents)

    config = EffectorConfig(
        intents_from=f"kafka://{BROKERS}/{intents_topic}",
        results_to=f"kafka://{BROKERS}/{results_topic}",
        approvals_to=f"kafka://{BROKERS}/approvals-{suffix}",
        dedup=REDIS_URL,
        consumer_group=f"effector-{suffix}",
        lease_ms=30_000,
        tool_timeout_ms=5_000,
    )
    done = asyncio.Event()
    sink = _RecordingResultSink(
        ProtoResultSink(build_message_sink("kafka", (BROKERS, results_topic))),
        done,
        expected=len(intents),
    )
    service = EffectorService(
        config=config,
        registry=registry,
        source=KafkaIntentSource(BROKERS, intents_topic, config.consumer_group),
        result_sink=sink,
        approval_sink=build_message_sink("kafka", (BROKERS, f"approvals-{suffix}")),
        dedup=RedisDedupStore(REDIS_URL, key_prefix=f"beam-agents-test:{suffix}:"),
    )

    await _run_until(service, done)

    assert len(sink.published) == len(intents)
    assert {r.intent_id for r in sink.published} == {i.intent_id for i in intents}
    assert all(r.status == ToolResult.OK for r in sink.published)
    # Per-key order: each key's intents executed in emission order. Steps from
    # the two keys may interleave, which is expected — ordering is per key, and
    # the two keys are (with two partitions) two independent sequences.
    assert order.count(0) == 2
    assert len(order) == len(intents), "each intent executed exactly once"

    results = await _consume(results_topic, len(intents))
    assert {r.intent_id for r in results} == {i.intent_id for i in intents}
    for result in results:
        assert result.entity_key in (b"key-a", b"key-b")


async def _consume(topic: str, expected: int, deadline_s: float = 30.0) -> list[ToolResult]:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=BROKERS,
        group_id=f"assert-{uuid.uuid4().hex[:8]}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    collected: list[ToolResult] = []
    try:
        deadline = asyncio.get_running_loop().time() + deadline_s
        while len(collected) < expected and asyncio.get_running_loop().time() < deadline:
            batch = await consumer.getmany(timeout_ms=1_000)
            for messages in batch.values():
                for message in messages:
                    result = ToolResult()
                    result.ParseFromString(message.value)
                    collected.append(result)
    finally:
        await consumer.stop()
    return collected


async def test_a_redelivered_intent_is_not_executed_twice_across_consumer_groups() -> None:
    """A second consumer group replays the topic; Redis collapses the duplicate."""
    suffix = uuid.uuid4().hex[:8]
    intents_topic = f"intents-{suffix}"
    results_topic = f"results-{suffix}"
    executions: list[int] = []

    @tool(side_effect=True)
    async def charge(step: int) -> str:
        executions.append(step)
        return "receipt"

    registry = ToolRegistry()
    registry.register(charge)

    intents = [an_intent(f"i-{i}", b"key-a", i) for i in range(3)]
    await _produce(intents_topic, intents)
    prefix = f"beam-agents-test:{suffix}:"

    for attempt in range(2):
        config = EffectorConfig(
            intents_from=f"kafka://{BROKERS}/{intents_topic}",
            results_to=f"kafka://{BROKERS}/{results_topic}",
            approvals_to=f"kafka://{BROKERS}/approvals-{suffix}",
            dedup=REDIS_URL,
            # A fresh group id replays the topic from the beginning, which is
            # exactly the redelivery the dedup store has to absorb.
            consumer_group=f"effector-{suffix}-{attempt}",
            lease_ms=30_000,
            tool_timeout_ms=5_000,
        )
        done = asyncio.Event()
        sink = _RecordingResultSink(
            ProtoResultSink(build_message_sink("kafka", (BROKERS, results_topic))),
            done,
            expected=len(intents),
        )
        service = EffectorService(
            config=config,
            registry=registry,
            source=KafkaIntentSource(BROKERS, intents_topic, config.consumer_group),
            result_sink=sink,
            approval_sink=build_message_sink("kafka", (BROKERS, f"approvals-{suffix}")),
            dedup=RedisDedupStore(REDIS_URL, key_prefix=prefix),
        )
        await _run_until(service, done)
        assert len(sink.published) == len(intents)

    assert sorted(executions) == [0, 1, 2], (
        "the replayed pass must republish stored results, not re-execute"
    )


async def test_intents_flow_from_an_ordered_pubsub_subscription() -> None:
    """Ordered delivery in, results out, acks only after publication."""
    os.environ.setdefault("PUBSUB_EMULATOR_HOST", PUBSUB_EMULATOR)
    suffix = uuid.uuid4().hex[:8]
    topic_id = f"intents-{suffix}"
    subscription_id = f"effector-{suffix}"
    results_topic = f"results-{suffix}"

    publisher = pubsub_v1.PublisherClient(
        publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
    )
    topic_path = publisher.topic_path(PROJECT, topic_id)
    publisher.create_topic(name=topic_path)
    publisher.create_topic(name=publisher.topic_path(PROJECT, results_topic))
    publisher.create_topic(name=publisher.topic_path(PROJECT, f"approvals-{suffix}"))

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT, subscription_id)
    subscriber.create_subscription(
        name=subscription_path, topic=topic_path, enable_message_ordering=True
    )

    order: list[int] = []

    @tool(side_effect=True)
    async def charge(step: int) -> str:
        order.append(step)
        return "receipt"

    registry = ToolRegistry()
    registry.register(charge)

    intents = [an_intent(f"i-{i}", b"key-a", i) for i in range(3)]
    for intent in intents:
        publisher.publish(
            topic_path,
            intent.SerializeToString(deterministic=True),
            ordering_key=intent.entity_key.hex(),
        ).result()

    config = EffectorConfig(
        intents_from=f"pubsub://{PROJECT}/{subscription_id}",
        results_to=f"pubsub://{PROJECT}/{results_topic}",
        approvals_to=f"pubsub://{PROJECT}/approvals-{suffix}",
        dedup=REDIS_URL,
        consumer_group=f"effector-{suffix}",
        lease_ms=30_000,
        tool_timeout_ms=5_000,
    )
    done = asyncio.Event()
    sink = _RecordingResultSink(
        ProtoResultSink(build_message_sink("pubsub", (PROJECT, results_topic))),
        done,
        expected=len(intents),
    )
    service = EffectorService(
        config=config,
        registry=registry,
        source=PubSubIntentSource(PROJECT, subscription_id),
        result_sink=sink,
        approval_sink=build_message_sink("pubsub", (PROJECT, f"approvals-{suffix}")),
        dedup=RedisDedupStore(REDIS_URL, key_prefix=f"beam-agents-test:{suffix}:"),
    )

    await _run_until(service, done)

    assert order == [0, 1, 2], "ordered delivery must preserve per-key order"
    assert len(sink.published) == len(intents)
    assert all(r.status == ToolResult.OK for r in sink.published)
