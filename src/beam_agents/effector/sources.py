"""Intent sources: where the effector reads the outbox from.

Ordering is inherited from the transport, not reconstructed here (see the change
design, D2). ``WriteIntents`` writes each intent under its ``entity_key`` as the
Kafka message key / Pub/Sub ordering key, so a key never spans partitions; a
consumer group gives each partition to exactly one member; and the service runs
one sequential task per partition. Scale-out therefore preserves per-key order
without any distributed lock.

Every delivery carries the ``partition`` it belongs to (the unit of sequencing)
and an opaque ``handle`` the source needs to commit it. Commits happen only
after the result is published, so a crash re-delivers rather than loses.

Client libraries are imported inside the adapter constructors: they are optional
dependencies and ``import beam_agents.effector`` must work without them.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import Protocol, cast, runtime_checkable

from beam_agents._protos import ToolIntent

# Called with a partition id when the transport takes that partition away (a
# consumer-group rebalance). The service stops the partition's task and releases
# any claim it holds but has not executed, so the new owner is not forced to
# wait out a lease.
RevocationHandler = Callable[[str], Awaitable[None]]


@dataclass(frozen=True)
class DeliveredIntent:
    """One intent as delivered, with what the source needs to commit it."""

    intent: ToolIntent
    partition: str
    handle: object = None


@runtime_checkable
class IntentSource(Protocol):
    """An ordered, committable stream of intents grouped into partitions."""

    async def start(self) -> None: ...

    def __aiter__(self) -> AsyncIterator[DeliveredIntent]: ...

    async def commit(self, delivered: DeliveredIntent) -> None: ...

    def set_revocation_handler(self, handler: RevocationHandler | None) -> None: ...

    async def close(self) -> None: ...


@dataclass
class InMemoryIntentSource:
    """A scripted `IntentSource` that records what was committed.

    Backs the offline unit lane and single-process embedding. ``committed``
    keeps deliveries in commit order, which is how "the offset is committed only
    after the result is published" becomes an assertion rather than a comment.
    """

    deliveries: list[DeliveredIntent] = field(default_factory=list)
    committed: list[DeliveredIntent] = field(default_factory=list)
    closed: bool = field(default=False, init=False)
    _revocation_handler: RevocationHandler | None = field(default=None, init=False)
    # Left open so a test can feed deliveries while the service runs; the
    # service stops when the source is exhausted and `complete` is set.
    _queue: asyncio.Queue[DeliveredIntent | None] = field(default_factory=asyncio.Queue, init=False)
    _started: bool = field(default=False, init=False)

    @classmethod
    def of(cls, intents: Iterable[ToolIntent], *, partition: str = "p-0") -> InMemoryIntentSource:
        """Build a source delivering ``intents`` in order on one partition."""
        source = cls()
        for index, intent in enumerate(intents):
            source.deliveries.append(
                DeliveredIntent(intent=intent, partition=partition, handle=index)
            )
        return source

    async def start(self) -> None:
        self._started = True
        for delivery in self.deliveries:
            self._queue.put_nowait(delivery)
        # Sentinel: the scripted stream is finite, so the service's dispatcher
        # terminates instead of hanging.
        self._queue.put_nowait(None)

    async def __aiter__(self) -> AsyncIterator[DeliveredIntent]:
        while True:
            item = await self._queue.get()
            if item is None:
                return
            yield item

    async def commit(self, delivered: DeliveredIntent) -> None:
        self.committed.append(delivered)

    def set_revocation_handler(self, handler: RevocationHandler | None) -> None:
        self._revocation_handler = handler

    async def revoke(self, partition: str) -> None:
        """Simulate a rebalance taking ``partition`` away."""
        if self._revocation_handler is not None:
            await self._revocation_handler(partition)

    async def close(self) -> None:
        self.closed = True

    @property
    def committed_intent_ids(self) -> list[str]:
        return [d.intent.intent_id for d in self.committed]


def _parse_intent(payload: bytes) -> ToolIntent:
    intent = ToolIntent()
    intent.ParseFromString(payload)
    return intent


class KafkaIntentSource:
    """`IntentSource` over a Kafka consumer group.

    The group is the ordering mechanism: partition assignment is exclusive, so a
    key (which hashes to one partition) is processed by exactly one member at a
    time. Auto-commit is disabled — the service commits explicitly, after
    publishing.
    """

    def __init__(self, brokers: str, topic: str, group_id: str) -> None:
        from aiokafka import AIOKafkaConsumer

        self._topic = topic
        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=brokers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        self._revocation_handler: RevocationHandler | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def _listener(self) -> object:
        from aiokafka import ConsumerRebalanceListener

        source = self

        class _Listener(ConsumerRebalanceListener):  # type: ignore[misc]
            async def on_partitions_revoked(self, revoked: Iterable[object]) -> None:
                handler = source._revocation_handler
                if handler is None:
                    return
                for tp in revoked:
                    await handler(_partition_id(tp))

            async def on_partitions_assigned(self, assigned: Iterable[object]) -> None:
                return None

        return _Listener()

    async def start(self) -> None:
        await self._consumer.start()
        self._consumer.subscribe([self._topic], listener=self._listener())

    async def __aiter__(self) -> AsyncIterator[DeliveredIntent]:
        async for message in self._consumer:
            yield DeliveredIntent(
                intent=_parse_intent(message.value),
                partition=f"{message.topic}:{message.partition}",
                handle=(message.topic, message.partition, message.offset),
            )

    async def commit(self, delivered: DeliveredIntent) -> None:
        from aiokafka import TopicPartition

        topic, partition, offset = cast("tuple[str, int, int]", delivered.handle)
        # Kafka commits the *next* offset to read.
        await self._consumer.commit({TopicPartition(topic, partition): offset + 1})

    def set_revocation_handler(self, handler: RevocationHandler | None) -> None:
        self._revocation_handler = handler

    async def close(self) -> None:
        await self._consumer.stop()


def _partition_id(topic_partition: object) -> str:
    return f"{topic_partition.topic}:{topic_partition.partition}"  # type: ignore[attr-defined]


class PubSubIntentSource:
    """`IntentSource` over a Pub/Sub subscription with message ordering.

    Ordered delivery must be enabled on the subscription — a deployment
    precondition this adapter cannot set, the mirror of the one ``WriteIntents``
    documents on the publish side. Pub/Sub withholds the next message of an
    ordering key until the previous one is acked, so acking only after
    publishing is what keeps per-key order intact.

    The client is callback- and thread-based, so deliveries are handed to the
    event loop through a bounded queue; the bound is the backpressure.
    """

    def __init__(self, project: str, subscription: str, *, max_queued: int = 64) -> None:
        # google.cloud is a namespace package; mypy can't see pubsub_v1 as an
        # attribute of it (same as actions/write_intents.py).
        from google.cloud import pubsub_v1  # type: ignore[attr-defined]

        self._client = pubsub_v1.SubscriberClient()
        self._path = self._client.subscription_path(project, subscription)
        self._queue: asyncio.Queue[DeliveredIntent] = asyncio.Queue(maxsize=max_queued)
        self._future: object | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def _warn_if_unordered(self) -> None:
        import logging

        try:
            subscription = self._client.get_subscription(subscription=self._path)
        except Exception:  # a permissions gap must not stop the service
            return
        if not getattr(subscription, "enable_message_ordering", False):
            logging.getLogger(__name__).warning(
                "subscription %s does not have message ordering enabled: per-key intent order "
                "is not guaranteed and the effector cannot enforce it",
                self._path,
            )

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._warn_if_unordered()

        def _on_message(message: object) -> None:
            delivered = DeliveredIntent(
                intent=_parse_intent(message.data),  # type: ignore[attr-defined]
                partition=message.ordering_key or "",  # type: ignore[attr-defined]
                handle=message,
            )
            assert self._loop is not None
            # Blocking put from the client's thread: full queue means the
            # service is saturated, and blocking here is the backpressure.
            asyncio.run_coroutine_threadsafe(self._queue.put(delivered), self._loop).result()

        self._future = self._client.subscribe(self._path, callback=_on_message)

    async def __aiter__(self) -> AsyncIterator[DeliveredIntent]:
        while True:
            yield await self._queue.get()

    async def commit(self, delivered: DeliveredIntent) -> None:
        delivered.handle.ack()  # type: ignore[attr-defined]

    def set_revocation_handler(self, handler: RevocationHandler | None) -> None:
        # Pub/Sub has no partition assignment to revoke: a redelivery simply
        # arrives at whichever subscriber holds the ordering key next.
        return None

    async def close(self) -> None:
        if self._future is not None:
            self._future.cancel()  # type: ignore[attr-defined]
        self._client.close()


def build_intent_source(
    scheme: str, parts: tuple[str, ...], *, consumer_group: str
) -> IntentSource:
    """Construct the source a parsed transport URI names."""
    if scheme == "kafka":
        brokers, topic = parts
        return KafkaIntentSource(brokers, topic, consumer_group)
    project, subscription = parts
    return PubSubIntentSource(project, subscription)
