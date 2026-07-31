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
from typing import TYPE_CHECKING, Protocol, cast, runtime_checkable

from beam_agents._protos import ToolIntent

if TYPE_CHECKING:
    from beam_agents.effector.config import TransportSecurity

__all__ = [
    "DeliveredIntent",
    "InMemoryIntentSource",
    "IntentSource",
    "KafkaIntentSource",
    "PubSubIntentSource",
    "RevocationHandler",
    "build_intent_source",
]

# Called with a partition id when the transport takes that partition away (a
# consumer-group rebalance). The service stops the partition's task and releases
# any claim it holds but has not executed, so the new owner is not forced to
# wait out a lease.
RevocationHandler = Callable[[str], Awaitable[None]]


@dataclass(frozen=True)
class DeliveredIntent:
    """One intent as delivered, with what the source needs to commit it.

    ``payload``/``key`` are the *raw delivered bytes*, kept alongside the parsed
    message because a delivery that fails signature verification is preserved
    verbatim on the dead-letter channel: re-serializing the parsed message would
    publish something subtly different from what arrived, which is the one thing
    a forensic record must not do. They default to empty for the sources and
    tests that predate the dead-letter path; :meth:`raw_payload` falls back to a
    deterministic re-encode there.
    """

    intent: ToolIntent
    partition: str
    handle: object = None
    payload: bytes = b""
    key: bytes = b""

    def raw_payload(self) -> bytes:
        """The bytes as delivered, for republishing an intent verbatim.

        A dead letter must carry what the broker actually delivered: re-serializing
        the parsed message would drop unknown fields a newer producer set, and
        publish something subtly different from what failed. Falls back to a
        deterministic re-encode only for sources that never captured the frame
        (the in-memory one, and older recorded fixtures).
        """
        return self.payload or self.intent.SerializeToString(deterministic=True)

    def raw_key(self) -> bytes:
        """The partition key as delivered, with the same verbatim rationale."""
        return self.key or self.intent.entity_key


@runtime_checkable
class IntentSource(Protocol):
    """An ordered, committable stream of intents grouped into partitions."""

    async def start(self) -> None:
        """Connect and begin delivering. Must be called before iteration."""
        ...

    def __aiter__(self) -> AsyncIterator[DeliveredIntent]: ...

    async def commit(self, delivered: DeliveredIntent) -> None:
        """Acknowledge ``delivered``, advancing the source past it.

        Called only after the intent reached a terminal dedup state, so a
        crash before the commit redelivers rather than loses the intent.
        """
        ...

    def set_revocation_handler(self, handler: RevocationHandler | None) -> None:
        """Install the callback invoked when partitions are revoked, or clear it.

        A rebalance can take a partition mid-flight; the handler is how the
        service abandons in-flight work it no longer owns.
        """
        ...

    async def close(self) -> None:
        """Stop delivering and release the transport's resources. Idempotent."""
        ...


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
        """Queue the scripted deliveries, then a sentinel that ends the stream."""
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
        """Record ``delivered`` as committed."""
        self.committed.append(delivered)

    def set_revocation_handler(self, handler: RevocationHandler | None) -> None:
        """Install or clear the revocation callback."""
        self._revocation_handler = handler

    async def revoke(self, partition: str) -> None:
        """Simulate a rebalance taking ``partition`` away."""
        if self._revocation_handler is not None:
            await self._revocation_handler(partition)

    async def close(self) -> None:
        """Mark the source closed; nothing to release."""
        self.closed = True

    @property
    def committed_intent_ids(self) -> list[str]:
        """The ``intent_id`` of every committed delivery, in commit order."""
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

    def __init__(
        self,
        brokers: str,
        topic: str,
        group_id: str,
        *,
        security: TransportSecurity | None = None,
    ) -> None:
        from aiokafka import AIOKafkaConsumer

        self._topic = topic
        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=brokers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            # Resolved here, at client construction: the secret exists only
            # inside the client object, never on the config that named it.
            **(security.client_kwargs() if security is not None else {}),
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
        """Start the consumer and subscribe, installing the rebalance listener."""
        await self._consumer.start()
        self._consumer.subscribe([self._topic], listener=self._listener())

    async def __aiter__(self) -> AsyncIterator[DeliveredIntent]:
        async for message in self._consumer:
            yield DeliveredIntent(
                intent=_parse_intent(message.value),
                partition=f"{message.topic}:{message.partition}",
                handle=(message.topic, message.partition, message.offset),
                payload=message.value,
                key=message.key or b"",
            )

    async def commit(self, delivered: DeliveredIntent) -> None:
        """Commit the delivery's offset + 1 — Kafka commits the *next* offset to read."""
        from aiokafka import TopicPartition

        topic, partition, offset = cast("tuple[str, int, int]", delivered.handle)
        # Kafka commits the *next* offset to read.
        await self._consumer.commit({TopicPartition(topic, partition): offset + 1})

    def set_revocation_handler(self, handler: RevocationHandler | None) -> None:
        """Install or clear the callback invoked when a partition is revoked."""
        self._revocation_handler = handler

    async def close(self) -> None:
        """Stop the consumer, releasing its partition assignment."""
        await self._consumer.stop()


def _ordering_key_bytes(ordering_key: str) -> bytes:
    """Recover the raw ``entity_key`` from a Pub/Sub ordering key.

    ``WriteIntents`` publishes ``entity_key.hex()``, so the inverse is a hex
    decode — but a message published by anything else may carry an arbitrary
    string, and this runs on the *unverified* delivery path. Falling back to
    empty (``DeliveredIntent.raw_key`` then uses the parsed ``entity_key``)
    keeps a malformed ordering key from raising inside the consumer callback.
    """
    try:
        return bytes.fromhex(ordering_key)
    except ValueError:
        return b""


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
        # `google.cloud` is a namespace package, so mypy resolves `pubsub_v1`
        # only when google-cloud-pubsub is actually installed in the typecheck
        # environment. It is — the `test` group mirrors `google-adk`, which
        # pulls it transitively — so the former `# type: ignore[attr-defined]`
        # here now reads as an unused ignore. Same note as
        # actions/write_intents.py: if that transitive edge goes away this line
        # fails loudly with attr-defined and wants its ignore back.
        from google.cloud import pubsub_v1

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
        """Open the streaming pull, bridging its callback thread onto this loop.

        Warns when the subscription is not ordering-key enabled: without
        ordering, results for one entity may be published out of order.
        """
        self._loop = asyncio.get_running_loop()
        self._warn_if_unordered()

        def _on_message(message: object) -> None:
            delivered = DeliveredIntent(
                intent=_parse_intent(message.data),  # type: ignore[attr-defined]
                partition=message.ordering_key or "",  # type: ignore[attr-defined]
                handle=message,
                payload=message.data,  # type: ignore[attr-defined]
                key=_ordering_key_bytes(message.ordering_key or ""),  # type: ignore[attr-defined]
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
        """Ack the Pub/Sub message behind ``delivered``."""
        delivered.handle.ack()  # type: ignore[attr-defined]

    def set_revocation_handler(self, handler: RevocationHandler | None) -> None:
        """No-op: Pub/Sub has no partition assignment that can be revoked.

        A redelivery simply arrives at whichever subscriber holds the
        ordering key next, so there is no in-flight work to abandon.
        """
        # Pub/Sub has no partition assignment to revoke: a redelivery simply
        # arrives at whichever subscriber holds the ordering key next.
        return None

    async def close(self) -> None:
        """Cancel the streaming pull and close the subscriber client."""
        if self._future is not None:
            self._future.cancel()  # type: ignore[attr-defined]
        self._client.close()


def build_intent_source(
    scheme: str,
    parts: tuple[str, ...],
    *,
    consumer_group: str,
    security: TransportSecurity | None = None,
) -> IntentSource:
    """Construct the source a parsed transport URI names.

    ``security`` reaches Kafka only: Pub/Sub authenticates through Application
    Default Credentials, so there is nothing to configure — only IAM roles to
    grant, which ``docs/security.md`` enumerates.
    """
    if scheme == "kafka":
        brokers, topic = parts
        return KafkaIntentSource(brokers, topic, consumer_group, security=security)
    project, subscription = parts
    return PubSubIntentSource(project, subscription)
