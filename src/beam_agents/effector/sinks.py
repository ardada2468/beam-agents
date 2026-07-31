"""Result and approval sinks: where the effector publishes what it decided.

Results are published under the originating ``entity_key`` so re-injection lands
on the key that emitted the intent — the pipeline resumes a continuation only on
its own key. Approval-kind intents go to a separate channel verbatim, keyed the
same way.

There is one transport implementation per scheme (:class:`MessageSink`, raw
bytes under a key) and a thin :class:`ProtoResultSink` that serializes a
``ToolResult`` on top of it, so Kafka/Pub/Sub wiring is written once.

Client libraries are imported inside the adapter constructors: they are optional
dependencies and ``import beam_agents.effector`` must work without them.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from beam_agents._protos import ToolResult

if TYPE_CHECKING:
    from beam_agents.effector.config import TransportSecurity

__all__ = [
    "InMemoryMessageSink",
    "InMemoryResultSink",
    "KafkaMessageSink",
    "MessageSink",
    "ProtoResultSink",
    "PubSubMessageSink",
    "ResultSink",
    "build_message_sink",
    "build_result_sink",
]


@runtime_checkable
class MessageSink(Protocol):
    """Publishes opaque payloads under a partition/ordering key."""

    async def publish(self, key: bytes, payload: bytes) -> None:
        """Publish ``payload`` under ``key``, durably, before returning.

        Ordering and partitioning follow ``key``, which is the entity key:
        results for one entity stay in order relative to each other.
        """
        ...

    async def close(self) -> None:
        """Release the transport's resources. Idempotent."""
        ...


@runtime_checkable
class ResultSink(Protocol):
    """Publishes a `ToolResult` under its own ``entity_key``."""

    async def publish(self, result: ToolResult) -> None:
        """Publish ``result`` under its own ``entity_key``."""
        ...

    async def close(self) -> None:
        """Release the transport's resources. Idempotent."""
        ...


@dataclass
class InMemoryMessageSink:
    """Records published messages. Backs the offline unit lane."""

    published: list[tuple[bytes, bytes]] = field(default_factory=list)
    closed: bool = field(default=False, init=False)
    # Injectable failure, so publish-retry behavior is testable without a broker.
    fail: Callable[[int], None] | None = None
    _attempts: int = field(default=0, init=False)

    async def publish(self, key: bytes, payload: bytes) -> None:
        """Record the message; invoke ``fail`` first when scripted to fail."""
        self._attempts += 1
        if self.fail is not None:
            self.fail(self._attempts)
        self.published.append((key, payload))

    async def close(self) -> None:
        """Mark the sink closed; nothing to release."""
        self.closed = True

    @property
    def attempts(self) -> int:
        """How many publishes were attempted, including the ones ``fail`` rejected."""
        return self._attempts


@dataclass
class ProtoResultSink:
    """Serializes a `ToolResult` and publishes it under its ``entity_key``.

    Serialization is deterministic so a republished stored result is
    byte-identical to the one written at completion time.
    """

    inner: MessageSink

    async def publish(self, result: ToolResult) -> None:
        """Serialize ``result`` deterministically and publish it under its entity key."""
        await self.inner.publish(result.entity_key, result.SerializeToString(deterministic=True))

    async def close(self) -> None:
        """Close the wrapped message sink."""
        await self.inner.close()


@dataclass
class InMemoryResultSink:
    """Records published results. Backs the offline unit lane."""

    published: list[ToolResult] = field(default_factory=list)
    closed: bool = field(default=False, init=False)
    fail: Callable[[int], None] | None = None
    _attempts: int = field(default=0, init=False)

    async def publish(self, result: ToolResult) -> None:
        """Record the result; invoke ``fail`` first when scripted to fail."""
        self._attempts += 1
        if self.fail is not None:
            self.fail(self._attempts)
        self.published.append(result)

    async def close(self) -> None:
        """Mark the sink closed; nothing to release."""
        self.closed = True

    @property
    def attempts(self) -> int:
        """How many publishes were attempted, including the ones ``fail`` rejected."""
        return self._attempts

    @property
    def statuses(self) -> list[ToolResult.Status]:
        """The statuses of the recorded results, in publication order."""
        return [r.status for r in self.published]


class KafkaMessageSink:
    """`MessageSink` over an idempotent Kafka producer.

    The message key is the raw ``entity_key``, so the default partitioner sends
    a key's messages to one partition and their relative order survives —
    matching how ``WriteIntents`` writes the outbox.
    """

    def __init__(
        self, brokers: str, topic: str, *, security: TransportSecurity | None = None
    ) -> None:
        from aiokafka import AIOKafkaProducer

        self._topic = topic
        self._producer = AIOKafkaProducer(
            bootstrap_servers=brokers,
            enable_idempotence=True,
            # Resolved here, at client construction: the secret exists only
            # inside the client object, never on the config that named it.
            **(security.client_kwargs() if security is not None else {}),
        )
        self._started = False

    async def _ensure_started(self) -> None:
        if not self._started:
            await self._producer.start()
            self._started = True

    async def publish(self, key: bytes, payload: bytes) -> None:
        """Publish and wait for the broker acknowledgement before returning.

        ``send_and_wait`` rather than ``send``: the result must be durable
        before its source offset is committed, or a crash loses it.
        """
        await self._ensure_started()
        # `send_and_wait`, not `send`: the publish must be durable before the
        # offset is committed, otherwise a crash could lose the result.
        await self._producer.send_and_wait(self._topic, value=payload, key=key)

    async def close(self) -> None:
        """Stop the producer, flushing anything still buffered."""
        if self._started:
            await self._producer.stop()
            self._started = False


class PubSubMessageSink:
    """`MessageSink` over Pub/Sub with message ordering enabled.

    The ordering key is the hex of ``entity_key``, matching the convention
    ``WriteIntents`` uses on the intents topic, so a key's messages stay in
    order on the results topic too.
    """

    def __init__(self, project: str, topic: str) -> None:
        # `google.cloud` is a namespace package, so mypy resolves `pubsub_v1`
        # only when google-cloud-pubsub is actually installed in the typecheck
        # environment. It is — the `test` group mirrors `google-adk`, which
        # pulls it transitively — so the former `# type: ignore[attr-defined]`
        # here now reads as an unused ignore. Same note as
        # actions/write_intents.py: if that transitive edge goes away this line
        # fails loudly with attr-defined and wants its ignore back.
        from google.cloud import pubsub_v1
        from google.cloud.pubsub_v1.types import PublisherOptions

        self._client = pubsub_v1.PublisherClient(
            publisher_options=PublisherOptions(enable_message_ordering=True)
        )
        self._topic = self._client.topic_path(project, topic)

    async def publish(self, key: bytes, payload: bytes) -> None:
        """Publish with ``key`` as the Pub/Sub ordering key, awaiting the ack."""
        import asyncio

        future = self._client.publish(self._topic, payload, ordering_key=key.hex())
        # The client is thread-based; wait for the publish to be acknowledged
        # without blocking the loop.
        await asyncio.to_thread(future.result)

    async def close(self) -> None:
        """Stop the publisher client's background threads."""
        self._client.stop()


def build_message_sink(
    scheme: str, parts: tuple[str, ...], *, security: TransportSecurity | None = None
) -> MessageSink:
    """Construct the sink a parsed transport URI names.

    ``security`` reaches Kafka only: Pub/Sub authenticates through Application
    Default Credentials.
    """
    if scheme == "kafka":
        brokers, topic = parts
        return KafkaMessageSink(brokers, topic, security=security)
    project, topic = parts
    return PubSubMessageSink(project, topic)


def build_result_sink(
    scheme: str, parts: tuple[str, ...], *, security: TransportSecurity | None = None
) -> ResultSink:
    """Construct a `ResultSink` over the transport a parsed URI names."""
    return ProtoResultSink(build_message_sink(scheme, parts, security=security))
