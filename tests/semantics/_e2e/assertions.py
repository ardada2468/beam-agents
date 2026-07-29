"""Post-run readers, quiescence, and the infra-vs-invariant boundary.

Everything is asserted from *outside* the pipeline (design D6): plain aiokafka
consumers over the run's topics and a plain Redis client over the ledger,
after the run is driven to quiescence. Nothing is read through Beam's
serialization boundary, and message counts are never the property — every
transport hop is at-least-once by design, so assertions collapse duplicates
by identity and then check the identities.

Quiescence is condition-driven under a hard deadline (design R1): a
population-completeness predicate polled on a short interval, never a bare
sleep. On deadline, the failure is *classified*: if the Flink job is dead, the
worker pool is gone, or every effector died, that is an `InfraFailure`
("fix the environment"), not a red invariant.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import defaultdict
from collections.abc import Callable
from typing import Any

from tests.semantics._e2e.stack import HOST_BROKERS, InfraFailure, RunConfig


async def read_topic_all(
    topic: str, *, idle_rounds: int = 3, round_ms: int = 1500
) -> list[tuple[bytes, bytes]]:
    """Everything currently on ``topic``: reads until several idle polls."""
    from aiokafka import AIOKafkaConsumer

    consumer = AIOKafkaConsumer(topic, bootstrap_servers=HOST_BROKERS, auto_offset_reset="earliest")
    await consumer.start()
    try:
        records: list[tuple[bytes, bytes]] = []
        idle = 0
        while idle < idle_rounds:
            batches = await consumer.getmany(timeout_ms=round_ms)
            got = False
            for _tp, messages in batches.items():
                for message in messages:
                    records.append((message.key or b"", message.value))
                    got = True
            idle = 0 if got else idle + 1
        return records
    finally:
        await consumer.stop()


def group_by_key(records: list[tuple[bytes, bytes]]) -> dict[bytes, list[bytes]]:
    grouped: dict[bytes, list[bytes]] = defaultdict(list)
    for key, value in records:
        grouped[key].append(value)
    return dict(grouped)


def distinct_by_key(records: list[tuple[bytes, bytes]]) -> dict[bytes, set[bytes]]:
    return {k: set(vs) for k, vs in group_by_key(records).items()}


async def await_condition(
    describe: str,
    condition: Callable[[], bool],
    *,
    deadline_s: float,
    poll_s: float = 2.0,
    infra_check: Callable[[], None] | None = None,
    progress: Callable[[], str] | None = None,
) -> None:
    """Poll ``condition`` until true; classify the failure on deadline.

    ``infra_check`` runs each poll and raises `InfraFailure` on a dead stack —
    so infrastructure death surfaces immediately and is never misread as the
    invariant failing. On deadline with healthy infrastructure the failure IS
    the run's verdict: raise AssertionError with the progress snapshot.
    """
    start = time.monotonic()
    while time.monotonic() - start < deadline_s:
        if infra_check is not None:
            infra_check()
        if condition():
            return
        await asyncio.sleep(poll_s)
    snapshot = progress() if progress is not None else "(no progress reporter)"
    raise AssertionError(
        f"'{describe}' still false after {deadline_s:.0f}s with healthy "
        f"infrastructure — this is an invariant/liveness failure, not an "
        f"environment problem. Progress: {snapshot}"
    )


class OutputWatcher:
    """Incrementally tracks distinct terminal outputs per key on one topic."""

    def __init__(self, config: RunConfig) -> None:
        self._config = config
        self._consumer: Any = None
        self.terminals: dict[bytes, set[bytes]] = defaultdict(set)

    async def start(self) -> None:
        from aiokafka import AIOKafkaConsumer

        self._consumer = AIOKafkaConsumer(
            self._config.output_topic,
            bootstrap_servers=HOST_BROKERS,
            auto_offset_reset="earliest",
        )
        await self._consumer.start()

    async def poll(self) -> None:
        assert self._consumer is not None
        batches = await self._consumer.getmany(timeout_ms=500)
        for _tp, messages in batches.items():
            for message in messages:
                value = message.value
                # Terminal outputs are "result|<key>|…" / "decision|<key>|…";
                # the embedded key is authoritative (the Kafka message key for
                # .output publications is a constant).
                parts = value.split(b"|")
                if len(parts) >= 3 and parts[0] in (b"result", b"decision"):
                    self.terminals[parts[1]].add(value)

    def keys_with_terminal(self) -> set[bytes]:
        return set(self.terminals)

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()


class ErrorsWatcher:
    """Incrementally tracks error *reasons* per key on the errors topic.

    The late-population liveness condition needs `orphaned_result` observed
    DURING phase A — waiting for it only post-run would let the TM kill race
    the late decision's processing and erase the evidence the assertion
    needs. Values are `reason|detail`; only the reason is tracked.
    """

    def __init__(self, config: RunConfig) -> None:
        self._config = config
        self._consumer: Any = None
        self.reasons: dict[bytes, set[bytes]] = defaultdict(set)

    async def start(self) -> None:
        from aiokafka import AIOKafkaConsumer

        self._consumer = AIOKafkaConsumer(
            self._config.errors_topic,
            bootstrap_servers=HOST_BROKERS,
            auto_offset_reset="earliest",
        )
        await self._consumer.start()

    async def poll(self) -> None:
        assert self._consumer is not None
        batches = await self._consumer.getmany(timeout_ms=500)
        for _tp, messages in batches.items():
            for message in messages:
                self.reasons[message.key or b""].add(message.value.split(b"|")[0])

    def has_reason(self, key: bytes, reason: bytes) -> bool:
        return reason in self.reasons.get(key, set())

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()


async def topic_end_offsets(topic: str) -> dict[int, int]:
    """Current end offset per partition — the quiescence stability signal."""
    from aiokafka import AIOKafkaConsumer, TopicPartition

    # Subscribed on construction: an unsubscribed consumer never fetches the
    # topic's metadata, `partitions_for_topic` returns None, and this helper
    # would silently report {} forever (which read as "replay never
    # progressed" in the gate's phase B — a measurement bug, not a stall).
    consumer = AIOKafkaConsumer(topic, bootstrap_servers=HOST_BROKERS)
    await consumer.start()
    try:
        partition_ids = consumer.partitions_for_topic(topic)
        if not partition_ids:
            raise InfraFailure(f"no partition metadata for topic {topic!r}")
        partitions = [TopicPartition(topic, index) for index in partition_ids]
        offsets = await consumer.end_offsets(partitions)
        return {tp.partition: int(off) for tp, off in offsets.items()}
    finally:
        # aiokafka quirk: stopping a group-less consumer that never subscribed
        # can surface its own internal task's CancelledError from stop(). That
        # cancellation is the consumer's, not ours — if THIS task is being
        # cancelled, the next await re-raises anyway.
        with contextlib.suppress(asyncio.CancelledError):
            await consumer.stop()


def make_infra_check(
    *,
    pool_healthy: Callable[[], None],
    job_alive: Callable[[], None] | None = None,
) -> Callable[[], None]:
    def check() -> None:
        try:
            pool_healthy()
        except RuntimeError as exc:
            raise InfraFailure(str(exc)) from exc
        if job_alive is not None:
            job_alive()

    return check
