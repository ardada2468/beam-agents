"""The spool drainer: Kafka → segment files, continuously, in the test process.

The pipeline's inputs are three topics — events, plus the two re-injection
streams (results, approvals) — and re-injection traffic arrives mid-run, so
the drainer tails all of them for the whole run rather than pre-filling the
spool once. Records are wrapped into ``AgentEnvelope`` exactly as production
re-injection would: a results-topic message becomes ``envelope.tool_result``,
an approvals-topic *decision* becomes ``envelope.approval``; events pass
through as-is (they are already serialized envelopes).

Host-side only (imports aiokafka); the container side of the spool is
``spool.SpoolSourceDoFn``.
"""

from __future__ import annotations

import asyncio
import contextlib
import time

from beam_agents._protos import AgentEnvelope, ToolResult
from tests.semantics._e2e.spool import SpoolWriter

# Seal cadence: small enough that re-injected results reach the pipeline
# quickly, large enough not to produce thousands of one-record segments.
_SEAL_INTERVAL_S = 0.5


def _now_ms() -> int:
    return int(time.time() * 1000)


def should_seal(pending: int, *, idle: bool, elapsed: bool) -> bool:
    """Seal when appended records are waiting AND the moment is right.

    "Right" is: the seal interval elapsed (bounds re-injection latency under
    sustained traffic), OR the poll came back idle (a burst that finished
    within the interval must still become visible — the original bug sealed
    only when new messages and an elapsed interval coincided, so a quick
    burst followed by silence sat unsealed forever).
    """
    return pending > 0 and (idle or elapsed)


class Drainer:
    """Tail events/results/approval-decision topics into one spool, in order.

    ``run()`` consumes until ``stop()`` is called, then seals and writes the
    EOF sentinel via ``writer.close()``. One drainer per run; per-partition
    consumption order is preserved into segment order by the single writer.
    """

    def __init__(
        self,
        writer: SpoolWriter,
        *,
        brokers: str,
        events_topic: str,
        results_topic: str,
        decisions_topic: str,
        group: str,
    ) -> None:
        self._writer = writer
        self._brokers = brokers
        self._events_topic = events_topic
        self._results_topic = results_topic
        self._decisions_topic = decisions_topic
        self._group = group
        self._stopping = asyncio.Event()
        self._drained = 0

    @property
    def drained_count(self) -> int:
        return self._drained

    def _wrap(self, topic: str, key: bytes, value: bytes) -> AgentEnvelope:
        if topic == self._events_topic:
            return AgentEnvelope.FromString(value)
        if topic == self._results_topic:
            result = ToolResult.FromString(value)
            return AgentEnvelope(entity_key=key, event_time_ms=_now_ms(), tool_result=result)
        envelope = AgentEnvelope(entity_key=key, event_time_ms=_now_ms())
        envelope.approval.ParseFromString(value)
        return envelope

    async def run(self) -> None:
        from aiokafka import AIOKafkaConsumer

        consumer = AIOKafkaConsumer(
            self._events_topic,
            self._results_topic,
            self._decisions_topic,
            bootstrap_servers=self._brokers,
            group_id=self._group,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        await consumer.start()
        try:
            last_seal = time.monotonic()
            pending = 0
            while not self._stopping.is_set():
                batches = await consumer.getmany(timeout_ms=200)
                got = False
                for tp, messages in batches.items():
                    for message in messages:
                        self._writer.append(self._wrap(tp.topic, message.key or b"", message.value))
                        self._drained += 1
                        pending += 1
                        got = True
                now = time.monotonic()
                if should_seal(pending, idle=not got, elapsed=now - last_seal >= _SEAL_INTERVAL_S):
                    self._writer.seal()
                    last_seal = now
                    pending = 0
        finally:
            with contextlib.suppress(Exception):
                await consumer.stop()
            self._writer.close()

    def stop(self) -> None:
        self._stopping.set()
