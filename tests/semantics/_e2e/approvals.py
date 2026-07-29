"""The approval feeder: the human in the loop, simulated deterministically.

Consumes approval *requests* (serialized ``ToolIntent``s the effector routes
to its approvals channel) and publishes *decisions* (serialized
``AgentEnvelope.Approval``) onto the decisions topic, where the drainer wraps
them back into the pipeline for re-injection on the same key.

Decision policy, all key-deterministic so the assertions can predict it:

- ``a-…`` keys: answered promptly; approved when the key's index is even,
  denied when odd.
- ``late-…`` keys: answered only once ``late_ready(entity_key)`` says the
  fail-closed timer has demonstrably decided — the gate wires it to "the
  ``decision|<key>|timeout`` terminal was observed on the output topic".
  Gating on the observed terminal rather than wall clock is what makes the
  orphan deterministic: a fixed delay races both the HITL timer (whose
  real-time firing lags under CI load) and the phase A→B boundary, and a
  "late" answer that loses either race resumes the suspension normally
  instead of surfacing as ``orphaned_result``. Without a predicate the
  feeder falls back to ``LATE_REPLY_DELAY_S`` from seeing the request.

Duplicate requests (the effector republishes on recovery) are answered
idempotently: the same intent_id always gets the same decision bytes, so
downstream sees byte-identical duplicates, never a disagreement.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Callable

from beam_agents._protos import AgentEnvelope, ToolIntent
from tests.semantics._e2e.agent import LATE_PREFIX
from tests.semantics._e2e.stack import HOST_BROKERS, RunConfig

# Fallback-only (no `late_ready` predicate): comfortably past
# agent.LATE_HITL_TIMEOUT_MS (30 s).
LATE_REPLY_DELAY_S = 45.0


def split_due(
    queue: list[tuple[float, ToolIntent]],
    now: float,
    late_ready: Callable[[bytes], bool] | None,
) -> tuple[list[tuple[float, ToolIntent]], list[tuple[float, ToolIntent]]]:
    """Partition the late queue into (due, kept), pure so the unit tier can
    pin it: gated mode releases exactly when the predicate confirms the
    fail-closed terminal; time mode releases at the recorded deadline."""
    due: list[tuple[float, ToolIntent]] = []
    kept: list[tuple[float, ToolIntent]] = []
    for not_before, intent in queue:
        ready = late_ready(intent.entity_key) if late_ready is not None else not_before <= now
        (due if ready else kept).append((not_before, intent))
    return due, kept


def decide(intent: ToolIntent) -> AgentEnvelope.Approval:
    """The deterministic decision for one approval request."""
    index = int(intent.entity_key.rsplit(b"-", 1)[-1])
    return AgentEnvelope.Approval(
        intent_id=intent.intent_id,
        approved=index % 2 == 0,
        approver="e2e-feeder",
        # Deterministic per intent, so republished requests produce
        # byte-identical decisions.
        decided_at_ms=intent.created_at_ms + 1,
    )


class ApprovalFeeder:
    def __init__(
        self, config: RunConfig, *, late_ready: Callable[[bytes], bool] | None = None
    ) -> None:
        self._config = config
        self._late_ready = late_ready
        self._stopping = asyncio.Event()
        self._answered: set[str] = set()
        self._late_queue: list[tuple[float, ToolIntent]] = []
        self.answered_count = 0

    async def run(self) -> None:
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

        consumer = AIOKafkaConsumer(
            self._config.approval_requests_topic,
            bootstrap_servers=HOST_BROKERS,
            group_id=f"{self._config.run_id}-feeder",
            auto_offset_reset="earliest",
        )
        producer = AIOKafkaProducer(bootstrap_servers=HOST_BROKERS)
        await consumer.start()
        await producer.start()
        try:
            while not self._stopping.is_set():
                batches = await consumer.getmany(timeout_ms=200)
                now = time.monotonic()
                for _tp, messages in batches.items():
                    for message in messages:
                        intent = ToolIntent.FromString(message.value)
                        if intent.entity_key.startswith(LATE_PREFIX):
                            self._late_queue.append((now + LATE_REPLY_DELAY_S, intent))
                        else:
                            await self._answer(producer, intent)
                due, self._late_queue = split_due(self._late_queue, now, self._late_ready)
                for _t, intent in due:
                    await self._answer(producer, intent)
        finally:
            with contextlib.suppress(Exception):
                await consumer.stop()
            with contextlib.suppress(Exception):
                await producer.stop()

    async def _answer(self, producer: object, intent: ToolIntent) -> None:
        decision = decide(intent)
        await producer.send_and_wait(  # type: ignore[attr-defined]
            self._config.decisions_topic,
            key=intent.entity_key,
            value=decision.SerializeToString(deterministic=True),
        )
        if intent.intent_id not in self._answered:
            self._answered.add(intent.intent_id)
            self.answered_count += 1

    @property
    def pending_late(self) -> int:
        return len(self._late_queue)

    def stop(self) -> None:
        self._stopping.set()
