"""The approval surface: consume -> post -> decide -> publish.

One class, four obligations:

- **Consume -> post.** Approval-kind intents arrive through the effector's
  `IntentSource` seam; each live one becomes exactly one interactive Slack
  message, and the delivery is committed strictly *after* the post succeeds, so
  a crash re-delivers rather than loses. Non-approval kinds (including
  `TOOL_KIND_UNSPECIFIED`) are skipped and committed — pointing the surface at
  the wrong topic is inert, never destructive. Duplicate deliveries (the
  effector publishes approval notifications before marking them terminal) are
  collapsed by `intent_id` within the process; across a restart a redelivered
  intent may post again, which is harmless — both messages carry the same
  `intent_id` and the pipeline admits at most one verdict.

- **Decide -> publish.** A verdict becomes one deterministically serialized
  `AgentEnvelope.Approval` published under the raw `entity_key` through the
  `MessageSink` seam — the same keying `WriteIntents` and the effector's result
  sink use, so the envelope lands on the suspended key's partition. Publish
  first, edit after: the envelope is the effect, the edit is cosmetic. The
  surface never enforces at-most-one-verdict globally — the pipeline's resume
  admission is the arbiter; it only stops *itself* from re-publishing, and
  answers later clicks as already decided.

- **TTL, fail closed, at three points** (all via `hitl.intent_expired`, the
  runtime's own guard, against an injectable clock): already-expired intents
  are surfaced as a non-interactive notice; a periodic sweep edits pending
  messages whose expiry passed; and every decision is re-checked against the
  `expires_at_ms` carried in its action value — independent of in-process
  state, so it holds for clicks on messages posted before a restart. The
  first two are UX; the third is the gate. Even a surface that got all three
  wrong is backstopped by the pipeline: `_resume` refuses late approvals.

- **Bounded memory.** The posted/decided/pending maps are evicted by the sweep
  once entries are long past expiry, and hard-capped oldest-first.

Imports no Beam and no slack-sdk: this is an out-of-pipeline service, like the
effector it borrows its transport seams from.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

from beam_agents._protos import AgentEnvelope, ToolIntent
from beam_agents.effector.sinks import MessageSink
from beam_agents.effector.sources import DeliveredIntent, IntentSource
from beam_agents.hitl import intent_expired

from .blocks import approval_message, decided_edit, expired_edit, expired_notice
from .slack import Decision, MessageRef, SlackGateway

_LOG = logging.getLogger(__name__)

# How often the sweep pass runs; well under the demo's 10-minute TTL.
DEFAULT_SWEEP_INTERVAL_MS = 30_000

# Hard cap on tracked intent_ids (posted/decided/pending each); beyond it the
# oldest entries are evicted first. Restart semantics already tolerate an empty
# map, so eviction costs at worst a duplicate post or a re-answered click.
MAX_TRACKED_INTENTS = 4096

# How long a tracked entry outlives its expiry before the sweep evicts it:
# long enough to still collapse the channel's redelivery window.
_TRACKED_RETENTION_MS = 3_600_000


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(frozen=True)
class _Pending:
    """A posted, undecided approval: what the sweep needs to expire it."""

    message: MessageRef
    expires_at_ms: int


class ApprovalSurface:
    """The consume/decide loop over an `IntentSource`, a `SlackGateway`, and a
    `MessageSink`. Construct with in-memory fakes for tests and single-process
    demos, or with the Kafka/Pub/Sub adapters for a deployment.
    """

    def __init__(
        self,
        *,
        source: IntentSource,
        sink: MessageSink,
        gateway: SlackGateway,
        channel: str,
        sweep_interval_ms: int = DEFAULT_SWEEP_INTERVAL_MS,
        time_fn: Callable[[], int] = _now_ms,
    ) -> None:
        if not channel:
            raise ValueError("ApprovalSurface.channel must be a non-empty Slack channel")
        if sweep_interval_ms <= 0:
            raise ValueError(
                f"ApprovalSurface.sweep_interval_ms must be positive; got {sweep_interval_ms!r}"
            )
        self._source = source
        self._sink = sink
        self._gateway = gateway
        self._channel = channel
        self._sweep_interval_ms = sweep_interval_ms
        self._time_fn = time_fn
        # intent_id -> expires_at_ms for everything ever posted (interactive or
        # notice) this process lifetime; the within-process double-post guard.
        self._posted: dict[str, int] = {}
        # intent_id -> _Pending for posted, undecided, unexpired messages.
        self._pending: dict[str, _Pending] = {}
        # intent_id -> expires_at_ms for published verdicts; the re-publish guard.
        self._decided: dict[str, int] = {}
        self._stopping = asyncio.Event()

    # -- consume -> post -------------------------------------------------------

    async def consume(self) -> None:
        """Start the source and process deliveries until it is exhausted.

        A failed post propagates with the delivery uncommitted — crash-stop is
        the crash-safety story: the transport re-delivers to the next instance.
        """
        await self._source.start()
        await self._consume_started()

    async def _consume_started(self) -> None:
        async for delivered in self._source:
            await self._handle_delivery(delivered)

    async def _handle_delivery(self, delivered: DeliveredIntent) -> None:
        intent = delivered.intent
        if intent.kind != ToolIntent.APPROVAL:
            # Not ours (TOOL_KIND_UNSPECIFIED included): skip and commit. A
            # TOOL intent can never become a button that fabricates a verdict.
            await self._source.commit(delivered)
            return
        if intent.intent_id in self._posted:
            # The effector notifies before marking terminal, so duplicates are
            # expected; one interactive message per intent_id per process.
            await self._source.commit(delivered)
            return
        if intent_expired(intent, self._time_fn()):
            text, blocks = expired_notice(intent)
            await self._gateway.post(self._channel, text=text, blocks=blocks)
            self._track_posted(intent)
        else:
            text, blocks = approval_message(intent)
            ref = await self._gateway.post(self._channel, text=text, blocks=blocks)
            self._track_posted(intent)
            self._pending[intent.intent_id] = _Pending(
                message=ref, expires_at_ms=intent.expires_at_ms
            )
        # Commit strictly after the post: a crash between them re-delivers and
        # (worst case, across a restart) re-posts — never loses the request.
        await self._source.commit(delivered)

    def _track_posted(self, intent: ToolIntent) -> None:
        self._posted[intent.intent_id] = intent.expires_at_ms
        _evict_oldest(self._posted)

    # -- decide -> publish -----------------------------------------------------

    async def process_decisions(self) -> None:
        """Handle gateway decisions until its stream ends (gateway closed)."""
        async for decision in self._gateway.decisions():
            await self.handle_decision(decision)

    async def handle_decision(self, decision: Decision) -> None:
        """One interaction: expiry-gate, publish, edit — in that order."""
        if decision.intent_id in self._decided:
            # First verdict won and was published; nothing further leaves the
            # surface for this intent. The pipeline would orphan a duplicate
            # anyway — this just answers the human instead of racing them.
            await self._gateway.answer(
                decision, f"Already decided: intent {decision.intent_id} has a published verdict."
            )
            return
        now_ms = self._time_fn()
        # THE fail-closed gate (design D6.3): checked against the expiry the
        # button value carries, not against `_pending`, so it survives sweeps
        # that have not run yet and restarts that emptied the maps.
        if intent_expired(ToolIntent(expires_at_ms=decision.expires_at_ms), now_ms):
            self._pending.pop(decision.intent_id, None)
            text, blocks = expired_edit(decision.intent_id)
            await self._gateway.update(decision.message, text=text, blocks=blocks)
            return
        entity_key = bytes.fromhex(decision.entity_key_hex)
        envelope = AgentEnvelope(entity_key=entity_key, event_time_ms=decision.decided_at_ms)
        envelope.approval.intent_id = decision.intent_id
        envelope.approval.approved = decision.approved
        envelope.approval.approver = decision.approver
        envelope.approval.decided_at_ms = decision.decided_at_ms
        # Deterministic bytes under the raw entity_key: lands on the suspended
        # key's partition/ordering key, byte-stable for replay comparison.
        await self._sink.publish(entity_key, envelope.SerializeToString(deterministic=True))
        self._decided[decision.intent_id] = decision.expires_at_ms
        _evict_oldest(self._decided)
        self._pending.pop(decision.intent_id, None)
        # Publish, then edit: the envelope is the effect, the edit is cosmetic.
        # A crash between them leaves a decided-but-live-looking message whose
        # next click is refused above.
        text, blocks = decided_edit(
            decision.intent_id, approved=decision.approved, approver=decision.approver
        )
        await self._gateway.update(decision.message, text=text, blocks=blocks)

    # -- the sweep -------------------------------------------------------------

    async def sweep_once(self) -> None:
        """Edit pending messages whose expiry passed; evict long-dead tracking.

        UX only — the layer-1 HITL timer resolves the suspension and the
        decision-time gate refuses late clicks regardless; the sweep just stops
        the UI from soliciting clicks that can no longer matter.
        """
        now_ms = self._time_fn()
        for intent_id, pending in list(self._pending.items()):
            if not intent_expired(ToolIntent(expires_at_ms=pending.expires_at_ms), now_ms):
                continue
            del self._pending[intent_id]
            text, blocks = expired_edit(intent_id)
            await self._gateway.update(pending.message, text=text, blocks=blocks)
        for tracked in (self._posted, self._decided):
            for intent_id, expires_at_ms in list(tracked.items()):
                if expires_at_ms + _TRACKED_RETENTION_MS <= now_ms:
                    del tracked[intent_id]

    async def _sweep_loop(self) -> None:
        while True:
            await asyncio.sleep(self._sweep_interval_ms / 1000)
            await self.sweep_once()

    # -- lifecycle -------------------------------------------------------------

    async def run(self) -> None:
        """Run consume, decisions, and the sweep until `stop()` or a failure.

        Shutdown order: stop consuming (workers cancelled — an in-flight post
        finishes or its delivery stays uncommitted), then close the gateway,
        the sink, and the source. A worker failure propagates after cleanup:
        crash-stop, so the transport's redelivery does the recovery.
        """
        await self._source.start()
        workers: set[asyncio.Task[None]] = {
            asyncio.create_task(self._consume_started(), name="consume"),
            asyncio.create_task(self.process_decisions(), name="decisions"),
            asyncio.create_task(self._sweep_loop(), name="sweep"),
        }
        stop_waiter = asyncio.create_task(self._stopping.wait(), name="stop")
        failure: BaseException | None = None
        try:
            while workers:
                done, _ = await asyncio.wait(
                    workers | {stop_waiter}, return_when=asyncio.FIRST_COMPLETED
                )
                if stop_waiter in done:
                    break
                for task in done & workers:
                    workers.discard(task)
                    if task.exception() is not None:
                        failure = task.exception()
                        break
                if failure is not None:
                    break
        finally:
            stop_waiter.cancel()
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, stop_waiter, return_exceptions=True)
            await self._gateway.close()
            await self._sink.close()
            await self._source.close()
        if failure is not None:
            raise failure

    def stop(self) -> None:
        """Ask `run()` to shut down; safe to call from a signal handler."""
        self._stopping.set()


def _evict_oldest(tracked: dict[str, int], cap: int = MAX_TRACKED_INTENTS) -> None:
    """Drop insertion-oldest entries until `tracked` fits the cap."""
    while len(tracked) > cap:
        oldest = next(iter(tracked))
        _LOG.warning("tracking cap reached; forgetting intent %s", oldest)
        del tracked[oldest]
