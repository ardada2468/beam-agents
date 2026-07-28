"""The effector loop: consume, refuse, claim, execute, complete, publish, commit.

The phase order is load-bearing (see the change design, D3); each edge is a
crash argument:

1. **Refuse-expired first.** Expiry is decided before the dedup store is
   touched, so an expired intent can never consume a claim, reach a tool, or
   depend on store availability to be refused (correctness invariant 6, layer 2).
2. **Claim before execute.** Nothing runs without exclusive ownership.
3. **Complete before publish.** The terminal result is durable *before* it is
   published, so a crash in between republishes on redelivery instead of
   re-executing. This is why a `Done` record stores the whole result rather
   than a tombstone.
4. **Commit after publish.** Delivery is at-least-once and every earlier phase
   is either idempotent or claim-guarded, so a crash re-delivers rather than
   loses. A duplicate result is harmless: the runtime admits one only against a
   live continuation with a matching pending ``intent_id`` and routes the rest
   to ``.errors`` as ``orphaned_result``.

Approval-kind intents invert one edge: they publish to the approval channel
*before* being marked terminal, because a lost notification is worse than a
duplicate one (the duplicate is collapsed downstream by ``intent_id``; the loss
would leave a human waiting until the HITL timer fires).

Ordering comes from the transport, not from bookkeeping here: one task per
partition, one intent at a time within it, concurrency only across partitions.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from beam_agents._protos import ToolIntent, ToolResult
from beam_agents.effector.dedup import Claimed, DedupStore, Done, InFlight
from beam_agents.effector.runner import EffectorToolRunner, execute_intent
from beam_agents.hitl import refuse_expired

if TYPE_CHECKING:
    from beam_agents.effector.config import EffectorConfig
    from beam_agents.effector.sinks import MessageSink, ResultSink
    from beam_agents.effector.sources import DeliveredIntent, IntentSource
    from beam_agents.tools.registry import ToolRegistry

_LOG = logging.getLogger(__name__)


class MetricsSink(Protocol):
    """Counters and timings. Wiring these to OTel belongs to `observability/`."""

    def incr(self, name: str, value: int = 1) -> None: ...

    def observe(self, name: str, value: float) -> None: ...


@dataclass
class CountingMetrics:
    """Default `MetricsSink`: in-process counters, useful in tests and logs."""

    counters: dict[str, int] = field(default_factory=dict)
    observations: dict[str, list[float]] = field(default_factory=dict)

    def incr(self, name: str, value: int = 1) -> None:
        self.counters[name] = self.counters.get(name, 0) + value

    def observe(self, name: str, value: float) -> None:
        self.observations.setdefault(name, []).append(value)


class PublishFailedError(RuntimeError):
    """A result could not be published within its retry budget.

    Raised rather than swallowed: the offset stays uncommitted, and continuing
    to the next intent would both hide the failure and break per-key ordering.
    """


@dataclass
class _ActiveClaim:
    """A claim currently held by a partition's worker."""

    intent_id: str
    token: str
    executing: bool = False


def _wall_clock_ms() -> int:
    return int(time.time() * 1000)


class EffectorService:
    """Drives intents from an `IntentSource` to terminal `ToolResult`s.

    Every collaborator is injected, so the whole loop — including its crash and
    ordering properties — runs offline against in-memory fakes.
    """

    def __init__(
        self,
        *,
        config: EffectorConfig,
        registry: ToolRegistry,
        source: IntentSource,
        result_sink: ResultSink,
        approval_sink: MessageSink,
        dedup: DedupStore,
        runner: EffectorToolRunner | None = None,
        clock: Callable[[], int] = _wall_clock_ms,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        metrics: MetricsSink | None = None,
    ) -> None:
        self._config = config
        self._registry = registry
        self._source = source
        self._result_sink = result_sink
        self._approval_sink = approval_sink
        self._dedup = dedup
        self._runner = runner or EffectorToolRunner(tool_timeout_ms=config.tool_timeout_ms)
        self._clock = clock
        self._sleep = sleep
        self.metrics: MetricsSink = metrics if metrics is not None else CountingMetrics()
        self._queues: dict[str, asyncio.Queue[DeliveredIntent | None]] = {}
        self._workers: dict[str, asyncio.Task[None]] = {}
        self._claims: dict[str, _ActiveClaim] = {}
        self._concurrency = asyncio.Semaphore(config.max_concurrent_partitions)

    # -- lifecycle -------------------------------------------------------------

    async def run(self) -> None:
        """Consume until the source is exhausted, then drain every partition."""
        await self._source.start()
        self._source.set_revocation_handler(self._on_revoked)
        try:
            async for delivered in self._source:
                queue = self._ensure_worker(delivered.partition)
                # A bounded queue is the backpressure: a partition that is busy
                # stops the dispatcher rather than buffering the topic.
                await queue.put(delivered)
        except BaseException:
            # Cancellation (a signalled shutdown) or a dispatcher failure: stop
            # the partitions rather than waiting on them, since a tool that
            # never returns would otherwise wedge shutdown indefinitely.
            await self._abort()
            raise
        else:
            await self._drain()

    async def _drain(self) -> None:
        """Stop every partition worker, surfacing the first failure.

        The stop sentinels are posted as tasks rather than awaited inline: a
        worker that already died leaves its bounded queue full, and awaiting
        ``put`` on it would hang the shutdown behind a worker that is never
        coming back.
        """
        workers = list(self._workers.values())
        queues = list(self._queues.values())
        self._queues.clear()
        self._workers.clear()
        if not workers:
            return
        sentinels = [asyncio.create_task(queue.put(None)) for queue in queues]
        try:
            await asyncio.gather(*workers)
        except BaseException:
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise
        finally:
            for sentinel in sentinels:
                sentinel.cancel()
            await asyncio.gather(*sentinels, return_exceptions=True)

    async def _abort(self) -> None:
        """Stop every partition worker without waiting for its current intent.

        Nothing was committed for an in-flight intent, so it is redelivered.
        Claims whose tool had not yet been invoked are handed back by
        :meth:`aclose`; one whose tool *was* invoked is left to its lease,
        because the effect may already have happened.
        """
        workers = list(self._workers.values())
        self._queues.clear()
        self._workers.clear()
        for worker in workers:
            worker.cancel()
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)

    async def aclose(self) -> None:
        """Release unexecuted claims and close every collaborator."""
        for partition in list(self._claims):
            await self._release_claim(partition)
        await self._source.close()
        await self._result_sink.close()
        await self._approval_sink.close()
        await self._dedup.close()

    def _ensure_worker(self, partition: str) -> asyncio.Queue[DeliveredIntent | None]:
        queue = self._queues.get(partition)
        if queue is None:
            queue = asyncio.Queue(maxsize=1)
            self._queues[partition] = queue
            self._workers[partition] = asyncio.create_task(
                self._worker(partition, queue), name=f"effector-partition-{partition}"
            )
        return queue

    async def _worker(self, partition: str, queue: asyncio.Queue[DeliveredIntent | None]) -> None:
        """Process one partition strictly sequentially."""
        while True:
            delivered = await queue.get()
            if delivered is None:
                return
            # The semaphore bounds how many partitions execute at once; within a
            # partition the await here is what serializes the key's intents.
            async with self._concurrency:
                await self.process(delivered)

    async def _on_revoked(self, partition: str) -> None:
        """Stop a revoked partition and hand back any claim it had not executed."""
        worker = self._workers.pop(partition, None)
        self._queues.pop(partition, None)
        if worker is not None:
            worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker
        await self._release_claim(partition)
        self.metrics.incr("partitions_revoked")

    async def _release_claim(self, partition: str) -> None:
        claim = self._claims.pop(partition, None)
        if claim is None:
            return
        if claim.executing:
            # The tool may already have run; abandoning the claim would let the
            # new owner execute it a second time. Let the lease expire instead.
            _LOG.warning(
                "partition %s revoked while executing intent %s; leaving the claim to expire",
                partition,
                claim.intent_id,
            )
            return
        await self._dedup.release(claim.intent_id, claim.token)
        self.metrics.incr("claims_released")

    # -- one intent ------------------------------------------------------------

    async def process(self, delivered: DeliveredIntent) -> None:
        """Take one delivery through every phase, in order."""
        intent = delivered.intent

        # Phase 1: expiry, before the dedup store is touched.
        refusal = refuse_expired(intent, self._clock())
        if refusal is not None:
            self.metrics.incr("intents_expired")
            await self._publish_result(refusal)
            await self._source.commit(delivered)
            return

        # Phase 2: claim. InFlight waits; it is never skipped.
        outcome = await self._claim(intent.intent_id)
        if isinstance(outcome, Done):
            self.metrics.incr("intents_deduped")
            if outcome.result is not None:
                await self._publish_result(outcome.result)
            await self._source.commit(delivered)
            return

        claim = _ActiveClaim(intent_id=intent.intent_id, token=outcome.token)
        self._claims[delivered.partition] = claim
        try:
            if intent.kind == ToolIntent.APPROVAL:
                await self._route_approval(delivered, claim)
            else:
                await self._execute(delivered, claim)
        except BaseException:
            # The claim stays registered on cancellation (a revoked partition)
            # or failure, so revocation/shutdown can hand it back if the tool
            # never ran. Clearing it here would strand it until its lease
            # expires for no reason.
            raise
        else:
            if self._claims.get(delivered.partition) is claim:
                del self._claims[delivered.partition]

    async def _claim(self, intent_id: str) -> Claimed | Done:
        """Claim ``intent_id``, waiting out any live lease held elsewhere.

        Waiting — rather than skipping — is what keeps a dead owner's intent
        from being silently dropped: the wait ends either when that owner
        completes (`Done`) or when its lease expires (`Claimed`).
        """
        backoff_ms = self._config.in_flight_backoff_ms
        waited = False
        while True:
            outcome = await self._dedup.claim(intent_id, self._config.lease_ms)
            if not isinstance(outcome, InFlight):
                if waited:
                    self.metrics.incr("in_flight_waits_resolved")
                return outcome
            if not waited:
                self.metrics.incr("in_flight_waits")
                waited = True
            await self._sleep(backoff_ms / 1000)
            backoff_ms = min(backoff_ms * 2, self._config.in_flight_backoff_max_ms)

    def _mark_executing(self, claim: _ActiveClaim) -> Callable[[], None]:
        """Signal that the tool is about to run and the effect may now happen.

        Everything before this point — resolving the tool, parsing and
        validating arguments — is safely abandonable, so a partition revoked
        during it hands the claim straight back. After it, the claim is left to
        expire instead: re-executing a side effect is worse than waiting.
        """

        def mark() -> None:
            claim.executing = True

        return mark

    async def _execute(self, delivered: DeliveredIntent, claim: _ActiveClaim) -> None:
        intent = delivered.intent
        started_ms = self._clock()
        result = await execute_intent(
            intent,
            self._registry,
            self._runner,
            now_ms=self._clock(),
            on_invoke=self._mark_executing(claim),
        )
        self.metrics.observe(f"tool_latency_ms.{intent.tool_name}", self._clock() - started_ms)
        self._count_status(result.status)

        # Phase 3: durable before published.
        stored = await self._dedup.complete(
            intent.intent_id, claim.token, result, self._config.result_ttl_ms
        )
        if not stored:
            # The lease expired mid-execution and another worker owns this
            # intent now. Publishing our result would race theirs, and
            # committing would advance past an intent we no longer own.
            _LOG.warning(
                "lost the claim on intent %s before completing it; leaving it to its new owner",
                intent.intent_id,
            )
            self.metrics.incr("claims_lost")
            return

        # Phase 4: publish, then commit.
        await self._publish_result(result)
        await self._source.commit(delivered)

    async def _route_approval(self, delivered: DeliveredIntent, claim: _ActiveClaim) -> None:
        """Post an approval request to its channel; never execute it.

        Unlike a tool, the notification is published *before* the terminal
        record is written: a duplicate notification is collapsed downstream by
        ``intent_id``, while a lost one would leave a human unaware until the
        HITL timer fires. No ``ToolResult`` is published — the decision returns
        separately as an approval envelope.
        """
        intent = delivered.intent
        claim.executing = True
        await self._publish_approval(intent)
        stored = await self._dedup.complete(
            intent.intent_id, claim.token, None, self._config.result_ttl_ms
        )
        if not stored:
            _LOG.warning(
                "lost the claim on approval intent %s before marking it routed",
                intent.intent_id,
            )
            self.metrics.incr("claims_lost")
            return
        self.metrics.incr("approvals_routed")
        await self._source.commit(delivered)

    def _count_status(self, status: ToolResult.Status) -> None:
        name = {
            ToolResult.OK: "results_ok",
            ToolResult.ERROR: "results_error",
            ToolResult.EXPIRED: "results_expired",
            ToolResult.REJECTED: "results_rejected",
        }.get(status)
        if name is not None:
            self.metrics.incr(name)

    # -- retries ---------------------------------------------------------------

    async def _publish_result(self, result: ToolResult) -> None:
        await self._with_retry(
            lambda: self._result_sink.publish(result),
            what=f"result for intent {result.intent_id}",
        )

    async def _publish_approval(self, intent: ToolIntent) -> None:
        payload = intent.SerializeToString(deterministic=True)
        await self._with_retry(
            lambda: self._approval_sink.publish(intent.entity_key, payload),
            what=f"approval request for intent {intent.intent_id}",
        )

    async def _with_retry(self, operation: Callable[[], Awaitable[None]], *, what: str) -> None:
        """Retry an idempotent infrastructure operation with exponential backoff.

        Only publishing and dedup RPCs come through here. A tool callable never
        does: it may already have performed part of its effect, so re-invoking
        it would be a second effect rather than a retry.
        """
        backoff_ms = self._config.publish_backoff_ms
        last: Exception | None = None
        for attempt in range(1, self._config.publish_max_attempts + 1):
            try:
                await operation()
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # transport errors are retryable by policy
                last = exc
                self.metrics.incr("publish_retries")
                _LOG.warning(
                    "publishing %s failed on attempt %d/%d: %s",
                    what,
                    attempt,
                    self._config.publish_max_attempts,
                    exc,
                )
                if attempt < self._config.publish_max_attempts:
                    await self._sleep(backoff_ms / 1000)
                    backoff_ms *= 2
        self.metrics.incr("publish_failures")
        raise PublishFailedError(
            f"could not publish {what} after {self._config.publish_max_attempts} attempts"
        ) from last
