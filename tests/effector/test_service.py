"""The effector loop for the effector-service and effector-dedup capabilities.

Covers "Each intent is processed in the order refuse-expired, claim, execute,
complete, publish, commit", "A Done record republishes rather than
re-executes", "An in-flight claim is waited on, never skipped", "Per-key order
is preserved through consumer-group partition affinity", and "Infrastructure
operations retry with backoff; the tool never does".
"""

from __future__ import annotations

import asyncio

import pytest

from beam_agents._protos import ToolIntent, ToolResult
from beam_agents.effector.dedup import Claimed, Done, InFlight, InMemoryDedupStore
from beam_agents.effector.runner import EffectorToolRunner
from beam_agents.effector.service import EffectorService, PublishFailedError
from beam_agents.effector.sinks import InMemoryMessageSink, InMemoryResultSink, ProtoResultSink
from beam_agents.effector.sources import DeliveredIntent, InMemoryIntentSource
from beam_agents.tools import ToolRegistry, tool

from ._fakes import (
    NOW_MS,
    CrashingResultSink,
    InjectedCrash,
    RecordingDedupStore,
    a_config,
    an_intent,
    build_harness,
)


def registry_with(*tools: object) -> ToolRegistry:
    registry = ToolRegistry()
    for t in tools:
        registry.register(t)  # type: ignore[arg-type]
    return registry


def charging_tool(calls: list[int]) -> object:
    @tool(side_effect=True)
    def charge(amount_cents: int) -> str:
        calls.append(amount_cents)
        return "receipt"

    return charge


# -- phase order ---------------------------------------------------------------


async def test_expiry_is_decided_before_the_dedup_store_is_touched() -> None:
    # Scenario: Expiry is decided before the dedup store is touched.
    calls: list[int] = []
    dedup = RecordingDedupStore(InMemoryDedupStore(clock=lambda: NOW_MS))
    harness = build_harness(
        registry=registry_with(charging_tool(calls)),
        intents=[an_intent(expires_at_ms=NOW_MS - 1)],
        dedup=dedup,
    )

    await harness.service.run()

    assert harness.statuses == [ToolResult.EXPIRED]
    assert harness.committed_intent_ids == ["intent-1"]
    assert dedup.calls == [], "an expired intent must never reach the dedup store"
    assert calls == []


async def test_an_intent_with_no_recorded_expiry_is_refused() -> None:
    # Scenario: An intent with no recorded expiry is refused (fail-closed:
    # "no expiry recorded" reads as expired, never as unbounded).
    calls: list[int] = []
    harness = build_harness(
        registry=registry_with(charging_tool(calls)),
        intents=[an_intent(expires_at_ms=0)],
    )

    await harness.service.run()

    assert harness.statuses == [ToolResult.EXPIRED]
    assert calls == []


async def test_a_crash_between_completion_and_publication_does_not_re_execute() -> None:
    # Scenario: A crash between completion and publication does not re-execute.
    calls: list[int] = []
    registry = registry_with(charging_tool(calls))
    store = InMemoryDedupStore(clock=lambda: NOW_MS)
    delivery = DeliveredIntent(intent=an_intent(), partition="p-0", handle=0)

    dying = build_harness(
        registry=registry,
        deliveries=[delivery],
        dedup=RecordingDedupStore(store, crash_after="complete"),
    )
    with pytest.raises(InjectedCrash):
        await dying.service.run()

    assert calls == [100], "the tool ran once before the crash"
    assert dying.results.published == [], "the crash landed before publication"
    assert dying.committed_intent_ids == []

    # The intent is redelivered to a fresh worker sharing the dedup store.
    reborn = build_harness(registry=registry, deliveries=[delivery], dedup=store)
    await reborn.service.run()

    assert calls == [100], "redelivery must republish, not re-execute"
    assert reborn.statuses == [ToolResult.OK]
    assert reborn.results.published[0].payload == b'"receipt"'
    assert reborn.committed_intent_ids == ["intent-1"]


async def test_a_crash_before_publication_does_not_commit_the_offset() -> None:
    # Scenario: A crash before publication does not commit the offset.
    calls: list[int] = []
    harness = build_harness(
        registry=registry_with(charging_tool(calls)),
        intents=[an_intent()],
        dedup=RecordingDedupStore(InMemoryDedupStore(clock=lambda: NOW_MS), crash_after="claim"),
    )

    with pytest.raises(InjectedCrash):
        await harness.service.run()

    assert harness.committed_intent_ids == []
    assert harness.results.published == []


async def test_a_crash_after_publication_redelivers_and_republishes() -> None:
    # The other side of "commit after publish": the result reached the broker
    # but the offset did not advance, so redelivery republishes the stored
    # result rather than re-running the tool.
    calls: list[int] = []
    registry = registry_with(charging_tool(calls))
    store = InMemoryDedupStore(clock=lambda: NOW_MS)
    delivery = DeliveredIntent(intent=an_intent(), partition="p-0", handle=0)
    crashing = CrashingResultSink()

    dying = build_harness(
        registry=registry, deliveries=[delivery], dedup=store, result_sink=crashing
    )
    with pytest.raises(InjectedCrash):
        await dying.service.run()

    assert len(crashing.published) == 1
    assert dying.committed_intent_ids == []

    reborn = build_harness(registry=registry, deliveries=[delivery], dedup=store)
    await reborn.service.run()

    assert calls == [100]
    assert reborn.statuses == [ToolResult.OK]
    assert reborn.committed_intent_ids == ["intent-1"]


async def test_a_redelivered_completed_intent_republishes_without_execution() -> None:
    # Scenario: A redelivered completed intent republishes without execution.
    calls: list[int] = []
    registry = registry_with(charging_tool(calls))
    store = InMemoryDedupStore(clock=lambda: NOW_MS)
    delivery = DeliveredIntent(intent=an_intent(), partition="p-0", handle=0)

    first = build_harness(registry=registry, deliveries=[delivery], dedup=store)
    await first.service.run()
    second = build_harness(registry=registry, deliveries=[delivery], dedup=store)
    await second.service.run()

    assert calls == [100]
    assert second.statuses == [ToolResult.OK]
    assert second.results.published[0].payload == first.results.published[0].payload
    assert second.committed_intent_ids == ["intent-1"]


# -- in-flight claims ----------------------------------------------------------


class _ScriptedDedup:
    """Returns a scripted sequence of claim outcomes."""

    def __init__(self, outcomes: list[object]) -> None:
        self.outcomes = outcomes
        self.claims = 0

    async def claim(self, intent_id: str, lease_ms: int) -> object:
        self.claims += 1
        # The last outcome repeats: a waiter polls an unresolved claim for as
        # long as the lease lasts, which is longer than any script.
        return self.outcomes.pop(0) if len(self.outcomes) > 1 else self.outcomes[0]

    async def complete(self, intent_id: str, token: str, result: object, ttl_ms: int) -> bool:
        return True

    async def release(self, intent_id: str, token: str) -> bool:
        return True

    async def close(self) -> None:
        return None


async def test_waiting_resolves_once_the_prior_owner_completes() -> None:
    # Scenario: Waiting resolves once the prior owner completes.
    calls: list[int] = []
    stored = ToolResult(
        intent_id="intent-1", entity_key=b"customer-7", seq=3, status=ToolResult.OK, payload=b"1"
    )
    dedup = _ScriptedDedup([InFlight(), InFlight(), Done(result=stored)])
    harness = build_harness(
        registry=registry_with(charging_tool(calls)),
        intents=[an_intent()],
        dedup=dedup,  # type: ignore[arg-type]
    )

    await harness.service.run()

    assert dedup.claims == 3, "the waiter re-claimed until the state resolved"
    assert calls == [], "a Done record must not re-execute"
    assert harness.results.published == [stored]
    assert harness.committed_intent_ids == ["intent-1"]


async def test_waiting_resolves_once_the_lease_expires() -> None:
    # Scenario: Waiting resolves once the lease expires.
    calls: list[int] = []
    dedup = _ScriptedDedup([InFlight(), Claimed(token="t-1")])
    harness = build_harness(
        registry=registry_with(charging_tool(calls)),
        intents=[an_intent()],
        dedup=dedup,  # type: ignore[arg-type]
    )

    await harness.service.run()

    assert dedup.claims == 2
    assert calls == [100], "once the lease expired the waiter executed"
    assert harness.statuses == [ToolResult.OK]


async def test_an_in_flight_intent_is_never_skipped() -> None:
    # Scenario: An in-flight intent is never skipped.
    calls: list[int] = []
    dedup = _ScriptedDedup([InFlight()])
    harness = build_harness(
        registry=registry_with(charging_tool(calls)),
        intents=[an_intent()],
        dedup=dedup,  # type: ignore[arg-type]
    )

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(harness.service.run(), timeout=0.2)

    assert harness.committed_intent_ids == [], "waiting must not commit"
    assert harness.results.published == []
    assert calls == []


async def test_a_lost_claim_neither_publishes_nor_commits() -> None:
    # If the lease expired mid-execution, another worker owns the intent now:
    # publishing would race its result and committing would advance past an
    # intent this worker no longer owns.
    class _LosingDedup(_ScriptedDedup):
        async def complete(self, intent_id: str, token: str, result: object, ttl_ms: int) -> bool:
            return False

    calls: list[int] = []
    harness = build_harness(
        registry=registry_with(charging_tool(calls)),
        intents=[an_intent()],
        dedup=_LosingDedup([Claimed(token="t-1")]),  # type: ignore[arg-type]
    )

    await harness.service.run()

    assert calls == [100]
    assert harness.results.published == []
    assert harness.committed_intent_ids == []
    assert harness.service.metrics.counters["claims_lost"] == 1  # type: ignore[attr-defined]


# -- ordering ------------------------------------------------------------------


async def test_intents_for_one_key_execute_in_emission_order() -> None:
    # Scenario: Intents for one key execute in emission order.
    order: list[str] = []

    @tool(side_effect=True)
    async def charge(step: int) -> str:
        order.append(f"start-{step}")
        # A later intent finishing first would show up as interleaving here.
        await asyncio.sleep(0.02 if step == 0 else 0)
        order.append(f"end-{step}")
        return "ok"

    harness = build_harness(
        registry=registry_with(charge),
        intents=[
            an_intent(f"intent-{i}", tool_name="charge", args_json=f'{{"step":{i}}}', step_index=i)
            for i in range(3)
        ],
    )

    await harness.service.run()

    assert order == ["start-0", "end-0", "start-1", "end-1", "start-2", "end-2"]
    assert harness.committed_intent_ids == ["intent-0", "intent-1", "intent-2"]


async def test_distinct_partitions_progress_concurrently() -> None:
    # Scenario: Distinct partitions progress concurrently.
    blocked = asyncio.Event()
    finished: list[str] = []

    @tool(side_effect=True)
    async def charge(key: str) -> str:
        if key == "slow":
            await blocked.wait()
        finished.append(key)
        return "ok"

    harness = build_harness(
        registry=registry_with(charge),
        deliveries=[
            DeliveredIntent(
                intent=an_intent("intent-slow", tool_name="charge", args_json='{"key":"slow"}'),
                partition="p-0",
                handle=0,
            ),
            DeliveredIntent(
                intent=an_intent("intent-fast", tool_name="charge", args_json='{"key":"fast"}'),
                partition="p-1",
                handle=1,
            ),
        ],
    )

    task = asyncio.create_task(harness.service.run())
    for _ in range(50):
        await asyncio.sleep(0)
        if finished:
            break
    assert finished == ["fast"], "the second partition finished while the first was blocked"

    blocked.set()
    await task
    assert sorted(finished) == ["fast", "slow"]


async def test_concurrency_is_bounded_by_the_configured_maximum() -> None:
    # Cross-partition parallelism is a budget, not unbounded fan-out: a runaway
    # effector would hammer the downstream systems its tools write to.
    live = 0
    peak = 0
    release = asyncio.Event()

    @tool(side_effect=True)
    async def charge(key: str) -> str:
        nonlocal live, peak
        live += 1
        peak = max(peak, live)
        await release.wait()
        live -= 1
        return "ok"

    harness = build_harness(
        registry=registry_with(charge),
        config=a_config(max_concurrent_partitions=2),
        deliveries=[
            DeliveredIntent(
                intent=an_intent(f"intent-{i}", tool_name="charge", args_json=f'{{"key":"{i}"}}'),
                partition=f"p-{i}",
                handle=i,
            )
            for i in range(5)
        ],
    )

    task = asyncio.create_task(harness.service.run())
    for _ in range(50):
        await asyncio.sleep(0)
    assert peak <= 2

    release.set()
    await task
    assert len(harness.results.published) == 5


async def test_a_revoked_partition_releases_unexecuted_claims() -> None:
    # Scenario: A revoked partition releases unexecuted claims.
    gate = asyncio.Event()
    calls: list[int] = []

    class _SlowStartRunner(EffectorToolRunner):
        """Stalls after the claim, before the callable is invoked."""

        async def run(self, t: object, arguments: object, *, on_invoke: object = None) -> object:
            await gate.wait()
            return await super().run(t, arguments, on_invoke=on_invoke)  # type: ignore[arg-type]

    store = InMemoryDedupStore(clock=lambda: NOW_MS)
    dedup = RecordingDedupStore(store)
    harness = build_harness(
        registry=registry_with(charging_tool(calls)),
        deliveries=[DeliveredIntent(intent=an_intent(), partition="p-0", handle=0)],
        dedup=dedup,
        runner=_SlowStartRunner(tool_timeout_ms=1_000),
    )

    task = asyncio.create_task(harness.service.run())
    for _ in range(50):
        await asyncio.sleep(0)
        if "claim" in dedup.calls:
            break
    assert "claim" in dedup.calls

    await harness.source.revoke("p-0")

    assert "release" in dedup.calls, "the claim was handed back, not left to expire"
    assert calls == [], "the tool never ran"
    assert harness.committed_intent_ids == []
    # The new owner can claim it immediately rather than waiting out the lease.
    assert isinstance(await store.claim("intent-1", 60_000), Claimed)

    gate.set()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_a_partition_revoked_mid_execution_leaves_the_claim_to_expire() -> None:
    # The inverse: once the callable has been invoked the effect may already
    # have happened, so handing the claim back would invite a second execution.
    started = asyncio.Event()
    blocked = asyncio.Event()

    @tool(side_effect=True)
    async def charge(amount_cents: int) -> str:
        started.set()
        await blocked.wait()
        return "ok"

    store = InMemoryDedupStore(clock=lambda: NOW_MS)
    dedup = RecordingDedupStore(store)
    harness = build_harness(
        registry=registry_with(charge),
        deliveries=[DeliveredIntent(intent=an_intent(), partition="p-0", handle=0)],
        dedup=dedup,
    )

    task = asyncio.create_task(harness.service.run())
    await asyncio.wait_for(started.wait(), timeout=1)

    await harness.source.revoke("p-0")

    assert "release" not in dedup.calls
    assert isinstance(await store.claim("intent-1", 60_000), InFlight)

    blocked.set()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# -- retries -------------------------------------------------------------------


async def test_a_transient_publish_failure_is_retried_and_then_committed() -> None:
    # Scenario: A transient publish failure is retried and then committed.
    def fail_first(attempt: int) -> None:
        if attempt == 1:
            raise ConnectionError("broker unavailable")

    sink = InMemoryResultSink(fail=fail_first)
    harness = build_harness(
        registry=registry_with(charging_tool([])),
        intents=[an_intent()],
        result_sink=sink,
    )

    await harness.service.run()

    assert sink.attempts == 2
    assert harness.statuses == [ToolResult.OK]
    assert harness.committed_intent_ids == ["intent-1"]


async def test_exhausted_publish_retries_leave_the_offset_uncommitted() -> None:
    # Scenario: Exhausted publish retries leave the offset uncommitted.
    def always_fail(attempt: int) -> None:
        raise ConnectionError("broker unavailable")

    sink = InMemoryResultSink(fail=always_fail)
    harness = build_harness(
        registry=registry_with(charging_tool([])),
        intents=[an_intent()],
        result_sink=sink,
        config=a_config(publish_max_attempts=3),
    )

    with pytest.raises(PublishFailedError):
        await harness.service.run()

    assert sink.attempts == 3
    assert harness.committed_intent_ids == []


async def test_a_failed_tool_is_not_re_invoked() -> None:
    # Scenario: A failed tool is not re-invoked.
    calls: list[int] = []

    @tool(side_effect=True)
    def charge(amount_cents: int) -> None:
        calls.append(amount_cents)
        raise RuntimeError("card declined")

    harness = build_harness(registry=registry_with(charge), intents=[an_intent()])

    await harness.service.run()

    assert calls == [100]
    assert harness.statuses == [ToolResult.ERROR]
    assert harness.committed_intent_ids == ["intent-1"]


# -- routing and rejection -----------------------------------------------------


async def test_an_unknown_tool_name_is_rejected_without_stalling_the_partition() -> None:
    # Scenario: An unknown tool name is rejected without stalling the partition.
    calls: list[int] = []
    harness = build_harness(
        registry=registry_with(charging_tool(calls)),
        intents=[an_intent("intent-0", tool_name="nope"), an_intent("intent-1")],
    )

    await harness.service.run()

    assert harness.statuses == [ToolResult.REJECTED, ToolResult.OK]
    assert harness.committed_intent_ids == ["intent-0", "intent-1"]
    assert calls == [100]


async def test_an_approval_intent_is_posted_to_the_approval_channel() -> None:
    # Scenario: An approval intent is posted to the approval channel.
    calls: list[int] = []
    intent = an_intent(kind=ToolIntent.APPROVAL, tool_name="approval")
    harness = build_harness(registry=registry_with(charging_tool(calls)), intents=[intent])

    await harness.service.run()

    assert harness.approvals.published == [
        (b"customer-7", intent.SerializeToString(deterministic=True))
    ]
    assert harness.results.published == [], "an approval has no ToolResult"
    assert calls == []
    assert harness.committed_intent_ids == ["intent-1"]


async def test_a_redelivered_approval_intent_does_not_double_notify() -> None:
    # Scenario: A redelivered approval intent does not double-notify.
    store = InMemoryDedupStore(clock=lambda: NOW_MS)
    intent = an_intent(kind=ToolIntent.APPROVAL, tool_name="approval")
    delivery = DeliveredIntent(intent=intent, partition="p-0", handle=0)

    first = build_harness(registry=ToolRegistry(), deliveries=[delivery], dedup=store)
    await first.service.run()
    second = build_harness(registry=ToolRegistry(), deliveries=[delivery], dedup=store)
    await second.service.run()

    assert len(first.approvals.published) == 1
    assert second.approvals.published == []
    assert second.results.published == []
    assert second.committed_intent_ids == ["intent-1"]


async def test_an_expired_approval_intent_is_refused_rather_than_routed() -> None:
    # Scenario: An expired approval intent is refused rather than routed.
    harness = build_harness(
        registry=ToolRegistry(),
        intents=[
            an_intent(kind=ToolIntent.APPROVAL, tool_name="approval", expires_at_ms=NOW_MS - 1)
        ],
    )

    await harness.service.run()

    assert harness.approvals.published == []
    assert harness.statuses == [ToolResult.EXPIRED]


async def test_an_unspecified_kind_is_executed_as_a_tool() -> None:
    # Scenario: An unspecified kind is executed as a tool.
    calls: list[int] = []
    harness = build_harness(
        registry=registry_with(charging_tool(calls)),
        intents=[an_intent(kind=ToolIntent.TOOL_KIND_UNSPECIFIED)],
    )

    await harness.service.run()

    assert calls == [100]
    assert harness.statuses == [ToolResult.OK]
    assert harness.approvals.published == []


# -- sinks ---------------------------------------------------------------------


async def test_results_are_published_under_the_originating_entity_key() -> None:
    # Scenario: Results are published under the originating entity key.
    inner = InMemoryMessageSink()
    harness = build_harness(
        registry=registry_with(charging_tool([])),
        intents=[an_intent()],
        result_sink=ProtoResultSink(inner),
    )

    await harness.service.run()

    (key, payload), *rest = inner.published
    assert rest == []
    assert key == b"customer-7"
    republished = ToolResult()
    republished.ParseFromString(payload)
    assert republished.intent_id == "intent-1"
    assert republished.seq == 3
    assert republished.entity_key == b"customer-7"


async def test_the_loop_runs_against_in_memory_implementations() -> None:
    # Scenario: The service loop runs against in-memory implementations.
    calls: list[int] = []
    service = EffectorService(
        config=a_config(),
        registry=registry_with(charging_tool(calls)),
        source=InMemoryIntentSource.of([an_intent()]),
        result_sink=InMemoryResultSink(),
        approval_sink=InMemoryMessageSink(),
        dedup=InMemoryDedupStore(clock=lambda: NOW_MS),
        clock=lambda: NOW_MS,
    )

    await service.run()

    assert calls == [100]
