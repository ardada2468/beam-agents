"""Effectively-once effects through the effector (semantics gate, offline).

This is the executor half of correctness invariant 2. The pipeline guarantees a
replayed bundle re-mints byte-identical ``intent_id``s; that guarantee is worth
nothing unless the effector collapses duplicates on it. Here the intent stream
is replayed with a process kill injected at *every* phase boundary, and the
property asserted is the one the whole design exists for: **at most one tool
invocation per intent_id, and one agreed terminal outcome**.

Offline by construction (no docker, no network): the transport and dedup store
are in-memory, and a "kill" is a `BaseException` raised at a named phase — the
same interleaving a SIGKILL produces, without the process management.

Also gates the fail-closed expiry rule (invariant 6, layer 2): an expired
intent must never reach the dedup store or a tool, whatever its kind.
"""

from __future__ import annotations

import pytest

from beam_agents._protos import ToolIntent, ToolResult
from beam_agents.effector.dedup import InMemoryDedupStore
from beam_agents.effector.sinks import InMemoryResultSink
from beam_agents.tools import ToolRegistry, tool
from tests.effector._fakes import (
    NOW_MS,
    CrashingResultSink,
    InjectedCrash,
    RecordingDedupStore,
    a_config,
    an_intent,
    build_harness,
)
from tests.effector.test_service import registry_with

pytestmark = pytest.mark.semantics

# Every boundary a kill can land on, in phase order. "none" is the control.
KILL_POINTS = ("claim", "complete", "publish", "none")


def _counting_registry(calls: list[str]) -> ToolRegistry:
    @tool(side_effect=True)
    def charge(amount_cents: int) -> str:
        calls.append(f"charge:{amount_cents}")
        return f"receipt-{amount_cents}"

    return registry_with(charge)


# A killed worker's claim is recovered by lease expiry, so replaying after a
# kill means letting the lease run out. The lease is deliberately far shorter
# than the intent's own TTL here: otherwise recovery would arrive after the
# deadline and the fail-closed rule would (correctly) refuse the intent — which
# is its own scenario, below.
LEASE_MS = 5_000
INTENT_TTL_MS = 600_000


class _MovableClock:
    def __init__(self, now_ms: int = NOW_MS) -> None:
        self.now_ms = now_ms

    def __call__(self) -> int:
        return self.now_ms


@pytest.mark.parametrize("kill_point", KILL_POINTS)
async def test_exactly_one_execution_per_intent_id_under_kills(kill_point: str) -> None:
    """A kill at any phase yields one execution and one agreed outcome."""
    calls: list[str] = []
    registry = _counting_registry(calls)
    clock = _MovableClock()
    # One durable dedup store outlives the "process", exactly as Redis or
    # Bigtable would; everything else is rebuilt per attempt.
    store = InMemoryDedupStore(clock=clock)
    intent = an_intent(expires_at_ms=NOW_MS + INTENT_TTL_MS)
    published: list[ToolResult] = []

    for attempt in range(3):
        crash_here = kill_point if attempt == 0 else None
        sink: object = CrashingResultSink() if crash_here == "publish" else InMemoryResultSink()
        dedup = RecordingDedupStore(
            store, crash_after=crash_here if crash_here in ("claim", "complete") else None
        )
        harness = build_harness(
            registry=registry,
            intents=[intent],
            dedup=dedup,
            result_sink=sink,
            config=a_config(lease_ms=LEASE_MS),
            clock=clock,
        )

        if crash_here in ("claim", "complete", "publish"):
            with pytest.raises(InjectedCrash):
                await harness.service.run()
        else:
            await harness.service.run()

        published.extend(
            sink.published if isinstance(sink, CrashingResultSink) else harness.results.published
        )
        if harness.committed_intent_ids:
            break
        # The dead worker's claim is not handed back — nobody is left to hand it
        # back. It is recovered by its lease running out.
        clock.now_ms += LEASE_MS + 1

    assert len(calls) <= 1, f"the tool ran {len(calls)} times: {calls}"
    assert published, "the intent produced no result at all"
    # Every publication of this intent — the original and any republish — must
    # carry the identical terminal outcome. A downstream consumer that sees two
    # different results for one intent_id has no way to pick.
    encoded = {result.SerializeToString(deterministic=True) for result in published}
    assert len(encoded) == 1, "republished results disagreed with the stored one"
    assert published[0].status == ToolResult.OK
    assert published[0].intent_id == intent.intent_id


async def test_an_intent_whose_deadline_passes_while_its_claim_is_stuck_is_refused() -> None:
    """Fail-closed beats retry: a late recovery refuses, it does not execute.

    If the worker holding a claim dies and the intent's own ``expires_at_ms``
    passes before the lease runs out, the retry must refuse rather than perform
    a side effect the agent already gave up waiting for (invariant 6).
    """
    calls: list[str] = []
    clock = _MovableClock()
    store = InMemoryDedupStore(clock=clock)
    # TTL shorter than the lease: recovery necessarily arrives too late.
    intent = an_intent(expires_at_ms=NOW_MS + 1_000)

    dying = build_harness(
        registry=_counting_registry(calls),
        intents=[intent],
        dedup=RecordingDedupStore(store, crash_after="claim"),
        config=a_config(lease_ms=LEASE_MS),
        clock=clock,
    )
    with pytest.raises(InjectedCrash):
        await dying.service.run()

    clock.now_ms += LEASE_MS + 1
    reborn = build_harness(
        registry=_counting_registry(calls),
        intents=[intent],
        dedup=store,
        config=a_config(lease_ms=LEASE_MS),
        clock=clock,
    )
    await reborn.service.run()

    assert calls == [], "an expired intent was executed after a late recovery"
    assert [r.status for r in reborn.results.published] == [ToolResult.EXPIRED]
    assert reborn.committed_intent_ids == ["intent-1"]


@pytest.mark.parametrize("kill_point", KILL_POINTS)
async def test_a_kill_never_commits_an_unpublished_intent(kill_point: str) -> None:
    """No offset advances past an intent whose result was not published."""
    calls: list[str] = []
    store = InMemoryDedupStore(clock=lambda: NOW_MS)
    sink: object = CrashingResultSink() if kill_point == "publish" else InMemoryResultSink()
    dedup = RecordingDedupStore(
        store, crash_after=kill_point if kill_point in ("claim", "complete") else None
    )
    harness = build_harness(
        registry=_counting_registry(calls),
        intents=[an_intent()],
        dedup=dedup,
        result_sink=sink,
    )

    if kill_point == "none":
        await harness.service.run()
        assert harness.committed_intent_ids == ["intent-1"]
        return

    with pytest.raises(InjectedCrash):
        await harness.service.run()

    assert harness.committed_intent_ids == [], "committed an intent it had not published"


async def test_a_replayed_stream_executes_each_intent_id_once() -> None:
    """Redelivering a whole batch re-executes nothing."""
    calls: list[str] = []
    registry = _counting_registry(calls)
    store = InMemoryDedupStore(clock=lambda: NOW_MS)
    intents = [
        an_intent(f"intent-{i}", args_json=f'{{"amount_cents":{i}}}', step_index=i)
        for i in range(5)
    ]

    for _ in range(3):
        harness = build_harness(registry=registry, intents=intents, dedup=store)
        await harness.service.run()
        assert harness.committed_intent_ids == [i.intent_id for i in intents]
        assert [r.status for r in harness.results.published] == [ToolResult.OK] * len(intents)

    assert sorted(calls) == sorted(f"charge:{i}" for i in range(5)), (
        "a replayed stream must execute each intent_id exactly once"
    )


@pytest.mark.parametrize("kind", [ToolIntent.TOOL, ToolIntent.APPROVAL])
async def test_an_expired_intent_never_reaches_the_dedup_store_or_a_tool(
    kind: ToolIntent.Kind,
) -> None:
    """Fail-closed expiry, layer 2: refused before anything can happen."""
    calls: list[str] = []
    dedup = RecordingDedupStore(InMemoryDedupStore(clock=lambda: NOW_MS))
    harness = build_harness(
        registry=_counting_registry(calls),
        intents=[an_intent(kind=kind, expires_at_ms=NOW_MS - 1)],
        dedup=dedup,
    )

    await harness.service.run()

    assert calls == []
    assert dedup.calls == [], "an expired intent consumed a claim"
    assert harness.approvals.published == []
    assert [r.status for r in harness.results.published] == [ToolResult.EXPIRED]
    assert harness.committed_intent_ids == ["intent-1"]


@pytest.mark.parametrize("kind", [ToolIntent.TOOL, ToolIntent.APPROVAL])
async def test_an_intent_with_no_expiry_recorded_is_refused(kind: ToolIntent.Kind) -> None:
    """`expires_at_ms == 0` reads as expired, never as unbounded."""
    calls: list[str] = []
    dedup = RecordingDedupStore(InMemoryDedupStore(clock=lambda: NOW_MS))
    harness = build_harness(
        registry=_counting_registry(calls),
        intents=[an_intent(kind=kind, expires_at_ms=0)],
        dedup=dedup,
    )

    await harness.service.run()

    assert calls == []
    assert dedup.calls == []
    assert [r.status for r in harness.results.published] == [ToolResult.EXPIRED]


async def test_expiry_is_refused_even_when_the_dedup_store_is_unavailable() -> None:
    """The refusal must not depend on the store being reachable.

    An outage that also blocked expiry refusals would turn a fail-closed
    deadline into a fail-open one exactly when the system is least healthy.
    """

    class _DeadStore:
        async def claim(self, intent_id: str, lease_ms: int) -> object:
            raise ConnectionError("dedup store is down")

        async def complete(self, intent_id: str, token: str, result: object, ttl: int) -> bool:
            raise ConnectionError("dedup store is down")

        async def release(self, intent_id: str, token: str) -> bool:
            raise ConnectionError("dedup store is down")

        async def close(self) -> None:
            return None

    harness = build_harness(
        registry=_counting_registry([]),
        intents=[an_intent(expires_at_ms=NOW_MS - 1)],
        dedup=_DeadStore(),  # type: ignore[arg-type]
    )

    await harness.service.run()

    assert [r.status for r in harness.results.published] == [ToolResult.EXPIRED]
    assert harness.committed_intent_ids == ["intent-1"]
