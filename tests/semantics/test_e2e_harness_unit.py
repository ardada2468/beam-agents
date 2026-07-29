"""Unit tests for the e2e gate's harness — the plumbing that could lie.

A gate whose own machinery is wrong reports a false green: a spool that
mutates records would fake intent determinism, a duplicate producer that
never duplicates would fake the duplicate-tolerance property, an unseeded
kill schedule would make failures unreplayable, and a quiescence detector
that returns early would assert against a half-finished run.

Unmarked (unit tier): everything here is pure or filesystem-local, no docker.
The Redis-backed ledger reader is covered by the docker-backed gate itself.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from beam_agents._protos import AgentEnvelope, ToolIntent, ToolResult
from tests.semantics._e2e.approvals import split_due
from tests.semantics._e2e.assertions import await_condition
from tests.semantics._e2e.chaos import ChaosExecutor, KillAction, build_schedule
from tests.semantics._e2e.drainer import Drainer, should_seal
from tests.semantics._e2e.outbox import duplicate_decision
from tests.semantics._e2e.spool import SpoolWriter, eof_total, read_segment
from tests.semantics._e2e.stack import InfraFailure


def _env(key: bytes, payload: bytes, t_ms: int = 1_700_000_000_000) -> AgentEnvelope:
    return AgentEnvelope(entity_key=key, event_time_ms=t_ms, external_event=payload)


# -- spool: the replay property everything rests on -----------------------------


def test_spool_round_trips_records_byte_identically(tmp_path: Path) -> None:
    writer = SpoolWriter(tmp_path)
    envelopes = [_env(b"k1", b"a"), _env(b"k2", b"b" * 1000), _env(b"k3", b"")]
    for envelope in envelopes:
        writer.append(envelope)
    writer.seal()

    payloads = list(read_segment(tmp_path / "00000000.seg"))
    assert payloads == [e.SerializeToString() for e in envelopes]


def test_rereading_a_sealed_segment_yields_identical_bytes(tmp_path: Path) -> None:
    """The replay argument: a sealed segment reads the same forever."""
    writer = SpoolWriter(tmp_path)
    for i in range(50):
        writer.append(_env(f"k{i}".encode(), b"x" * i))
    writer.seal()

    first = list(read_segment(tmp_path / "00000000.seg"))
    second = list(read_segment(tmp_path / "00000000.seg"))
    assert first == second


def test_unsealed_segments_are_invisible_and_eof_counts_sealed_only(tmp_path: Path) -> None:
    writer = SpoolWriter(tmp_path)
    writer.append(_env(b"k1", b"sealed"))
    writer.seal()
    writer.append(_env(b"k2", b"pending"))  # open, never visible as .seg

    assert sorted(p.name for p in tmp_path.glob("*.seg")) == ["00000000.seg"]
    assert eof_total(tmp_path) is None
    writer.close()
    assert eof_total(tmp_path) == 2  # close seals the tail first
    assert sorted(p.name for p in tmp_path.glob("*.seg")) == ["00000000.seg", "00000001.seg"]


def test_a_truncated_record_is_an_error_not_a_short_read(tmp_path: Path) -> None:
    writer = SpoolWriter(tmp_path)
    writer.append(_env(b"k1", b"whole"))
    writer.seal()
    segment = tmp_path / "00000000.seg"
    segment.write_bytes(segment.read_bytes()[:-3])

    with pytest.raises(ValueError, match="truncated record"):
        list(read_segment(segment))


# -- duplicate producer: deterministic, seed-scoped, actually duplicates --------


def test_duplicate_decision_is_deterministic_and_seed_scoped() -> None:
    payloads = [f"payload-{i}".encode() for i in range(2000)]
    first = [duplicate_decision(7, p, 0.05) for p in payloads]
    second = [duplicate_decision(7, p, 0.05) for p in payloads]
    assert first == second, "the duplicate schedule must be reproducible"
    other_seed = [duplicate_decision(8, p, 0.05) for p in payloads]
    assert first != other_seed, "the seed must actually participate"


def test_duplicate_decision_edges_and_rate() -> None:
    payloads = [f"payload-{i}".encode() for i in range(5000)]
    assert not any(duplicate_decision(1, p, 0.0) for p in payloads)
    assert all(duplicate_decision(1, p, 1.0) for p in payloads)
    rate = sum(duplicate_decision(1, p, 0.05) for p in payloads) / len(payloads)
    assert 0.02 < rate < 0.09, f"5% target produced {rate:.3f} — the hash bucketing is off"


# -- kill schedule: seeded, replayable, in-window --------------------------------


def test_kill_schedule_is_reproducible_from_its_seed() -> None:
    a = build_schedule(42, effector_kills=5)
    b = build_schedule(42, effector_kills=5)
    assert a == b
    c = build_schedule(43, effector_kills=5)
    assert a != c


def test_kill_schedule_shape() -> None:
    schedule = build_schedule(7, effector_kills=4)
    assert len(schedule) == 4
    assert all(0.05 <= s.at_progress <= 0.95 for s in schedule), (
        "kills must land strictly inside the run, never before traffic or after it"
    )
    assert schedule == sorted(schedule, key=lambda s: s.at_progress)


def test_chaos_executor_fires_as_progress_crosses_thresholds() -> None:
    progress = {"value": 0.0}
    killed: list[int] = []
    executor = ChaosExecutor(
        [KillAction(0.3, 1), KillAction(0.6, 2)],
        progress=lambda: progress["value"],
        on_kill_effector=killed.append,
    )
    executor.start()
    time.sleep(0.4)
    assert killed == [], "no kill before its threshold"
    progress["value"] = 0.35
    time.sleep(0.6)
    assert killed == [1], "first threshold crossed, second still pending"
    progress["value"] = 1.0
    executor.join(3)
    assert killed == [1, 2]
    assert len(executor.executed) == 2


# -- quiescence detector: never returns early, classifies infra ------------------


async def test_await_condition_returns_when_condition_holds() -> None:
    calls = {"n": 0}

    def condition() -> bool:
        calls["n"] += 1
        return calls["n"] >= 2

    await await_condition("two polls", condition, deadline_s=10, poll_s=0.01)
    assert calls["n"] == 2


async def test_await_condition_deadline_is_an_invariant_failure() -> None:
    start = time.monotonic()
    with pytest.raises(AssertionError, match="invariant/liveness"):
        await await_condition(
            "never true",
            lambda: False,
            deadline_s=0.05,
            poll_s=0.01,
            progress=lambda: "0/10 done",
        )
    assert time.monotonic() - start < 5


async def test_await_condition_surfaces_infra_failure_immediately() -> None:
    def dead_stack() -> None:
        raise InfraFailure("worker pool is dead")

    with pytest.raises(InfraFailure, match="worker pool"):
        await await_condition(
            "anything", lambda: False, deadline_s=30, poll_s=0.01, infra_check=dead_stack
        )


# -- drainer wrapping: re-injection envelopes carry the right payload ------------


def test_drainer_wraps_each_topic_into_the_right_envelope_field(tmp_path: Path) -> None:
    drainer = Drainer(
        SpoolWriter(tmp_path),
        brokers="unused:0",
        events_topic="events",
        results_topic="results",
        decisions_topic="decisions",
        group="g",
    )

    event = _env(b"k1", b"raw")
    wrapped_event = drainer._wrap("events", b"k1", event.SerializeToString())
    assert wrapped_event == event

    result = ToolResult(intent_id="i-1", entity_key=b"k1", status=ToolResult.OK)
    wrapped_result = drainer._wrap("results", b"k1", result.SerializeToString())
    assert wrapped_result.WhichOneof("payload") == "tool_result"
    assert wrapped_result.tool_result == result
    assert wrapped_result.entity_key == b"k1"

    decision = AgentEnvelope.Approval(intent_id="i-2", approved=True, approver="t")
    wrapped_decision = drainer._wrap("decisions", b"k2", decision.SerializeToString())
    assert wrapped_decision.WhichOneof("payload") == "approval"
    assert wrapped_decision.approval == decision
    assert wrapped_decision.entity_key == b"k2"


# -- drainer sealing: a finished burst must become visible ------------------------


def test_should_seal_covers_burst_then_silence() -> None:
    # The original bug: a burst inside the seal interval followed by silence
    # never sealed (seal required new messages AND an elapsed interval at once).
    assert should_seal(5, idle=True, elapsed=False), "idle with pending must seal"
    assert should_seal(5, idle=False, elapsed=True), "elapsed with pending must seal"
    assert not should_seal(0, idle=True, elapsed=True), "nothing pending, nothing to seal"
    assert not should_seal(5, idle=False, elapsed=False), (
        "mid-burst inside the interval keeps batching"
    )


# -- feeder late-release gating: the orphan's determinism rests on it ------------


def _late_intent(key: bytes = b"late-r-00000") -> ToolIntent:
    return ToolIntent(intent_id="i-late", entity_key=key, kind=ToolIntent.APPROVAL)


def test_split_due_time_mode_releases_at_the_recorded_deadline() -> None:
    queue = [(10.0, _late_intent(b"late-r-00000")), (20.0, _late_intent(b"late-r-00001"))]
    due, kept = split_due(queue, now=15.0, late_ready=None)
    assert [t for t, _ in due] == [10.0]
    assert [t for t, _ in kept] == [20.0]


def test_split_due_gated_mode_releases_only_on_the_predicate() -> None:
    # Wall clock is irrelevant in gated mode: an "overdue" entry stays queued
    # until the fail-closed terminal is observed, and a not-yet-due entry
    # releases the moment it is.
    ready_keys = {b"late-r-00001"}
    queue = [(10.0, _late_intent(b"late-r-00000")), (99.0, _late_intent(b"late-r-00001"))]
    due, kept = split_due(queue, now=50.0, late_ready=lambda k: k in ready_keys)
    assert [i.entity_key for _, i in due] == [b"late-r-00001"]
    assert [i.entity_key for _, i in kept] == [b"late-r-00000"]
