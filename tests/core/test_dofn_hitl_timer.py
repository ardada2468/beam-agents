"""HITL timer dispatch: the stale-handle guard and the three timeout routes.

`on_hitl` is driven directly with fake state/timer doubles rather than through
a pipeline: the scenarios under test are about *what the callback does with the
handle it is given* (a fire timestamp against a stored deadline), and a
TestStream can only produce the one fire timestamp the runner chooses. The
end-to-end fail-closed ordering lives in `test_dofn_streaming`.
"""

from __future__ import annotations

from typing import Any

import apache_beam as beam
import pytest
from apache_beam.utils.timestamp import Timestamp

from beam_agents._protos import Continuation, ToolIntent, TraceEvent
from beam_agents.core.agent import Complete, FallbackContext, intent_id_for
from beam_agents.core.context import ActivationContext
from beam_agents.core.dofn import (
    HITL_TIMEOUT_OUTPUT,
    REASON_HITL_TIMEOUT,
    ActivationError,
    _AgentDoFn,
    _error,
)
from beam_agents.hitl import Deny, Drop, Escalate, HitlPolicy, Route
from beam_agents.model.fake import FakeLLM
from beam_agents.observability import (
    ROLE_ACTIVATION,
    ROLE_TIMER,
    span_id_for,
    trace_id_for,
)

_KEY = b"k"
_DEADLINE_MS = 2_000


class _FakeValueState:
    """Stand-in for a ReadModifyWriteState holding one `Continuation` (or none)."""

    def __init__(self, value: Continuation | None = None) -> None:
        self.value = value
        self.cleared = False

    def read(self) -> Continuation | None:
        return self.value

    def write(self, value: Continuation) -> None:
        self.value = value

    def clear(self) -> None:
        self.value = None
        self.cleared = True


class _FakeBagState:
    def __init__(self, items: list[ToolIntent] | None = None) -> None:
        self.items = list(items or [])
        self.cleared = False

    def read(self) -> list[ToolIntent]:
        return list(self.items)

    def add(self, item: ToolIntent) -> None:
        self.items.append(item)

    def clear(self) -> None:
        self.items = []
        self.cleared = True


class _FakeTimer:
    def __init__(self) -> None:
        self.set_to: Timestamp | None = None
        self.cleared = False

    def set(self, ts: Timestamp) -> None:
        self.set_to = ts

    def clear(self) -> None:
        self.cleared = True


def _continuation(*, deadline_ms: int = _DEADLINE_MS, escalations: int = 0) -> Continuation:
    return Continuation(
        state_schema_version=1,
        seq=3,
        step_index=2,
        pending_intent_ids=["intent-1"],
        adapter="test",
        snapshot=b"waiting",
        suspended_at_ms=1_000,
        deadline_ms=deadline_ms,
        escalations=escalations,
    )


async def _unused_agent(ctx: ActivationContext) -> Complete:  # pragma: no cover - never invoked
    raise AssertionError("the HITL fallback must not run an activation")


def _dofn(policy: HitlPolicy | None = None) -> _AgentDoFn:
    return _AgentDoFn(_unused_agent, provider_factory=FakeLLM, hitl_policy=policy)


def _fire(
    dofn: _AgentDoFn,
    *,
    cont: Continuation | None,
    fired_at_ms: int = _DEADLINE_MS,
    pending: list[ToolIntent] | None = None,
    ttl_timer: _FakeTimer | None = None,
) -> tuple[list[Any], _FakeValueState, _FakeBagState, _FakeTimer]:
    # `ttl_timer` is an optional out-param rather than a fifth return value:
    # only the escalation scenarios care what the callback does with the
    # working-memory mark, and the other dozen call sites should not have to
    # unpack a handle they ignore.
    continuation = _FakeValueState(cont)
    bag = _FakeBagState(pending)
    timer = _FakeTimer()
    emitted = list(
        dofn.on_hitl(
            key=_KEY,
            timestamp=Timestamp(micros=fired_at_ms * 1000),
            continuation=continuation,
            pending=bag,
            hitl_timer=timer,
            ttl_timer=ttl_timer if ttl_timer is not None else _FakeTimer(),
        )
    )
    return emitted, continuation, bag, timer


def _is_trace(element: Any) -> bool:
    return isinstance(element, beam.pvalue.TaggedOutput) and element.tag == "traces"


def outputs(emitted: list[Any]) -> list[Any]:
    """Everything the callback emitted except its `.traces` records.

    Every route that ends a wait also traces it, so the routing assertions
    below stay about routing.
    """
    return [element for element in emitted if not _is_trace(element)]


def traces(emitted: list[Any]) -> list[TraceEvent]:
    return [element.value for element in emitted if _is_trace(element)]


# --- Requirement: the timer dispatches a pure policy fallback -----------------


def _capturing(seen: list[FallbackContext]) -> HitlPolicy:
    # Module-level closure would not pickle, but this policy is only ever
    # driven directly (no pipeline), so a local capture is fine here.
    def on_timeout(fallback: FallbackContext) -> Route:
        seen.append(fallback)
        return Deny(b"denied")

    return HitlPolicy(on_timeout=on_timeout)


def test_policy_receives_kind_timer_and_the_expired_handle() -> None:
    # Scenario: Timer fire invokes the policy with kind=timer and the expired handle.
    seen: list[FallbackContext] = []
    emitted, _, _, _ = _fire(_dofn(_capturing(seen)), cont=_continuation(), fired_at_ms=2_500)

    assert outputs(emitted) == [b"denied"]
    assert seen == [
        FallbackContext(
            entity_key=_KEY,
            seq=3,
            snapshot=b"waiting",
            kind="timer",
            deadline_ms=_DEADLINE_MS,
            fired_at_ms=2_500,
            pending_intent_ids=("intent-1",),
        )
    ]


def test_a_raising_policy_fails_closed_to_the_errors_output() -> None:
    # Scenario: A raising policy fails closed to the errors output.
    def boom(fallback: FallbackContext) -> Route:
        raise RuntimeError("policy exploded")

    emitted, cont_state, bag, timer = _fire(
        _dofn(HitlPolicy(on_timeout=boom)), cont=_continuation()
    )

    (error,) = outputs(emitted)
    assert isinstance(error, beam.pvalue.TaggedOutput)
    assert error.tag == "errors"
    assert error.value.reason == REASON_HITL_TIMEOUT
    # The detail names the suspension *and* the failure, so triage does not
    # have to guess which key's policy blew up.
    assert error.value.detail == "seq=3 policy_error=RuntimeError('policy exploded')"
    assert cont_state.value is None
    assert bag.cleared is True
    assert timer.cleared is True


# --- Requirement: a stale HITL timer handle mutates nothing -------------------


def test_a_fire_earlier_than_the_live_deadline_is_a_no_op() -> None:
    # Scenario: A superseded timer handle does not kill a live continuation.
    seen: list[FallbackContext] = []
    cont = _continuation()
    emitted, cont_state, bag, timer = _fire(
        _dofn(_capturing(seen)), cont=cont, fired_at_ms=_DEADLINE_MS - 1
    )

    assert emitted == []
    assert seen == []
    assert cont_state.value == cont
    assert cont_state.cleared is False
    assert bag.cleared is False
    assert timer.cleared is False and timer.set_to is None


def test_a_fire_with_no_continuation_is_a_no_op() -> None:
    # Scenario: A timer fire with no continuation is a no-op.
    seen: list[FallbackContext] = []
    emitted, cont_state, bag, _ = _fire(_dofn(_capturing(seen)), cont=None)

    assert emitted == []
    assert seen == []
    assert bag.cleared is False
    assert cont_state.cleared is False


def test_a_fire_exactly_at_the_deadline_runs_the_fallback() -> None:
    # Scenario: A fire exactly at the deadline is live, not stale.
    emitted, _, _, _ = _fire(_dofn(), cont=_continuation(), fired_at_ms=_DEADLINE_MS)
    assert outputs(emitted) == [HITL_TIMEOUT_OUTPUT]


# --- Requirement: HitlPolicy routes a timeout to deny, drop, or escalate ------


def test_default_policy_denies_with_the_runtime_timeout_output() -> None:
    # Scenario: The default policy preserves existing behavior.
    emitted, cont_state, bag, timer = _fire(_dofn(), cont=_continuation())

    assert outputs(emitted) == [HITL_TIMEOUT_OUTPUT]
    assert cont_state.value is None
    assert bag.cleared is True
    assert timer.cleared is True


def test_deny_emits_its_bytes_and_clears_the_continuation() -> None:
    # Scenario: Deny emits deterministic bytes and clears the continuation.
    def route(fallback: FallbackContext) -> Route:
        return Deny(b"degraded-answer")

    emitted, cont_state, bag, _ = _fire(_dofn(HitlPolicy(on_timeout=route)), cont=_continuation())

    assert outputs(emitted) == [b"degraded-answer"]
    assert cont_state.value is None
    assert bag.cleared is True


def test_drop_routes_the_timeout_to_errors_with_no_main_output() -> None:
    # Scenario: Drop routes the timeout to the errors output.
    def route(fallback: FallbackContext) -> Route:
        return Drop("gave_up")

    emitted, cont_state, bag, _ = _fire(_dofn(HitlPolicy(on_timeout=route)), cont=_continuation())

    (dropped,) = outputs(emitted)
    assert isinstance(dropped, beam.pvalue.TaggedOutput)
    assert dropped.value.reason == "gave_up"
    assert dropped.value.entity_key == _KEY
    assert dropped.value.detail == "seq=3"
    assert cont_state.value is None
    assert bag.cleared is True


# --- Requirement: Failure routes emit ERROR trace events ---------------------


def test_a_hitl_timeout_is_traced_in_the_suspended_activations_trace() -> None:
    # Scenario: A HITL timeout is traced. The wait ended without an answer, on
    # both the deny and the drop route, and the trace says so in the trace the
    # suspended activation's own events are already in.
    def dropped(fallback: FallbackContext) -> Route:
        return Drop("gave_up")

    for policy in (_dofn(), _dofn(HitlPolicy(on_timeout=dropped))):
        emitted, _, _, _ = _fire(policy, cont=_continuation(), fired_at_ms=2_500)

        (trace,) = traces(emitted)
        assert trace.event_type == TraceEvent.ERROR
        assert trace.attributes["beam_agents.reason"] == REASON_HITL_TIMEOUT
        assert trace.trace_id == trace_id_for(_KEY, 3)
        assert trace.start_ms == 2_500
        # The `timer` role: fired outside any activation, so its span must not
        # collide with a step the activation itself may have traced.
        assert trace.span_id == span_id_for(_KEY, 3, ROLE_TIMER, 0)


def test_a_stale_timer_handle_emits_no_trace() -> None:
    # A no-op route must stay a no-op: manufacturing a trace for a handle that
    # was superseded would report a timeout that never happened.
    emitted, _, _, _ = _fire(_dofn(), cont=_continuation(), fired_at_ms=_DEADLINE_MS - 1)
    assert traces(emitted) == []


def _escalate(fallback: FallbackContext) -> Route:
    return Escalate(tool_name="pager", args_json='{"level":2}', timeout_ms=5_000)


def test_an_escalation_intent_is_traced_and_carries_the_trace_id() -> None:
    # Scenario: An escalation intent is traced from the timer callback.
    # Scenario: An escalation intent carries the suspended activation's trace id.
    policy = HitlPolicy(on_timeout=_escalate, max_escalations=2)
    emitted, _, _, _ = _fire(_dofn(policy), cont=_continuation(), fired_at_ms=2_500)

    (tagged,) = outputs(emitted)
    intent = tagged.value
    assert intent.trace_id == trace_id_for(_KEY, 3)

    (trace,) = traces(emitted)
    assert trace.event_type == TraceEvent.INTENT_EMITTED
    assert trace.trace_id == intent.trace_id
    # Timed at the fire, and placed at the step the continuation had reached,
    # under that suspension's activation span -- so the escalation reads as
    # part of the wait rather than as a detached event.
    assert trace.start_ms == 2_500
    assert trace.step_index == 2
    assert trace.parent_span_id == span_id_for(_KEY, 3, ROLE_ACTIVATION, 2)
    assert trace.attributes["beam_agents.intent_id"] == intent.intent_id
    assert trace.attributes["beam_agents.tool_name"] == "pager"
    assert trace.attributes["beam_agents.intent_kind"] == "APPROVAL"
    assert trace.attributes["beam_agents.expires_at_ms"] == "7500"


def test_a_retried_timer_bundle_remints_an_identical_escalation_trace() -> None:
    # The escalation's determinism now covers the trace id it carries: a
    # retried timer bundle must re-mint byte-identical intents.
    policy = HitlPolicy(on_timeout=_escalate, max_escalations=2)
    cont = _continuation()
    first, _, _, _ = _fire(_dofn(policy), cont=cont, fired_at_ms=2_500)
    retry, _, _, _ = _fire(_dofn(policy), cont=cont, fired_at_ms=2_500)

    assert outputs(first)[0].value.SerializeToString(deterministic=True) == outputs(retry)[
        0
    ].value.SerializeToString(deterministic=True)
    assert traces(first)[0].SerializeToString(deterministic=True) == traces(retry)[
        0
    ].SerializeToString(deterministic=True)


def test_escalate_stages_a_deterministic_intent_and_rearms_the_deadline() -> None:
    # Scenario: Escalate re-arms the deadline with a deterministic intent.
    policy = HitlPolicy(on_timeout=_escalate, max_escalations=2)
    pending = [ToolIntent(intent_id="intent-1", expires_at_ms=60_000)]
    emitted, cont_state, bag, timer = _fire(
        _dofn(policy), cont=_continuation(), fired_at_ms=2_500, pending=pending
    )

    (tagged,) = outputs(emitted)
    assert tagged.tag == "intents"
    intent = tagged.value
    # Consumes the continuation's next free step (2), so it collides with
    # neither the suspended activation's steps (0,1) nor a later resume.
    assert intent.intent_id == intent_id_for(_KEY, 3, 2)
    assert intent.kind == ToolIntent.APPROVAL
    assert intent.tool_name == "pager"
    assert intent.args_json == '{"level":2}'
    assert intent.created_at_ms == 2_500
    assert intent.expires_at_ms == 7_500
    # Correlation fields: the effector dedups on intent_id, but the result it
    # publishes is routed back by entity_key and matched by seq/step_index.
    assert intent.entity_key == _KEY
    assert intent.seq == 3
    assert intent.step_index == 2

    updated = cont_state.value
    assert updated is not None
    assert updated.deadline_ms == 7_500
    assert updated.escalations == 1
    assert updated.step_index == 3
    # The original request stays answerable; escalation adds a channel.
    assert list(updated.pending_intent_ids) == ["intent-1", intent.intent_id]
    assert updated.snapshot == b"waiting"
    assert updated.seq == 3
    assert bag.cleared is False
    assert bag.items == [*pending, intent]
    assert timer.set_to == Timestamp(micros=7_500 * 1000)
    assert timer.cleared is False


def test_escalate_carries_the_memory_ttl_mark_past_the_new_deadline() -> None:
    # Scenario: An escalation carries the memory TTL forward with the deadline.
    # Escalation walks `deadline_ms` past the mark the original suspension armed
    # `TTL_TIMER` at; without re-arming, working-memory GC would fire mid-wait
    # and wipe the very continuation the escalation is waiting on.
    policy = HitlPolicy(on_timeout=_escalate, max_escalations=2)
    ttl_timer = _FakeTimer()
    dofn = _AgentDoFn(_unused_agent, provider_factory=FakeLLM, ttl_ms=100, hitl_policy=policy)
    _, cont_state, _, _ = _fire(dofn, cont=_continuation(), fired_at_ms=2_500, ttl_timer=ttl_timer)

    updated = cont_state.value
    assert updated is not None
    assert updated.deadline_ms == 7_500
    # Strictly after the new deadline, so the GC cannot beat the HITL timer.
    assert ttl_timer.set_to == Timestamp(micros=(7_500 + 100) * 1000)
    assert ttl_timer.cleared is False


def test_deny_and_drop_leave_the_memory_ttl_mark_alone() -> None:
    # Deny/Drop end the suspension, so the mark armed at commit is already
    # correct and firing it later is the desired GC -- only Escalate re-arms.
    for route in (Deny(b"denied"), Drop("gave_up")):
        ttl_timer = _FakeTimer()
        _fire(
            _dofn(HitlPolicy(on_timeout=lambda _f, r=route: r)),  # type: ignore[misc]
            cont=_continuation(),
            ttl_timer=ttl_timer,
        )

        assert ttl_timer.set_to is None, route
        assert ttl_timer.cleared is False, route


def test_escalating_twice_mints_distinct_deterministic_intents() -> None:
    policy = HitlPolicy(on_timeout=_escalate, max_escalations=2)
    first, cont_state, _, _ = _fire(_dofn(policy), cont=_continuation(), fired_at_ms=2_500)
    second, _, _, _ = _fire(_dofn(policy), cont=cont_state.value, fired_at_ms=7_500)

    assert first[0].value.intent_id == intent_id_for(_KEY, 3, 2)
    assert second[0].value.intent_id == intent_id_for(_KEY, 3, 3)


def test_a_retried_timer_bundle_remints_an_identical_escalation() -> None:
    # A timer callback re-executes on bundle retry with state rolled back, so
    # the second attempt must produce byte-identical bytes for the effector to
    # dedup.
    policy = HitlPolicy(on_timeout=_escalate, max_escalations=2)
    cont = _continuation()
    first, _, _, _ = _fire(_dofn(policy), cont=cont, fired_at_ms=2_500)
    retry, _, _, _ = _fire(_dofn(policy), cont=cont, fired_at_ms=2_500)

    assert first[0].value.SerializeToString(deterministic=True) == retry[0].value.SerializeToString(
        deterministic=True
    )


@pytest.mark.parametrize(("max_escalations", "escalations"), [(0, 0), (1, 1), (2, 2)])
def test_escalation_is_bounded(max_escalations: int, escalations: int) -> None:
    # Scenario: Escalation is bounded.
    policy = HitlPolicy(on_timeout=_escalate, max_escalations=max_escalations)
    emitted, cont_state, bag, timer = _fire(
        _dofn(policy), cont=_continuation(escalations=escalations)
    )

    assert outputs(emitted) == [HITL_TIMEOUT_OUTPUT]
    assert cont_state.value is None
    assert bag.cleared is True
    assert timer.cleared is True


def test_the_fallback_never_touches_seq() -> None:
    # Scenario: A timer fire does not increment SEQ. The callback does not even
    # declare the SEQ state param, so it cannot.
    params = _AgentDoFn.on_hitl.__defaults__ or ()
    state_tags = {p.state_spec.name for p in params if isinstance(p, beam.DoFn.StateParam)}
    assert state_tags == {"continuation", "pending"}


# --- Construction: the DoFn holds exactly what it was configured with ---------


def test_dofn_holds_its_configured_knobs() -> None:
    # Every knob is read later on a different code path (setup, commit, timer),
    # so a constructor that drops or swaps one fails far from here.
    policy = HitlPolicy(timeout_ms=1_234, approval_channel="pager")
    dofn = _AgentDoFn(
        _unused_agent,
        provider_factory=FakeLLM,
        activation_timeout_s=12.5,
        ttl_ms=99_000,
        cancel_grace_s=1.5,
        hitl_policy=policy,
    )

    assert dofn._agent is _unused_agent
    assert dofn._provider_factory is FakeLLM
    assert dofn._activation_timeout_s == 12.5
    assert dofn._ttl_ms == 99_000
    assert dofn._cancel_grace_s == 1.5
    assert dofn._hitl_policy is policy
    # Not built until setup(): a DoFn is constructed at pipeline-construction
    # time and pickled to the worker, so neither may exist yet.
    assert dofn._bridge is None
    assert dofn._provider is None


def test_dofn_defaults_are_the_documented_ones() -> None:
    dofn = _AgentDoFn(_unused_agent, provider_factory=FakeLLM)

    assert dofn._activation_timeout_s == 30.0
    assert dofn._ttl_ms == 3_600_000
    assert dofn._cancel_grace_s == 5.0
    assert dofn._hitl_policy == HitlPolicy()


def test_error_records_carry_an_empty_detail_by_default() -> None:
    # `_error(key, reason)` is the two-argument form used by the timeout path;
    # its record must carry no detail rather than a placeholder.
    tagged = _error(b"k", "some_reason")

    assert tagged.tag == "errors"
    assert tagged.value == ActivationError(b"k", "some_reason", "")
    assert tagged.value.detail == ""
