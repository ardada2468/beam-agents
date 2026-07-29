"""`ERROR` trace events on the DoFn's failure routes.

Driven with fake state/timer doubles rather than through a pipeline, for the
same reason `test_dofn_ttl` and `test_dofn_hitl_timer` are: the scenario under
test is *what the route emits and what it leaves untouched*, and this keeps the
new branches inside the mutation gate's test selection (the pipeline suites are
deselected there).

The HITL-timeout and TTL-wiped-suspension routes are covered in their own
files, alongside the routing behavior they extend.
"""

from __future__ import annotations

from typing import Any

import apache_beam as beam

from beam_agents._protos import (
    AgentEnvelope,
    Continuation,
    LlmCacheBlob,
    MemoryBlob,
    ToolIntent,
    ToolResult,
)
from beam_agents._protos import TraceEvent as TraceEventProto
from beam_agents.core.dofn import (
    DETAIL_INTENT_EXPIRED,
    DETAIL_NO_CONTINUATION,
    REASON_ERROR,
    REASON_ORPHANED,
    REASON_TIMEOUT,
    ActivationError,
    _AgentDoFn,
)
from beam_agents.observability import ROLE_ACTIVATION, span_id_for, trace_id_for
from tests.core._dofn_helpers import (
    hang_agent,
    make_briefly_slow_provider,
    make_pong_provider,
    make_slow_provider,
    model_act_then_fail_agent,
    raising_agent,
    seq_agent,
    suspend_then_fail_agent,
)

_KEY = b"k"
_NOW_MS = 5_000
_SEQ = 3


class _FakeState:
    """Stand-in for any of the DoFn's value/bag/combining state specs."""

    def __init__(self, value: Any = None) -> None:
        self.value = value
        self.writes: list[Any] = []
        self.cleared = False

    def read(self) -> Any:
        return self.value

    def write(self, value: Any) -> None:
        self.writes.append(value)
        self.value = value

    def add(self, value: Any) -> None:
        self.writes.append(value)

    def clear(self) -> None:
        self.cleared = True


class _FakeTimer:
    def __init__(self) -> None:
        self.set_to: Any = None
        self.cleared = False

    def set(self, ts: Any) -> None:
        self.set_to = ts

    def clear(self) -> None:
        self.cleared = True


class _Handles:
    def __init__(self, cont: Continuation | None = None) -> None:
        self.memory = _FakeState(MemoryBlob())
        self.continuation = _FakeState(cont)
        self.llm_cache = _FakeState(LlmCacheBlob())
        self.pending = _FakeState([])
        self.seq = _FakeState(_SEQ)
        self.ttl_timer = _FakeTimer()
        self.hitl_timer = _FakeTimer()

    @property
    def states(self) -> list[_FakeState]:
        return [self.memory, self.continuation, self.llm_cache, self.pending, self.seq]

    def untouched(self) -> bool:
        """True when no state spec was written, added to, or cleared."""
        return all(not state.writes and not state.cleared for state in self.states)


def _process(dofn: _AgentDoFn, envelope: AgentEnvelope, handles: _Handles) -> list[Any]:
    dofn.setup()
    try:
        return list(
            dofn.process(
                (_KEY, envelope),
                memory=handles.memory,
                continuation=handles.continuation,
                llm_cache=handles.llm_cache,
                pending=handles.pending,
                seq=handles.seq,
                ttl_timer=handles.ttl_timer,
                hitl_timer=handles.hitl_timer,
            )
        )
    finally:
        dofn.teardown()


def _tagged(emitted: list[Any], tag: str) -> list[Any]:
    return [e.value for e in emitted if isinstance(e, beam.pvalue.TaggedOutput) and e.tag == tag]


def _event(envelope_bytes: bytes = b"go") -> AgentEnvelope:
    return AgentEnvelope(entity_key=_KEY, event_time_ms=_NOW_MS, external_event=envelope_bytes)


# --- Requirement: Failure routes emit ERROR trace events ---------------------


def test_an_activation_timeout_is_traced_and_commits_nothing() -> None:
    # Scenario: An activation timeout is traced and commits nothing.
    # `make_briefly_slow_provider` outlasts the 50ms budget but finishes in
    # ~300ms, so an activation that failed to *apply* its timeout would commit
    # a result here rather than hang.
    dofn = _AgentDoFn(
        hang_agent,
        provider_factory=make_briefly_slow_provider,
        activation_timeout_s=0.05,
        cancel_grace_s=0.5,
    )
    handles = _Handles()

    emitted = _process(dofn, _event(), handles)

    assert _tagged(emitted, "errors") == [ActivationError(_KEY, REASON_TIMEOUT, "", _NOW_MS)]
    (trace,) = _tagged(emitted, "traces")
    assert trace.event_type == TraceEventProto.ERROR
    assert trace.attributes["beam_agents.reason"] == REASON_TIMEOUT
    assert trace.trace_id == trace_id_for(_KEY, _SEQ)
    # A child of the activation span it belongs to, not a second root: an
    # activation that committed events earlier reads as one trace.
    assert trace.parent_span_id == span_id_for(_KEY, _SEQ, ROLE_ACTIVATION, 0)
    assert trace.span_id == span_id_for(_KEY, _SEQ, "ERROR", 0)
    assert trace.start_ms == _NOW_MS
    # No exception to name on the timeout route: the attribute is absent
    # rather than an empty string. Likewise the failure position — the
    # coroutine may still be running, so its context is unreachable by
    # construction, and unknown means absent, never defaulted.
    assert "error.type" not in trace.attributes
    assert not any(key.startswith("beam_agents.failure.") for key in trace.attributes)
    assert handles.untouched()


def test_a_resume_that_times_out_is_traced_under_the_continuations_seq() -> None:
    # The resume path has its own timeout branch, and it must scope the trace
    # to the *continuation's* seq -- the resumed activation's, not the key's
    # current counter -- or the timeout lands in a different trace from the
    # suspension that led to it.
    dofn = _AgentDoFn(
        hang_agent,
        provider_factory=make_briefly_slow_provider,
        activation_timeout_s=0.05,
        cancel_grace_s=0.5,
    )
    cont = Continuation(
        state_schema_version=1,
        seq=1,
        step_index=2,
        pending_intent_ids=["intent-1"],
        snapshot=b"waiting",
        suspended_at_ms=1_000,
        deadline_ms=_NOW_MS + 1_000,
    )
    handles = _Handles(cont=cont)
    handles.pending.value = [ToolIntent(intent_id="intent-1", expires_at_ms=_NOW_MS + 1_000)]
    envelope = AgentEnvelope(
        entity_key=_KEY,
        event_time_ms=_NOW_MS,
        tool_result=ToolResult(intent_id="intent-1", entity_key=_KEY, status=ToolResult.OK),
    )

    emitted = _process(dofn, envelope, handles)

    assert _tagged(emitted, "errors") == [ActivationError(_KEY, REASON_TIMEOUT, "", _NOW_MS)]
    (trace,) = _tagged(emitted, "traces")
    assert trace.attributes["beam_agents.reason"] == REASON_TIMEOUT
    assert trace.trace_id == trace_id_for(_KEY, 1)
    assert trace.start_ms == _NOW_MS
    assert handles.untouched()


def test_a_resume_that_raises_is_traced_under_the_continuations_seq() -> None:
    dofn = _AgentDoFn(raising_agent, provider_factory=make_slow_provider)
    cont = Continuation(
        state_schema_version=1,
        seq=1,
        step_index=2,
        pending_intent_ids=["intent-1"],
        snapshot=b"waiting",
        suspended_at_ms=1_000,
        deadline_ms=_NOW_MS + 1_000,
    )
    handles = _Handles(cont=cont)
    handles.pending.value = [ToolIntent(intent_id="intent-1", expires_at_ms=_NOW_MS + 1_000)]
    envelope = AgentEnvelope(
        entity_key=_KEY,
        event_time_ms=_NOW_MS,
        tool_result=ToolResult(intent_id="intent-1", entity_key=_KEY, status=ToolResult.OK),
    )

    emitted = _process(dofn, envelope, handles)

    # `raising_agent` writes memory then raises, staging nothing past the
    # driver's ACTIVATION_START; the cursor sits at the continuation's seed.
    assert _tagged(emitted, "errors") == [
        ActivationError(
            _KEY,
            REASON_ERROR,
            "RuntimeError('agent blew up') failed_at_step=2 after=ACTIVATION_START",
            _NOW_MS,
        )
    ]
    (trace,) = _tagged(emitted, "traces")
    assert trace.attributes["beam_agents.reason"] == REASON_ERROR
    assert trace.attributes["error.type"] == "RuntimeError"
    assert trace.trace_id == trace_id_for(_KEY, 1)
    assert trace.start_ms == _NOW_MS
    assert handles.untouched()


def test_a_raising_activation_is_traced_with_its_error_type() -> None:
    # Scenario: A raising activation is traced with its error type.
    dofn = _AgentDoFn(raising_agent, provider_factory=make_slow_provider)
    handles = _Handles()

    emitted = _process(dofn, _event(), handles)

    assert _tagged(emitted, "errors") == [
        ActivationError(
            _KEY,
            REASON_ERROR,
            "RuntimeError('agent blew up') failed_at_step=0 after=ACTIVATION_START",
            _NOW_MS,
        )
    ]
    (trace,) = _tagged(emitted, "traces")
    assert trace.attributes["beam_agents.reason"] == REASON_ERROR
    assert trace.attributes["error.type"] == "RuntimeError"
    assert trace.trace_id == trace_id_for(_KEY, _SEQ)
    assert trace.start_ms == _NOW_MS
    assert handles.untouched()


def test_a_start_failure_carries_its_position_in_both_records() -> None:
    # Scenario: A raising activation is traced with its error type and failure
    # position. Scenario: The dead letter names the failure position. One
    # provider-reached call, one staged intent, then the raise: both records
    # name step 2 after the INTENT_EMITTED, and `error.type` names the
    # *original* exception class, not the runtime's wrapper.
    dofn = _AgentDoFn(model_act_then_fail_agent, provider_factory=make_pong_provider)
    handles = _Handles()

    emitted = _process(dofn, _event(), handles)

    assert _tagged(emitted, "errors") == [
        ActivationError(
            _KEY,
            REASON_ERROR,
            "RuntimeError('agent blew up') failed_at_step=2 after=INTENT_EMITTED",
            _NOW_MS,
        )
    ]
    (trace,) = _tagged(emitted, "traces")
    assert trace.attributes["beam_agents.reason"] == REASON_ERROR
    assert trace.attributes["error.type"] == "RuntimeError"
    assert trace.attributes["beam_agents.failure.step"] == "2"
    assert trace.attributes["beam_agents.failure.last_event"] == "INTENT_EMITTED"
    assert trace.attributes["beam_agents.failure.staged_intents"] == "1"
    assert trace.attributes["beam_agents.failure.llm_calls"] == "1"
    assert trace.trace_id == trace_id_for(_KEY, _SEQ)
    assert trace.start_ms == _NOW_MS
    assert handles.untouched()


def test_a_resume_failure_carries_its_position_in_both_records() -> None:
    # The resume route reads the same wrapper: `suspend_then_fail_agent` raises
    # before staging anything of its own, so the position is the continuation's
    # seed cursor after the driver's ACTIVATION_START — with all four
    # attributes present (zeros are known values here, not defaults).
    dofn = _AgentDoFn(suspend_then_fail_agent, provider_factory=make_pong_provider)
    cont = Continuation(
        state_schema_version=1,
        seq=1,
        step_index=2,
        pending_intent_ids=["intent-1"],
        snapshot=b"waiting",
        suspended_at_ms=1_000,
        deadline_ms=_NOW_MS + 1_000,
    )
    handles = _Handles(cont=cont)
    handles.pending.value = [ToolIntent(intent_id="intent-1", expires_at_ms=_NOW_MS + 1_000)]
    envelope = AgentEnvelope(
        entity_key=_KEY,
        event_time_ms=_NOW_MS,
        tool_result=ToolResult(intent_id="intent-1", entity_key=_KEY, status=ToolResult.OK),
    )

    emitted = _process(dofn, envelope, handles)

    assert _tagged(emitted, "errors") == [
        ActivationError(
            _KEY,
            REASON_ERROR,
            "RuntimeError('resume blew up') failed_at_step=2 after=ACTIVATION_START",
            _NOW_MS,
        )
    ]
    (trace,) = _tagged(emitted, "traces")
    assert trace.attributes["error.type"] == "RuntimeError"
    assert trace.attributes["beam_agents.failure.step"] == "2"
    assert trace.attributes["beam_agents.failure.last_event"] == "ACTIVATION_START"
    assert trace.attributes["beam_agents.failure.staged_intents"] == "0"
    assert trace.attributes["beam_agents.failure.llm_calls"] == "0"
    assert trace.trace_id == trace_id_for(_KEY, 1)
    assert handles.untouched()


def test_a_failure_outside_the_wrap_keeps_the_un_enriched_shape() -> None:
    # Failures raised outside `run_activation`'s wrap window (here: the
    # setup() guard, which fires before the bridge ever runs) take the generic
    # fallback, and its two records keep today's exact shape: the bare
    # exception repr as the detail, `error.type`, and no position — there is
    # none to report, and absent beats defaulted.
    dofn = _AgentDoFn(seq_agent, provider_factory=make_pong_provider)
    dofn._provider = make_pong_provider()  # bridge deliberately absent
    handles = _Handles()

    emitted = list(
        dofn.process(
            (_KEY, _event()),
            memory=handles.memory,
            continuation=handles.continuation,
            llm_cache=handles.llm_cache,
            pending=handles.pending,
            seq=handles.seq,
            ttl_timer=handles.ttl_timer,
            hitl_timer=handles.hitl_timer,
        )
    )

    assert _tagged(emitted, "errors") == [
        ActivationError(_KEY, REASON_ERROR, "AssertionError('setup() not called')", _NOW_MS)
    ]
    (trace,) = _tagged(emitted, "traces")
    assert trace.attributes["beam_agents.reason"] == REASON_ERROR
    assert trace.attributes["error.type"] == "AssertionError"
    assert not any(key.startswith("beam_agents.failure.") for key in trace.attributes)
    assert trace.trace_id == trace_id_for(_KEY, _SEQ)
    assert trace.start_ms == _NOW_MS
    assert handles.untouched()


def test_a_resume_failure_outside_the_wrap_keeps_the_un_enriched_shape() -> None:
    # The resume route has its own generic fallback, scoped to the
    # continuation's seq; it too keeps the un-enriched shape for anything the
    # wrap never saw.
    dofn = _AgentDoFn(seq_agent, provider_factory=make_pong_provider)
    dofn._provider = make_pong_provider()  # bridge deliberately absent
    cont = Continuation(
        state_schema_version=1,
        seq=1,
        step_index=2,
        pending_intent_ids=["intent-1"],
        snapshot=b"waiting",
        suspended_at_ms=1_000,
        deadline_ms=_NOW_MS + 1_000,
    )
    handles = _Handles(cont=cont)
    handles.pending.value = [ToolIntent(intent_id="intent-1", expires_at_ms=_NOW_MS + 1_000)]
    envelope = AgentEnvelope(
        entity_key=_KEY,
        event_time_ms=_NOW_MS,
        tool_result=ToolResult(intent_id="intent-1", entity_key=_KEY, status=ToolResult.OK),
    )

    emitted = list(
        dofn.process(
            (_KEY, envelope),
            memory=handles.memory,
            continuation=handles.continuation,
            llm_cache=handles.llm_cache,
            pending=handles.pending,
            seq=handles.seq,
            ttl_timer=handles.ttl_timer,
            hitl_timer=handles.hitl_timer,
        )
    )

    assert _tagged(emitted, "errors") == [
        ActivationError(_KEY, REASON_ERROR, "AssertionError('setup() not called')", _NOW_MS)
    ]
    (trace,) = _tagged(emitted, "traces")
    assert trace.attributes["beam_agents.reason"] == REASON_ERROR
    assert trace.attributes["error.type"] == "AssertionError"
    assert not any(key.startswith("beam_agents.failure.") for key in trace.attributes)
    assert trace.trace_id == trace_id_for(_KEY, 1)
    assert trace.start_ms == _NOW_MS
    assert handles.untouched()


def test_two_identical_failing_runs_synthesize_byte_identical_events() -> None:
    # Scenario: A replayed failure synthesizes a byte-identical enriched event.
    # Every captured field is a pure function of the deterministic path, so
    # trace dedup on content still collapses a retried bundle's duplicate.
    serialized = []
    for _ in range(2):
        dofn = _AgentDoFn(model_act_then_fail_agent, provider_factory=make_pong_provider)
        handles = _Handles()
        emitted = _process(dofn, _event(), handles)
        (trace,) = _tagged(emitted, "traces")
        serialized.append(trace.SerializeToString(deterministic=True))

    assert serialized[0] == serialized[1]


def test_the_staged_traces_of_a_failed_activation_are_not_emitted() -> None:
    # Scenario: Staged traces of a failed activation are discarded.
    # `raising_agent` runs far enough to stage an ACTIVATION_START (and a
    # memory write); correctness invariant 1 says none of it survives. The
    # synthesized ERROR event is the only trace, and it comes from the DoFn's
    # own knowledge, not from the rolled-back context.
    dofn = _AgentDoFn(raising_agent, provider_factory=make_slow_provider)
    handles = _Handles()

    emitted = _process(dofn, _event(), handles)

    assert [trace.event_type for trace in _tagged(emitted, "traces")] == [TraceEventProto.ERROR]
    assert handles.untouched()


def test_an_orphaned_resume_is_traced() -> None:
    # Scenario: An orphaned resume is traced.
    dofn = _AgentDoFn(seq_agent, provider_factory=make_slow_provider)
    handles = _Handles(cont=None)
    envelope = AgentEnvelope(
        entity_key=_KEY,
        event_time_ms=_NOW_MS,
        tool_result=ToolResult(intent_id="ghost", entity_key=_KEY, status=ToolResult.OK),
    )

    emitted = _process(dofn, envelope, handles)

    assert _tagged(emitted, "errors") == [
        ActivationError(_KEY, REASON_ORPHANED, f"{DETAIL_NO_CONTINUATION}:ghost", _NOW_MS)
    ]
    (trace,) = _tagged(emitted, "traces")
    assert trace.attributes["beam_agents.reason"] == REASON_ORPHANED
    assert trace.attributes["error.type"] == DETAIL_NO_CONTINUATION
    # No continuation to scope against, so the key's current seq is used.
    assert trace.trace_id == trace_id_for(_KEY, _SEQ)
    assert trace.start_ms == _NOW_MS
    assert handles.untouched()


def test_a_successful_activation_emits_no_error_trace() -> None:
    # The failure routes must not fire on the happy path: a committed
    # activation's traces are its own staged ones, with no ERROR among them.
    dofn = _AgentDoFn(seq_agent, provider_factory=make_slow_provider)
    handles = _Handles()

    emitted = _process(dofn, _event(), handles)

    assert _tagged(emitted, "errors") == []
    assert TraceEventProto.ERROR not in {trace.event_type for trace in _tagged(emitted, "traces")}
    assert not handles.untouched()


def test_a_resume_whose_pending_intent_expired_is_refused() -> None:
    # Fail-closed layer 1 reads PENDING, not just the continuation: an intent
    # past its expiry can never be answered, because the effector refuses it.
    # A resume that skipped the PENDING read would admit this one.
    dofn = _AgentDoFn(seq_agent, provider_factory=make_slow_provider)
    cont = Continuation(
        state_schema_version=1,
        seq=1,
        step_index=2,
        pending_intent_ids=["intent-1"],
        snapshot=b"waiting",
        suspended_at_ms=1_000,
        deadline_ms=_NOW_MS + 1_000,
    )
    handles = _Handles(cont=cont)
    handles.pending.value = [ToolIntent(intent_id="intent-1", expires_at_ms=_NOW_MS - 1)]
    envelope = AgentEnvelope(
        entity_key=_KEY,
        event_time_ms=_NOW_MS,
        tool_result=ToolResult(intent_id="intent-1", entity_key=_KEY, status=ToolResult.OK),
    )

    emitted = _process(dofn, envelope, handles)

    assert _tagged(emitted, "errors") == [
        ActivationError(_KEY, REASON_ORPHANED, f"{DETAIL_INTENT_EXPIRED}:intent-1", _NOW_MS)
    ]
    assert handles.untouched()
