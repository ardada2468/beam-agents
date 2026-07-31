"""The `budget_exceeded` dead-letter route (`token-budgets` capability).

Driven with the fake state/timer handles rather than through a pipeline, for the
same reason `test_dofn_failure_traces` is: the scenario under test is *what the
route emits and what it leaves untouched*, and this keeps the new branches
inside the mutation gate's test selection (the pipeline suites are deselected
there).
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
from beam_agents.core.agent import Complete
from beam_agents.core.context import ActivationContext
from beam_agents.core.dofn import (
    REASON_BUDGET_EXCEEDED,
    REASON_ERROR,
    ActivationError,
    _AgentDoFn,
)
from beam_agents.observability import trace_id_for
from beam_agents.observability.metrics import COUNTER_AGENT_ERRORS, DISTRIBUTION_ACTIVATION_MS
from tests.core._context_helpers import decode_len_based
from tests.core._dofn_fakes import FakeBag, FakeSum, FakeTimer, FakeValue, RecordingMetrics
from tests.core._dofn_helpers import make_pong_provider, raising_agent, request

_KEY = b"k"
_NOW_MS = 5_000
_SEQ = 3
_TTL_MS = 60_000
# b"pong" decodes to 2 * 4 = 8 tokens under `decode_len_based`, so a budget of
# 10 admits the first call and is crossed by the second at 16.
_BUDGET = 10


async def spend_then_bust_agent(ctx: ActivationContext) -> Complete:
    """Two model calls with an intent between them; the second busts the budget.

    Gives the failure position something to name — one staged intent, two
    provider-reached calls — and stages a memory write, so "nothing staged
    escapes" is a claim about real staged effects rather than an empty context.
    """
    ctx.memory.set("scratch", b"should-not-persist")
    await ctx.call_model(request())
    ctx.act("http.post", '{"url":"x"}', ttl_ms=_TTL_MS)
    await ctx.call_model(request("second"))
    return Complete(output=b"unreachable")


class _Handles:
    """The five state specs plus the two element-path timers, as fakes."""

    def __init__(
        self, cont: Continuation | None = None, pending: list[ToolIntent] | None = None
    ) -> None:
        self.memory = FakeValue(MemoryBlob())
        self.continuation = FakeValue(cont)
        self.llm_cache = FakeValue(LlmCacheBlob())
        self.pending = FakeBag(pending)
        self.seq = FakeSum(_SEQ)
        self.batch = FakeBag()
        self.ttl_timer = FakeTimer()
        self.hitl_timer = FakeTimer()
        self.flush_timer = FakeTimer()
        self._before = self.snapshot()

    def snapshot(self) -> tuple[object, ...]:
        """Every state spec's committed content, byte-for-byte where protobuf."""
        return (
            _blob(self.memory.value),
            _blob(self.continuation.value),
            _blob(self.llm_cache.value),
            tuple(_blob(item) for item in self.pending.items),
            self.seq.value,
            tuple(_blob(item) for item in self.batch.items),
        )

    def unchanged(self) -> bool:
        return self.snapshot() == self._before


def _blob(message: Any) -> bytes | None:
    if message is None:
        return None
    encoded: bytes = message.SerializeToString(deterministic=True)
    return encoded


def _budgeted_dofn(agent: Any, metrics: RecordingMetrics | None = None) -> _AgentDoFn:
    return _AgentDoFn(
        agent,
        provider_factory=make_pong_provider,
        decode=decode_len_based,
        max_tokens_per_activation=_BUDGET,
        metrics=metrics if metrics is not None else RecordingMetrics(),
    )


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
                batch=handles.batch,
                flush_timer=handles.flush_timer,
            )
        )
    finally:
        dofn.teardown()


def _tagged(emitted: list[Any], tag: str) -> list[Any]:
    return [e.value for e in emitted if isinstance(e, beam.pvalue.TaggedOutput) and e.tag == tag]


def _event() -> AgentEnvelope:
    return AgentEnvelope(entity_key=_KEY, event_time_ms=_NOW_MS, external_event=b"go")


def _resume_envelope() -> AgentEnvelope:
    return AgentEnvelope(
        entity_key=_KEY,
        event_time_ms=_NOW_MS,
        tool_result=ToolResult(intent_id="intent-1", entity_key=_KEY, status=ToolResult.OK),
    )


def _live_continuation() -> Continuation:
    return Continuation(
        state_schema_version=1,
        seq=1,
        step_index=2,
        pending_intent_ids=["intent-1"],
        snapshot=b"waiting",
        suspended_at_ms=1_000,
        deadline_ms=_NOW_MS + 1_000,
    )


#: The crossing call stages its own `LLM_CALL` event before the charge raises
#: (design D3), so the position names that event at the cursor the call
#: consumed: memory write (no step), call (0), intent (1), crossing call (2).
_EXPECTED_DETAIL = "BudgetExceeded(limit=10, consumed=16) failed_at_step=3 after=LLM_CALL"


# --- Requirement: A budget-exceeded activation is dead-lettered ---------------


def test_the_budget_kill_produces_both_enriched_records() -> None:
    # Scenario: The budget kill produces both enriched records. One `.errors`
    # record whose detail leads with the `BudgetExceeded` repr and carries the
    # established position suffix, and one `ERROR` trace carrying the new reason,
    # the error type, and the four failure-position attributes.
    dofn = _budgeted_dofn(spend_then_bust_agent)
    handles = _Handles()

    emitted = _process(dofn, _event(), handles)

    assert _tagged(emitted, "errors") == [
        ActivationError(_KEY, REASON_BUDGET_EXCEEDED, _EXPECTED_DETAIL, _NOW_MS)
    ]
    (trace,) = _tagged(emitted, "traces")
    assert trace.event_type == TraceEventProto.ERROR
    assert trace.attributes["beam_agents.reason"] == REASON_BUDGET_EXCEEDED
    assert trace.attributes["error.type"] == "BudgetExceeded"
    assert trace.attributes["beam_agents.failure.step"] == "3"
    assert trace.attributes["beam_agents.failure.last_event"] == "LLM_CALL"
    assert trace.attributes["beam_agents.failure.staged_intents"] == "1"
    assert trace.attributes["beam_agents.failure.llm_calls"] == "2"
    assert trace.trace_id == trace_id_for(_KEY, _SEQ)
    assert trace.start_ms == _NOW_MS


def test_a_resume_that_trips_its_budget_takes_the_same_route() -> None:
    # `_resume` has its own failure branch; the reason dispatch lives in the one
    # shared handler, so a resumed activation's trip routes identically -- scoped
    # to the continuation's seq, like every other resume failure.
    dofn = _budgeted_dofn(spend_then_bust_agent)
    handles = _Handles(
        cont=_live_continuation(),
        pending=[ToolIntent(intent_id="intent-1", expires_at_ms=_NOW_MS + 1_000)],
    )

    emitted = _process(dofn, _resume_envelope(), handles)

    (record,) = _tagged(emitted, "errors")
    assert record.reason == REASON_BUDGET_EXCEEDED
    assert record.detail.startswith("BudgetExceeded(limit=10, consumed=16)")
    (trace,) = _tagged(emitted, "traces")
    assert trace.attributes["beam_agents.reason"] == REASON_BUDGET_EXCEEDED
    assert trace.attributes["error.type"] == "BudgetExceeded"
    assert trace.trace_id == trace_id_for(_KEY, 1)
    assert handles.unchanged()


def test_nothing_staged_escapes_a_budget_kill() -> None:
    # Scenario: Nothing staged escapes a budget kill. The agent wrote memory,
    # staged an intent, and its first call inserted a replay-cache entry; the
    # atomic-commit invariant discards all of it, `SEQ` does not advance, and
    # the element's only outputs are the dead letter and the ERROR event.
    dofn = _budgeted_dofn(spend_then_bust_agent)
    handles = _Handles()

    emitted = _process(dofn, _event(), handles)

    assert _tagged(emitted, "intents") == []
    assert [e for e in emitted if not isinstance(e, beam.pvalue.TaggedOutput)] == []
    assert [trace.event_type for trace in _tagged(emitted, "traces")] == [TraceEventProto.ERROR]
    assert handles.unchanged()
    assert handles.seq.value == _SEQ


def test_a_budget_kill_is_an_agent_error_not_a_committed_activation() -> None:
    # Scenario: A budget kill is an agent error, not a committed activation.
    # The dead letter still flows through the single counting chokepoint, so it
    # lands in `agent_errors` with no new counter wiring -- and nothing on the
    # commit path moves, because the commit path was never reached.
    metrics = RecordingMetrics()
    dofn = _budgeted_dofn(spend_then_bust_agent, metrics=metrics)
    handles = _Handles()

    _process(dofn, _event(), handles)

    assert metrics.counters == {COUNTER_AGENT_ERRORS: 1}
    # `activation_ms` is sampled on every exit including failure, by design (the
    # timeout tail is the interesting part); no other distribution is.
    assert set(metrics.samples) == {DISTRIBUTION_ACTIVATION_MS}


def test_a_non_budget_raise_still_routes_as_an_activation_error() -> None:
    # The dispatch is on the cause's class, and it must not widen: every other
    # agent raise keeps today's `activation_error` reason byte-identically.
    dofn = _budgeted_dofn(raising_agent)
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


def test_a_replayed_budget_kill_produces_byte_identical_records() -> None:
    # Scenario: A replayed budget kill produces byte-identical records. Both
    # halves of the detail are pure functions of the deterministic walk -- the
    # exception's `repr` over (limit, consumed), and the failure position -- so
    # a retried bundle's dead letter and ERROR event dedup on content.
    records: list[tuple[ActivationError, bytes]] = []
    for _ in range(2):
        dofn = _budgeted_dofn(spend_then_bust_agent)
        handles = _Handles()
        emitted = _process(dofn, _event(), handles)
        (record,) = _tagged(emitted, "errors")
        (trace,) = _tagged(emitted, "traces")
        records.append((record, trace.SerializeToString(deterministic=True)))

    assert records[0] == records[1]


def test_an_unbudgeted_dofn_runs_the_same_agent_to_completion() -> None:
    # Scenario: Unset means unlimited. The same agent that busts a 10-token
    # budget commits normally with the knob unset, so the route above is the
    # budget's doing and not the agent's.
    dofn = _AgentDoFn(
        spend_then_bust_agent,
        provider_factory=make_pong_provider,
        decode=decode_len_based,
        metrics=RecordingMetrics(),
    )
    handles = _Handles()

    emitted = _process(dofn, _event(), handles)

    assert _tagged(emitted, "errors") == []
    assert handles.seq.value == _SEQ + 1
