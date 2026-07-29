"""The element path — routing, activation inputs, and commit — with fake handles.

`test_dofn_pipeline` and `test_dofn_streaming` already assert these behaviors
end to end, but both are deselected under mutmut (they drive the DirectRunner,
whose worker subprocesses mutmut's child reaping cannot survive). Driving
`process`/`_commit` directly with the fake state handles in `_dofn_fakes` puts
the same assertions inside the mutation gate's selection: what the DoFn reads
out of keyed state, what it forwards into the activation, what it writes back,
and what each failure exit dead-letters.

Nothing here is about metrics; that is `test_dofn_metrics`.
"""

from __future__ import annotations

from typing import Any

import apache_beam as beam
import pytest

from beam_agents._protos import (
    AgentEnvelope,
    Continuation,
    LlmCacheBlob,
    MemoryBlob,
    ToolIntent,
    ToolResult,
    TraceEvent,
)
from beam_agents.core.agent import Complete, Suspend, intent_id_for
from beam_agents.core.context import ActivationContext
from beam_agents.core.dofn import (
    DETAIL_INTENT_EXPIRED,
    REASON_ERROR,
    REASON_ORPHANED,
    REASON_TIMEOUT,
    ActivationError,
    _AgentDoFn,
)
from beam_agents.core.loop import ActivationResult
from beam_agents.hitl import HitlPolicy
from beam_agents.memory import Memory
from beam_agents.model.fake import FakeLLM, match_any, respond_with
from beam_agents.model.replay_cache import ReplayCache, compute_cache_key
from beam_agents.observability.metrics import ActivationTally
from tests.core._dofn_fakes import (
    FakeBag,
    FakeSum,
    FakeTimer,
    FakeValue,
    RecordingMetrics,
    scripted_clock,
)
from tests.core._dofn_helpers import (
    append_agent,
    approval_agent,
    make_pong_provider,
    model_agent,
    raising_agent,
    request,
    seq_agent,
    suspend_then_complete_agent,
)

_KEY = b"k"
_NOW_MS = 1_000
_TTL_MS = 100_000


def _event(payload: bytes = b"go", *, now_ms: int = _NOW_MS) -> AgentEnvelope:
    return AgentEnvelope(entity_key=_KEY, event_time_ms=now_ms, external_event=payload)


def _tool_result(intent_id: str, payload: bytes = b"done") -> AgentEnvelope:
    envelope = AgentEnvelope(entity_key=_KEY, event_time_ms=_NOW_MS)
    envelope.tool_result.intent_id = intent_id
    envelope.tool_result.entity_key = _KEY
    envelope.tool_result.payload = payload
    envelope.tool_result.status = ToolResult.OK
    return envelope


def _approval(intent_id: str, *, approved: bool = True) -> AgentEnvelope:
    envelope = AgentEnvelope(entity_key=_KEY, event_time_ms=_NOW_MS)
    envelope.approval.intent_id = intent_id
    envelope.approval.approved = approved
    envelope.approval.approver = "alice@example.test"
    return envelope


def _live_continuation(*, seq: int = 7, step_index: int = 2) -> Continuation:
    return Continuation(
        state_schema_version=1,
        seq=seq,
        step_index=step_index,
        pending_intent_ids=[intent_id_for(_KEY, seq, step_index - 1)],
        adapter="test",
        snapshot=b"waiting",
        suspended_at_ms=500,
        deadline_ms=60_000,
    )


class _Driver:
    """One DoFn plus the fake handles a single `process` call is given."""

    def __init__(
        self,
        agent: Any,
        *,
        provider_factory: Any = make_pong_provider,
        activation_timeout_s: float = 30.0,
        hitl_policy: HitlPolicy | None = None,
        memory_blob: MemoryBlob | None = None,
        cache_blob: LlmCacheBlob | None = None,
        continuation: Continuation | None = None,
        pending: list[ToolIntent] | None = None,
        seq: int = 0,
        monotonic_ns: Any = None,
    ) -> None:
        self.metrics = RecordingMetrics()
        self.dofn = _AgentDoFn(
            agent,
            provider_factory=provider_factory,
            activation_timeout_s=activation_timeout_s,
            ttl_ms=_TTL_MS,
            hitl_policy=hitl_policy,
            metrics=self.metrics,
            monotonic_ns=monotonic_ns if monotonic_ns is not None else scripted_clock(),
        )
        self.memory = FakeValue(memory_blob if memory_blob is not None else MemoryBlob())
        self.continuation = FakeValue(continuation)
        self.llm_cache = FakeValue(cache_blob if cache_blob is not None else LlmCacheBlob())
        self.pending = FakeBag(pending)
        self.seq = FakeSum(seq)
        self.ttl_timer = FakeTimer()
        self.hitl_timer = FakeTimer()

    def process(self, envelope: AgentEnvelope) -> list[Any]:
        self.dofn.setup()
        try:
            return list(
                self.dofn.process(
                    (_KEY, envelope),
                    memory=self.memory,
                    continuation=self.continuation,
                    llm_cache=self.llm_cache,
                    pending=self.pending,
                    seq=self.seq,
                    ttl_timer=self.ttl_timer,
                    hitl_timer=self.hitl_timer,
                )
            )
        finally:
            self.dofn.teardown()


def _main(emitted: list[Any]) -> list[Any]:
    return [e for e in emitted if not isinstance(e, beam.pvalue.TaggedOutput)]


def _tagged(emitted: list[Any], tag: str) -> list[Any]:
    return [e.value for e in emitted if isinstance(e, beam.pvalue.TaggedOutput) and e.tag == tag]


def _mark_ms(timer: FakeTimer) -> int:
    """The epoch-ms a timer was armed at; fails if it was never armed."""
    assert timer.set_to is not None, "timer was not armed"
    return int(timer.set_to.micros // 1000)


def _slow_provider() -> FakeLLM:
    """Provider slow enough to blow a 50ms budget, fast enough that a runtime
    which failed to apply the budget returns instead of hanging.
    """
    return FakeLLM([(match_any(), respond_with(b"pong", latency_ms=1_000))])


def _memory_blob_with(key: str, item: bytes) -> MemoryBlob:
    memory = Memory(now_ms=_NOW_MS)
    memory.append(key, item, max_items=64)
    return memory.to_blob()


def _cache_blob_holding(response: bytes, *, seq: int) -> LlmCacheBlob:
    """A replay cache pre-loaded with the response `model_agent`'s call hashes to."""
    req = request()
    cache = ReplayCache(None, now_ms=_NOW_MS)
    cache.put(
        compute_cache_key(
            req.model_id, req.messages, req.tools_schema, req.sampling_params, _KEY, seq
        ),
        response,
    )
    return cache.to_blob()


# --- Lifecycle: the activation cannot run before setup() ----------------------


def _process_unsetup(dofn: _AgentDoFn) -> list[Any]:
    return list(
        dofn.process(
            (_KEY, _event()),
            memory=FakeValue(MemoryBlob()),
            continuation=FakeValue(None),
            llm_cache=FakeValue(LlmCacheBlob()),
            pending=FakeBag(),
            seq=FakeSum(),
            ttl_timer=FakeTimer(),
            hitl_timer=FakeTimer(),
        )
    )


def test_activating_before_setup_is_refused_and_named() -> None:
    # `setup()` builds the bridge and the provider. Reaching `_activate` without
    # them is a wiring bug, not a data condition -- the assertion names it, and
    # the element's fail-closed path turns it into a dead letter rather than
    # letting an activation run against half a runtime.
    dofn = _AgentDoFn(seq_agent, provider_factory=make_pong_provider)

    error = _tagged(_process_unsetup(dofn), "errors")[0]

    assert error.reason == REASON_ERROR
    # The exact message, not a substring: the detail is what an operator reads
    # off the dead-letter sink, and it has to name the missing call precisely.
    assert error.detail == repr(AssertionError("setup() not called"))


def test_a_half_built_runtime_is_refused_too() -> None:
    # The check is on *both* handles: a bridge with no provider would otherwise
    # run the activation and commit it, with the model call failing much later
    # and much less legibly.
    dofn = _AgentDoFn(seq_agent, provider_factory=make_pong_provider)
    dofn.setup()
    dofn._provider = None
    try:
        error = _tagged(_process_unsetup(dofn), "errors")[0]
    finally:
        dofn.teardown()

    assert error.reason == REASON_ERROR
    assert error.detail == repr(AssertionError("setup() not called"))


# --- Requirement: resume admission reads the pending intents ------------------


def test_a_resume_whose_pending_intent_expired_is_refused() -> None:
    # Fail-closed layer 1 reads PENDING to check the matching intent's expiry:
    # a runtime that admitted the resume without loading the bag would resume
    # against an intent the effector has already refused, and the two layers
    # would disagree about what is still answerable.
    cont = _live_continuation(seq=0, step_index=1)
    intent_id = cont.pending_intent_ids[0]
    driver = _Driver(
        suspend_then_complete_agent,
        continuation=cont,
        # Live continuation (deadline 60_000), but this intent expired at 900 --
        # before the element's 1_000ms clock.
        pending=[ToolIntent(intent_id=intent_id, expires_at_ms=900)],
    )

    emitted = driver.process(_tool_result(intent_id))

    assert _main(emitted) == []
    assert _tagged(emitted, "errors") == [
        ActivationError(_KEY, REASON_ORPHANED, f"{DETAIL_INTENT_EXPIRED}:{intent_id}")
    ]
    assert driver.seq.value == 0


# --- Requirement: each resume variant reaches only its own field --------------


def test_a_tool_result_resume_leaves_the_approval_field_empty() -> None:
    # The two resume variants are mutually exclusive: an agent that branches on
    # `resume_approval` must not see a placeholder when a *tool result* arrives.
    cont = _live_continuation(seq=0, step_index=1)
    driver = _Driver(
        approval_agent,
        continuation=cont,
        pending=[ToolIntent(intent_id=cont.pending_intent_ids[0], expires_at_ms=60_000)],
    )

    emitted = driver.process(_tool_result(cont.pending_intent_ids[0]))

    # `approval_agent` falls through to its tool-result branch only when
    # `resume_approval` is genuinely absent; the payload is the OK status.
    assert _main(emitted) == [b"result:" + str(ToolResult.OK).encode()]


def test_an_approval_resume_leaves_the_tool_result_field_empty() -> None:
    async def require_no_tool_result(ctx: ActivationContext) -> Complete:
        assert ctx.resume_result is None
        assert ctx.resume_approval is not None
        return Complete(output=b"approval-only")

    cont = _live_continuation(seq=0, step_index=1)
    driver = _Driver(
        require_no_tool_result,
        continuation=cont,
        pending=[ToolIntent(intent_id=cont.pending_intent_ids[0], expires_at_ms=60_000)],
    )

    emitted = driver.process(_approval(cont.pending_intent_ids[0]))

    assert _main(emitted) == [b"approval-only"]


# --- Requirement: an activation reads the committed state for its key ---------


def test_a_start_reads_committed_working_memory() -> None:
    driver = _Driver(append_agent, memory_blob=_memory_blob_with("log", b"a"))

    emitted = driver.process(_event(b"b"))

    # The ring the agent appends to is the committed one, not an empty facade.
    assert _main(emitted) == [b"a,b#0"]


def test_a_start_reads_the_committed_replay_cache() -> None:
    # Correctness invariant 3 at the DoFn boundary: the cache blob loaded from
    # keyed state is what makes a replayed activation free.
    provider = make_pong_provider()
    driver = _Driver(model_agent, provider_factory=lambda: provider)
    driver.llm_cache.value = _cache_blob_holding(b"cached", seq=0)

    emitted = driver.process(_event())

    assert _main(emitted) == [b"cached"]
    assert provider.call_count == 0


def test_a_resume_reads_committed_memory_and_cache() -> None:
    cont = _live_continuation(seq=0, step_index=1)
    provider = make_pong_provider()

    async def echo_memory_and_model(ctx: ActivationContext) -> Complete:
        response = await ctx.call_model(request())
        ring = b",".join(ctx.memory.ring("log"))
        return Complete(output=ring + b"|" + response.response)

    driver = _Driver(
        echo_memory_and_model,
        provider_factory=lambda: provider,
        memory_blob=_memory_blob_with("log", b"remembered"),
        cache_blob=_cache_blob_holding(b"cached", seq=0),
        continuation=cont,
        pending=[ToolIntent(intent_id=cont.pending_intent_ids[0], expires_at_ms=60_000)],
    )

    emitted = driver.process(_tool_result(cont.pending_intent_ids[0]))

    assert _main(emitted) == [b"remembered|cached"]
    assert provider.call_count == 0


# --- Requirement: the activation's inputs come from the element and state -----


def test_the_activation_clock_is_the_elements_event_time() -> None:
    async def echo_clock(ctx: ActivationContext) -> Complete:
        return Complete(output=str(ctx.now_ms).encode())

    assert _main(_Driver(echo_clock).process(_event(now_ms=4_242))) == [b"4242"]


def test_a_start_activation_sees_no_snapshot_and_a_zero_step_cursor() -> None:
    # A fresh activation has no continuation to resume, so both defaults matter:
    # an empty snapshot, and a step cursor at zero (which is what makes the
    # first intent's ID `intent_id_for(key, seq, 0)`).
    async def echo_start_state(ctx: ActivationContext) -> Complete:
        assert ctx.snapshot == b""
        assert ctx.event == b"go"
        ctx.act("http.post", "{}", ttl_ms=1_000)
        return Complete(output=b"ok")

    driver = _Driver(echo_start_state, seq=3)

    emitted = driver.process(_event())

    assert _main(emitted) == [b"ok"]
    assert _tagged(emitted, "intents")[0].intent_id == intent_id_for(_KEY, 3, 0)


def test_a_resume_continues_the_suspensions_seq_snapshot_and_step_cursor() -> None:
    # The resume shares the suspended activation's `seq` and continues its step
    # cursor, so a re-minted intent cannot collide with one the suspension
    # already emitted (correctness invariant 2).
    cont = _live_continuation(seq=7, step_index=2)

    async def echo_resume_state(ctx: ActivationContext) -> Complete:
        assert ctx.event == b""
        ctx.act("http.post", "{}", ttl_ms=1_000)
        return Complete(output=ctx.snapshot + b"#" + str(ctx.seq).encode())

    driver = _Driver(
        echo_resume_state,
        continuation=cont,
        pending=[ToolIntent(intent_id=cont.pending_intent_ids[0], expires_at_ms=60_000)],
        seq=1,
    )

    emitted = driver.process(_tool_result(cont.pending_intent_ids[0]))

    assert _main(emitted) == [b"waiting#7"]
    assert _tagged(emitted, "intents")[0].intent_id == intent_id_for(_KEY, 7, 2)


def test_a_resuming_tool_result_reaches_the_agent() -> None:
    cont = _live_continuation(seq=0, step_index=1)
    driver = _Driver(
        suspend_then_complete_agent,
        continuation=cont,
        pending=[ToolIntent(intent_id=cont.pending_intent_ids[0], expires_at_ms=60_000)],
    )

    emitted = driver.process(_tool_result(cont.pending_intent_ids[0], b"payload"))

    assert _main(emitted) == [b"resumed:payload"]


def test_a_resuming_approval_reaches_the_agent() -> None:
    # The approval variant routes through its own branch in `process` and lands
    # on a different resume field than a tool result.
    cont = _live_continuation(seq=0, step_index=1)
    driver = _Driver(
        approval_agent,
        continuation=cont,
        pending=[ToolIntent(intent_id=cont.pending_intent_ids[0], expires_at_ms=60_000)],
    )

    emitted = driver.process(_approval(cont.pending_intent_ids[0], approved=True))

    assert _main(emitted) == [b"approved"]


def test_a_denied_approval_reaches_the_agent_as_a_denial() -> None:
    cont = _live_continuation(seq=0, step_index=1)
    driver = _Driver(
        approval_agent,
        continuation=cont,
        pending=[ToolIntent(intent_id=cont.pending_intent_ids[0], expires_at_ms=60_000)],
    )

    emitted = driver.process(_approval(cont.pending_intent_ids[0], approved=False))

    assert _main(emitted) == [b"rejected"]


# --- Requirement: the HITL policy's defaults reach the activation -------------


async def _suspend_with_policy_defaults(ctx: ActivationContext) -> Suspend:
    """Suspend and stage an approval taking every default from the policy."""
    ctx.request_approval("{}")
    return Suspend(snapshot=b"s", adapter="test")


def test_the_policy_supplies_the_suspension_timeout_and_intent_ttl_and_channel() -> None:
    # All three are configured on `HitlPolicy` and forwarded per activation; a
    # runtime that dropped one would silently fall back to the driver's own
    # defaults, which are a different number.
    policy = HitlPolicy(timeout_ms=30_000, intent_ttl_ms=20_000, approval_channel="pager")
    driver = _Driver(_suspend_with_policy_defaults, hitl_policy=policy)

    emitted = driver.process(_event())

    intent = _tagged(emitted, "intents")[0]
    assert intent.tool_name == "pager"
    assert intent.expires_at_ms == _NOW_MS + 20_000
    # The deadline is the earlier of the suspension timeout and the intent's
    # expiry, so this also pins the timeout as the *later* of the two.
    assert driver.continuation.value.deadline_ms == _NOW_MS + 20_000
    assert _mark_ms(driver.hitl_timer) == _NOW_MS + 20_000


def test_a_longer_intent_ttl_leaves_the_suspension_timeout_in_charge() -> None:
    policy = HitlPolicy(timeout_ms=5_000, intent_ttl_ms=90_000, approval_channel="pager")
    driver = _Driver(_suspend_with_policy_defaults, hitl_policy=policy)

    driver.process(_event())

    assert driver.continuation.value.deadline_ms == _NOW_MS + 5_000


# --- Requirement: every failure exit dead-letters key, reason, and detail -----


def test_a_start_failure_dead_letters_the_key_reason_and_exception() -> None:
    # The detail leads with the original exception's repr and ends with the
    # failure-position suffix (add-failure-context).
    emitted = _Driver(raising_agent).process(_event())

    assert _tagged(emitted, "errors") == [
        ActivationError(
            _KEY,
            REASON_ERROR,
            f"{RuntimeError('agent blew up')!r} failed_at_step=0 after=ACTIVATION_START",
        )
    ]


def test_a_start_timeout_dead_letters_with_no_detail() -> None:
    # The timeout record carries no detail: there is no exception to name, and a
    # placeholder would be indistinguishable from a real one downstream.
    driver = _Driver(model_agent, provider_factory=_slow_provider, activation_timeout_s=0.05)

    emitted = driver.process(_event())

    assert _tagged(emitted, "errors") == [ActivationError(_KEY, REASON_TIMEOUT, "")]


def test_a_resume_failure_dead_letters_the_key_reason_and_exception() -> None:
    cont = _live_continuation(seq=0, step_index=1)

    async def fail_on_resume(ctx: ActivationContext) -> Complete:
        raise RuntimeError("resume blew up")

    driver = _Driver(
        fail_on_resume,
        continuation=cont,
        pending=[ToolIntent(intent_id=cont.pending_intent_ids[0], expires_at_ms=60_000)],
    )

    emitted = driver.process(_tool_result(cont.pending_intent_ids[0]))

    # The cursor sits at the continuation's seed; nothing past the driver's
    # ACTIVATION_START was staged before the raise.
    assert _tagged(emitted, "errors") == [
        ActivationError(
            _KEY,
            REASON_ERROR,
            f"{RuntimeError('resume blew up')!r} failed_at_step=1 after=ACTIVATION_START",
        )
    ]
    # Fail-closed: the continuation the resume was running against is untouched.
    assert driver.continuation.value == cont
    assert driver.seq.value == 0


def test_a_resume_timeout_dead_letters_with_no_detail() -> None:
    cont = _live_continuation(seq=0, step_index=1)
    driver = _Driver(
        model_agent,
        provider_factory=_slow_provider,
        activation_timeout_s=0.05,
        continuation=cont,
        pending=[ToolIntent(intent_id=cont.pending_intent_ids[0], expires_at_ms=60_000)],
    )

    emitted = driver.process(_tool_result(cont.pending_intent_ids[0]))

    assert _tagged(emitted, "errors") == [ActivationError(_KEY, REASON_TIMEOUT, "")]
    assert driver.seq.value == 0


# --- Requirement: the commit applies every staged effect, in a fixed order ----


def _result(
    *,
    status: str = "completed",
    intents: list[ToolIntent] | None = None,
    traces: list[TraceEvent] | None = None,
    outputs: list[bytes] | None = None,
    continuation: Continuation | None = None,
    hitl_deadline_ms: int | None = None,
) -> ActivationResult:
    return ActivationResult(
        status=status,  # type: ignore[arg-type]
        seq=0,
        memory_blob=MemoryBlob(state_schema_version=1, total_value_bytes=3),
        cache_blob=LlmCacheBlob(state_schema_version=1),
        intents=intents if intents is not None else [],
        traces=traces if traces is not None else [],
        outputs=outputs if outputs is not None else [],
        continuation=continuation,
        hitl_deadline_ms=hitl_deadline_ms,
        tally=ActivationTally(),
    )


def _commit(driver: _Driver, result: ActivationResult) -> list[Any]:
    return list(
        driver.dofn._commit(
            result,
            _NOW_MS,
            5,  # activation_ms: only `overhead_ms` consumes it, not the state writes
            driver.memory,
            driver.continuation,
            driver.llm_cache,
            driver.pending,
            driver.seq,
            driver.ttl_timer,
            driver.hitl_timer,
        )
    )


def test_a_completing_commit_clears_the_continuation_and_arms_only_the_ttl() -> None:
    driver = _Driver(seq_agent, continuation=_live_continuation())
    intent = ToolIntent(intent_id="i-1", tool_name="http.post", expires_at_ms=9_000)
    trace = TraceEvent(entity_key=_KEY, seq=0, event_type=TraceEvent.ACTIVATION_END)

    emitted = _commit(
        driver, _result(intents=[intent], traces=[trace], outputs=[b"out-1", b"out-2"])
    )

    assert driver.memory.value.total_value_bytes == 3
    assert driver.llm_cache.value.state_schema_version == 1
    # No continuation to persist: the previous one is cleared, not overwritten.
    assert driver.continuation.value is None
    assert driver.continuation.cleared is True
    # PENDING is replaced wholesale by exactly the intents this activation staged.
    assert driver.pending.items == [intent]
    assert driver.seq.value == 1
    assert driver.hitl_timer.cleared is True
    assert driver.hitl_timer.set_to is None
    assert _mark_ms(driver.ttl_timer) == _NOW_MS + _TTL_MS
    # Emission order: outputs, then intents, then traces -- each on its own tag.
    assert _main(emitted) == [b"out-1", b"out-2"]
    assert _tagged(emitted, "intents") == [intent]
    assert _tagged(emitted, "traces") == [trace]


def test_a_suspending_commit_persists_the_continuation_and_arms_both_timers() -> None:
    driver = _Driver(seq_agent)
    cont = _live_continuation(seq=0, step_index=1)
    deadline_ms = _NOW_MS + 50_000

    _commit(
        driver,
        _result(status="suspended", continuation=cont, hitl_deadline_ms=deadline_ms),
    )

    assert driver.continuation.value == cont
    assert _mark_ms(driver.hitl_timer) == deadline_ms
    # The memory mark is measured from the deadline, so working memory outlives
    # the wait a resume will read it in (fix-hitl-ttl-preemption).
    assert _mark_ms(driver.ttl_timer) == deadline_ms + _TTL_MS
    assert driver.seq.value == 1


def test_a_suspension_with_no_deadline_falls_back_to_the_activation_clock() -> None:
    # `status == "suspended"` alone does not carry a mark; both halves of the
    # condition have to hold before the HITL timer is armed.
    driver = _Driver(seq_agent)

    _commit(driver, _result(status="suspended", continuation=_live_continuation()))

    assert driver.hitl_timer.set_to is None
    assert driver.hitl_timer.cleared is True
    assert _mark_ms(driver.ttl_timer) == _NOW_MS + _TTL_MS


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
