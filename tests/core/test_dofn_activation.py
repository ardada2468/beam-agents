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
from beam_agents.core.transform import AgentConfig
from beam_agents.hitl import HitlPolicy
from beam_agents.memory import Memory, SummarizeCompactor
from beam_agents.memory.facade import HARD_CAP_BYTES
from beam_agents.memory.stores import InMemoryMemoryStore
from beam_agents.model.client import LlmRequest
from beam_agents.model.fake import FakeLLM, match_any, respond_with
from beam_agents.model.replay_cache import ReplayCache, compute_cache_key
from beam_agents.observability.metrics import COUNTER_LONGTERM_UPSERTS, ActivationTally
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
    bulk_write_agent,
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
        compactor: Any = None,
        summarizer: Any = None,
        longterm_memory: str | None = None,
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
            compactor=compactor,
            summarizer=summarizer,
            longterm_memory=longterm_memory,
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
        ActivationError(_KEY, REASON_ORPHANED, f"{DETAIL_INTENT_EXPIRED}:{intent_id}", _NOW_MS)
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
            _NOW_MS,
        )
    ]


def test_a_start_timeout_dead_letters_with_no_detail() -> None:
    # The timeout record carries no detail: there is no exception to name, and a
    # placeholder would be indistinguishable from a real one downstream.
    driver = _Driver(model_agent, provider_factory=_slow_provider, activation_timeout_s=0.05)

    emitted = driver.process(_event())

    assert _tagged(emitted, "errors") == [ActivationError(_KEY, REASON_TIMEOUT, "", _NOW_MS)]


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
            _NOW_MS,
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

    assert _tagged(emitted, "errors") == [ActivationError(_KEY, REASON_TIMEOUT, "", _NOW_MS)]
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


# --- Requirement: the default compactor is wired through AgentConfig ----------


def _crowded_memory_blob() -> MemoryBlob:
    """Six 150 KiB-ish entries in LRU order `old-0 .. old-5` (900 006 bytes).

    Sized so `bulk_write_agent`'s write crosses the 1 MiB hard cap, and so the
    post-eviction total lands under the soft cap — the eviction under test is
    then the hard-cap one, with no soft-cap pass following it.
    """
    blob = MemoryBlob(state_schema_version=1)
    total = 0
    for index in range(6):
        encoded = b"\x00" + b"x" * 150_000
        blob.entries.add(key=f"old-{index}", value=encoded, last_access_ms=index)
        total += len(encoded)
    blob.total_value_bytes = total
    return blob


def test_an_unconfigured_pipeline_survives_a_hard_cap_crossing_write() -> None:
    # Scenario: An unconfigured pipeline survives a hard-cap-crossing write.
    # The compactor parameter used to be dead — `AgentConfig` had no field and
    # `_activate` passed nothing — so the hard cap's only behavior was
    # `MemoryOverflow` -> dead letter, forever, on every later over-cap write.
    default_compactor = AgentConfig(provider_factory=make_pong_provider).compactor
    driver = _Driver(
        bulk_write_agent, memory_blob=_crowded_memory_blob(), compactor=default_compactor
    )

    emitted = driver.process(_event())

    assert _tagged(emitted, "errors") == []
    assert _main(emitted) == [b"written"]
    committed = {entry.key for entry in driver.memory.value.entries}
    # LRU-first eviction, stopping at the default target (half the hard cap).
    assert committed == {"old-3", "old-4", "old-5", "bulk"}
    assert driver.memory.value.total_value_bytes <= HARD_CAP_BYTES
    assert driver.seq.value == 1


def test_opting_out_restores_strict_overflow() -> None:
    # Scenario: Opting out restores strict overflow. `compactor=None` is the
    # documented migration escape hatch for pipelines that relied on
    # overflow-as-failure.
    driver = _Driver(bulk_write_agent, memory_blob=_crowded_memory_blob(), compactor=None)

    emitted = driver.process(_event())

    errors = _tagged(emitted, "errors")
    assert len(errors) == 1
    assert errors[0].entity_key == _KEY
    assert errors[0].reason == REASON_ERROR
    assert "MemoryOverflow" in errors[0].detail
    # Fail-closed: nothing committed, so the key's memory and seq are untouched.
    assert {entry.key for entry in driver.memory.value.entries} == {
        f"old-{index}" for index in range(6)
    }
    assert driver.seq.value == 0


# --- Requirement: the DoFn's configuration reaches the activation ------------
#
# `_activate` is the seam between the DoFn's per-instance configuration and the
# loop driver. Each knob below is forwarded across it and used only *inside* the
# activation, so a knob dropped in transit produces an activation that runs to a
# perfectly ordinary-looking completion with the feature silently off — which
# `test_dofn_pipeline` cannot catch either, since it configures the same knobs
# through the same seam.


def _summarizer(*, trigger_bytes: int) -> SummarizeCompactor:
    """Tier-2 compactor folding the `log` ring into a `summary` entry."""
    return SummarizeCompactor(
        build_request=lambda items, prior: LlmRequest(
            model_id="summarizer",
            messages=[[item.decode() for item in items], (prior or b"").decode()],
            tools_schema=None,
            sampling_params=None,
        ),
        extract_summary=lambda response: b"summary:" + response,
        source_keys=("log",),
        keep_recent=2,
        trigger_bytes=trigger_bytes,
    )


async def appending_agent(ctx: ActivationContext) -> Complete:
    """Append eight items to the `log` ring — enough to cross a small trigger."""
    for index in range(8):
        ctx.memory.append("log", f"item-{index}".encode(), max_items=64)
    return Complete(output=b"appended")


def test_a_configured_summarizer_reaches_the_activation() -> None:
    # Tier-2 compaction runs inside the activation, so the DoFn's only part in
    # it is handing the summarizer across. Everything else about the committed
    # element — the output, the seq increment, the working-tier writes — is
    # identical whether or not it arrives, and the summary entry is the one
    # thing that is not.
    driver = _Driver(appending_agent, summarizer=_summarizer(trigger_bytes=1))

    emitted = driver.process(_event())

    assert _main(emitted) == [b"appended"]
    committed = Memory(driver.memory.value, now_ms=_NOW_MS)
    assert committed.get("summary") == b"summary:pong"
    assert committed.ring("log") == (b"item-6", b"item-7")


def test_an_unconfigured_summarizer_leaves_the_ring_whole() -> None:
    # The opt-out shape, so the assertion above is a statement about the
    # summarizer rather than about `appending_agent`.
    driver = _Driver(appending_agent)

    driver.process(_event())

    committed = Memory(driver.memory.value, now_ms=_NOW_MS)
    assert committed.get("summary") is None
    assert len(committed.ring("log")) == 8


async def longterm_saving_agent(ctx: ActivationContext) -> Complete:
    """Stage three long-term upserts, then complete.

    Three rather than one so the upsert counter is a count: `incr(name)`
    defaults to a step of 1, which a single-upsert activation cannot tell from
    `incr(name, len(upserts))`.
    """
    for index in range(3):
        ctx.memory.longterm.save(f"profile-{index}", b"v1")
    return Complete(output=b"saved")


def test_the_configured_long_term_store_reaches_the_activation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The store the DoFn built in `setup()` is what makes `ctx.memory.longterm`
    # exist at all; without it the accessor raises and the element dead-letters
    # instead of committing. The commit-tail flush runs inside the activation,
    # so the store's own record is the proof it arrived, and the counter is the
    # DoFn's own accounting of what that flush wrote.
    store = InMemoryMemoryStore()
    monkeypatch.setattr("beam_agents.core.dofn.build_memory_store", lambda scheme, parts: store)
    driver = _Driver(longterm_saving_agent, longterm_memory="memory://")

    emitted = driver.process(_event())

    assert _main(emitted) == [b"saved"]
    assert _tagged(emitted, "errors") == []
    # One count per upsert the commit-tail flush wrote, not one per activation.
    assert driver.metrics.counters[COUNTER_LONGTERM_UPSERTS] == 3


def test_without_a_configured_store_the_same_agent_dead_letters() -> None:
    # The opt-out shape of the same wiring: no URI, no store, and the accessor
    # raises actionably rather than silently dropping the save.
    driver = _Driver(longterm_saving_agent)

    emitted = driver.process(_event())

    assert _main(emitted) == []
    (error,) = _tagged(emitted, "errors")
    assert error.reason == REASON_ERROR
    assert "AgentConfig.longterm_memory" in error.detail
    assert COUNTER_LONGTERM_UPSERTS not in driver.metrics.counters


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
