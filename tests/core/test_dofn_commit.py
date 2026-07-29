"""What `process()` commits, and what it forwards to the activation.

Driven with fake state/timer doubles rather than through a pipeline, for the
same reason `test_dofn_ttl` and `test_dofn_hitl_timer` are: the scenarios are
about *what the callback does with the handles it is given*, and this keeps the
element-routing and commit surface inside the mutation gate's test selection
(the pipeline suites are deselected there). The end-to-end behavior over a real
runner lives in `test_dofn_pipeline` and `test_dofn_streaming`.

Two things are asserted here that a pipeline test cannot see directly: the exact
value written to each state spec, and the exact argument every layer forwards
down to the activation.
"""

from __future__ import annotations

from typing import Any

import apache_beam as beam
from apache_beam.utils.timestamp import Timestamp

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
from beam_agents.core.dofn import REASON_ERROR, _AgentDoFn
from beam_agents.core.loop import ActivationResult
from beam_agents.hitl import HitlPolicy
from beam_agents.memory import Memory
from beam_agents.model.fake import FakeLLM

from ._context_helpers import decode_len_based
from ._dofn_helpers import make_pong_provider, request

_KEY = b"k"
_NOW_MS = 5_000
_SEQ = 3
_TTL_MS = 60_000


class _FakeState:
    """Records every read/write/add/clear so a commit can be asserted exactly."""

    def __init__(self, value: Any = None) -> None:
        self.value = value
        self.writes: list[Any] = []
        self.added: list[Any] = []
        self.cleared = False

    def read(self) -> Any:
        return self.value

    def write(self, value: Any) -> None:
        self.writes.append(value)
        self.value = value

    def add(self, value: Any) -> None:
        self.added.append(value)

    def clear(self) -> None:
        self.cleared = True
        self.value = None


class _FakeTimer:
    def __init__(self) -> None:
        self.set_to: Timestamp | None = None
        self.cleared = False

    def set(self, ts: Timestamp) -> None:
        self.set_to = ts

    def clear(self) -> None:
        self.cleared = True


class _Handles:
    def __init__(
        self,
        *,
        cont: Continuation | None = None,
        pending: list[ToolIntent] | None = None,
        memory: MemoryBlob | None = None,
        cache: LlmCacheBlob | None = None,
    ) -> None:
        self.memory = _FakeState(memory)
        self.continuation = _FakeState(cont)
        self.llm_cache = _FakeState(cache)
        self.pending = _FakeState(pending if pending is not None else [])
        self.seq = _FakeState(_SEQ)
        self.ttl_timer = _FakeTimer()
        self.hitl_timer = _FakeTimer()

    def as_kwargs(self) -> dict[str, Any]:
        return {
            "memory": self.memory,
            "continuation": self.continuation,
            "llm_cache": self.llm_cache,
            "pending": self.pending,
            "seq": self.seq,
            "ttl_timer": self.ttl_timer,
            "hitl_timer": self.hitl_timer,
        }


def _run(dofn: _AgentDoFn, envelope: AgentEnvelope, handles: _Handles) -> list[Any]:
    dofn.setup()
    try:
        return list(dofn.process((_KEY, envelope), **handles.as_kwargs()))
    finally:
        dofn.teardown()


def _tagged(emitted: list[Any], tag: str) -> list[Any]:
    return [e.value for e in emitted if isinstance(e, beam.pvalue.TaggedOutput) and e.tag == tag]


def _untagged(emitted: list[Any]) -> list[Any]:
    return [e for e in emitted if not isinstance(e, beam.pvalue.TaggedOutput)]


def _event(payload: bytes = b"go") -> AgentEnvelope:
    return AgentEnvelope(entity_key=_KEY, event_time_ms=_NOW_MS, external_event=payload)


def _tool_result(intent_id: str, payload: bytes = b"done") -> AgentEnvelope:
    return AgentEnvelope(
        entity_key=_KEY,
        event_time_ms=_NOW_MS,
        tool_result=ToolResult(
            intent_id=intent_id, entity_key=_KEY, status=ToolResult.OK, payload=payload
        ),
    )


def _approval(intent_id: str) -> AgentEnvelope:
    return AgentEnvelope(
        entity_key=_KEY,
        event_time_ms=_NOW_MS,
        approval=AgentEnvelope.Approval(intent_id=intent_id, approved=True, approver="alice"),
    )


def _live_continuation(intent_id: str, *, step_index: int = 2) -> Continuation:
    return Continuation(
        state_schema_version=1,
        seq=1,
        step_index=step_index,
        pending_intent_ids=[intent_id],
        adapter="test",
        snapshot=b"waiting",
        suspended_at_ms=1_000,
        deadline_ms=_NOW_MS + 1_000,
    )


class _Recorder:
    """An agent that records the activation surface it was handed."""

    def __init__(self, outcome: str = "complete") -> None:
        self.seen: list[dict[str, Any]] = []
        self._outcome = outcome

    async def __call__(self, ctx: ActivationContext) -> Complete | Suspend:
        self.seen.append(
            {
                "entity_key": ctx.entity_key,
                "seq": ctx.seq,
                "now_ms": ctx.now_ms,
                "event": ctx.event,
                "snapshot": ctx.snapshot,
                "step_index": ctx.step_index,
                "is_resume": ctx.is_resume,
                "resume_result": ctx.resume_result,
                "resume_approval": ctx.resume_approval,
                "seeded": ctx.memory.get("seeded"),
            }
        )
        ctx.memory.set("log", b"written")
        intent_id = ctx.act("http.post", '{"u":1}', ttl_ms=_TTL_MS)
        if self._outcome == "suspend":
            return Suspend(snapshot=b"waiting", adapter="test", timeout_ms=1_000)
        return Complete(output=b"out:" + intent_id.encode())


# --- Requirement: Atomic staged commit with fixed ordering -------------------


def test_a_successful_start_commits_every_spec_exactly_once() -> None:
    agent = _Recorder()
    dofn = _AgentDoFn(agent, provider_factory=make_pong_provider, ttl_ms=_TTL_MS)
    handles = _Handles()

    emitted = _run(dofn, _event(), handles)

    # Every state spec receives the activation's own value -- not another
    # spec's, and not None.
    (memory_written,) = handles.memory.writes
    assert isinstance(memory_written, MemoryBlob)
    assert {entry.key for entry in memory_written.entries} == {"log"}
    (cache_written,) = handles.llm_cache.writes
    assert isinstance(cache_written, LlmCacheBlob)

    # A completing activation clears the continuation rather than writing one.
    assert handles.continuation.cleared is True
    assert handles.continuation.writes == []

    # PENDING is replaced, not appended to.
    assert handles.pending.cleared is True
    (pended,) = handles.pending.added
    assert pended.intent_id == intent_id_for(_KEY, _SEQ, 0)

    # Exactly one SEQ increment, of exactly one.
    assert handles.seq.added == [1]

    # Timers: memory GC re-armed from the activation clock, HITL cleared.
    assert handles.ttl_timer.set_to == Timestamp(micros=(_NOW_MS + _TTL_MS) * 1000)
    assert handles.hitl_timer.cleared is True
    assert handles.hitl_timer.set_to is None

    # Emission order: main outputs, then intents, then traces.
    assert _untagged(emitted) == [b"out:" + intent_id_for(_KEY, _SEQ, 0).encode()]
    tags = [e.tag for e in emitted if isinstance(e, beam.pvalue.TaggedOutput)]
    assert tags == ["intents"] + ["traces"] * (len(tags) - 1)
    assert len(_tagged(emitted, "intents")) == 1
    assert [e.event_type for e in _tagged(emitted, "traces")] == [
        TraceEvent.ACTIVATION_START,
        TraceEvent.INTENT_EMITTED,
        TraceEvent.ACTIVATION_END,
    ]


def test_a_start_forwards_the_element_and_the_loaded_state() -> None:
    agent = _Recorder()
    dofn = _AgentDoFn(agent, provider_factory=make_pong_provider, ttl_ms=_TTL_MS)
    seed = Memory(now_ms=1)
    seed.set("seeded", b"prior")
    handles = _Handles(memory=seed.to_blob(), cache=LlmCacheBlob(state_schema_version=1))

    _run(dofn, _event(b"payload"), handles)

    (seen,) = agent.seen
    assert seen["entity_key"] == _KEY
    assert seen["seq"] == _SEQ
    assert seen["now_ms"] == _NOW_MS
    assert seen["event"] == b"payload"
    # A fresh activation starts at step 0 with no snapshot and no resume state.
    assert seen["snapshot"] == b""
    assert seen["step_index"] == 0
    assert seen["is_resume"] is False
    assert seen["resume_result"] is None
    assert seen["resume_approval"] is None
    # The loaded MEMORY blob reached the activation rather than an empty one.
    assert seen["seeded"] == b"prior"


def test_a_successful_resume_runs_under_the_continuations_scope() -> None:
    intent_id = "intent-1"
    agent = _Recorder()
    dofn = _AgentDoFn(agent, provider_factory=make_pong_provider, ttl_ms=_TTL_MS)
    cont = _live_continuation(intent_id)
    handles = _Handles(
        cont=cont, pending=[ToolIntent(intent_id=intent_id, expires_at_ms=_NOW_MS + 1_000)]
    )

    emitted = _run(dofn, _tool_result(intent_id, b"tool-output"), handles)

    (seen,) = agent.seen
    # The continuation's scope, not the key's current SEQ or a reset cursor.
    assert seen["seq"] == cont.seq
    assert seen["step_index"] == cont.step_index
    assert seen["snapshot"] == b"waiting"
    assert seen["is_resume"] is True
    assert seen["resume_result"] is not None
    assert seen["resume_result"].payload == b"tool-output"
    assert seen["resume_approval"] is None
    # `_resume` does not forward an event; the resume payload is the input.
    assert seen["event"] == b""
    # The resumed step cursor is what mints the intent, so a reset cursor would
    # re-mint an ID the suspension already used.
    assert _tagged(emitted, "intents")[0].intent_id == intent_id_for(
        _KEY, cont.seq, cont.step_index
    )
    assert handles.seq.added == [1]


def test_an_approval_resume_forwards_the_approval_not_a_tool_result() -> None:
    intent_id = "intent-1"
    agent = _Recorder()
    dofn = _AgentDoFn(agent, provider_factory=make_pong_provider, ttl_ms=_TTL_MS)
    handles = _Handles(
        cont=_live_continuation(intent_id),
        pending=[ToolIntent(intent_id=intent_id, expires_at_ms=_NOW_MS + 1_000)],
    )

    _run(dofn, _approval(intent_id), handles)

    (seen,) = agent.seen
    assert seen["resume_result"] is None
    assert seen["resume_approval"] is not None
    assert seen["resume_approval"].approver == "alice"
    assert seen["is_resume"] is True


def test_a_suspending_activation_persists_the_continuation_and_arms_hitl() -> None:
    agent = _Recorder(outcome="suspend")
    dofn = _AgentDoFn(agent, provider_factory=make_pong_provider, ttl_ms=_TTL_MS)
    handles = _Handles()

    emitted = _run(dofn, _event(), handles)

    (written,) = handles.continuation.writes
    assert written.seq == _SEQ
    assert written.snapshot == b"waiting"
    assert written.adapter == "test"
    assert handles.continuation.cleared is False

    # The suspension deadline: min(now + Suspend.timeout_ms, intent expiry).
    deadline_ms = _NOW_MS + 1_000
    assert written.deadline_ms == deadline_ms
    assert handles.hitl_timer.set_to == Timestamp(micros=deadline_ms * 1000)
    assert handles.hitl_timer.cleared is False
    # The memory mark is measured from the deadline, not the activation clock,
    # so GC cannot preempt the wait.
    assert handles.ttl_timer.set_to == Timestamp(micros=(deadline_ms + _TTL_MS) * 1000)
    # A suspension emits its intent but no main output.
    assert _untagged(emitted) == []
    assert len(_tagged(emitted, "intents")) == 1


def test_a_completing_activation_arms_ttl_from_the_activation_clock() -> None:
    # The `status == "suspended"` guard, from the other side: a completing
    # activation must not reach for a deadline it does not have.
    agent = _Recorder()
    dofn = _AgentDoFn(agent, provider_factory=make_pong_provider, ttl_ms=_TTL_MS)
    handles = _Handles()

    _run(dofn, _event(), handles)

    assert handles.ttl_timer.set_to == Timestamp(micros=(_NOW_MS + _TTL_MS) * 1000)
    assert handles.hitl_timer.set_to is None


# --- Requirement: Async-bridge activation bounded by activation_timeout ------


def test_the_hitl_policy_and_decode_reach_the_activation() -> None:
    async def agent(ctx: ActivationContext) -> Complete:
        await ctx.call_model(request())
        ctx.request_approval("{}")
        return Complete(output=b"done")

    policy = HitlPolicy(timeout_ms=7_000, intent_ttl_ms=11_000, approval_channel="pager")
    dofn = _AgentDoFn(
        agent,
        provider_factory=make_pong_provider,
        ttl_ms=_TTL_MS,
        hitl_policy=policy,
        decode=decode_len_based,
    )
    handles = _Handles()

    emitted = _run(dofn, _event(), handles)

    (intent,) = _tagged(emitted, "intents")
    # `approval_channel` and `intent_ttl_ms` came from the configured policy.
    assert intent.tool_name == "pager"
    assert intent.expires_at_ms == _NOW_MS + 11_000
    # The provider decode reached the model call, so the usage is reported.
    (llm_call,) = [e for e in _tagged(emitted, "traces") if e.event_type == TraceEvent.LLM_CALL]
    assert llm_call.attributes["gen_ai.usage.input_tokens"] == str(len(b"pong"))


def test_the_suspend_timeout_default_comes_from_the_policy() -> None:
    async def agent(ctx: ActivationContext) -> Suspend:
        ctx.act("http.post", "{}", ttl_ms=90_000)
        # No explicit timeout: the policy's default is what sizes the deadline.
        return Suspend(snapshot=b"s", adapter="test")

    policy = HitlPolicy(timeout_ms=7_000)
    dofn = _AgentDoFn(
        agent, provider_factory=make_pong_provider, ttl_ms=_TTL_MS, hitl_policy=policy
    )
    handles = _Handles()

    _run(dofn, _event(), handles)

    (written,) = handles.continuation.writes
    assert written.deadline_ms == _NOW_MS + 7_000


def test_process_before_setup_fails_the_documented_way() -> None:
    # The guard is an assert over *both* handles: with only the provider set, a
    # mutant that weakens it to `or` lets a half-initialized DoFn reach a
    # `None` bridge and fail with an incidental AttributeError instead. The
    # activation routes fail closed, so the message is what distinguishes them.
    dofn = _AgentDoFn(_Recorder(), provider_factory=make_pong_provider)
    dofn._provider = FakeLLM([])
    handles = _Handles()

    emitted = list(dofn.process((_KEY, _event()), **handles.as_kwargs()))

    (error,) = _tagged(emitted, "errors")
    assert error.reason == REASON_ERROR
    assert error.detail == "AssertionError('setup() not called')"
    assert handles.seq.added == []


def test_teardown_releases_the_bridge_and_provider() -> None:
    dofn = _AgentDoFn(_Recorder(), provider_factory=make_pong_provider)
    dofn.setup()
    bridge = dofn._bridge
    assert bridge is not None

    dofn.teardown()

    assert dofn._bridge is None
    assert dofn._provider is None
    # Idempotent: a second teardown must not re-stop a bridge it already released.
    dofn.teardown()
    assert dofn._bridge is None


def test_the_loaded_replay_cache_reaches_the_activation() -> None:
    # Correctness invariant 3 depends on LLM_CACHE actually being handed to the
    # activation: a start that loaded an empty one would re-call the provider
    # on every retry while still looking correct from the outside.
    async def agent(ctx: ActivationContext) -> Complete:
        response = await ctx.call_model(request())
        return Complete(output=response.response)

    dofn = _AgentDoFn(agent, provider_factory=make_pong_provider, ttl_ms=_TTL_MS)
    provider = make_pong_provider()

    # First activation: a miss that populates the cache blob.
    warm = _Handles()
    dofn._provider_factory = lambda: provider
    _run(dofn, _event(), warm)
    assert provider.call_count == 1
    (cache_blob,) = warm.llm_cache.writes
    assert len(cache_blob.entries) == 1

    # Second activation for the same key and seq, handed that blob: the cached
    # response is served and the provider is not called again.
    replay = _Handles(cache=cache_blob)
    emitted = _run(dofn, _event(), replay)

    assert provider.call_count == 1
    (llm_call,) = [e for e in _tagged(emitted, "traces") if e.event_type == TraceEvent.LLM_CALL]
    assert llm_call.attributes["beam_agents.cache_hit"] == "true"


def test_a_resume_loads_the_keys_memory_and_cache() -> None:
    seen: list[bytes | None] = []

    async def agent(ctx: ActivationContext) -> Complete:
        seen.append(ctx.memory.get("seeded"))
        response = await ctx.call_model(request())
        return Complete(output=response.response)

    intent_id = "intent-1"
    provider = make_pong_provider()
    dofn = _AgentDoFn(agent, provider_factory=lambda: provider, ttl_ms=_TTL_MS)
    seed = Memory(now_ms=1)
    seed.set("seeded", b"prior")
    cont = _live_continuation(intent_id)

    # A first pass at the continuation's seq populates the replay cache, the
    # way the suspending activation would have.
    warm = _Handles(
        cont=cont,
        pending=[ToolIntent(intent_id=intent_id, expires_at_ms=_NOW_MS + 1_000)],
        memory=seed.to_blob(),
    )
    _run(dofn, _tool_result(intent_id), warm)
    assert provider.call_count == 1
    (warm_cache,) = warm.llm_cache.writes

    handles = _Handles(
        cont=cont,
        pending=[ToolIntent(intent_id=intent_id, expires_at_ms=_NOW_MS + 1_000)],
        memory=seed.to_blob(),
        cache=warm_cache,
    )
    emitted = _run(dofn, _tool_result(intent_id), handles)

    # The memory a resume reads is the whole point of keeping it alive across
    # the suspension.
    assert seen == [b"prior", b"prior"]
    # And the replay cache is what makes the retried resume free: a resume that
    # loaded an empty one would re-call the provider (correctness invariant 3).
    assert provider.call_count == 1
    (llm_call,) = [e for e in _tagged(emitted, "traces") if e.event_type == TraceEvent.LLM_CALL]
    assert llm_call.attributes["beam_agents.cache_hit"] == "true"


def test_the_commit_arms_hitl_on_status_not_on_the_presence_of_a_deadline() -> None:
    # `_commit` takes the result it is given: a completed one must not arm the
    # HITL timer even if a deadline is somehow attached, or a key that finished
    # would be woken to time out a suspension it no longer has.
    dofn = _AgentDoFn(_Recorder(), provider_factory=make_pong_provider, ttl_ms=_TTL_MS)
    handles = _Handles()
    result = ActivationResult(
        status="completed",
        seq=_SEQ,
        memory_blob=MemoryBlob(state_schema_version=1),
        cache_blob=LlmCacheBlob(state_schema_version=1),
        intents=[],
        traces=[],
        outputs=[],
        continuation=None,
        hitl_deadline_ms=_NOW_MS + 900_000,
    )

    list(dofn._commit(result, _NOW_MS, 5, **handles.as_kwargs()))

    assert handles.hitl_timer.cleared is True
    assert handles.hitl_timer.set_to is None
    assert handles.ttl_timer.set_to == Timestamp(micros=(_NOW_MS + _TTL_MS) * 1000)


def test_setup_hands_the_bridge_its_configured_cancel_grace() -> None:
    dofn = _AgentDoFn(_Recorder(), provider_factory=make_pong_provider, cancel_grace_s=2.5)
    dofn.setup()
    try:
        assert dofn._bridge is not None
        assert dofn._bridge._cancel_grace_s == 2.5
    finally:
        dofn.teardown()
