from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import cast
from uuid import NAMESPACE_DNS, uuid5

import pytest
from apache_beam.pvalue import TaggedOutput

from beam_agents._protos import (
    AgentEnvelope,
    Continuation,
    LlmCacheBlob,
    MemoryBlob,
    ToolIntent,
    ToolResult,
    TraceEvent,
)
from beam_agents._protos import (
    RuntimeError as RuntimeErrorProto,
)
from beam_agents.core.dofn import (
    _ActivationBridge,
    _ActivationContext,
    _ActivationInput,
    _AgentDoFn,
    _derive_hitl_deadline_ms,
)
from beam_agents.memory import Memory
from beam_agents.model import FakeLLM, LlmRequest, match_any, respond_with
from beam_agents.model.replay_cache import ReplayCache, compute_cache_key


class _FakeReadState:
    def __init__(self, value: object | None = None) -> None:
        self.value = value
        self.ops: list[str] = []

    def read(self) -> object:
        return self.value

    def write(self, value: object) -> None:
        self.value = value
        self.ops.append("write")

    def clear(self) -> None:
        self.value = None
        self.ops.append("clear")


class _FakeBagState:
    def __init__(self, values: Iterable[ToolIntent] = ()) -> None:
        self.values = list(values)
        self.ops: list[str] = []

    def read(self) -> Iterable[object]:
        return tuple(self.values)

    def add(self, value: object) -> None:
        assert isinstance(value, ToolIntent)
        self.values.append(value)
        self.ops.append(f"add:{value.intent_id}")

    def clear(self) -> None:
        self.values.clear()
        self.ops.append("clear")


class _FakeCombiningState:
    def __init__(self, value: int = 0) -> None:
        self.value = value
        self.ops: list[str] = []

    def read(self) -> object:
        return self.value

    def add(self, value: int) -> None:
        self.value += value
        self.ops.append(f"add:{value}")


class _FakeTimer:
    def __init__(self) -> None:
        self.set_calls: list[object] = []
        self.clear_calls = 0

    def set(self, timestamp: object) -> None:
        self.set_calls.append(timestamp)

    def clear(self) -> None:
        self.clear_calls += 1


@dataclass
class _RecordingDriver:
    seen: list[_ActivationInput] = field(default_factory=list)
    loop_ids: list[int] = field(default_factory=list)

    async def run(self, activation_input: _ActivationInput, context: _ActivationContext) -> None:
        self.seen.append(activation_input)
        self.loop_ids.append(id(asyncio.get_running_loop()))
        context.stage_output((context.entity_key, context.seq, activation_input.kind))


@dataclass
class _SuspendResumeDriver:
    async def run(self, activation_input: _ActivationInput, context: _ActivationContext) -> None:
        if activation_input.kind == "external_event":
            z_intent = ToolIntent(intent_id="z", entity_key=context.entity_key, seq=context.seq)
            a_intent = ToolIntent(intent_id="a", entity_key=context.entity_key, seq=context.seq)
            context.replace_pending([z_intent, a_intent])
            context.set_continuation(
                Continuation(
                    state_schema_version=1,
                    seq=context.seq,
                    pending_intent_ids=["z", "a"],
                    deadline_ms=9_000,
                )
            )
            context.stage_tagged_output("intents", z_intent)
            context.stage_tagged_output("intents", a_intent)
            return
        if activation_input.kind in {"tool_result", "approval", "hitl_timeout"}:
            context.stage_output(
                (activation_input.kind, context.seq, tuple(context.pending_intents))
            )
            context.set_continuation(None)
            context.replace_pending(())


@dataclass
class _FailingDriver:
    async def run(self, activation_input: _ActivationInput, context: _ActivationContext) -> None:
        context.memory.set("mut", b"x")
        context.stage_output("should-not-commit")
        raise RuntimeError("driver boom")


@dataclass
class _SlowDriver:
    sleep_s: float
    touched: bool = False

    async def run(self, activation_input: _ActivationInput, context: _ActivationContext) -> None:
        self.touched = True
        context.memory.set("mut", b"x")
        context.stage_trace(TraceEvent(entity_key=context.entity_key, seq=context.seq))
        context.accumulate_usage(prompt_tokens=1, completion_tokens=2, total_tokens=3)
        await asyncio.sleep(self.sleep_s)
        context.stage_output("late-output")


class _RaceFuture:
    def __init__(self, context: _ActivationContext) -> None:
        self._context = context
        self._first = True
        self.cancelled = False

    def result(self, timeout: float | None = None) -> _ActivationContext:
        if self._first:
            self._first = False
            raise concurrent.futures.TimeoutError
        return self._context

    def cancel(self) -> bool:
        self.cancelled = True
        return False

    def exception(self, timeout: float | None = None) -> BaseException | None:
        _ = timeout
        return None

    def add_done_callback(self, callback: object) -> None:
        _ = callback


class _RaceBridge(_ActivationBridge):
    def __init__(self, context: _ActivationContext) -> None:
        super().__init__()
        self._context = context
        self.future = _RaceFuture(context)

    def start(self) -> None:
        return None

    def submit(self, coro: object) -> concurrent.futures.Future[_ActivationContext]:
        close = getattr(coro, "close", None)
        if callable(close):
            close()
        return cast(concurrent.futures.Future[_ActivationContext], self.future)

    def close(self, cancellation_grace_s: float) -> None:
        _ = cancellation_grace_s


@dataclass
class _ReplayDriver:
    llm: FakeLLM

    async def run(self, activation_input: _ActivationInput, context: _ActivationContext) -> None:
        _ = activation_input
        request = LlmRequest(
            model_id="fake-model",
            messages=[{"role": "user", "content": "hello"}],
            tools_schema=[],
            sampling_params={"temperature": 0},
        )
        cache_key = compute_cache_key(
            request.model_id,
            request.messages,
            request.tools_schema,
            request.sampling_params,
            context.entity_key,
            context.seq,
        )
        hit = context.replay_cache.get(cache_key)
        if hit is None:
            response = await self.llm.complete(request)
            context.replay_cache.put(cache_key, response.response)
        intent_id = str(uuid5(NAMESPACE_DNS, f"{context.entity_key.hex()}:{context.seq}:0"))
        intent = ToolIntent(intent_id=intent_id, entity_key=context.entity_key, seq=context.seq)
        context.stage_tagged_output("intents", intent)


def _run_once(
    dofn: _AgentDoFn,
    element: tuple[bytes, AgentEnvelope],
    *,
    memory: object | None = None,
    continuation: object | None = None,
    cache: object | None = None,
    pending: Iterable[ToolIntent] = (),
    seq: int = 0,
) -> tuple[
    list[object], _FakeReadState, _FakeReadState, _FakeReadState, _FakeBagState, _FakeCombiningState
]:
    memory_state = _FakeReadState(memory)
    continuation_state = _FakeReadState(continuation)
    cache_state = _FakeReadState(cache)
    pending_state = _FakeBagState(pending)
    seq_state = _FakeCombiningState(seq)
    ttl_timer = _FakeTimer()
    hitl_timer = _FakeTimer()
    outputs = list(
        dofn._run_envelope(
            element=element,
            memory_state=memory_state,
            continuation_state=continuation_state,
            cache_state=cache_state,
            pending_state=pending_state,
            seq_state=seq_state,
            ttl_timer=ttl_timer,
            hitl_timer=hitl_timer,
        )
    )
    return outputs, memory_state, continuation_state, cache_state, pending_state, seq_state


def test_activation_context_loads_state_and_stages_effects() -> None:
    memory_blob = MemoryBlob(state_schema_version=1)
    memory_blob.entries.add(key="k", value=b"\x00v", last_access_ms=10)
    cache_blob = LlmCacheBlob(state_schema_version=1)
    cache_blob.entries.add(cache_key="a", response=b"r", response_digest=b"d", created_at_ms=1)
    continuation = Continuation(state_schema_version=1, seq=8, pending_intent_ids=["b"])
    pending = [ToolIntent(intent_id="b"), ToolIntent(intent_id="a")]
    dofn = _AgentDoFn(driver=_RecordingDriver())
    routed = _ActivationInput(
        kind="tool_result",
        envelope=AgentEnvelope(
            entity_key=b"key",
            event_time_ms=1234,
            tool_result=ToolResult(intent_id="b"),
        ),
    )
    context = dofn._load_context(
        routed=routed,
        loaded_seq=5,
        memory_state=_FakeReadState(memory_blob),
        continuation_state=_FakeReadState(continuation),
        cache_state=_FakeReadState(cache_blob),
        pending_state=_FakeBagState(pending),
        remove_resolved_intent="b",
    )
    assert context.seq == 8
    assert context.seq_delta == 0
    assert context.memory.get("k") == b"v"
    assert context.replay_cache.get("a") is not None
    assert tuple(context.pending_intents) == ("a",)
    context.set_ttl_deadline_ms(2000)
    context.set_hitl_deadline_ms(3000)
    context.stage_tagged_output("intents", ToolIntent(intent_id="a"))
    context.stage_trace(TraceEvent(entity_key=b"key"))
    context.accumulate_usage(prompt_tokens=1, completion_tokens=2, total_tokens=3)
    assert context.ttl_deadline_ms == 2000
    assert context.hitl_deadline_ms == 3000
    assert context.usage.total_tokens == 3


def test_commit_preparation_sorts_pending_and_precedes_output_iteration() -> None:
    driver = _SuspendResumeDriver()
    commit_steps: list[str] = []
    dofn = _AgentDoFn(driver=driver, commit_audit_hook=commit_steps.append, memory_ttl_ms=1000)
    dofn.setup()
    try:
        iterator = dofn._run_envelope(
            element=(
                b"k",
                AgentEnvelope(entity_key=b"k", event_time_ms=100, external_event=b"start"),
            ),
            memory_state=_FakeReadState(),
            continuation_state=_FakeReadState(),
            cache_state=_FakeReadState(),
            pending_state=_FakeBagState(),
            seq_state=_FakeCombiningState(0),
            ttl_timer=_FakeTimer(),
            hitl_timer=_FakeTimer(),
        )
        first = next(iterator)
        assert isinstance(first, TaggedOutput)
        assert commit_steps == [
            "MEMORY",
            "LLM_CACHE",
            "CONTINUATION",
            "PENDING",
            "SEQ",
            "TTL_TIMER",
            "HITL_TIMER",
        ]
    finally:
        dofn.teardown()


def test_commit_application_order_and_deterministic_pending_replacement() -> None:
    commit_steps: list[str] = []
    dofn = _AgentDoFn(driver=_SuspendResumeDriver(), commit_audit_hook=commit_steps.append)
    dofn.setup()
    try:
        outputs, _memory, continuation, _cache, pending, seq = _run_once(
            dofn,
            (
                b"k",
                AgentEnvelope(entity_key=b"k", event_time_ms=1000, external_event=b"start"),
            ),
            seq=0,
        )
    finally:
        dofn.teardown()
    assert [item.intent_id for item in pending.values] == ["a", "z"]
    assert isinstance(continuation.value, Continuation)
    assert seq.value == 1
    assert commit_steps == [
        "MEMORY",
        "LLM_CACHE",
        "CONTINUATION",
        "PENDING",
        "SEQ",
        "TTL_TIMER",
        "HITL_TIMER",
    ]
    assert sum(isinstance(item, TaggedOutput) for item in outputs) >= 2


def test_staged_emissions_preserve_interleaved_order() -> None:
    context = _ActivationContext(
        entity_key=b"k",
        activation_time_ms=1,
        seq=1,
        seq_delta=1,
        memory=Memory(now_ms=1),
        replay_cache=ReplayCache(now_ms=1),
        continuation=None,
        pending_intents={},
    )
    trace = TraceEvent(entity_key=b"k", seq=1)
    context.stage_output("main-before")
    context.stage_tagged_output("intents", ToolIntent(intent_id="a"))
    context.stage_trace(trace)
    context.stage_output("main-after")

    prepared = _AgentDoFn(driver=_RecordingDriver())._prepare_commit(context)

    assert prepared.emissions[0] == "main-before"
    assert isinstance(prepared.emissions[1], TaggedOutput)
    assert prepared.emissions[1].tag == "intents"
    assert isinstance(prepared.emissions[2], TaggedOutput)
    assert prepared.emissions[2].tag == "traces"
    assert prepared.emissions[2].value == trace
    assert prepared.emissions[3] == "main-after"


def test_async_bridge_reuses_single_loop_and_teardown_joins_thread() -> None:
    driver = _RecordingDriver()
    dofn = _AgentDoFn(driver=driver)
    dofn.setup()
    bridge_thread = dofn._bridge.thread if dofn._bridge is not None else None
    assert bridge_thread is not None and bridge_thread.is_alive()
    try:
        _run_once(
            dofn,
            (b"a", AgentEnvelope(entity_key=b"a", event_time_ms=1, external_event=b"1")),
            seq=0,
        )
        _run_once(
            dofn,
            (b"a", AgentEnvelope(entity_key=b"a", event_time_ms=2, external_event=b"2")),
            seq=1,
        )
    finally:
        dofn.teardown()
    assert len(set(driver.loop_ids)) == 1
    assert dofn._bridge is None
    assert bridge_thread.is_alive() is False


def test_loop_owned_resources_setup_and_close_on_loop() -> None:
    seen: dict[str, int] = {}

    class _Resource:
        async def setup(self) -> None:
            seen["setup_loop"] = id(asyncio.get_running_loop())
            seen["setup_thread"] = threading.get_ident()

        async def close(self) -> None:
            seen["close_loop"] = id(asyncio.get_running_loop())
            seen["close_thread"] = threading.get_ident()

    dofn = _AgentDoFn(
        driver=_RecordingDriver(),
        bridge_factory=lambda: _ActivationBridge(_Resource()),
    )
    dofn.setup()
    bridge = dofn._bridge
    assert bridge is not None and bridge.loop is not None and bridge.thread is not None
    loop_id = id(bridge.loop)
    thread_id = bridge.thread.ident
    dofn.teardown()
    assert seen["setup_loop"] == loop_id
    assert seen["close_loop"] == loop_id
    assert seen["setup_thread"] == thread_id
    assert seen["close_thread"] == thread_id


def test_timeout_cancels_and_discards_staged_effects() -> None:
    driver = _SlowDriver(sleep_s=0.2)
    dofn = _AgentDoFn(driver=driver, activation_timeout_s=0.01, cancellation_grace_s=0.01)
    dofn.setup()
    try:
        outputs, memory, continuation, cache, pending, seq = _run_once(
            dofn,
            (b"k", AgentEnvelope(entity_key=b"k", event_time_ms=1000, external_event=b"x")),
            seq=0,
        )
    finally:
        dofn.teardown()
    assert len(outputs) == 1
    err = outputs[0]
    assert isinstance(err, TaggedOutput)
    assert isinstance(err.value, RuntimeErrorProto)
    assert err.value.error_type == RuntimeErrorProto.ACTIVATION_TIMEOUT
    assert memory.value is None
    assert continuation.value is None
    assert cache.value is None
    assert pending.values == []
    assert seq.value == 0


def test_timeout_completion_race_is_fail_closed() -> None:
    race_context = _ActivationContext(
        entity_key=b"k",
        activation_time_ms=1000,
        seq=1,
        seq_delta=1,
        memory=Memory(now_ms=1000),
        replay_cache=ReplayCache(now_ms=1000),
        continuation=None,
        pending_intents={},
    )
    dofn = _AgentDoFn(
        driver=_RecordingDriver(),
        bridge_factory=lambda: _RaceBridge(race_context),
        activation_timeout_s=0.01,
    )
    dofn.setup()
    try:
        outputs, memory, continuation, cache, pending, seq = _run_once(
            dofn,
            (b"k", AgentEnvelope(entity_key=b"k", event_time_ms=1, external_event=b"x")),
            seq=0,
        )
    finally:
        dofn.teardown()
    assert isinstance(outputs[0], TaggedOutput)
    assert outputs[0].value.error_type == RuntimeErrorProto.ACTIVATION_TIMEOUT
    assert memory.value is None
    assert continuation.value is None
    assert cache.value is None
    assert pending.values == []
    assert seq.value == 0


def test_state_specs_and_coders_are_exact_and_deterministic() -> None:
    assert _AgentDoFn.MEMORY.name == "MEMORY"
    assert _AgentDoFn.CONTINUATION.name == "CONTINUATION"
    assert _AgentDoFn.LLM_CACHE.name == "LLM_CACHE"
    assert _AgentDoFn.PENDING.name == "PENDING"
    assert _AgentDoFn.SEQ.name == "SEQ"
    assert _AgentDoFn.TTL_TIMER.name.endswith("TTL_TIMER")
    assert _AgentDoFn.HITL_TIMER.name.endswith("HITL_TIMER")
    assert _AgentDoFn.MEMORY.coder.is_deterministic() is True
    assert _AgentDoFn.CONTINUATION.coder.is_deterministic() is True
    assert _AgentDoFn.LLM_CACHE.coder.is_deterministic() is True
    assert _AgentDoFn.PENDING.coder.is_deterministic() is True
    assert _AgentDoFn.SEQ.coder.is_deterministic() is True


@pytest.mark.parametrize(
    ("element", "continuation", "pending", "expected_error"),
    [
        (
            (b"other", AgentEnvelope(entity_key=b"k", event_time_ms=1, external_event=b"x")),
            None,
            (),
            RuntimeErrorProto.INVALID_ENVELOPE,
        ),
        (
            (b"k", AgentEnvelope(entity_key=b"k", event_time_ms=1)),
            None,
            (),
            RuntimeErrorProto.INVALID_ENVELOPE,
        ),
        (
            (b"k", AgentEnvelope(entity_key=b"k", event_time_ms=1, external_event=b"x")),
            Continuation(state_schema_version=1, seq=9, pending_intent_ids=["a"]),
            (ToolIntent(intent_id="a"),),
            RuntimeErrorProto.BUSY_KEY,
        ),
        (
            (
                b"k",
                AgentEnvelope(
                    entity_key=b"k",
                    event_time_ms=1,
                    tool_result=ToolResult(intent_id="missing"),
                ),
            ),
            None,
            (),
            RuntimeErrorProto.ORPHANED_RESULT,
        ),
    ],
)
def test_routing_errors(
    element: tuple[bytes, AgentEnvelope],
    continuation: Continuation | None,
    pending: Iterable[ToolIntent],
    expected_error: int,
) -> None:
    dofn = _AgentDoFn(driver=_RecordingDriver())
    dofn.setup()
    try:
        outputs, *_ = _run_once(
            dofn,
            element,
            continuation=continuation,
            pending=pending,
            seq=3,
        )
    finally:
        dofn.teardown()
    assert len(outputs) == 1
    tagged = outputs[0]
    assert isinstance(tagged, TaggedOutput)
    assert isinstance(tagged.value, RuntimeErrorProto)
    assert tagged.value.error_type == expected_error


@pytest.mark.parametrize("payload", ["tool_result", "approval"])
def test_expired_reinjection_is_orphaned(payload: str) -> None:
    intent = ToolIntent(intent_id="a", seq=7, expires_at_ms=100)
    continuation = Continuation(
        state_schema_version=1,
        seq=7,
        pending_intent_ids=["a"],
        deadline_ms=200,
    )
    if payload == "tool_result":
        envelope = AgentEnvelope(
            entity_key=b"k",
            tool_result=ToolResult(intent_id="a"),
        )
    else:
        envelope = AgentEnvelope(
            entity_key=b"k",
            approval=AgentEnvelope.Approval(intent_id="a", approved=True),
        )
    driver = _RecordingDriver()
    dofn = _AgentDoFn(driver=driver, now_ms_fn=lambda: 100)
    dofn.setup()
    try:
        outputs, *_ = _run_once(
            dofn,
            (b"k", envelope),
            continuation=continuation,
            pending=[intent],
            seq=7,
        )
    finally:
        dofn.teardown()

    assert len(outputs) == 1
    assert isinstance(outputs[0], TaggedOutput)
    assert outputs[0].value.error_type == RuntimeErrorProto.ORPHANED_RESULT
    assert driver.seen == []


def test_correlated_result_and_approval_resume_continuation_seq() -> None:
    dofn = _AgentDoFn(driver=_SuspendResumeDriver())
    dofn.setup()
    try:
        continuation = Continuation(state_schema_version=1, seq=7, pending_intent_ids=["a"])
        pending = [ToolIntent(intent_id="a", seq=7)]
        result_outputs, *_ = _run_once(
            dofn,
            (
                b"k",
                AgentEnvelope(
                    entity_key=b"k",
                    event_time_ms=1,
                    tool_result=ToolResult(intent_id="a"),
                ),
            ),
            continuation=continuation,
            pending=pending,
            seq=11,
        )
        approval_outputs, *_ = _run_once(
            dofn,
            (
                b"k",
                AgentEnvelope(
                    entity_key=b"k",
                    event_time_ms=1,
                    approval=AgentEnvelope.Approval(intent_id="a", approved=True),
                ),
            ),
            continuation=continuation,
            pending=pending,
            seq=11,
        )
    finally:
        dofn.teardown()
    assert ("tool_result", 7, ()) in result_outputs
    assert ("approval", 7, ()) in approval_outputs


def test_hitl_deadline_decision_uses_earliest_or_none() -> None:
    continuation = Continuation(state_schema_version=1, seq=1, deadline_ms=5000)
    pending = [
        ToolIntent(intent_id="a", expires_at_ms=3000),
        ToolIntent(intent_id="b", expires_at_ms=7000),
    ]
    assert _derive_hitl_deadline_ms(continuation, pending) == 3000
    assert _derive_hitl_deadline_ms(None, pending) == 3000
    assert (
        _derive_hitl_deadline_ms(Continuation(state_schema_version=1, seq=1, deadline_ms=0), [])
        is None
    )


def test_failure_and_commit_validation_are_atomic() -> None:
    dofn = _AgentDoFn(driver=_FailingDriver())
    dofn.setup()
    try:
        outputs, memory, continuation, cache, pending, seq = _run_once(
            dofn,
            (b"k", AgentEnvelope(entity_key=b"k", event_time_ms=1, external_event=b"x")),
            memory=MemoryBlob(state_schema_version=1),
            seq=3,
        )
    finally:
        dofn.teardown()
    assert isinstance(outputs[0], TaggedOutput)
    assert outputs[0].value.error_type == RuntimeErrorProto.ACTIVATION_FAILED
    assert isinstance(memory.value, MemoryBlob)
    assert continuation.value is None
    assert cache.value is None
    assert pending.values == []
    assert seq.value == 3


def test_commit_application_failure_escapes_to_abort_bundle() -> None:
    def fail_after_first_state_write(step: str) -> None:
        if step == "LLM_CACHE":
            raise RuntimeError("commit application failed")

    dofn = _AgentDoFn(
        driver=_RecordingDriver(),
        commit_audit_hook=fail_after_first_state_write,
    )
    dofn.setup()
    try:
        with pytest.raises(RuntimeError, match="commit application failed"):
            _run_once(
                dofn,
                (b"k", AgentEnvelope(entity_key=b"k", external_event=b"x")),
            )
    finally:
        dofn.teardown()


def test_replay_uses_cache_and_emits_byte_identical_intent() -> None:
    llm = FakeLLM([(match_any(), respond_with(b"response"))])
    driver = _ReplayDriver(llm=llm)
    dofn = _AgentDoFn(driver=driver)
    dofn.setup()
    try:
        first_outputs, memory, continuation, cache, pending, seq = _run_once(
            dofn,
            (b"k", AgentEnvelope(entity_key=b"k", event_time_ms=10, external_event=b"x")),
            seq=0,
        )
        first_intent = next(
            tagged.value
            for tagged in first_outputs
            if isinstance(tagged, TaggedOutput) and tagged.tag == "intents"
        )
        assert llm.call_count == 1
        replay_outputs, *_ = _run_once(
            dofn,
            (b"k", AgentEnvelope(entity_key=b"k", event_time_ms=10, external_event=b"x")),
            memory=memory.value,
            continuation=continuation.value,
            cache=cache.value,
            pending=pending.values,
            seq=0,
        )
    finally:
        dofn.teardown()
    replay_intent = next(
        tagged.value
        for tagged in replay_outputs
        if isinstance(tagged, TaggedOutput) and tagged.tag == "intents"
    )
    assert llm.call_count == 1
    assert first_intent.SerializeToString(deterministic=True) == replay_intent.SerializeToString(
        deterministic=True
    )
    assert seq.value == 1
