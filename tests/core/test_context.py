"""Tests for the `agent-context` capability's `AgentContext` and `AgentResult`.

Covers: activation-scope exposure, the staged-effects accumulator and
drain-once semantics, StagingSink conformance with the model facade,
deterministic `ctx.act` intents, read-only tool execution, output staging,
and the immutable `AgentResult` bundle.
"""

from __future__ import annotations

import dataclasses
import uuid

import pytest

import beam_agents.core.context as context_module
from beam_agents._protos import LlmCacheBlob, MemoryBlob, ToolResult, TraceEvent
from beam_agents.core.agent import intent_id_for
from beam_agents.core.context import (
    INTENT_NAMESPACE,
    ActivationContext,
    AgentContext,
    AgentResult,
)
from beam_agents.model import FakeLLM, LlmRequest, StagingSink, TokenUsage, match_any, respond_with
from beam_agents.model.replay_cache import compute_cache_key as real_compute_cache_key
from beam_agents.tools import SideEffectToolError, ToolRegistry, tool

from ._context_helpers import make_context


def _register_side_effect_tool(registry: ToolRegistry, calls: list[object]) -> None:
    @tool(side_effect=True)
    def charge_card(amount: int) -> None:
        calls.append(amount)

    registry.register(charge_card)


# --- Requirement: Activation-scoped AgentContext surface ---------------------


def test_context_exposes_the_injected_activation_scope() -> None:
    # Scenario: Context exposes the injected activation scope.
    ctx = make_context(entity_key=b"k1", seq=7, now_ms=42)

    assert ctx.entity_key == b"k1"
    assert ctx.seq == 7
    assert ctx.now_ms == 42


def test_constructing_a_context_has_no_side_effects() -> None:
    # Scenario: Constructing a context has no side effects.
    ctx = make_context()

    result = ctx.drain()

    assert result.outputs == ()
    assert result.intents == ()
    assert result.traces == ()
    assert result.usage == TokenUsage(0, 0, 0)
    assert result.memory_blob is None
    assert result.cache_blob is None


# --- Requirement: Staged-effects accumulator applied only on success --------


def test_effects_are_withheld_until_drain() -> None:
    # Scenario: Effects are withheld until drain.
    ctx = make_context()
    ctx.memory.set("k", b"v")
    ctx.emit("output-a")

    result = ctx.drain()

    assert result.outputs == ("output-a",)
    assert result.memory_blob is not None


def test_drain_returns_the_full_accumulated_bundle() -> None:
    # Scenario: Drain returns the full accumulated bundle.
    calls: list[object] = []
    registry = ToolRegistry()
    _register_side_effect_tool(registry, calls)
    ctx = make_context(tool_registry=registry)

    ctx.memory.set("k", b"v")
    ctx.emit("output-a")
    ctx.emit("output-b")
    ctx.act("charge_card", {"amount": 5})

    result = ctx.drain()

    assert result.outputs == ("output-a", "output-b")
    assert len(result.intents) == 1
    assert result.intents[0].tool_name == "charge_card"
    assert result.memory_blob is not None
    assert calls == []


async def test_a_failed_activation_contributes_nothing() -> None:
    # Scenario: A failed activation contributes nothing.
    ctx = make_context()

    async def agent() -> None:
        ctx.memory.set("k", b"v")
        ctx.emit("output-a")
        raise RuntimeError("boom")

    result: AgentResult | None = None
    try:
        await agent()
    except RuntimeError:
        pass
    else:
        result = ctx.drain()  # unreachable: agent always raises

    assert result is None


def test_draining_twice_is_refused() -> None:
    # Scenario: Draining twice is refused.
    ctx = make_context()
    ctx.drain()

    with pytest.raises(RuntimeError, match=r"^AgentContext\.drain\(\) called more than once$"):
        ctx.drain()


# --- Requirement: Context is the model facade's staging sink ----------------


def test_context_satisfies_staging_sink_protocol() -> None:
    ctx = make_context()
    assert isinstance(ctx, StagingSink)


async def test_facade_staged_traces_land_in_the_context_bundle() -> None:
    # Scenario: Facade-staged traces land in the context bundle.
    fake = FakeLLM([(match_any(), respond_with(b"hello"))])
    ctx = make_context(provider=fake)

    await ctx.model.complete(
        LlmRequest(model_id="m-1", messages=[], tools_schema=[], sampling_params={}),
        entity_key=ctx.entity_key,
        seq=ctx.seq,
        step_index=0,
    )

    result = ctx.drain()
    assert len(result.traces) == 1


def test_stage_trace_event_preserves_the_exact_event() -> None:
    ctx = make_context()
    event = TraceEvent(seq=8, event_type=TraceEvent.LLM_CALL)

    ctx.stage_trace_event(event)

    result = ctx.drain()
    assert result.traces == (event,)
    assert result.traces[0] is event


async def test_accumulated_usage_survives_to_the_result() -> None:
    # Scenario: Accumulated usage survives to the result.
    fake = FakeLLM([(match_any(), respond_with(b"hello"))])
    ctx = make_context(provider=fake)
    req1 = LlmRequest(model_id="m-1", messages=[], tools_schema=[], sampling_params={})
    req2 = LlmRequest(model_id="m-2", messages=[], tools_schema=[], sampling_params={})

    await ctx.model.complete(req1, entity_key=ctx.entity_key, seq=ctx.seq, step_index=0)
    await ctx.model.complete(req2, entity_key=ctx.entity_key, seq=ctx.seq, step_index=1)

    result = ctx.drain()
    n = len(b"hello")
    assert result.usage.prompt_tokens == 2 * n
    assert result.usage.completion_tokens == 2 * n
    assert result.usage.total_tokens == 4 * n


# --- Requirement: Side effects flow only through ctx.act as deterministic intents


def test_act_stages_an_intent_without_executing_the_tool() -> None:
    # Scenario: act stages an intent without executing the tool.
    calls: list[object] = []
    registry = ToolRegistry()
    _register_side_effect_tool(registry, calls)
    ctx = make_context(
        entity_key=b"entity-a",
        seq=7,
        now_ms=1234,
        tool_registry=registry,
    )

    ctx.act("charge_card", {"amount": 5})

    result = ctx.drain()
    assert len(result.intents) == 1
    intent = result.intents[0]
    assert intent.entity_key == b"entity-a"
    assert intent.seq == 7
    assert intent.step_index == 0
    assert intent.tool_name == "charge_card"
    assert intent.args_json == '{"amount":5}'
    assert intent.created_at_ms == 1234
    assert intent.expires_at_ms == 0
    assert intent.attempt == 0
    assert calls == []


def test_act_preserves_unicode_and_rejects_non_finite_numbers() -> None:
    calls: list[object] = []
    registry = ToolRegistry()
    _register_side_effect_tool(registry, calls)
    ctx = make_context(tool_registry=registry)

    ctx.act("charge_card", {"amount": "€5"})
    assert ctx.drain().intents[0].args_json == '{"amount":"€5"}'

    ctx = make_context(tool_registry=registry)
    with pytest.raises(ValueError, match="Out of range float values"):
        ctx.act("charge_card", {"amount": float("nan")})


def test_intent_ids_are_deterministic_and_match_the_uuid5_formula() -> None:
    # Scenario: Intent IDs are deterministic across replay.
    calls: list[object] = []
    registry = ToolRegistry()
    _register_side_effect_tool(registry, calls)
    ctx = make_context(entity_key=b"key-x", seq=9, tool_registry=registry)

    ctx.act("charge_card", {"amount": 5})
    result = ctx.drain()

    expected = str(uuid.uuid5(INTENT_NAMESPACE, f"{b'key-x'.hex()}:9:0"))
    assert result.intents[0].intent_id == expected


def test_replayed_activation_produces_byte_identical_intents() -> None:
    # Scenario: Intent IDs are deterministic across replay.
    def run_once() -> AgentResult:
        calls: list[object] = []
        registry = ToolRegistry()
        _register_side_effect_tool(registry, calls)
        ctx = make_context(entity_key=b"key-x", seq=9, tool_registry=registry)
        ctx.act("charge_card", {"amount": 5})
        ctx.act("charge_card", {"amount": 7})
        return ctx.drain()

    first, second = run_once(), run_once()

    assert first.intents == second.intents


def test_act_sorts_argument_keys_independent_of_insertion_order() -> None:
    calls: list[object] = []
    registry = ToolRegistry()
    _register_side_effect_tool(registry, calls)
    ctx = make_context(tool_registry=registry)

    ctx.act("charge_card", {"z": 1, "amount": 5, "a": 2})

    assert ctx.drain().intents[0].args_json == '{"a":2,"amount":5,"z":1}'


def test_step_index_advances_per_act_call() -> None:
    # Scenario: step_index advances per act call.
    calls: list[object] = []
    registry = ToolRegistry()
    _register_side_effect_tool(registry, calls)
    ctx = make_context(tool_registry=registry)

    ctx.act("charge_card", {"amount": 1})
    ctx.act("charge_card", {"amount": 2})
    ctx.act("charge_card", {"amount": 3})

    result = ctx.drain()
    assert [i.step_index for i in result.intents] == [0, 1, 2]
    assert len({i.intent_id for i in result.intents}) == 3


def test_act_on_a_read_only_tool_is_refused() -> None:
    # Misuse guard (design D3): act() requires a side_effect=True tool.
    @tool
    def lookup(customer_id: str) -> str:
        return customer_id

    registry = ToolRegistry()
    registry.register(lookup)
    ctx = make_context(tool_registry=registry)

    with pytest.raises(
        ValueError,
        match=(
            r"^tool 'lookup' is side_effect=False; call it via run_tool\(\.\.\.\) "
            r"instead of act\(\.\.\.\)$"
        ),
    ):
        ctx.act("lookup", {"customer_id": "abc"})

    assert ctx.drain().intents == ()


async def test_a_side_effect_tool_cannot_execute_inline() -> None:
    # Scenario: A side-effect tool cannot execute inline.
    calls: list[object] = []
    registry = ToolRegistry()
    _register_side_effect_tool(registry, calls)
    ctx = make_context(tool_registry=registry)

    with pytest.raises(SideEffectToolError):
        await ctx.run_tool("charge_card", {"amount": 5})

    result = ctx.drain()
    assert result.intents == ()
    assert calls == []


# --- Requirement: Read-only tool execution through the context --------------


async def test_a_read_only_tool_runs_inline_and_returns_its_value() -> None:
    # Scenario: A read-only tool runs inline and returns its value.
    calls: list[str] = []

    @tool
    def lookup(customer_id: str) -> str:
        calls.append(customer_id)
        return customer_id.upper()

    registry = ToolRegistry()
    registry.register(lookup)
    ctx = make_context(tool_registry=registry)

    value = await ctx.run_tool("lookup", {"customer_id": "abc"})

    result = ctx.drain()
    assert value == "ABC"
    assert calls == ["abc"]
    assert result.intents == ()


# --- Requirement: Output staging ---------------------------------------------


def test_outputs_are_ordered_and_withheld_until_drain() -> None:
    # Scenario: Outputs are ordered and withheld until drain.
    ctx = make_context()

    ctx.emit("A")
    ctx.emit("B")

    result = ctx.drain()
    assert result.outputs == ("A", "B")


# --- Requirement: AgentResult is the immutable drained bundle ----------------


async def test_result_carries_every_staged_effect_category() -> None:
    # Scenario: Result carries every staged effect category.
    calls: list[object] = []
    registry = ToolRegistry()
    _register_side_effect_tool(registry, calls)
    fake = FakeLLM([(match_any(), respond_with(b"hi"))])
    ctx = make_context(tool_registry=registry, provider=fake)

    ctx.memory.set("k", b"v")
    ctx.emit("output-a")
    ctx.act("charge_card", {"amount": 5})
    await ctx.model.complete(
        LlmRequest(model_id="m-1", messages=[], tools_schema=[], sampling_params={}),
        entity_key=ctx.entity_key,
        seq=ctx.seq,
        step_index=1,
    )

    result = ctx.drain()

    assert result.outputs == ("output-a",)
    assert len(result.intents) == 1
    assert len(result.traces) == 1
    n = len(b"hi")
    assert result.usage.total_tokens == 2 * n
    assert result.memory_blob is not None
    assert result.cache_blob is not None


def test_a_clean_activation_yields_an_empty_result() -> None:
    # Scenario: A clean activation yields an empty result.
    ctx = make_context()

    result = ctx.drain()

    assert result.outputs == ()
    assert result.intents == ()
    assert result.traces == ()
    assert result.usage == TokenUsage(0, 0, 0)
    assert result.memory_blob is None
    assert result.cache_blob is None


def test_agent_result_is_frozen() -> None:
    ctx = make_context()
    result = ctx.drain()

    with pytest.raises(dataclasses.FrozenInstanceError):
        result.outputs = ("x",)  # type: ignore[misc]


def test_agent_context_wires_every_model_facade_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    facade = object()

    def fake_facade(provider: object, replay_cache: object, **kwargs: object) -> object:
        captured["provider"] = provider
        captured["replay_cache"] = replay_cache
        captured.update(kwargs)
        return facade

    monkeypatch.setattr(context_module, "LlmFacade", fake_facade)
    provider = object()
    replay_cache = object()
    rng = object()
    sleep = object()
    breaker = object()
    retry_policy = object()
    decode = object()
    memory = object()
    registry = ToolRegistry()

    ctx = AgentContext(
        entity_key=b"key",
        seq=8,
        now_ms=123,
        memory=memory,  # type: ignore[arg-type]
        replay_cache=replay_cache,  # type: ignore[arg-type]
        provider=provider,  # type: ignore[arg-type]
        rng=rng,  # type: ignore[arg-type]
        sleep=sleep,  # type: ignore[arg-type]
        breaker=breaker,  # type: ignore[arg-type]
        retry_policy=retry_policy,  # type: ignore[arg-type]
        decode=decode,  # type: ignore[arg-type]
        tool_registry=registry,
    )

    assert ctx.model is facade
    assert captured == {
        "provider": provider,
        "replay_cache": replay_cache,
        "now_ms": 123,
        "rng": rng,
        "sleep": sleep,
        "breaker": breaker,
        "retry_policy": retry_policy,
        "decode": decode,
        "staging": ctx,
    }


def test_activation_context_preserves_inputs_and_wires_state_facades(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, tuple[object, int, object | None]] = {}
    memory = object()
    replay_cache = object()

    def fake_memory(blob: object, *, now_ms: int, compactor: object | None = None) -> object:
        captured["memory"] = (blob, now_ms, compactor)
        return memory

    def fake_replay_cache(blob: object, *, now_ms: int) -> object:
        captured["cache"] = (blob, now_ms, None)
        return replay_cache

    monkeypatch.setattr(context_module, "Memory", fake_memory)
    monkeypatch.setattr(context_module, "ReplayCache", fake_replay_cache)
    provider = object()
    memory_blob = MemoryBlob(state_schema_version=1)
    cache_blob = LlmCacheBlob(state_schema_version=1)
    resume_result = ToolResult(intent_id="intent-1", status=ToolResult.OK)
    resume_approval = object()
    compactor = object()

    ctx = ActivationContext(
        entity_key=b"key",
        seq=8,
        now_ms=123,
        provider=provider,  # type: ignore[arg-type]
        memory_blob=memory_blob,
        cache_blob=cache_blob,
        event=b"event",
        resume_result=resume_result,
        resume_approval=resume_approval,
        snapshot=b"snapshot",
        compactor=compactor,  # type: ignore[arg-type]
    )

    assert ctx.entity_key == b"key"
    assert ctx.seq == 8
    assert ctx.now_ms == 123
    assert ctx.event == b"event"
    assert ctx.snapshot == b"snapshot"
    assert ctx.resume_result is resume_result
    assert ctx.resume_approval is resume_approval
    assert ctx.is_resume is True
    assert ctx.memory is memory
    assert captured == {
        "memory": (memory_blob, 123, compactor),
        "cache": (cache_blob, 123, None),
    }


def test_activation_context_defaults_event_snapshot_and_resume_state() -> None:
    ctx = ActivationContext(
        entity_key=b"key",
        seq=1,
        now_ms=2,
        provider=FakeLLM([]),
        memory_blob=None,
        cache_blob=None,
    )

    assert ctx.event == b""
    assert ctx.snapshot == b""
    assert ctx.resume_result is None
    assert ctx.resume_approval is None
    assert ctx.is_resume is False


def test_activation_context_stages_complete_intents_and_continuations() -> None:
    ctx = ActivationContext(
        entity_key=b"key",
        seq=8,
        now_ms=123,
        provider=FakeLLM([]),
        memory_blob=None,
        cache_blob=None,
    )

    intent_id = ctx.act("charge", '{"amount":5}', ttl_ms=60)
    second_id = ctx.act("notify", '{"channel":"email"}', ttl_ms=0)
    continuation = ctx.build_continuation(
        snapshot=b"state",
        adapter="langgraph",
        deadline_ms=999,
    )

    assert len(ctx.staged_intents) == 2
    intent = ctx.staged_intents[0]
    assert intent_id == intent_id_for(b"key", 8, 0)
    assert intent.intent_id == intent_id
    assert intent.entity_key == b"key"
    assert intent.seq == 8
    assert intent.step_index == 0
    assert intent.tool_name == "charge"
    assert intent.args_json == '{"amount":5}'
    assert intent.created_at_ms == 123
    assert intent.expires_at_ms == 183
    assert intent.attempt == 0
    assert second_id != intent_id
    assert ctx.staged_intents[1].step_index == 1
    assert ctx.step_index == 2
    assert continuation.state_schema_version == 1
    assert continuation.seq == 8
    assert continuation.step_index == 2
    assert list(continuation.pending_intent_ids) == [intent_id, second_id]
    assert continuation.adapter == "langgraph"
    assert continuation.snapshot == b"state"
    assert continuation.suspended_at_ms == 123
    assert continuation.deadline_ms == 999


def test_activation_context_stages_trace_objects_without_rewriting_them() -> None:
    ctx = ActivationContext(
        entity_key=b"key",
        seq=1,
        now_ms=2,
        provider=FakeLLM([]),
        memory_blob=None,
        cache_blob=None,
    )
    direct = TraceEvent(seq=7, event_type=TraceEvent.ERROR)
    sink = TraceEvent(seq=8, event_type=TraceEvent.TOOL_CALL)

    ctx.stage_trace(direct)
    ctx.stage_trace_event(sink)

    assert ctx.staged_traces == [direct, sink]
    assert ctx.staged_traces[0] is direct
    assert ctx.staged_traces[1] is sink


async def test_activation_context_model_call_uses_all_cache_dimensions_and_traces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[object, ...]] = []

    def capture_cache_key(
        model_id: str,
        messages: object,
        tools_schema: object,
        sampling_params: object,
        entity_key: bytes,
        seq: int,
    ) -> str:
        captured.append((model_id, messages, tools_schema, sampling_params, entity_key, seq))
        return real_compute_cache_key(
            model_id, messages, tools_schema, sampling_params, entity_key, seq
        )

    monkeypatch.setattr(context_module, "compute_cache_key", capture_cache_key)
    fake = FakeLLM([(match_any(), respond_with(b"response"))])
    ctx = ActivationContext(
        entity_key=b"key",
        seq=8,
        now_ms=123,
        provider=fake,
        memory_blob=None,
        cache_blob=None,
    )
    request = LlmRequest(
        model_id="model-1",
        messages=[{"role": "user", "content": "hello"}],
        tools_schema=[{"name": "lookup"}],
        sampling_params={"temperature": 0.25},
    )

    ctx.act("prepare", "{}", ttl_ms=0)
    first = await ctx.call_model(request)
    second = await ctx.call_model(request)

    assert first.response == b"response"
    assert second.response == b"response"
    assert fake.call_count == 1
    assert captured == [
        (
            "model-1",
            [{"role": "user", "content": "hello"}],
            [{"name": "lookup"}],
            {"temperature": 0.25},
            b"key",
            8,
        ),
        (
            "model-1",
            [{"role": "user", "content": "hello"}],
            [{"name": "lookup"}],
            {"temperature": 0.25},
            b"key",
            8,
        ),
    ]
    assert ctx.step_index == 3
    assert len(ctx.staged_traces) == 2
    miss, hit = ctx.staged_traces
    assert miss.entity_key == hit.entity_key == b"key"
    assert miss.seq == hit.seq == 8
    assert miss.step_index == 1
    assert hit.step_index == 2
    assert miss.event_type == hit.event_type == TraceEvent.LLM_CALL
    assert dict(miss.attributes) == {
        "gen_ai.request.model": "model-1",
        "beam_agents.cache_hit": "false",
    }
    assert dict(hit.attributes) == {
        "gen_ai.request.model": "model-1",
        "beam_agents.cache_hit": "true",
    }
    assert miss.start_ms == miss.end_ms == 123
    assert hit.start_ms == hit.end_ms == 123
    assert ctx.cache_blob().entries[0].response == b"response"
