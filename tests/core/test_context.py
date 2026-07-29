"""Tests for the `agent-context` capability's `AgentContext` and `AgentResult`.

Covers: activation-scope exposure, the staged-effects accumulator and
drain-once semantics, StagingSink conformance with the model facade,
deterministic `ctx.act` intents, read-only tool execution, output staging,
and the immutable `AgentResult` bundle.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable

import pytest

import beam_agents.core.context as context_module
from beam_agents._protos import (
    AgentEnvelope,
    LlmCacheBlob,
    MemoryBlob,
    ToolIntent,
    ToolResult,
    TraceEvent,
)
from beam_agents.core.agent import intent_id_for
from beam_agents.core.context import (
    ActivationContext,
    AgentContext,
    AgentResult,
)
from beam_agents.hitl import DEFAULT_APPROVAL_CHANNEL, DEFAULT_INTENT_TTL_MS
from beam_agents.model import FakeLLM, LlmRequest, StagingSink, TokenUsage, match_any, respond_with
from beam_agents.model.replay_cache import compute_cache_key as real_compute_cache_key
from beam_agents.observability import ROLE_ACTIVATION, span_id_for, trace_id_for
from beam_agents.tools import SideEffectToolError, ToolNotFoundError, ToolRegistry, tool

from ._context_helpers import decode_len_based, make_context


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
    assert intent.expires_at_ms == 1234 + DEFAULT_INTENT_TTL_MS
    assert intent.attempt == 0
    assert intent.kind == ToolIntent.TOOL
    assert calls == []


def test_act_stamps_a_positive_expiry_from_the_intent_ttl() -> None:
    # Scenario: Intents staged through the authoring surface carry an expiry.
    # A non-positive expires_at_ms means "already expired" to every consumer,
    # so this surface may never leave it at zero.
    calls: list[object] = []
    registry = ToolRegistry()
    _register_side_effect_tool(registry, calls)
    ctx = make_context(now_ms=5_000, intent_ttl_ms=90_000, tool_registry=registry)

    ctx.act("charge_card", {"amount": 5})

    intent = ctx.drain().intents[0]
    assert intent.expires_at_ms == 95_000
    assert intent.expires_at_ms > 0


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

    assert result.intents[0].intent_id == intent_id_for(b"key-x", 9, 0)


def test_agent_context_and_activation_context_mint_the_same_intent_id() -> None:
    # Scenario: AgentContext.act and ActivationContext.act are two independent
    # entry points into intent-ID minting; both must agree for the same
    # (entity_key, seq, step_index), not just each match its own formula.
    calls: list[object] = []
    registry = ToolRegistry()
    _register_side_effect_tool(registry, calls)
    agent_ctx = make_context(entity_key=b"key-y", seq=4, tool_registry=registry)
    agent_ctx.act("charge_card", {"amount": 5})
    agent_result = agent_ctx.drain()

    activation_ctx = ActivationContext(
        entity_key=b"key-y",
        seq=4,
        now_ms=0,
        provider=FakeLLM([]),
        memory_blob=None,
        cache_blob=None,
    )
    activation_intent_id = activation_ctx.act("charge_card", '{"amount":5}', ttl_ms=0)

    assert agent_result.intents[0].intent_id == activation_intent_id
    assert agent_result.intents[0].intent_id == intent_id_for(b"key-y", 4, 0)


# --- Requirement: An activation can request a human approval -----------------


def test_request_approval_stages_an_approval_intent() -> None:
    # Scenario: Requesting an approval stages an APPROVAL intent.
    ctx = make_context(
        entity_key=b"entity-a",
        seq=7,
        now_ms=1234,
        approval_channel="pager",
        intent_ttl_ms=60_000,
    )

    intent_id = ctx.request_approval({"amount": 5, "reason": "refund"})

    intent = ctx.drain().intents[0]
    assert intent.kind == ToolIntent.APPROVAL
    assert intent.tool_name == "pager"
    assert intent.args_json == '{"amount":5,"reason":"refund"}'
    assert intent.created_at_ms == 1234
    assert intent.expires_at_ms == 1234 + 60_000
    assert intent.intent_id == intent_id == intent_id_for(b"entity-a", 7, 0)


def test_request_approval_looks_up_and_executes_nothing() -> None:
    # Scenario: Requesting an approval executes nothing.
    # The approval channel is not a registered tool: an empty registry must be
    # no obstacle, and a same-named registered tool must never be invoked.
    calls: list[object] = []
    registry = ToolRegistry()
    _register_side_effect_tool(registry, calls)
    ctx = make_context(approval_channel="charge_card", tool_registry=registry)

    ctx.request_approval({"amount": 5})

    assert calls == []
    assert ctx.drain().intents[0].kind == ToolIntent.APPROVAL

    # And with nothing registered at all.
    bare = make_context(approval_channel="nowhere", tool_registry=ToolRegistry())
    bare.request_approval({"amount": 5})
    assert bare.drain().intents[0].tool_name == "nowhere"


def test_request_approval_shares_the_step_sequence_with_act() -> None:
    # Approval and tool intents draw from one monotonic step sequence, so their
    # IDs cannot collide within an activation.
    calls: list[object] = []
    registry = ToolRegistry()
    _register_side_effect_tool(registry, calls)
    ctx = make_context(entity_key=b"k", seq=3, tool_registry=registry)

    ctx.act("charge_card", {"amount": 5})
    ctx.request_approval({"amount": 5})

    intents = ctx.drain().intents
    assert [i.step_index for i in intents] == [0, 1]
    assert intents[0].intent_id != intents[1].intent_id


def test_replayed_activation_produces_byte_identical_approval_intents() -> None:
    # Scenario: Approval requests are deterministic under replay.
    def run_once() -> AgentResult:
        ctx = make_context(entity_key=b"key-x", seq=9, now_ms=77)
        ctx.request_approval({"amount": 5})
        return ctx.drain()

    first, second = run_once(), run_once()

    assert first.intents == second.intents
    assert first.intents[0].SerializeToString(deterministic=True) == second.intents[
        0
    ].SerializeToString(deterministic=True)


def test_both_surfaces_mint_identical_approval_intents() -> None:
    # The two entry points must agree field-for-field, not just on the ID.
    agent_ctx = make_context(
        entity_key=b"key-y", seq=4, now_ms=100, approval_channel="pager", intent_ttl_ms=60_000
    )
    agent_ctx.request_approval({"amount": 5})
    agent_intent = agent_ctx.drain().intents[0]

    activation_ctx = ActivationContext(
        entity_key=b"key-y",
        seq=4,
        now_ms=100,
        provider=FakeLLM([]),
        memory_blob=None,
        cache_blob=None,
        approval_channel="pager",
        intent_ttl_ms=60_000,
    )
    activation_ctx.request_approval('{"amount":5}')

    assert agent_intent == activation_ctx.staged_intents[0]


def test_activation_context_request_approval_stages_an_approval_intent() -> None:
    # Scenario: Requesting an approval stages an APPROVAL intent (runtime surface).
    ctx = ActivationContext(
        entity_key=b"key",
        seq=8,
        now_ms=123,
        provider=FakeLLM([]),
        memory_blob=None,
        cache_blob=None,
    )

    intent_id = ctx.request_approval('{"amount":5}', ttl_ms=60_000)

    intent = ctx.staged_intents[0]
    assert intent.kind == ToolIntent.APPROVAL
    assert intent.tool_name == DEFAULT_APPROVAL_CHANNEL
    assert intent.args_json == '{"amount":5}'
    assert intent.expires_at_ms == 123 + 60_000
    assert intent.intent_id == intent_id == intent_id_for(b"key", 8, 0)
    assert ctx.step_index == 1


def test_activation_context_act_defaults_its_ttl_and_marks_kind_tool() -> None:
    # Scenario: Every staged intent carries a positive expiry.
    ctx = ActivationContext(
        entity_key=b"key",
        seq=8,
        now_ms=123,
        provider=FakeLLM([]),
        memory_blob=None,
        cache_blob=None,
        intent_ttl_ms=45_000,
    )

    ctx.act("charge", '{"amount":5}')

    intent = ctx.staged_intents[0]
    assert intent.kind == ToolIntent.TOOL
    assert intent.expires_at_ms == 123 + 45_000


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
    # One INTENT_EMITTED for the staged intent, one LLM_CALL for the model call.
    assert [event.event_type for event in result.traces] == [
        TraceEvent.INTENT_EMITTED,
        TraceEvent.LLM_CALL,
    ]
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
    resume_approval = AgentEnvelope.Approval(intent_id="intent-1", approved=True)
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


def test_activation_context_stages_the_same_trace_objects_it_was_given() -> None:
    # Staging correlates in place (see the stamping tests below) but never
    # substitutes a different object or reorders the staged sequence.
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
    # The `act` above stages its own INTENT_EMITTED ahead of the two calls.
    assert [event.event_type for event in ctx.staged_traces] == [
        TraceEvent.INTENT_EMITTED,
        TraceEvent.LLM_CALL,
        TraceEvent.LLM_CALL,
    ]
    _, miss, hit = ctx.staged_traces
    assert miss.entity_key == hit.entity_key == b"key"
    assert miss.seq == hit.seq == 8
    assert miss.step_index == 1
    assert hit.step_index == 2
    # No `decode` is configured here, so the token counts are genuinely
    # unknown and are omitted rather than reported as zero.
    assert dict(miss.attributes) == {
        "gen_ai.operation.name": "chat",
        "gen_ai.request.model": "model-1",
        "beam_agents.cache_hit": "false",
        "beam_agents.billed": "true",
    }
    assert dict(hit.attributes) == {
        "gen_ai.operation.name": "chat",
        "gen_ai.request.model": "model-1",
        "beam_agents.cache_hit": "true",
        "beam_agents.billed": "false",
    }
    assert miss.start_ms == miss.end_ms == 123
    assert hit.start_ms == hit.end_ms == 123
    assert ctx.cache_blob().entries[0].response == b"response"


# --- Requirement: Correlation stamped at the staging boundary ----------------


def test_an_uncorrelated_event_is_stamped_on_staging() -> None:
    # Scenario: An uncorrelated event is stamped on staging.
    ctx = _activation_context()
    ctx.stage_trace_event(TraceEvent(event_type=TraceEvent.LLM_CALL, step_index=2))

    event = ctx.staged_traces[0]
    assert event.trace_id == trace_id_for(b"key", 8)
    assert event.span_id == span_id_for(b"key", 8, "LLM_CALL", 2)
    assert event.parent_span_id == span_id_for(b"key", 8, ROLE_ACTIVATION, 0)


def test_a_producer_supplied_parent_is_preserved_by_the_context() -> None:
    # Scenario: A producer-supplied parent is preserved.
    ctx = _activation_context()
    ctx.stage_trace(
        TraceEvent(event_type=TraceEvent.LLM_CALL, step_index=2, parent_span_id=bytes(range(8)))
    )

    assert ctx.staged_traces[0].parent_span_id == bytes(range(8))


def test_both_context_surfaces_stamp_identically() -> None:
    # Scenario: Both context surfaces emit the same event shape — the two
    # surfaces must not disagree about identity for the same activation.
    agent_ctx = make_context(entity_key=b"key", seq=8, now_ms=123)
    activation_ctx = _activation_context()

    agent_ctx.stage_trace_event(TraceEvent(event_type=TraceEvent.LLM_CALL, step_index=2))
    activation_ctx.stage_trace_event(TraceEvent(event_type=TraceEvent.LLM_CALL, step_index=2))

    staged = agent_ctx.drain().traces[0]
    assert staged.SerializeToString(deterministic=True) == activation_ctx.staged_traces[
        0
    ].SerializeToString(deterministic=True)


def test_a_resumed_context_parents_its_children_to_its_own_span() -> None:
    ctx = _activation_context(
        step_index=4, resume_result=ToolResult(intent_id="i-1", status=ToolResult.OK)
    )
    ctx.stage_trace_event(TraceEvent(event_type=TraceEvent.LLM_CALL, step_index=5))

    event = ctx.staged_traces[0]
    assert event.trace_id == trace_id_for(b"key", 8)
    assert event.parent_span_id == span_id_for(b"key", 8, ROLE_ACTIVATION, 4)


# --- Requirement: Intent, tool, and suspension child events ------------------


def test_each_staged_intent_is_traced() -> None:
    # Scenario: Each staged intent is traced.
    ctx = _activation_context()

    first = ctx.act("charge", '{"amount":5}', ttl_ms=60)
    second = ctx.request_approval('{"amount":5}', ttl_ms=90)

    events = [e for e in ctx.staged_traces if e.event_type == TraceEvent.INTENT_EMITTED]
    assert len(events) == 2
    assert events[0].attributes["beam_agents.intent_id"] == first
    assert events[0].attributes["beam_agents.tool_name"] == "charge"
    assert events[0].attributes["beam_agents.intent_kind"] == "TOOL"
    assert events[0].attributes["beam_agents.expires_at_ms"] == "183"
    assert events[1].attributes["beam_agents.intent_id"] == second
    assert events[1].attributes["beam_agents.intent_kind"] == "APPROVAL"
    assert events[1].attributes["beam_agents.expires_at_ms"] == "213"
    # Each event sits at its own step, so the two get distinct spans and a
    # reader can order them against the activation's other events.
    assert [e.step_index for e in events] == [0, 1]
    assert events[0].span_id == span_id_for(b"key", 8, "INTENT_EMITTED", 0)
    assert events[1].span_id == span_id_for(b"key", 8, "INTENT_EMITTED", 1)
    assert all(e.start_ms == e.end_ms == 123 for e in events)


async def test_a_read_only_tool_call_is_traced_without_perturbing_intent_ids() -> None:
    # Scenario: A read-only tool call is traced without perturbing intent ids.
    # The regression this guards: if `run_tool` advanced the intent step
    # cursor, every intent staged after a tool call would get a different
    # intent_id, silently invalidating in-flight continuations.
    registry = ToolRegistry()
    calls: list[object] = []
    _register_side_effect_tool(registry, calls)

    @tool()
    def lookup(customer: str) -> str:
        return f"found {customer}"

    registry.register(lookup)
    ctx = make_context(entity_key=b"key-1", seq=3, now_ms=456, tool_registry=registry)

    await ctx.run_tool("lookup", {"customer": "alice"})
    ctx.act("charge_card", {"amount": 5})
    # A second call *after* an intent: its `step_index` must have moved with
    # the intent cursor while its span index moved on the tool counter, which
    # is what keeps the two numbering schemes visibly separate.
    await ctx.run_tool("lookup", {"customer": "bob"})
    # A third call: two are not enough to tell `+= 1` from `= 1`.
    await ctx.run_tool("lookup", {"customer": "carol"})

    result = ctx.drain()
    assert result.intents[0].intent_id == intent_id_for(b"key-1", 3, 0)
    tool_events = [e for e in result.traces if e.event_type == TraceEvent.TOOL_CALL]
    assert len(tool_events) == 3
    assert tool_events[0].attributes["beam_agents.tool_name"] == "lookup"
    assert tool_events[0].parent_span_id == span_id_for(b"key-1", 3, ROLE_ACTIVATION, 0)
    assert [e.step_index for e in tool_events] == [0, 1, 1]
    assert [e.span_id for e in tool_events] == [
        span_id_for(b"key-1", 3, "TOOL_CALL", index) for index in (0, 1, 2)
    ]
    assert all(e.start_ms == e.end_ms == 456 for e in tool_events)


# --- Requirement: trace_id propagates into emitted intents -------------------


def test_a_staged_intent_carries_the_activations_trace_id() -> None:
    # Scenario: A committed intent carries the activation's trace id.
    ctx = _activation_context()
    ctx.act("charge", "{}", ttl_ms=60)

    assert ctx.staged_intents[0].trace_id == trace_id_for(b"key", 8)


def test_the_agent_context_surface_also_propagates_trace_id() -> None:
    registry = ToolRegistry()
    _register_side_effect_tool(registry, [])
    ctx = make_context(entity_key=b"key-1", seq=3, now_ms=456, tool_registry=registry)

    ctx.act("charge_card", {"amount": 5})
    ctx.act("charge_card", {"amount": 6})

    result = ctx.drain()
    assert result.intents[0].trace_id == trace_id_for(b"key-1", 3)
    events = [e for e in result.traces if e.event_type == TraceEvent.INTENT_EMITTED]
    assert [e.step_index for e in events] == [0, 1]
    assert events[0].attributes["beam_agents.expires_at_ms"] == str(456 + DEFAULT_INTENT_TTL_MS)
    assert all(e.start_ms == e.end_ms == 456 for e in events)


def test_replay_restages_a_byte_identical_intent_including_trace_id() -> None:
    # Scenario: Replay produces byte-identical intents with the trace id populated.
    def stage() -> bytes:
        ctx = _activation_context()
        ctx.act("charge", '{"amount":5}', ttl_ms=60)
        return ctx.staged_intents[0].SerializeToString(deterministic=True)

    assert stage() == stage()


def test_intent_ids_are_unchanged_by_the_new_field() -> None:
    # Scenario: Intent ids are unchanged by the new field.
    ctx = _activation_context()
    intent_id = ctx.act("charge", "{}", ttl_ms=60)

    assert intent_id == intent_id_for(b"key", 8, 0)
    assert ctx.staged_intents[0].intent_id == intent_id


# --- Requirement: Token counts are truthful or absent ------------------------


async def test_activation_context_cache_hit_reports_stored_usage() -> None:
    # Scenario: A cache-hit call reports the stored response's real token counts.
    # Scenario: A cache hit is marked unbilled.
    fake = FakeLLM([(match_any(), respond_with(b"0123456789"))])
    ctx = _activation_context(provider=fake, decode=decode_len_based)
    request = LlmRequest(model_id="m-1", messages=[], tools_schema=[], sampling_params={})

    await ctx.call_model(request)
    await ctx.call_model(request)

    miss, hit = [e for e in ctx.staged_traces if e.event_type == TraceEvent.LLM_CALL]
    assert miss.attributes["beam_agents.cache_hit"] == "false"
    assert miss.attributes["beam_agents.billed"] == "true"
    assert hit.attributes["beam_agents.cache_hit"] == "true"
    assert hit.attributes["beam_agents.billed"] == "false"
    # The stored response is decoded on the hit: same real counts, not zeros
    # and not absent.
    assert hit.attributes["gen_ai.usage.input_tokens"] == "10"
    assert hit.attributes["gen_ai.usage.output_tokens"] == "10"
    assert miss.attributes["gen_ai.usage.input_tokens"] == "10"


async def test_activation_context_omits_usage_when_no_decode_is_configured() -> None:
    # Scenario: Unknown usage is omitted, not zeroed. Without a provider decode
    # the counts are genuinely unknown, and a "0" would read as a real
    # zero-token call to anything summing the attribute.
    fake = FakeLLM([(match_any(), respond_with(b"hello"))])
    ctx = _activation_context(provider=fake)

    request = LlmRequest(model_id="m-1", messages=[], tools_schema=[], sampling_params={})
    await ctx.call_model(request)

    event = ctx.staged_traces[0]
    assert "gen_ai.usage.input_tokens" not in event.attributes
    assert "gen_ai.usage.output_tokens" not in event.attributes
    assert event.attributes["beam_agents.billed"] == "true"


# --- Requirement: the per-activation metric tally ----------------------------


def _scripted_clock(*readings_ns: int) -> Callable[[], int]:
    """Monotonic-clock double returning `readings_ns` in order.

    Exhausting it raises `StopIteration`, so a test that scripts exactly the
    readings it expects also proves no *extra* reading was taken -- which is how
    the cache-hit path is pinned as untimed.
    """
    remaining = iter(readings_ns)
    return lambda: next(remaining)


def _activation_context(**kwargs: object) -> ActivationContext:
    defaults: dict[str, object] = {
        "entity_key": b"key",
        "seq": 8,
        "now_ms": 123,
        "provider": FakeLLM([(match_any(), respond_with(b"response"))]),
        "memory_blob": None,
        "cache_blob": None,
    }
    defaults.update(kwargs)
    return ActivationContext(**defaults)  # type: ignore[arg-type]


async def test_a_provider_reached_model_call_is_counted_and_timed() -> None:
    # Scenario: A provider-reached call is timed once. The duration comes from
    # the injected monotonic clock -- never from `now_ms`, which is frozen per
    # activation and would make every duration zero.
    ctx = _activation_context(monotonic_ns=_scripted_clock(1_000_000, 6_500_000))

    await ctx.call_model(
        LlmRequest(model_id="m", messages=["hello"], tools_schema=None, sampling_params=None)
    )

    tally = ctx.tally()
    assert tally.llm_calls == 1
    # 5.5ms of elapsed nanoseconds, floored: the sample is whole milliseconds,
    # which is what a Beam distribution (integer-only) can carry.
    assert tally.llm_ms == [5]
    assert isinstance(tally.llm_ms[0], int)


async def test_a_cache_hit_is_neither_counted_nor_timed() -> None:
    # Scenario: A cache hit is not a call. The clock is scripted with exactly
    # the two readings the single miss needs, so a hit that reached for the
    # clock at all would raise StopIteration instead of quietly recording a
    # near-zero sample that deflates the latency distribution.
    fake = FakeLLM([(match_any(), respond_with(b"response"))])
    ctx = _activation_context(provider=fake, monotonic_ns=_scripted_clock(0, 2_000_000))
    request = LlmRequest(model_id="m", messages=["hello"], tools_schema=None, sampling_params=None)

    await ctx.call_model(request)
    await ctx.call_model(request)

    tally = ctx.tally()
    assert fake.call_count == 1
    assert tally.llm_calls == 1
    assert tally.llm_ms == [2]


async def test_llm_ms_records_one_sample_per_provider_reached_call() -> None:
    # The sample count equals `llm_calls` by construction: both move at the
    # same site, so a dashboard can divide one by the other.
    ctx = _activation_context(monotonic_ns=_scripted_clock(0, 3_000_000, 10_000_000, 17_000_000))

    await ctx.call_model(
        LlmRequest(model_id="m", messages=["a"], tools_schema=None, sampling_params=None)
    )
    await ctx.call_model(
        LlmRequest(model_id="m", messages=["b"], tools_schema=None, sampling_params=None)
    )

    tally = ctx.tally()
    assert tally.llm_calls == len(tally.llm_ms) == 2
    assert tally.llm_ms == [3, 7]


def test_activation_context_accumulates_decoded_usage() -> None:
    # Scenario: Decoded usage is sampled. The model facade reports usage through
    # this sink on provider-reached calls only; before this capability the
    # runtime discarded it.
    ctx = _activation_context()

    assert ctx.tally().usage_observed is False
    ctx.accumulate_usage(TokenUsage(prompt_tokens=3, completion_tokens=4, total_tokens=7))
    ctx.accumulate_usage(TokenUsage(prompt_tokens=1, completion_tokens=1, total_tokens=2))

    tally = ctx.tally()
    assert tally.total_tokens == 9
    assert tally.usage_observed is True


def test_an_activation_that_decoded_no_usage_observes_none() -> None:
    # Scenario: An activation with no decoded usage contributes no sample. The
    # flag, not the count, is what distinguishes "nobody decoded" from "the
    # model genuinely reported zero".
    ctx = _activation_context()

    assert ctx.tally().usage_observed is False
    assert ctx.tally().total_tokens == 0


async def test_iterations_counts_this_activations_own_steps() -> None:
    ctx = _activation_context(monotonic_ns=_scripted_clock(0, 0))

    assert ctx.tally().iterations == 0
    await ctx.call_model(
        LlmRequest(model_id="m", messages=["hello"], tools_schema=None, sampling_params=None)
    )
    ctx.act("http.post", "{}", ttl_ms=1_000)

    assert ctx.tally().iterations == 2


async def test_a_resumed_activation_counts_only_its_own_steps() -> None:
    # Scenario: A resumed activation samples only its own steps. The cursor is
    # seeded from the continuation so intent IDs cannot collide; sampling the
    # cursor itself would re-count the suspended activation's work on resume.
    ctx = _activation_context(step_index=3, monotonic_ns=_scripted_clock(0, 0))

    await ctx.call_model(
        LlmRequest(model_id="m", messages=["hello"], tools_schema=None, sampling_params=None)
    )
    ctx.act("http.post", "{}", ttl_ms=1_000)

    assert ctx.step_index == 5
    assert ctx.tally().iterations == 2


async def test_activation_context_runs_a_read_only_tool_inline_and_counts_it() -> None:
    # Scenario: A read-only tool runs inline on the runtime surface and is
    # counted. The runtime surface previously had no inline-tool path at all,
    # which is why `tool_calls` could only ever read zero in a pipeline.
    calls: list[str] = []

    @tool
    def lookup(customer_id: str) -> str:
        calls.append(customer_id)
        return customer_id.upper()

    registry = ToolRegistry()
    registry.register(lookup)
    ctx = _activation_context(
        tool_registry=registry,
        monotonic_ns=_scripted_clock(1_000_000, 3_500_000, 4_000_000, 8_000_000),
    )

    value = await ctx.run_tool("lookup", {"customer_id": "abc"})
    second = await ctx.run_tool("lookup", {"customer_id": "def"})

    tally = ctx.tally()
    assert value == "ABC"
    assert second == "DEF"
    assert calls == ["abc", "def"]
    # One increment per execution -- two calls accumulate, not overwrite.
    assert tally.tool_calls == 2
    # Timed with the same injected clock as model calls, so `overhead_ms` can
    # exclude tool time from the activation's wall clock. 2.5ms floors to a
    # whole-millisecond integer -- the only thing a Beam distribution carries.
    assert tally.tool_ms == [2, 4]
    assert all(isinstance(sample, int) for sample in tally.tool_ms)


async def test_activation_context_run_tool_is_traced_without_perturbing_intent_ids() -> None:
    # Scenario: A read-only tool call is traced without perturbing intent ids —
    # on the runtime surface. Each call gets its own TOOL_CALL span from the
    # tool counter (never the intent cursor, design D8), while `step_index`
    # records where the call sat relative to the activation's other events.
    @tool()
    def lookup(customer_id: str) -> str:
        return customer_id.upper()

    registry = ToolRegistry()
    registry.register(lookup)
    ctx = _activation_context(
        tool_registry=registry,
        monotonic_ns=_scripted_clock(
            1_000_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000, 6_000_000
        ),
    )

    await ctx.run_tool("lookup", {"customer_id": "a"})
    intent_id = ctx.act("charge", "{}", ttl_ms=60)
    await ctx.run_tool("lookup", {"customer_id": "b"})
    # A third call: two are not enough to tell `+= 1` from `= 1`.
    await ctx.run_tool("lookup", {"customer_id": "c"})

    assert intent_id == intent_id_for(b"key", 8, 0)
    tool_events = [e for e in ctx.staged_traces if e.event_type == TraceEvent.TOOL_CALL]
    assert len(tool_events) == 3
    assert tool_events[0].attributes["beam_agents.tool_name"] == "lookup"
    # Span ids ride the tool counter (0, 1, 2); step_index rides the cursor.
    assert [e.span_id for e in tool_events] == [
        span_id_for(b"key", 8, "TOOL_CALL", index) for index in (0, 1, 2)
    ]
    assert [e.step_index for e in tool_events] == [0, 1, 1]
    assert all(e.parent_span_id == span_id_for(b"key", 8, ROLE_ACTIVATION, 0) for e in tool_events)
    assert all(e.start_ms == e.end_ms == 123 for e in tool_events)


async def test_activation_context_refuses_a_side_effect_tool_uncounted() -> None:
    # Scenario: A side-effecting tool is refused and not counted. Correctness
    # invariant 5: `ctx.act(...)` is the only effect path.
    executed: list[object] = []

    @tool(side_effect=True)
    def charge_card(amount: int) -> None:
        executed.append(amount)

    registry = ToolRegistry()
    registry.register(charge_card)
    ctx = _activation_context(tool_registry=registry)

    with pytest.raises(SideEffectToolError):
        await ctx.run_tool("charge_card", {"amount": 5})

    assert executed == []
    assert ctx.tally().tool_calls == 0
    assert ctx.tally().tool_ms == []


async def test_activation_context_run_tool_unknown_tool_is_refused() -> None:
    ctx = _activation_context()

    with pytest.raises(ToolNotFoundError):
        await ctx.run_tool("nope", {})

    assert ctx.tally().tool_calls == 0


async def test_run_tool_does_not_advance_the_step_cursor() -> None:
    # Scenario: Inline tool execution does not advance the step cursor. The
    # cursor mints intent IDs; a tool call that moved it would change every
    # subsequent intent_id and break replay identity.
    @tool
    def lookup(customer_id: str) -> str:
        return customer_id

    registry = ToolRegistry()
    registry.register(lookup)
    ctx = _activation_context(tool_registry=registry)

    first = ctx.act("http.post", "{}", ttl_ms=1_000)
    await ctx.run_tool("lookup", {"customer_id": "abc"})
    second = ctx.act("http.post", "{}", ttl_ms=1_000)

    assert first == intent_id_for(b"key", 8, 0)
    assert second == intent_id_for(b"key", 8, 1)
    assert ctx.tally().iterations == 2
    assert ctx.tally().tool_calls == 1


def test_the_tally_never_reaches_the_persisted_blobs() -> None:
    # Scenario: The tally never reaches keyed state. It is worker-local
    # measurement; persisting it would put a wall-clock reading into a blob the
    # retry-determinism gate compares byte for byte.
    clean = _activation_context()
    counted = _activation_context()
    counted.accumulate_usage(TokenUsage(prompt_tokens=9, completion_tokens=9, total_tokens=18))
    counted.tally().llm_ms.append(41)

    assert counted.memory_blob().SerializeToString(
        deterministic=True
    ) == clean.memory_blob().SerializeToString(deterministic=True)
    assert counted.cache_blob().SerializeToString(
        deterministic=True
    ) == clean.cache_blob().SerializeToString(deterministic=True)
    continuation = counted.build_continuation(snapshot=b"s", adapter="a", deadline_ms=9)
    assert continuation.SerializeToString(deterministic=True) == clean.build_continuation(
        snapshot=b"s", adapter="a", deadline_ms=9
    ).SerializeToString(deterministic=True)


async def test_agent_context_counts_an_inline_tool_call() -> None:
    # Inline read-only tools are the only tools that execute in the pipeline;
    # side-effecting ones are counted by `intents_emitted` instead, because they
    # never run here.
    @tool
    def lookup(customer_id: str) -> str:
        return customer_id.upper()

    registry = ToolRegistry()
    registry.register(lookup)
    ctx = make_context(tool_registry=registry)

    await ctx.run_tool("lookup", {"customer_id": "abc"})
    await ctx.run_tool("lookup", {"customer_id": "def"})

    result = ctx.drain()
    assert result.tally.tool_calls == 2


async def test_a_refused_side_effect_tool_is_not_counted_as_a_tool_call() -> None:
    calls: list[object] = []
    registry = ToolRegistry()
    _register_side_effect_tool(registry, calls)
    ctx = make_context(tool_registry=registry)

    with pytest.raises(SideEffectToolError):
        await ctx.run_tool("charge_card", {"amount": 5})

    assert ctx.drain().tally.tool_calls == 0


def test_agent_context_usage_accumulates_across_calls() -> None:
    # Two provider-reached calls in one activation report usage twice; the tally
    # is the activation's total, not its last call's.
    ctx = make_context()

    ctx.accumulate_usage(TokenUsage(prompt_tokens=1, completion_tokens=2, total_tokens=3))
    ctx.accumulate_usage(TokenUsage(prompt_tokens=4, completion_tokens=6, total_tokens=10))

    result = ctx.drain()
    assert result.tally.total_tokens == 13
    assert result.tally.usage_observed is True


async def test_agent_result_carries_the_tally() -> None:
    calls: list[object] = []
    registry = ToolRegistry()
    _register_side_effect_tool(registry, calls)
    fake = FakeLLM([(match_any(), respond_with(b"hi"))])
    ctx = make_context(provider=fake, tool_registry=registry)

    await ctx.model.complete(
        LlmRequest(model_id="m-1", messages=[], tools_schema=[], sampling_params={}),
        entity_key=ctx.entity_key,
        seq=ctx.seq,
        step_index=0,
    )
    ctx.act("charge_card", {"amount": 5})

    result = ctx.drain()
    n = len(b"hi")
    assert result.tally.total_tokens == 2 * n
    assert result.tally.usage_observed is True
    # One staged intent, so one step consumed.
    assert result.tally.iterations == 1
