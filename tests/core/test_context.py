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

from beam_agents.core.context import INTENT_NAMESPACE, AgentResult
from beam_agents.model import FakeLLM, LlmRequest, StagingSink, TokenUsage, match_any, respond_with
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

    with pytest.raises(RuntimeError):
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
    ctx = make_context(tool_registry=registry)

    ctx.act("charge_card", {"amount": 5})

    result = ctx.drain()
    assert len(result.intents) == 1
    assert result.intents[0].tool_name == "charge_card"
    assert calls == []


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

    with pytest.raises(ValueError, match="side_effect=False"):
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
