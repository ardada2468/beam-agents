"""Tool execution for the effector-execution capability.

Covers "EffectorToolRunner is the only sanctioned executor of side-effecting
tools", "Arguments are validated before the callable runs", "Tools are resolved
from the shared ToolRegistry", and "Every intent maps to exactly one terminal
ToolResult status".
"""

from __future__ import annotations

import asyncio
import json
import threading

import pytest

from beam_agents._protos import ToolIntent, ToolResult
from beam_agents.effector.runner import EffectorToolRunner, ReadOnlyToolError, execute_intent
from beam_agents.tools import SideEffectToolError, ToolArgumentError, ToolRegistry, tool

NOW_MS = 1_700_000_000_000


def an_intent(
    tool_name: str = "charge",
    args_json: str = '{"amount_cents":100}',
    **overrides: object,
) -> ToolIntent:
    fields: dict[str, object] = {
        "intent_id": "intent-1",
        "entity_key": b"customer-7",
        "seq": 3,
        "step_index": 1,
        "tool_name": tool_name,
        "args_json": args_json,
        "created_at_ms": NOW_MS - 1_000,
        "expires_at_ms": NOW_MS + 60_000,
        "kind": ToolIntent.TOOL,
    }
    fields.update(overrides)
    return ToolIntent(**fields)  # type: ignore[arg-type]


def a_registry(*tools: object) -> ToolRegistry:
    registry = ToolRegistry()
    for t in tools:
        registry.register(t)  # type: ignore[arg-type]
    return registry


# -- EffectorToolRunner --------------------------------------------------------


async def test_a_side_effecting_tool_executes_through_the_effector_runner() -> None:
    # Scenario: A side-effecting tool executes through the effector runner.
    calls: list[int] = []

    @tool(side_effect=True)
    def charge(amount_cents: int) -> str:
        calls.append(amount_cents)
        return "receipt-1"

    result = await EffectorToolRunner(tool_timeout_ms=1_000).run(charge, {"amount_cents": 100})

    assert result == "receipt-1"
    assert calls == [100]


async def test_a_read_only_tool_is_refused_by_the_effector_runner() -> None:
    # Scenario: A read-only tool is refused by the effector runner.
    calls: list[object] = []

    @tool
    def lookup(customer_id: str) -> str:
        calls.append(customer_id)
        return customer_id

    with pytest.raises(ReadOnlyToolError, match="lookup"):
        await EffectorToolRunner(tool_timeout_ms=1_000).run(lookup, {"customer_id": "c-1"})

    assert calls == []


async def test_an_async_side_effecting_tool_is_awaited() -> None:
    # Scenario: An async side-effecting tool is awaited.
    @tool(side_effect=True)
    async def charge(amount_cents: int) -> str:
        await asyncio.sleep(0)
        return f"receipt-{amount_cents}"

    result = await EffectorToolRunner(tool_timeout_ms=1_000).run(charge, {"amount_cents": 100})

    assert result == "receipt-100"
    assert not asyncio.iscoroutine(result)


async def test_invalid_arguments_are_rejected_before_invocation() -> None:
    # Scenario: Invalid arguments are rejected before invocation.
    calls: list[object] = []

    @tool(side_effect=True)
    def charge(amount_cents: int) -> None:
        calls.append(amount_cents)

    with pytest.raises(ToolArgumentError, match="charge"):
        await EffectorToolRunner(tool_timeout_ms=1_000).run(charge, {"wrong_field": 1})

    assert calls == []


async def test_valid_arguments_are_coerced_and_passed_through() -> None:
    # Scenario: Valid arguments are coerced and passed through.
    calls: list[tuple[int, str]] = []

    @tool(side_effect=True)
    def charge(amount_cents: int, currency: str = "usd") -> None:
        calls.append((amount_cents, currency))

    # "100" is coerced to int by the tool's Pydantic model, as in-pipeline.
    await EffectorToolRunner(tool_timeout_ms=1_000).run(charge, {"amount_cents": "100"})

    assert calls == [(100, "usd")]


async def test_a_sync_tool_does_not_block_the_event_loop() -> None:
    # A blocking side-effecting tool (an HTTP call, a DB write) must not stall
    # every other partition's task: sync callables run off-loop.
    seen: list[int] = []

    @tool(side_effect=True)
    def charge(amount_cents: int) -> str:
        seen.append(threading.get_ident())
        return "ok"

    await EffectorToolRunner(tool_timeout_ms=1_000).run(charge, {"amount_cents": 1})

    assert seen and seen[0] != threading.get_ident()


# -- status mapping ------------------------------------------------------------


async def test_a_successful_tool_call_publishes_ok_with_its_encoded_return_value() -> None:
    # Scenario: A successful tool call publishes OK with its encoded return value.
    @tool(side_effect=True)
    def charge(amount_cents: int) -> dict[str, object]:
        return {"receipt": "r-1", "cents": amount_cents}

    result = await execute_intent(
        an_intent(), a_registry(charge), EffectorToolRunner(tool_timeout_ms=1_000), now_ms=NOW_MS
    )

    assert result.status == ToolResult.OK
    assert json.loads(result.payload) == {"receipt": "r-1", "cents": 100}
    assert result.error_message == ""


async def test_a_raising_tool_publishes_error_with_its_message() -> None:
    # Scenario: A raising tool publishes ERROR with its message.
    calls: list[object] = []

    @tool(side_effect=True)
    def charge(amount_cents: int) -> None:
        calls.append(amount_cents)
        raise RuntimeError("card declined")

    result = await execute_intent(
        an_intent(), a_registry(charge), EffectorToolRunner(tool_timeout_ms=1_000), now_ms=NOW_MS
    )

    assert result.status == ToolResult.ERROR
    assert "card declined" in result.error_message
    assert calls == [100], "a failed tool must not be retried within its claim"


async def test_a_tool_exceeding_its_timeout_publishes_error_not_rejected() -> None:
    # Scenario: A tool exceeding its timeout publishes ERROR, not REJECTED.
    @tool(side_effect=True)
    async def charge(amount_cents: int) -> None:
        await asyncio.sleep(5)

    result = await execute_intent(
        an_intent(), a_registry(charge), EffectorToolRunner(tool_timeout_ms=10), now_ms=NOW_MS
    )

    assert result.status == ToolResult.ERROR
    assert "timed out" in result.error_message


async def test_an_unknown_tool_name_is_rejected() -> None:
    # Scenario: An unknown tool name is rejected without stalling the partition
    # (the no-stall half is asserted in the service tests).
    result = await execute_intent(
        an_intent(tool_name="nope"),
        a_registry(),
        EffectorToolRunner(tool_timeout_ms=1_000),
        now_ms=NOW_MS,
    )

    assert result.status == ToolResult.REJECTED
    assert "nope" in result.error_message


async def test_a_read_only_tool_on_the_outbox_is_rejected() -> None:
    # A side_effect=False tool reaching the outbox means the pipeline failed to
    # run it inline: a bug to surface, not an effect to perform.
    calls: list[object] = []

    @tool
    def charge(amount_cents: int) -> None:
        calls.append(amount_cents)

    result = await execute_intent(
        an_intent(), a_registry(charge), EffectorToolRunner(tool_timeout_ms=1_000), now_ms=NOW_MS
    )

    assert result.status == ToolResult.REJECTED
    assert calls == []


async def test_malformed_args_json_is_rejected_before_invocation() -> None:
    # Scenario: Invalid arguments are rejected before invocation (the
    # malformed-JSON half).
    calls: list[object] = []

    @tool(side_effect=True)
    def charge(amount_cents: int) -> None:
        calls.append(amount_cents)

    result = await execute_intent(
        an_intent(args_json="{not json"),
        a_registry(charge),
        EffectorToolRunner(tool_timeout_ms=1_000),
        now_ms=NOW_MS,
    )

    assert result.status == ToolResult.REJECTED
    assert calls == []


async def test_args_json_that_is_not_an_object_is_rejected() -> None:
    @tool(side_effect=True)
    def charge(amount_cents: int) -> None: ...

    result = await execute_intent(
        an_intent(args_json="[1, 2]"),
        a_registry(charge),
        EffectorToolRunner(tool_timeout_ms=1_000),
        now_ms=NOW_MS,
    )

    assert result.status == ToolResult.REJECTED


async def test_a_result_that_cannot_be_encoded_is_an_error_not_a_lost_intent() -> None:
    # Scenario: A result that cannot be encoded is an ERROR, not a lost intent.
    @tool(side_effect=True)
    def charge(amount_cents: int) -> object:
        return object()

    result = await execute_intent(
        an_intent(), a_registry(charge), EffectorToolRunner(tool_timeout_ms=1_000), now_ms=NOW_MS
    )

    assert result.status == ToolResult.ERROR
    assert result.intent_id == "intent-1"


async def test_every_result_correlates_with_its_intent() -> None:
    # Scenario: Every result correlates with its intent.
    @tool(side_effect=True)
    def charge(amount_cents: int) -> str:
        return "ok"

    intent = an_intent()
    result = await execute_intent(
        intent, a_registry(charge), EffectorToolRunner(tool_timeout_ms=1_000), now_ms=NOW_MS
    )

    assert result.intent_id == intent.intent_id
    assert result.entity_key == intent.entity_key
    assert result.seq == intent.seq
    assert result.completed_at_ms == NOW_MS


async def test_a_none_returning_tool_publishes_ok_with_a_null_payload() -> None:
    # The common shape for a side effect: it did the thing and returns nothing.
    @tool(side_effect=True)
    def charge(amount_cents: int) -> None:
        return None

    result = await execute_intent(
        an_intent(), a_registry(charge), EffectorToolRunner(tool_timeout_ms=1_000), now_ms=NOW_MS
    )

    assert result.status == ToolResult.OK
    assert json.loads(result.payload) is None


async def test_the_in_pipeline_guard_still_refuses_the_same_tool() -> None:
    # Scenario: The in-pipeline guard is unchanged (tool-registry delta).
    @tool(side_effect=True)
    def charge(amount_cents: int) -> None: ...

    with pytest.raises(SideEffectToolError):
        charge(amount_cents=1)


async def test_a_sync_tool_returning_an_awaitable_is_awaited() -> None:
    # A sync factory that hands back a coroutine is a real shape (a client whose
    # `.send()` is sync but returns an awaitable); the runner must not publish a
    # coroutine object as the result.
    @tool(side_effect=True)
    def charge(amount_cents: int) -> object:
        async def _later() -> str:
            return f"receipt-{amount_cents}"

        return _later()

    result = await EffectorToolRunner(tool_timeout_ms=1_000).run(charge, {"amount_cents": 100})

    assert result == "receipt-100"
