"""Intent-identity injection for the effector-execution capability.

Covers the "Intent identity is injected into tools that declare it"
requirement (change ``add-intent-info-for-tools``): `_execute_intent` builds an
`IntentInfo` from the executing `ToolIntent`'s wire fields and passes it as the
keyword argument ``intent`` iff the tool's ``accepts_intent`` is true.
"""

from __future__ import annotations

import asyncio
import json

from beam_agents._protos import ToolIntent, ToolResult
from beam_agents.effector.runner import EffectorToolRunner, _execute_intent
from beam_agents.tools import IntentInfo, ToolRegistry, tool

NOW_MS = 1_700_000_000_000


def an_intent(
    tool_name: str = "charge",
    args_json: str = '{"key":"k-1"}',
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
        "attempt": 2,
        "kind": ToolIntent.TOOL,
    }
    fields.update(overrides)
    return ToolIntent(**fields)  # type: ignore[arg-type]


def a_registry(*tools: object) -> ToolRegistry:
    registry = ToolRegistry()
    for t in tools:
        registry.register(t)  # type: ignore[arg-type]
    return registry


a_runner = EffectorToolRunner(tool_timeout_ms=1_000)


async def test_a_declaring_tool_receives_the_executing_intents_identity() -> None:
    # Scenario: A declaring tool receives the executing intent's identity.
    received: list[IntentInfo] = []

    @tool(side_effect=True)
    def charge(key: str, *, intent: IntentInfo) -> str:
        received.append(intent)
        return f"receipt-{key}"

    intent = an_intent()
    result = await _execute_intent(intent, a_registry(charge), a_runner, now_ms=NOW_MS)

    assert result.status == ToolResult.OK
    assert json.loads(result.payload) == "receipt-k-1"
    assert received == [
        IntentInfo(
            intent_id="intent-1",
            entity_key=b"customer-7",
            seq=3,
            step_index=1,
            attempt=2,
        )
    ]


async def test_a_non_declaring_tool_is_invoked_unchanged() -> None:
    # Scenario: A non-declaring tool is invoked unchanged — no `intent`
    # keyword arrives (its signature could not accept one).
    calls: list[str] = []

    @tool(side_effect=True)
    def charge(key: str) -> str:
        calls.append(key)
        return f"receipt-{key}"

    result = await _execute_intent(an_intent(), a_registry(charge), a_runner, now_ms=NOW_MS)

    assert result.status == ToolResult.OK
    assert calls == ["k-1"]


async def test_injection_does_not_alter_argument_validation() -> None:
    # Scenario: Injection does not alter argument validation — valid args
    # validate without an `intent` value present; invalid args are still
    # REJECTED before the callable runs.
    calls: list[str] = []

    @tool(side_effect=True)
    def charge(key: str, *, intent: IntentInfo) -> str:
        calls.append(key)
        return "receipt"

    ok = await _execute_intent(an_intent(), a_registry(charge), a_runner, now_ms=NOW_MS)
    assert ok.status == ToolResult.OK

    bad = await _execute_intent(
        an_intent(args_json='{"wrong":"field"}'), a_registry(charge), a_runner, now_ms=NOW_MS
    )
    assert bad.status == ToolResult.REJECTED
    assert calls == ["k-1"]


async def test_an_intent_key_inside_args_json_is_rejected_not_shadowed() -> None:
    # Scenario: An intent key inside args_json is rejected, not shadowed.
    calls: list[object] = []

    @tool(side_effect=True)
    def charge(key: str, *, intent: IntentInfo) -> str:
        calls.append(intent)
        return "receipt"

    result = await _execute_intent(
        an_intent(args_json='{"key":"k-1","intent":{"intent_id":"spoof"}}'),
        a_registry(charge),
        a_runner,
        now_ms=NOW_MS,
    )

    assert result.status == ToolResult.REJECTED
    assert calls == []


async def test_a_re_executed_intent_carries_identical_identity() -> None:
    # Scenario: A re-executed intent carries identical identity — the dedup
    # store permitting a second invocation (lease expiry, duplicate delivery),
    # the injected identity is byte-identical, so an intent-keyed downstream
    # performs one effective effect.
    received: list[IntentInfo] = []

    @tool(side_effect=True)
    def charge(key: str, *, intent: IntentInfo) -> str:
        received.append(intent)
        return "receipt"

    registry = a_registry(charge)
    await _execute_intent(an_intent(), registry, a_runner, now_ms=NOW_MS)
    await _execute_intent(an_intent(), registry, a_runner, now_ms=NOW_MS + 5_000)

    assert len(received) == 2
    assert received[0] == received[1]
    assert received[0].intent_id == "intent-1"


async def test_an_async_declaring_tool_is_injected_identically() -> None:
    # Scenario: Async declaring tools are injected identically.
    received: list[IntentInfo] = []

    @tool(side_effect=True)
    async def charge(key: str, *, intent: IntentInfo) -> str:
        await asyncio.sleep(0)
        received.append(intent)
        return f"receipt-{key}"

    result = await _execute_intent(an_intent(), a_registry(charge), a_runner, now_ms=NOW_MS)

    assert result.status == ToolResult.OK
    assert json.loads(result.payload) == "receipt-k-1"
    assert [i.intent_id for i in received] == ["intent-1"]
