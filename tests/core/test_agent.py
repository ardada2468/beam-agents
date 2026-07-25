"""Tests for the `agent-context` capability's `StreamAgent` protocol and
`FunctionAgent` wrapper.

Covers: a class implementing `activate` satisfies `StreamAgent` without a
base class, the activation drives all work through the context, and
`FunctionAgent` adapts a plain async function into a `StreamAgent`.
"""

from __future__ import annotations

import uuid

from beam_agents.core.agent import (
    _INTENT_NAMESPACE,
    FunctionAgent,
    StreamAgent,
    intent_id_for,
)
from beam_agents.core.context import AgentContext

from ._context_helpers import make_context


class _Echo:
    async def activate(self, ctx: AgentContext) -> None:
        ctx.emit("echoed")


# --- Requirement: StreamAgent authoring protocol -----------------------------


def test_a_class_implementing_activate_satisfies_the_protocol() -> None:
    # Scenario: A class implementing activate satisfies the protocol.
    agent = _Echo()
    assert isinstance(agent, StreamAgent)


async def test_the_activation_drives_all_work_through_the_context() -> None:
    # Scenario: The activation drives all work through the context.
    ctx = make_context()
    agent = _Echo()

    await agent.activate(ctx)

    result = ctx.drain()
    assert result.outputs == ("echoed",)


# --- Requirement: FunctionAgent wraps a plain async function ----------------


async def test_a_function_is_adapted_into_a_streamagent() -> None:
    # Scenario: A function is adapted into a StreamAgent.
    calls: list[AgentContext] = []

    async def fn(ctx: AgentContext) -> None:
        calls.append(ctx)
        ctx.emit("from-function")

    wrapped = FunctionAgent(fn)
    assert isinstance(wrapped, StreamAgent)

    ctx = make_context()
    await wrapped.activate(ctx)

    assert calls == [ctx]
    result = ctx.drain()
    assert result.outputs == ("from-function",)


def test_intent_id_is_deterministic_and_sensitive_to_every_input() -> None:
    expected = uuid.uuid5(_INTENT_NAMESPACE, "74656e616e742d37|9|3")

    assert uuid.UUID(intent_id_for(b"tenant-7", 9, 3)) == expected
    assert intent_id_for(b"tenant-8", 9, 3) != str(expected)
    assert intent_id_for(b"tenant-7", 10, 3) != str(expected)
    assert intent_id_for(b"tenant-7", 9, 4) != str(expected)
