"""Tests for the `agent-context` capability's `StreamAgent` protocol and
`FunctionAgent` wrapper.

Covers: a class implementing `activate` satisfies `StreamAgent` without a
base class, the activation drives all work through the context, and
`FunctionAgent` adapts a plain async function into a `StreamAgent`.
"""

from __future__ import annotations

from beam_agents.core.agent import FunctionAgent, StreamAgent
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
