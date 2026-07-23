"""The `StreamAgent` authoring protocol and the `FunctionAgent` wrapper.

See :mod:`beam_agents.core` for the capability overview and the change design
(``openspec/changes/add-agent-context/design.md``, decision D5) for why
`StreamAgent` is a structural, `runtime_checkable` `Protocol` rather than an
ABC: adapters (langgraph, adk, pydantic_ai) and hand-written agents must
satisfy one contract without inheriting a shared base class.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

from beam_agents.core.context import AgentContext


@runtime_checkable
class StreamAgent(Protocol):
    """The agent-authoring runtime contract: one async entry point.

    An activation performs all of its work through `ctx` — reading and
    writing memory, calling the model, running read-only tools, staging side
    effects via `ctx.act(...)`, and emitting outputs via `ctx.emit(...)`.
    This is a runtime contract only; it defines no prompt templating or
    orchestration DSL.
    """

    async def activate(self, ctx: AgentContext) -> None: ...


class FunctionAgent:
    """Adapts a plain `async def fn(ctx) -> None` into a `StreamAgent`."""

    def __init__(self, fn: Callable[[AgentContext], Awaitable[None]]) -> None:
        self._fn = fn

    async def activate(self, ctx: AgentContext) -> None:
        await self._fn(ctx)
