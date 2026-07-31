"""Inline execution for read-only tools on the fast path.

See the change design (``openspec/changes/archive/2026-07-23-add-tool-registry/design.md``)
for the two-layer side-effect guard: `Tool.__call__` and `ToolRunner.run` both
refuse a `side_effect=True` tool before the underlying callable ever runs,
enforcing correctness invariant 5.

See the change design (``openspec/changes/fix-async-tool-execution/design.md``)
for why `run` is async and awaits the call result when it is awaitable rather
than tagging `Tool` with an `is_async` flag: this handles `async def` tools
uniformly with any sync tool that happens to return an awaitable.
"""

from __future__ import annotations

import inspect
from collections.abc import Mapping

from pydantic import ValidationError

from beam_agents.tools.errors import SideEffectToolError, ToolArgumentError
from beam_agents.tools.registry import Tool

__all__ = [
    "ToolRunner",
]


class ToolRunner:
    """Validates arguments against a tool's Pydantic model, then invokes it.

    `run` is async so both sync and async tools are called the same way: the
    result is awaited when it's awaitable, and returned directly otherwise.
    """

    async def run(self, t: Tool, arguments: Mapping[str, object]) -> object:
        """Validate ``arguments`` against the tool's model and run it inline.

        Raises :class:`SideEffectToolError` for a ``side_effect`` tool
        (invariant 5) and :class:`ToolArgumentError` when the arguments do
        not validate, so a malformed model-produced call becomes a typed
        error rather than an exception from inside user code.
        """
        if t.side_effect:
            raise SideEffectToolError(t.name)
        try:
            validated = t.argument_model.model_validate(dict(arguments))
        except ValidationError as exc:
            raise ToolArgumentError(t.name, str(exc)) from exc
        result = t(**validated.model_dump())
        if inspect.isawaitable(result):
            return await result
        return result
