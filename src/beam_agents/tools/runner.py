"""Inline execution for read-only tools on the fast path.

See the change design (``openspec/changes/add-tool-registry/design.md``) for
the two-layer side-effect guard: `Tool.__call__` and `ToolRunner.run` both
refuse a `side_effect=True` tool before the underlying callable ever runs,
enforcing correctness invariant 5.
"""

from __future__ import annotations

from collections.abc import Mapping

from pydantic import ValidationError

from beam_agents.tools.errors import SideEffectToolError, ToolArgumentError
from beam_agents.tools.registry import Tool


class ToolRunner:
    """Validates arguments against a tool's Pydantic model, then invokes it."""

    def run(self, t: Tool, arguments: Mapping[str, object]) -> object:
        if t.side_effect:
            raise SideEffectToolError(t.name)
        try:
            validated = t.argument_model.model_validate(dict(arguments))
        except ValidationError as exc:
            raise ToolArgumentError(t.name, str(exc)) from exc
        return t(**validated.model_dump())
