"""Tool declaration and inline execution: the `@tool` decorator, Pydantic-derived
schema generation, the `ToolRegistry`, and the fast-path `ToolRunner`.

Correctness invariant 5 requires that a `side_effect=True` tool never execute
inside the pipeline. `Tool.__call__` and `ToolRunner.run` both enforce this by
raising `SideEffectToolError`; side-effecting tools are requested only through
`ctx.act(...)`, implemented by a later change's intents path.

Importing this package has no side effects.
"""

from beam_agents.tools.errors import (
    SideEffectToolError,
    ToolArgumentError,
    ToolDefinitionError,
    ToolError,
    ToolNotFoundError,
)
from beam_agents.tools.registry import Tool, ToolRegistry, tool
from beam_agents.tools.runner import ToolRunner

__all__ = [
    "SideEffectToolError",
    "Tool",
    "ToolArgumentError",
    "ToolDefinitionError",
    "ToolError",
    "ToolNotFoundError",
    "ToolRegistry",
    "ToolRunner",
    "tool",
]
