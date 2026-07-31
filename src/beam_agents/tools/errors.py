"""Error taxonomy for the tool-registry capability.

See the change design (``openspec/changes/add-tool-registry/design.md``,
decision 5) for the split: `ToolDefinitionError` is a construction-time
misconfiguration (bad signature); `ToolNotFoundError` and `ToolArgumentError`
are runtime lookup/validation failures; `SideEffectToolError` enforces
correctness invariant 5 (a `side_effect=True` tool must never execute inside
the pipeline).
"""

from __future__ import annotations

__all__ = [
    "SideEffectToolError",
    "ToolArgumentError",
    "ToolDefinitionError",
    "ToolError",
    "ToolNotFoundError",
]


class ToolError(Exception):
    """Base class for every error raised by the tool-registry capability."""


class ToolDefinitionError(ToolError):
    """Raised at `@tool` decoration time when a callable can't yield a sound schema."""


class ToolNotFoundError(ToolError):
    """Raised when a `ToolRegistry` is asked to resolve an unregistered tool name."""

    def __init__(self, name: str) -> None:
        super().__init__(f"no tool registered under name {name!r}")
        self.name = name


class ToolArgumentError(ToolError):
    """Raised when arguments passed to a tool fail validation against its argument model."""

    def __init__(self, name: str, reason: str) -> None:
        super().__init__(f"invalid arguments for tool {name!r}: {reason}")
        self.name = name
        self.reason = reason


class SideEffectToolError(ToolError):
    """Raised when a `side_effect=True` tool is invoked directly instead of via intents.

    Direct execution of a side-effecting tool would violate correctness
    invariant 5 (external writes never execute inside the pipeline); such
    tools may only be requested through `ctx.act(...)`.
    """

    def __init__(self, name: str) -> None:
        super().__init__(
            f"tool {name!r} is side_effect=True and cannot be invoked directly; "
            "request it through ctx.act(...) instead"
        )
        self.name = name
