"""`BeamToolset`: runtime `@tool` objects presented to Pydantic AI.

Accepts runtime :class:`~beam_agents.tools.registry.Tool` objects (the `@tool`
decorator kind) and maps each to the framework's tool-kind vocabulary:

- ``side_effect=False`` tools are ordinary ``function`` tools whose executor
  routes through ``ActivationContext.run_tool`` — validated arguments,
  ``SideEffectToolError`` protection, the tally count, and a ``TOOL_CALL``
  trace event, all on the one runtime path (change design D4).
- ``side_effect=True`` tools are declared ``external`` (deferred): their
  schema is visible to the model, their callables never execute in-pipeline,
  and a run that reaches such a call ends with a ``DeferredToolRequests``
  output the adapter maps to intents (design D3). Even a mis-wired toolset
  cannot execute an effect: ``run_tool`` refuses ``side_effect=True`` tools
  with ``SideEffectToolError`` before execution.
- names listed in ``approval_required`` are declared ``unapproved``
  (approval-gated): their calls surface as approval requests, map to
  ``ctx.request_approval``, and — once the re-injected ``Approval`` says so —
  execute through the same ``run_tool`` path. Only ``side_effect=False``
  tools may be approval-gated: a side-effecting tool's sanctioned path is
  always the intent/effector one.

Executors reach the activation via the shared adapter contextvar (the same
mechanism the transport hook uses) — race-free under per-key serialization
and the single bridge loop, and independent of the user's ``deps_type``
(design D4's context-carrier decision).
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Any

from pydantic_ai.tools import ToolDefinition
from pydantic_ai.toolsets import AbstractToolset
from pydantic_ai.toolsets.abstract import ToolsetTool
from pydantic_core import SchemaValidator, core_schema

from beam_agents.adapters._transport import _current_activation
from beam_agents.tools.errors import ToolError
from beam_agents.tools.registry import Tool

if TYPE_CHECKING:
    from collections.abc import Collection, Sequence

    from pydantic_ai._run_context import RunContext

__all__ = [
    "BeamToolset",
]

# Args arrive schema-validated by the model-side JSON schema; the real
# validation happens in the runtime path (`ToolRunner` against the tool's
# pydantic argument model), so the toolset-level validator is pass-through —
# the same posture as the framework's own ExternalToolset.
_ANY_VALIDATOR = SchemaValidator(schema=core_schema.any_schema())


class BeamToolset(AbstractToolset[Any]):
    """Runtime tools for a Pydantic AI agent: read-only inline via the runtime
    tool path, side effects deferred to intents, approvals gated."""

    def __init__(
        self,
        tools: Sequence[Tool],
        *,
        approval_required: Collection[str] = (),
        id: str | None = None,
    ) -> None:
        for candidate in tools:
            if not isinstance(candidate, Tool):
                raise ToolError(
                    f"BeamToolset requires runtime Tool objects (@tool-decorated); "
                    f"got {type(candidate).__name__!r}"
                )
        self._tools = {t.name: t for t in tools}
        for name in approval_required:
            gated = self._tools.get(name)
            if gated is None:
                raise ToolError(f"approval_required names unknown tool {name!r}")
            if gated.side_effect:
                raise ToolError(
                    f"tool {name!r} is side_effect=True; side-effecting tools always "
                    "defer to the intent/effector path and cannot be approval-gated "
                    "in-pipeline"
                )
        self._approval_required = frozenset(approval_required)
        self._id = id

    @property
    def id(self) -> str | None:
        """The toolset id Pydantic AI uses to namespace these tools."""
        return self._id

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        """Expose every registered beam-agents tool as a Pydantic AI tool."""
        return {
            tool.name: ToolsetTool(
                toolset=self,
                tool_def=self._tool_def(tool),
                max_retries=0,
                args_validator=_ANY_VALIDATOR,
            )
            for tool in self._tools.values()
        }

    async def call_tool(
        self, name: str, tool_args: dict[str, Any], ctx: RunContext[Any], tool: ToolsetTool[Any]
    ) -> Any:
        """Run one tool call through the current activation.

        Raises ``ToolError`` when called outside an activation — the tool
        needs the activation to stage intents and record traces. A
        ``side_effect`` tool is staged rather than executed (invariant 5).
        """
        activation = _current_activation.get()
        if activation is None:
            raise ToolError(
                f"tool {name!r} was called outside a beam-agents activation; "
                "BeamToolset tools only execute inside PydanticAIAgent runs"
            )
        return await activation.run_tool(name, tool_args)

    def _tool_def(self, tool: Tool) -> ToolDefinition:
        if tool.side_effect:
            kind = "external"
        elif tool.name in self._approval_required:
            kind = "unapproved"
        else:
            kind = "function"
        definition = ToolDefinition(
            name=tool.name,
            description=tool.description or None,
            parameters_json_schema=tool.argument_model.model_json_schema(),
        )
        # `kind` is set via replace: ToolDefinition validates the literal.
        return replace(definition, kind=kind)  # type: ignore[arg-type]
