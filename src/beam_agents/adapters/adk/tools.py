"""The tool tagging shim: runtime `@tool` objects as ADK tools.

Accepts runtime :class:`~beam_agents.tools.registry.Tool` objects (the `@tool`
decorator kind) and maps each onto the ADK tool model per its effect class
(change design D4/D5):

- ``side_effect=False`` tools become plain function tools that execute inline
  with ``argument_model``-validated arguments — unchanged ADK semantics — and
  stage a ``TOOL_CALL`` trace event through the activation's tee.
- ``side_effect=True`` tools become **long-running** function tools that never
  execute in the pipeline: the model's call is recorded (validated arguments
  plus the ADK function-call id) in the per-activation collector, the adapter
  stages one ``ToolIntent`` per recorded call, and the activation suspends.
  Even a mis-wired agent cannot execute an effect in-pipeline — calling a
  ``side_effect=True`` tool directly raises ``SideEffectToolError`` (registry
  design D3); the collector is the sanctioned detour.

Approvals ride the same long-running mechanism through
:class:`BeamApprovalTool` — ADK has no ``interrupt()`` primitive, so a pending
approval call is what the adapter turns into ``ctx.request_approval`` and the
HITL timer.

Adoption is exactly "re-tag + wrap": replace ``tools=[charge]`` with
``tools=beam_tools([charge])`` where ``charge`` is now
``@tool(side_effect=True)``-decorated. No agent-tree changes.
"""

from __future__ import annotations

import inspect
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from google.adk.tools.function_tool import FunctionTool
from google.adk.tools.long_running_tool import LongRunningFunctionTool
from typing_extensions import override

from beam_agents.adapters.adk.events import stage_tool_call
from beam_agents.tools.errors import ToolError
from beam_agents.tools.registry import Tool

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from google.adk.tools.tool_context import ToolContext
    from pydantic import BaseModel

KIND_TOOL = "tool"
KIND_APPROVAL = "approval"

#: The ADK-facing name of the approval shim tool — the function-call name a
#: model uses to request human approval.
APPROVAL_TOOL_NAME = "request_approval"


@dataclass(frozen=True, slots=True)
class PendingCall:
    """One collected long-running call awaiting its intent."""

    kind: str
    tool_name: str
    args: dict[str, Any]
    function_call_id: str


class CallCollector:
    """The per-activation collector of pending long-running calls, keyed by
    ADK function-call id (ADK may execute parallel calls concurrently; the
    adapter re-derives a deterministic order from the event stream)."""

    def __init__(self) -> None:
        self.calls: dict[str, PendingCall] = {}

    def record(self, call: PendingCall) -> None:
        self.calls[call.function_call_id] = call


#: The collector of the activation currently driving the ADK run; set by
#: `AdkAgent` around `run_async` (same contextvar pattern as the transport).
_current_collector: ContextVar[CallCollector | None] = ContextVar(
    "beam_agents_adk_collector", default=None
)


def _collector() -> CallCollector:
    collector = _current_collector.get()
    if collector is None:
        raise ToolError(
            "beam-agents ADK shim tools only run inside an AdkAgent activation; "
            "side-effect calls have no sanctioned path outside the runtime"
        )
    return collector


def _stub_for(name: str, description: str, argument_model: type[BaseModel]) -> Callable[..., Any]:
    """A declaration-only callable: carries the tool's name, description, and a
    synthesized signature derived from the runtime tool's argument model, so
    ADK's declaration builder sees the real schema. Never invoked — the shim
    classes override ``run_async`` entirely."""

    def _stub(**_kwargs: Any) -> Any:
        raise ToolError(f"declaration stub for {name!r} must never be invoked")

    parameters = []
    annotations: dict[str, Any] = {}
    for field_name, field in argument_model.model_fields.items():
        annotation = field.annotation if field.annotation is not None else Any
        default = inspect.Parameter.empty if field.is_required() else field.default
        parameters.append(
            inspect.Parameter(
                field_name,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=default,
                annotation=annotation,
            )
        )
        annotations[field_name] = annotation
    _stub.__name__ = name
    _stub.__qualname__ = name
    _stub.__doc__ = description
    _stub.__signature__ = inspect.Signature(parameters)  # type: ignore[attr-defined]
    _stub.__annotations__ = annotations
    return _stub


class BeamFunctionTool(FunctionTool):
    """A read-only runtime tool as a plain ADK function tool: executes inline
    with validated arguments and stages a ``TOOL_CALL`` trace event."""

    def __init__(self, tool: Tool) -> None:
        if tool.side_effect:
            raise ToolError(
                f"tool {tool.name!r} is side_effect=True; wrap it via beam_tools() so it "
                "becomes a long-running declaration, never an inline execution"
            )
        super().__init__(_stub_for(tool.name, tool.description, tool.argument_model))
        self._beam_tool = tool

    @override
    async def run_async(self, *, args: dict[str, Any], tool_context: ToolContext) -> Any:
        validated = self._beam_tool.argument_model(**args).model_dump()
        value = self._beam_tool(**validated)
        if inspect.isawaitable(value):
            value = await value
        # Traced only after a successful execution, while the activation
        # contextvar is held — the seam the LangGraph BeamToolNode lacked
        # (design D7 closes that gap for this adapter).
        stage_tool_call(self._beam_tool.name)
        return value


class BeamLongRunningTool(LongRunningFunctionTool):
    """A side-effect runtime tool as an ADK long-running declaration: the call
    is collected for intent staging; the tool's callable never runs here."""

    def __init__(self, tool: Tool) -> None:
        if not tool.side_effect:
            raise ToolError(
                f"tool {tool.name!r} is side_effect=False; wrap it via beam_tools() so it "
                "executes inline instead of suspending the activation"
            )
        super().__init__(_stub_for(tool.name, tool.description, tool.argument_model))
        self._beam_tool = tool

    @override
    async def run_async(self, *, args: dict[str, Any], tool_context: ToolContext) -> Any:
        validated = self._beam_tool.argument_model(**args).model_dump()
        function_call_id = tool_context.function_call_id
        if not function_call_id:
            raise ToolError(
                f"long-running call to {self._beam_tool.name!r} carries no function-call id; "
                "the adapter cannot correlate its result"
            )
        _collector().record(
            PendingCall(KIND_TOOL, self._beam_tool.name, validated, function_call_id)
        )
        return None  # no function response: the call stays pending (suspension)


def _approval_stub(**_kwargs: Any) -> Any:
    """Request human approval for the arguments of this call. The decision is
    returned asynchronously as this call's function response, carrying
    ``approved``, ``approver``, and ``decided_at_ms``."""
    raise ToolError("declaration stub for the approval shim must never be invoked")


_approval_stub.__name__ = APPROVAL_TOOL_NAME
_approval_stub.__qualname__ = APPROVAL_TOOL_NAME
_approval_stub.__signature__ = inspect.Signature([])  # type: ignore[attr-defined]


class BeamApprovalTool(LongRunningFunctionTool):
    """The approval shim: a long-running tool whose pending call the adapter
    stages via ``ctx.request_approval`` (APPROVAL-kind intent on the runtime's
    approval channel) before suspending. Free-form arguments are forwarded as
    the approval request's payload."""

    def __init__(self) -> None:
        super().__init__(_approval_stub)

    @override
    async def run_async(self, *, args: dict[str, Any], tool_context: ToolContext) -> Any:
        function_call_id = tool_context.function_call_id
        if not function_call_id:
            raise ToolError(
                "approval request carries no function-call id; the adapter cannot "
                "correlate its decision"
            )
        _collector().record(
            PendingCall(KIND_APPROVAL, APPROVAL_TOOL_NAME, dict(args), function_call_id)
        )
        return None


def beam_tools(tools: Sequence[Tool]) -> list[FunctionTool]:
    """Map runtime ``Tool`` objects to ADK tools by effect class."""
    shims: list[FunctionTool] = []
    for candidate in tools:
        if not isinstance(candidate, Tool):
            raise ToolError(
                f"beam_tools requires runtime Tool objects (@tool-decorated); "
                f"got {type(candidate).__name__!r}"
            )
        shims.append(
            BeamLongRunningTool(candidate) if candidate.side_effect else BeamFunctionTool(candidate)
        )
    return shims
