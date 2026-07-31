"""Spec: adk-adapter / Requirement: Side-effect tools suspend via long-running
function calls — the shim's own contract (`beam_tools`).
"""

from __future__ import annotations

from typing import cast

import pytest

pytest.importorskip("google.adk")

from google.adk.tools.tool_context import ToolContext

from beam_agents.adapters.adk.tools import (
    _APPROVAL_TOOL_NAME,
    BeamApprovalTool,
    BeamFunctionTool,
    BeamLongRunningTool,
    beam_tools,
)
from beam_agents.tools.errors import ToolError
from beam_agents.tools.registry import tool
from tests.conformance._spec import charge, lookup_a


def test_beam_tools_maps_each_tool_by_its_effect_class() -> None:
    shims = beam_tools([lookup_a, charge])

    assert isinstance(shims[0], BeamFunctionTool)
    assert isinstance(shims[1], BeamLongRunningTool)
    assert shims[0].is_long_running is False
    assert shims[1].is_long_running is True
    assert [s.name for s in shims] == ["lookup_a", "charge"]


def test_the_shim_declaration_carries_the_tools_schema() -> None:
    # ADK builds the model-facing declaration from the wrapped callable's
    # signature, so the shim must synthesize it from the runtime tool's
    # argument model — otherwise the model sees a no-argument tool.
    declaration = beam_tools([lookup_a])[0]._get_declaration()

    assert declaration is not None
    assert declaration.name == "lookup_a"
    parameters = declaration.parameters or declaration.parameters_json_schema
    assert "customer_id" in str(parameters)


def test_beam_tools_rejects_a_non_runtime_tool() -> None:
    def plain(customer_id: str) -> str:
        return customer_id

    with pytest.raises(ToolError, match="runtime Tool objects"):
        beam_tools([plain])  # type: ignore[list-item]


def test_the_shim_classes_reject_a_mismatched_effect_class() -> None:
    with pytest.raises(ToolError, match="side_effect=True"):
        BeamFunctionTool(charge)
    with pytest.raises(ToolError, match="side_effect=False"):
        BeamLongRunningTool(lookup_a)


def test_the_approval_shim_is_a_long_running_declaration() -> None:
    approval = BeamApprovalTool()

    assert approval.is_long_running is True
    assert approval.name == _APPROVAL_TOOL_NAME


async def test_a_shim_tool_outside_an_activation_fails_closed() -> None:
    # There is no sanctioned path for a side-effect call outside the runtime:
    # without a collector the shim must refuse, never silently drop the call.
    shim = BeamLongRunningTool(charge)
    with pytest.raises(ToolError, match="only run inside an AdkAgent activation"):
        await shim.run_async(args={"amount": "5"}, tool_context=_fake_tool_context())


async def test_a_read_only_shim_validates_its_arguments() -> None:
    @tool
    def typed(count: int) -> int:
        """Doubles its argument."""
        return count * 2

    shim = BeamFunctionTool(typed)
    # The argument model coerces the model's stringly-typed argument.
    assert await shim.run_async(args={"count": "3"}, tool_context=_fake_tool_context()) == 6


class _FakeToolContext:
    """The only surface the shims touch on ADK's ToolContext."""

    function_call_id = "adk-fake-call-id"


def _fake_tool_context() -> ToolContext:
    """The double, typed as the ADK context the shims are declared against."""
    return cast("ToolContext", _FakeToolContext())
