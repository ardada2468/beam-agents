"""Cell parameterization: one ``pytest.param`` per registered adapter, each
tagged with the ``conformance_cell(adapter, scenario, leg)`` marker the
inventory hook and meta-test count."""

from __future__ import annotations

import importlib.util

import pytest

from tests.conformance._registry import ADAPTERS, ADAPTERS_BY_NAME
from tests.conformance._spec import ScenarioSpec


def adapter_params(scenario: ScenarioSpec, leg: str) -> list[object]:
    """Parametrize a scenario's cell test over the registered adapter axis."""
    return [
        pytest.param(
            adapter.name,
            id=adapter.name,
            marks=pytest.mark.conformance_cell(adapter.name, scenario.name, leg),
        )
        for adapter in ADAPTERS
    ]


def require_framework(adapter_name: str) -> None:
    """Skip the cell cleanly when the adapter's optional framework is absent."""
    requires = ADAPTERS_BY_NAME[adapter_name].requires
    if requires is None:
        return
    try:
        spec = importlib.util.find_spec(requires)
    except ModuleNotFoundError:
        # A meta-path blocker (the import-isolation test pattern) raises from
        # find_spec instead of returning None; both mean "not installed".
        spec = None
    if spec is None:
        pytest.skip(f"optional framework {requires!r} is not installed")
