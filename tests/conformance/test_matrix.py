"""The matrix meta-test: the cell inventory cannot silently shrink.

Expected cells are computed from the registry x the scenario list x the
per-scenario leg declarations (a declared skip is still a cell); actual cells
are what this session's collection hook recorded, before any ``-m``
deselection. A refactor that de-parameterizes a scenario, drops an adapter, or
loses a leg module therefore fails here with the exact difference.
"""

from __future__ import annotations

import pytest

from tests.conformance._registry import ADAPTERS
from tests.conformance._spec import LEGS, SCENARIOS
from tests.conformance.conftest import collected_cells

pytestmark = pytest.mark.semantics


def _expected_cells() -> set[tuple[str, str, str]]:
    return {
        (adapter.name, scenario.name, leg)
        for adapter in ADAPTERS
        for scenario in SCENARIOS
        for leg in LEGS
    }


def test_every_expected_cell_was_collected(request: pytest.FixtureRequest) -> None:
    collected = collected_cells(request.config)
    if not collected:
        pytest.fail(
            "no conformance cells were collected in this session — the meta-test "
            "audits the whole matrix, so run it as `pytest tests/conformance` "
            "(collecting only test_matrix.py cannot see the cells)"
        )
    expected = _expected_cells()
    missing = expected - collected
    unexpected = collected - expected
    assert not missing and not unexpected, (
        f"conformance matrix drifted from registry x scenario x leg: "
        f"missing cells {sorted(missing)!r}; unexpected cells {sorted(unexpected)!r}"
    )
    assert len(expected) == len(ADAPTERS) * len(SCENARIOS) * len(LEGS)
