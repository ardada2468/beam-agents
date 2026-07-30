"""Conformance-suite wiring: the registry guard and the cell inventory.

The guard runs at conftest import, so collecting ``tests/conformance`` with an
importable-but-unregistered adapter package fails collection outright (never a
skip, never a smaller matrix). The inventory hook records every collected
matrix cell — *before* any ``-m`` deselection — for the meta-test in
``test_matrix.py`` to audit against the registry x scenario x leg expectation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from tests.conformance._registry import enforce_registry

if TYPE_CHECKING:
    import pytest

enforce_registry()

#: ``(adapter, scenario, leg)`` for every collected cell, keyed by the config
#: object so parallel sessions cannot bleed into each other.
_INVENTORY_ATTR = "_conformance_cell_inventory"


def pytest_configure(config: pytest.Config) -> None:
    setattr(config, _INVENTORY_ATTR, set())


def pytest_itemcollected(item: pytest.Item) -> None:
    marker = item.get_closest_marker("conformance_cell")
    if marker is not None:
        inventory: set[tuple[str, str, str]] = getattr(item.config, _INVENTORY_ATTR)
        inventory.add(marker.args)


def collected_cells(config: pytest.Config) -> set[tuple[str, str, str]]:
    """The cells this session collected (read by the meta-test)."""
    cells: set[tuple[str, str, str]] = getattr(config, _INVENTORY_ATTR, set())
    return set(cells)
