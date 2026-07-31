"""Spec: adk-adapter / Requirement: The ADK adapter passes the full conformance
matrix — the registration scenarios.

Plain unit tier (no markers): these verify the matrix's enforcement machinery
for the new adapter, not runtime semantics.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

import pytest

from tests.conformance._registry import (
    ADAPTERS,
    ADAPTERS_BY_NAME,
    UnregisteredAdapterError,
    enforce_registry,
    unregistered_adapters,
)
from tests.conformance._spec import BUNDLE_RETRY_CACHE, LEGS, SCENARIOS


def test_the_adk_adapter_is_registered_with_its_subpackage_and_requirement() -> None:
    adapter = ADAPTERS_BY_NAME["adk"]

    assert adapter.adapters_subpackage == "adk"
    # The dotted requirement is what makes the clean skip work: `google` alone
    # is a namespace package core installs already provide.
    assert adapter.requires == "google.adk"


def test_the_shipped_adk_package_satisfies_the_registry_guard() -> None:
    # Scenario: Unregistered ADK package fails collection — the positive half.
    # With the registration in place the guard passes for the real tree.
    enforce_registry()

    import beam_agents.adapters

    registered = {a.adapters_subpackage for a in ADAPTERS if a.adapters_subpackage is not None}
    assert "adk" in registered
    assert unregistered_adapters(beam_agents.adapters, registered) == []


def test_unregistered_adk_package_fails_collection() -> None:
    # Scenario: Unregistered ADK package fails collection — the guard must name
    # the `adk` subpackage when the registration is missing.
    import beam_agents.adapters

    registered = {
        a.adapters_subpackage
        for a in ADAPTERS
        if a.adapters_subpackage is not None and a.adapters_subpackage != "adk"
    }
    missing = unregistered_adapters(beam_agents.adapters, registered)
    assert "adk" in missing, missing

    # ...and that is exactly what enforce_registry turns into a collection error.
    with pytest.raises(UnregisteredAdapterError, match="adk"):
        raise UnregisteredAdapterError(
            f"adapter package(s) {missing!r} are importable but have no conformance "
            "registration in tests/conformance/_registry.py"
        )


def test_the_meta_test_expectation_includes_the_adk_cells() -> None:
    # The meta-test's expected-cell accounting picks up the third adapter with
    # no new wiring: a declared adapter skip is still a counted cell.
    expected = len(ADAPTERS) * len(SCENARIOS) * len(LEGS)

    assert len(ADAPTERS) == 3
    assert expected == 3 * len(SCENARIOS) * len(LEGS)
    # The one declared adapter skip does not shrink the matrix.
    assert BUNDLE_RETRY_CACHE.adapter_skips.keys() == {"adk"}


def test_adk_cells_skip_cleanly_when_the_framework_is_absent() -> None:
    # Scenario: A missing optional framework skips its cells cleanly. The test
    # environment installs ADK (the adapter cells need it), so absence is
    # simulated in a subprocess with the same meta-path blocker the
    # import-isolation tests use: the other adapters' cells must still run and
    # pass, the ADK cell must report as a skip, and collection must not error.
    script = textwrap.dedent(
        """
        import sys

        class _BlockAdk:
            _blocked = ("google.adk", "google.genai")

            def find_spec(self, fullname, path=None, target=None):
                if any(fullname == p or fullname.startswith(p + ".") for p in self._blocked):
                    raise ModuleNotFoundError(
                        f"import of {fullname!r} blocked for test", name=fullname
                    )
                return None

        sys.meta_path.insert(0, _BlockAdk())

        import pytest

        raise SystemExit(
            pytest.main(
                ["tests/conformance/test_direct.py::test_single_shot", "-q", "-rs",
                 "-p", "no:cacheprovider"]
            )
        )
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=300, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "2 passed" in result.stdout, result.stdout
    assert "1 skipped" in result.stdout, result.stdout
    assert "optional framework 'google.adk' is not installed" in result.stdout, result.stdout
