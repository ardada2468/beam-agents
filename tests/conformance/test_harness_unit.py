"""Unit tests for the conformance harness itself: the registry guard and the
bundle-equivalence check. Plain unit tier (no markers) — these verify the
matrix's enforcement machinery, not runtime semantics."""

from __future__ import annotations

import dataclasses
import importlib
import sys
import textwrap
from pathlib import Path
from types import ModuleType

import pytest

from beam_agents.tools import ToolRegistry
from tests.conformance._registry import (
    ADAPTERS,
    bundle_for,
    unregistered_adapters,
    validate_bundle,
)
from tests.conformance._spec import (
    MULTI_TOOL_INLINE,
    SCENARIOS,
    SUSPENSION_RESUME,
    tool_for,
)


def _fake_adapters_package(tmp_path: Path) -> ModuleType:
    """A synthetic adapters package with three subpackages: one registered,
    one importable-but-unregistered, one whose optional framework is absent."""
    root = tmp_path / "fake_adapters_root"
    package = root / "fakeadapters"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    for name, body in (
        ("registered", ""),
        ("orphan", ""),
        (
            "extra_missing",
            textwrap.dedent(
                """
                import definitely_not_an_installed_framework  # noqa: F401
                """
            ),
        ),
    ):
        subpackage = package / name
        subpackage.mkdir()
        (subpackage / "__init__.py").write_text(body)
    sys.path.insert(0, str(root))
    try:
        return importlib.import_module("fakeadapters")
    finally:
        sys.path.remove(str(root))


def test_guard_names_the_importable_unregistered_adapter(tmp_path: Path) -> None:
    # Scenario: New adapter without a conformance factory breaks the build.
    package = _fake_adapters_package(tmp_path)
    missing = unregistered_adapters(package, registered={"registered"})
    assert missing == ["orphan"], missing


def test_guard_ignores_an_adapter_whose_framework_is_absent(tmp_path: Path) -> None:
    # Scenario: A missing optional framework skips its cells cleanly — the
    # guard must not fail collection for a subpackage this environment cannot
    # even import (its cells could not have run here).
    package = _fake_adapters_package(tmp_path)
    missing = unregistered_adapters(package, registered={"registered", "orphan"})
    assert missing == []


def test_every_shipped_adapter_is_registered_here() -> None:
    # The real-tree guard holds right now (conftest enforces it at collection;
    # this pins the same fact as an assertable unit).
    import beam_agents.adapters

    registered = {a.adapters_subpackage for a in ADAPTERS if a.adapters_subpackage is not None}
    assert unregistered_adapters(beam_agents.adapters, registered) == []


def test_equivalence_check_names_a_tool_divergence() -> None:
    bundle = bundle_for("reference", MULTI_TOOL_INLINE.name)
    doctored_registry = ToolRegistry()
    doctored_registry.register(tool_for("lookup_a"))  # lookup_b dropped
    doctored = dataclasses.replace(bundle, tool_registry=doctored_registry)
    with pytest.raises(AssertionError, match="tool set diverged"):
        validate_bundle(doctored, MULTI_TOOL_INLINE)


def test_equivalence_check_names_a_script_length_divergence() -> None:
    bundle = bundle_for("reference", MULTI_TOOL_INLINE.name)
    shorter = dataclasses.replace(bundle, provider=bundle_for("reference", "single_shot").provider)
    with pytest.raises(AssertionError, match="provider rules"):
        validate_bundle(shorter, MULTI_TOOL_INLINE)


def test_equivalence_check_names_a_deadline_divergence() -> None:
    bundle = bundle_for("reference", SUSPENSION_RESUME.name)
    doctored = dataclasses.replace(bundle, hitl_timeout_ms=1)
    with pytest.raises(AssertionError, match="hitl_timeout_ms"):
        validate_bundle(doctored, SUSPENSION_RESUME)


def test_langgraph_cells_skip_cleanly_when_the_framework_is_absent() -> None:
    # Scenario: A missing optional framework skips its cells cleanly. The test
    # environment installs LangGraph (the adapter cells need it), so absence
    # is simulated in a subprocess with the same meta-path blocker the
    # import-isolation tests use: the reference cell must still run and pass,
    # the LangGraph cell must report as a skip, and collection must not error.
    import subprocess

    script = textwrap.dedent(
        """
        import sys

        class _BlockLangGraph:
            _blocked = ("langgraph", "langchain", "langchain_core")

            def find_spec(self, fullname, path=None, target=None):
                if fullname.partition(".")[0] in self._blocked:
                    raise ModuleNotFoundError(
                        f"import of {fullname!r} blocked for test", name=fullname
                    )
                return None

        sys.meta_path.insert(0, _BlockLangGraph())

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
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=180, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "1 passed" in result.stdout, result.stdout
    assert "1 skipped" in result.stdout, result.stdout
    assert "optional framework 'langgraph' is not installed" in result.stdout, result.stdout


def test_every_scenario_declares_every_leg() -> None:
    # The meta-test's expected-cell arithmetic assumes a complete legs mapping;
    # an absent declaration must be impossible, not defaulted.
    for spec in SCENARIOS:
        assert set(spec.legs) == {"direct", "flink"}, (
            f"scenario {spec.name!r} must declare both legs, got {sorted(spec.legs)}"
        )
