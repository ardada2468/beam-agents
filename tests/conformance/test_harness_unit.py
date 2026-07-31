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
    APPROVAL_TIMEOUT_FALLBACK,
    DIRECT,
    FLINK,
    LEGS,
    MULTI_TOOL_INLINE,
    SCENARIOS,
    SINGLE_SHOT,
    SPARK,
    SPARK_SCENARIOS,
    SUSPENSION_RESUME,
    skip_inventory,
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


def _run_single_shot_without(blocked: tuple[str, ...]) -> str:
    """Collect and run the single_shot cells in a subprocess where `blocked`
    distributions are unimportable, returning pytest's output.

    The test environment installs every adapter framework (the cells need
    them), so absence is simulated with the same meta-path blocker the
    import-isolation tests use.
    """
    import subprocess

    script = textwrap.dedent(
        f"""
        import sys

        class _Blocker:
            _blocked = {blocked!r}

            def find_spec(self, fullname, path=None, target=None):
                # Exact-or-dotted-prefix match, not a top-level-name match:
                # ADK lives under the `google` namespace package that core
                # installs already populate, so blocking "google" wholesale
                # would take out google-cloud too.
                if any(
                    fullname == blocked or fullname.startswith(f"{blocked}.")
                    for blocked in self._blocked
                ):
                    raise ModuleNotFoundError(
                        f"import of {{fullname!r}} blocked for test", name=fullname
                    )
                return None

        sys.meta_path.insert(0, _Blocker())

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
    return result.stdout


def _passing_cells() -> int:
    """Cells that still run when exactly one adapter's framework is absent."""
    return len(ADAPTERS) - 1


def test_langgraph_cells_skip_cleanly_when_the_framework_is_absent() -> None:
    # Scenario: A missing optional framework skips its cells cleanly — the
    # other adapters' cells still run and pass, the LangGraph cell reports as a
    # skip naming the missing package, and collection does not error.
    stdout = _run_single_shot_without(("langgraph", "langchain", "langchain_core"))
    assert f"{_passing_cells()} passed" in stdout, stdout
    assert "1 skipped" in stdout, stdout
    assert "optional framework 'langgraph' is not installed" in stdout, stdout


def test_pydantic_ai_cells_skip_cleanly_when_the_framework_is_absent() -> None:
    # Scenario: Missing extra skips cells without shrinking the matrix silently
    # (pydantic-ai-adapter) — the same clean-skip contract for the Pydantic AI
    # axis entry.
    stdout = _run_single_shot_without(("pydantic_ai", "pydantic_graph"))
    assert f"{_passing_cells()} passed" in stdout, stdout
    assert "1 skipped" in stdout, stdout
    assert "optional framework 'pydantic_ai' is not installed" in stdout, stdout


def test_every_scenario_declares_every_leg() -> None:
    # Scenario: A scenario without a spark declaration cannot build a cell.
    # The meta-test's expected-cell arithmetic assumes a complete legs mapping;
    # an absent declaration must be impossible, not defaulted. Derived from
    # LEGS, so adding a leg to the vocabulary without declaring it on every
    # scenario fails here by name instead of as a KeyError at cell build.
    for spec in SCENARIOS:
        assert set(spec.legs) == set(LEGS), (
            f"scenario {spec.name!r} must declare every leg {sorted(LEGS)}, "
            f"got {sorted(spec.legs)} (undeclared: {sorted(set(LEGS) - set(spec.legs))})"
        )


def test_leg_vocabulary_is_the_declared_legs() -> None:
    # Pins the vocabulary itself (names, not counts): the offline DirectRunner
    # leg, the docker-backed Flink leg, and the weekly Spark leg.
    assert LEGS == (DIRECT, FLINK, SPARK)


def test_every_spark_skip_names_a_specific_constraint() -> None:
    # Scenario: A spark-inexpressible scenario is an explicit skip with a
    # reason — and the reason must name the concrete missing runner feature or
    # harness constraint, never "doesn't work" (design D6).
    for name, reason in skip_inventory(SPARK).items():
        assert len(reason) > 60, f"spark skip for {name!r} is not a specific reason: {reason!r}"
        assert not reason.lower().startswith(("doesn't", "does not work", "unsupported")), (
            f"spark skip for {name!r} states no constraint: {reason!r}"
        )


def test_spark_runnable_scenarios_exclude_the_declared_skips() -> None:
    # SPARK_SCENARIOS is what the harness publishes events for; a declared skip
    # must never reach the job.
    assert {spec.name for spec in SPARK_SCENARIOS} == {spec.name for spec in SCENARIOS} - set(
        skip_inventory(SPARK)
    )


def test_real_time_variant_applies_to_both_portable_legs() -> None:
    # design D1: the flink and spark legs share the one real-time HITL
    # override; the DirectRunner leg keeps the scripted deadline.
    spec = APPROVAL_TIMEOUT_FALLBACK
    assert spec.variant_for(DIRECT).hitl_timeout_ms == spec.hitl_timeout_ms
    assert spec.variant_for(FLINK).hitl_timeout_ms == spec.flink_hitl_timeout_ms
    assert spec.variant_for(SPARK).hitl_timeout_ms == spec.flink_hitl_timeout_ms
    # A scenario with no real-time override is unchanged on every leg.
    assert all(SINGLE_SHOT.variant_for(leg) is SINGLE_SHOT for leg in LEGS)
