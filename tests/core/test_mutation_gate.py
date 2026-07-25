from __future__ import annotations

import json
from pathlib import Path

import pytest
from scripts import mutation_gate


def _write_meta(
    root: Path,
    module: str,
    statuses: dict[str, int | None],
) -> None:
    path = root / "mutants" / "src" / "beam_agents" / "core" / f"{module}.py.meta"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"exit_code_by_key": statuses}), encoding="utf-8")


def _write_baseline(root: Path, **counts: int) -> None:
    lines = ["[no_tests]", *(f'"{module}.py" = {count}' for module, count in counts.items())]
    (root / "mutation-baseline.toml").write_text("\n".join(lines), encoding="utf-8")


def _write_exclusions(root: Path, **entries: str) -> None:
    lines = ["[mutants]", *(f'"{name}" = "{reason}"' for name, reason in entries.items())]
    (root / "mutation-exclusions.toml").write_text("\n".join(lines), encoding="utf-8")


@pytest.fixture
def gate_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    _write_baseline(tmp_path)
    _write_exclusions(tmp_path)
    return tmp_path


@pytest.mark.parametrize(
    ("exit_code", "status"),
    [
        (0, "survived"),
        (-24, "timeout"),
        (4, "suspicious"),
        (-11, "segfault"),
        (None, "not checked"),
        (2, "check was interrupted by user"),
    ],
)
def test_each_failing_status_is_reported(
    gate_root: Path,
    capsys: pytest.CaptureFixture[str],
    exit_code: int | None,
    status: str,
) -> None:
    _write_meta(gate_root, "agent", {"beam_agents.core.agent.x_run__mutmut_1": exit_code})

    assert mutation_gate.main() == 1
    captured = capsys.readouterr()
    assert status in captured.err
    assert "src/beam_agents/core/agent.py" in captured.err
    assert "beam_agents.core.agent.x_run__mutmut_1" in captured.err


@pytest.mark.parametrize("exit_code", [1, 34, 37])
def test_passing_statuses_pass(gate_root: Path, exit_code: int) -> None:
    _write_meta(gate_root, "agent", {"beam_agents.core.agent.x_run__mutmut_1": exit_code})

    assert mutation_gate.main() == 0


def test_declared_survivor_is_excluded(gate_root: Path) -> None:
    name = "beam_agents.core.agent.x_run__mutmut_1"
    _write_meta(gate_root, "agent", {name: 0})
    _write_exclusions(gate_root, **{name: "Equivalent return expression."})

    assert mutation_gate.main() == 0


@pytest.mark.parametrize(("exit_code", "status"), [(1, "killed"), (-24, "timeout")])
def test_exclusion_must_name_a_live_survivor(
    gate_root: Path,
    capsys: pytest.CaptureFixture[str],
    exit_code: int,
    status: str,
) -> None:
    name = "beam_agents.core.agent.x_run__mutmut_1"
    _write_meta(gate_root, "agent", {name: exit_code})
    _write_exclusions(gate_root, **{name: "No longer a valid exclusion."})

    assert mutation_gate.main() == 1
    assert f"[{status}] {name}" in capsys.readouterr().err


def test_missing_excluded_mutant_fails(
    gate_root: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_meta(gate_root, "agent", {"beam_agents.core.agent.x_run__mutmut_1": 1})
    missing = "beam_agents.core.agent.x_missing__mutmut_1"
    _write_exclusions(gate_root, **{missing: "Equivalent but removed."})

    assert mutation_gate.main() == 1
    assert missing in capsys.readouterr().err


def test_undeclared_survivor_fails(gate_root: Path) -> None:
    _write_meta(
        gate_root,
        "agent",
        {"beam_agents.core.agent.x_run__mutmut_1": 0},
    )

    assert mutation_gate.main() == 1


def test_module_at_baseline_passes(gate_root: Path) -> None:
    _write_baseline(gate_root, dofn=1)
    _write_meta(gate_root, "dofn", {"beam_agents.core.dofn.x_run__mutmut_1": 5})

    assert mutation_gate.main() == 0


def test_module_above_baseline_fails(
    gate_root: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_baseline(gate_root, dofn=1)
    _write_meta(
        gate_root,
        "dofn",
        {
            "beam_agents.core.dofn.x_run__mutmut_1": 5,
            "beam_agents.core.dofn.x_run__mutmut_2": 5,
        },
    )

    assert mutation_gate.main() == 1
    assert "dofn.py rose from 1 to 2" in capsys.readouterr().err


def test_module_below_baseline_passes_and_requests_tightening(
    gate_root: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_baseline(gate_root, dofn=2)
    _write_meta(gate_root, "dofn", {"beam_agents.core.dofn.x_run__mutmut_1": 5})

    assert mutation_gate.main() == 0
    assert "lower `dofn.py` to 1" in capsys.readouterr().out


def test_module_improvement_cannot_offset_another_module_regression(
    gate_root: Path,
) -> None:
    _write_baseline(gate_root, dofn=2, transform=1)
    _write_meta(gate_root, "dofn", {"beam_agents.core.dofn.x_run__mutmut_1": 5})
    _write_meta(
        gate_root,
        "transform",
        {
            "beam_agents.core.transform.x_run__mutmut_1": 5,
            "beam_agents.core.transform.x_run__mutmut_2": 5,
        },
    )

    assert mutation_gate.main() == 1


def test_new_module_has_implicit_zero_baseline(
    gate_root: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_meta(gate_root, "new_module", {"beam_agents.core.new.x_run__mutmut_1": 5})

    assert mutation_gate.main() == 1
    assert "new_module.py rose from 0 to 1" in capsys.readouterr().err


@pytest.mark.parametrize(
    "payload",
    ["not json", "{}", '{"exit_code_by_key": []}', '{"exit_code_by_key": {"mutant": true}}'],
)
def test_malformed_metadata_fails(
    gate_root: Path,
    capsys: pytest.CaptureFixture[str],
    payload: str,
) -> None:
    path = gate_root / "mutants/src/beam_agents/core/agent.py.meta"
    path.parent.mkdir(parents=True)
    path.write_text(payload, encoding="utf-8")

    assert mutation_gate.main() == 1
    assert "agent.py.meta" in capsys.readouterr().err


def test_missing_mutants_directory_fails(
    gate_root: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert mutation_gate.main() == 1
    assert "mutants/ not found" in capsys.readouterr().err
