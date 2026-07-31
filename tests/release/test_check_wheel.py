"""Unit tests for the distribution-content verification script.

`scripts/check_wheel.py` is what stands between a mis-built artifact and PyPI:
it opens the wheel as a zip and the sdist as a tarball (no install, no
network) and fails on a missing typing marker, missing generated proto
bindings, leaked test/docker/CI content, a missing console script, or metadata
drift. Because a defect in the verifier is only observable at release time,
the logic lives in a standalone script and is exercised here against synthetic
archives built in-test — the same "release-critical logic runs in the unit
lane" stance as `scripts/coverage_ratchet.py` and
`scripts/check_semantics_partition.py`.

Spec: openspec/changes/add-0-1-0-release/specs/release-process —
"Distribution contents are verified before publishing".
"""

from __future__ import annotations

import tarfile
import zipfile
from collections.abc import Iterable
from pathlib import Path

import pytest
from scripts.check_wheel import (
    Expected,
    expected_from_pyproject,
    main,
    read_sdist,
    read_wheel,
    sdist_problems,
    wheel_problems,
)

# --- synthetic-archive fixtures ------------------------------------------

VERSION = "0.1.0"
DIST_INFO = f"beam_agents-{VERSION}.dist-info"

# Ordered exactly as hatchling emits it (normalized specifier order differs
# from pyproject's `>=3.11,<3.13` — the check must compare sets, not strings).
METADATA = f"""\
Metadata-Version: 2.4
Name: beam-agents
Version: {VERSION}
Requires-Python: <3.13,>=3.11
Requires-Dist: apache-beam[gcp]>=2.60
Provides-Extra: effector
Provides-Extra: langgraph
Provides-Extra: otlp
Provides-Extra: vllm
"""

ENTRY_POINTS = """\
[console_scripts]
beam-agents-effector = beam_agents.effector.__main__:main
"""

COMPLIANT_MEMBERS = (
    "beam_agents/__init__.py",
    "beam_agents/py.typed",
    "beam_agents/_protos/__init__.py",
    "beam_agents/_protos/beam_agents_pb2.py",
    "beam_agents/_protos/beam_agents_pb2.pyi",
    "beam_agents/effector/__main__.py",
    "beam_agents/testing/chaos.py",
)

EXPECTED = Expected(
    requires_python=">=3.11,<3.13",
    extras=frozenset({"effector", "langgraph", "otlp", "vllm"}),
)


def write_wheel(
    tmp_path: Path,
    *,
    members: Iterable[str] = COMPLIANT_MEMBERS,
    metadata: str = METADATA,
    entry_points: str | None = ENTRY_POINTS,
    name: str = f"beam_agents-{VERSION}-py3-none-any.whl",
) -> Path:
    """Build a minimal but structurally real wheel."""
    path = tmp_path / name
    with zipfile.ZipFile(path, "w") as zf:
        for member in members:
            zf.writestr(member, "# synthetic\n")
        zf.writestr(f"{DIST_INFO}/METADATA", metadata)
        zf.writestr(f"{DIST_INFO}/WHEEL", "Wheel-Version: 1.0\n")
        if entry_points is not None:
            zf.writestr(f"{DIST_INFO}/entry_points.txt", entry_points)
    return path


SDIST_MEMBERS = (
    "pyproject.toml",
    "PKG-INFO",
    "README.md",
    "protos/beam_agents.proto",
    "src/beam_agents/py.typed",
    "src/beam_agents/_protos/beam_agents_pb2.py",
    "tests/test_import.py",
)


def write_sdist(
    tmp_path: Path,
    *,
    members: Iterable[str] = SDIST_MEMBERS,
    name: str = f"beam_agents-{VERSION}.tar.gz",
) -> Path:
    path = tmp_path / name
    root = f"beam_agents-{VERSION}"
    with tarfile.open(path, "w:gz") as tf:
        for member in members:
            info = tarfile.TarInfo(f"{root}/{member}")
            info.size = 0
            tf.addfile(info)
    return path


def problems_for(
    tmp_path: Path,
    *,
    members: Iterable[str] = COMPLIANT_MEMBERS,
    metadata: str = METADATA,
    entry_points: str | None = ENTRY_POINTS,
) -> list[str]:
    wheel = write_wheel(tmp_path, members=members, metadata=metadata, entry_points=entry_points)
    return wheel_problems(read_wheel(wheel), EXPECTED)


# --- the compliant baseline ----------------------------------------------


class TestCompliantWheelPasses:
    """From scenario: Verification logic is unit-tested offline (passing half)."""

    def test_compliant_wheel_reports_no_problems(self, tmp_path: Path) -> None:
        assert problems_for(tmp_path) == []

    def test_compliant_sdist_reports_no_problems(self, tmp_path: Path) -> None:
        assert sdist_problems(read_sdist(write_sdist(tmp_path))) == []

    def test_main_exits_zero_on_a_compliant_pair(self, tmp_path: Path) -> None:
        write_wheel(tmp_path)
        write_sdist(tmp_path)
        assert main([str(tmp_path), "--requires-python", ">=3.11,<3.13", *_extras_args()]) == 0


def _extras_args() -> list[str]:
    args: list[str] = []
    for extra in sorted(EXPECTED.extras):
        args += ["--extra", extra]
    return args


# --- one failing case per check ------------------------------------------


class TestTypingMarker:
    """From scenario: Wheel missing the typing marker fails verification."""

    def test_missing_py_typed_is_reported_by_name(self, tmp_path: Path) -> None:
        members = [m for m in COMPLIANT_MEMBERS if m != "beam_agents/py.typed"]
        problems = problems_for(tmp_path, members=members)
        assert any("py.typed" in problem for problem in problems)

    def test_main_exits_non_zero_when_the_marker_is_missing(self, tmp_path: Path) -> None:
        members = [m for m in COMPLIANT_MEMBERS if m != "beam_agents/py.typed"]
        wheel = write_wheel(tmp_path, members=members)
        assert main([str(wheel), "--requires-python", ">=3.11,<3.13", *_extras_args()]) == 1


class TestProtoBindings:
    """From scenario: Wheel missing generated proto bindings fails verification."""

    def test_missing_pb2_modules_are_reported(self, tmp_path: Path) -> None:
        members = [m for m in COMPLIANT_MEMBERS if not m.endswith("_pb2.py")]
        problems = problems_for(tmp_path, members=members)
        assert any("_pb2.py" in problem for problem in problems)

    def test_a_stub_only_protos_package_still_fails(self, tmp_path: Path) -> None:
        """A `.pyi` next to no `.py` imports fine to mypy and dies at runtime."""
        members = [m for m in COMPLIANT_MEMBERS if not m.endswith("_pb2.py")]
        problems = problems_for(tmp_path, members=members)
        assert any("_protos" in problem for problem in problems)


class TestLeakedContent:
    """From scenario: the wheel contains no test, docker, or CI content."""

    @pytest.mark.parametrize(
        "leaked",
        [
            "tests/test_import.py",
            "beam_agents/tests/test_internal.py",
            "docker/compose.yaml",
            ".github/workflows/ci.yml",
        ],
    )
    def test_leaked_member_is_reported_by_path(self, tmp_path: Path, leaked: str) -> None:
        problems = problems_for(tmp_path, members=[*COMPLIANT_MEMBERS, leaked])
        assert any(leaked in problem for problem in problems)

    def test_the_testing_subpackage_is_not_mistaken_for_tests(self, tmp_path: Path) -> None:
        """`beam_agents/testing/` is shipped runtime code, not test content."""
        assert problems_for(tmp_path) == []


class TestConsoleScript:
    """From scenario: the `beam-agents-effector` console script is declared."""

    def test_missing_entry_points_file_is_reported(self, tmp_path: Path) -> None:
        problems = problems_for(tmp_path, entry_points=None)
        assert any("beam-agents-effector" in problem for problem in problems)

    def test_retargeted_console_script_is_reported(self, tmp_path: Path) -> None:
        drifted = "[console_scripts]\nbeam-agents-effector = beam_agents.__main__:main\n"
        problems = problems_for(tmp_path, entry_points=drifted)
        assert any("beam_agents.effector.__main__:main" in problem for problem in problems)

    def test_renamed_console_script_is_reported(self, tmp_path: Path) -> None:
        renamed = "[console_scripts]\neffector = beam_agents.effector.__main__:main\n"
        problems = problems_for(tmp_path, entry_points=renamed)
        assert any("beam-agents-effector" in problem for problem in problems)


class TestMetadataDrift:
    """From scenario: Metadata drift fails verification."""

    def test_drifted_requires_python_is_reported_with_both_values(self, tmp_path: Path) -> None:
        drifted = METADATA.replace("<3.13,>=3.11", ">=3.10")
        problems = problems_for(tmp_path, metadata=drifted)
        assert any(">=3.10" in problem and ">=3.11" in problem for problem in problems)

    def test_specifier_ordering_alone_is_not_drift(self, tmp_path: Path) -> None:
        reordered = METADATA.replace("<3.13,>=3.11", ">=3.11, <3.13")
        assert problems_for(tmp_path, metadata=reordered) == []

    def test_missing_extra_is_reported_by_name(self, tmp_path: Path) -> None:
        dropped = METADATA.replace("Provides-Extra: otlp\n", "")
        problems = problems_for(tmp_path, metadata=dropped)
        assert any("otlp" in problem for problem in problems)

    def test_unexpected_extra_is_reported_by_name(self, tmp_path: Path) -> None:
        extra = METADATA + "Provides-Extra: experimental\n"
        problems = problems_for(tmp_path, metadata=extra)
        assert any("experimental" in problem for problem in problems)


class TestSdistContents:
    """The sdist half: proto sources ship (design open question, resolved)."""

    def test_missing_proto_sources_are_reported(self, tmp_path: Path) -> None:
        members = [m for m in SDIST_MEMBERS if not m.endswith(".proto")]
        problems = sdist_problems(read_sdist(write_sdist(tmp_path, members=members)))
        assert any(".proto" in problem for problem in problems)

    def test_missing_pyproject_is_reported(self, tmp_path: Path) -> None:
        members = [m for m in SDIST_MEMBERS if m != "pyproject.toml"]
        problems = sdist_problems(read_sdist(write_sdist(tmp_path, members=members)))
        assert any("pyproject.toml" in problem for problem in problems)

    def test_sdist_keeps_the_package_sources(self, tmp_path: Path) -> None:
        members = [m for m in SDIST_MEMBERS if not m.startswith("src/")]
        problems = sdist_problems(read_sdist(write_sdist(tmp_path, members=members)))
        assert any("src/beam_agents" in problem for problem in problems)


class TestExpectedFromPyproject:
    """The expected metadata is derived from pyproject, not hardcoded twice."""

    def test_requires_python_and_extras_are_read_from_the_source_of_truth(self) -> None:
        text = (
            '[project]\nname = "beam-agents"\nrequires-python = ">=3.11,<3.13"\n\n'
            "[project.optional-dependencies]\n"
            'effector = ["aiokafka"]\nlanggraph = ["langgraph"]\n'
            'otlp = ["opentelemetry-proto"]\nvllm = ["vllm"]\n'
        )
        assert expected_from_pyproject(text) == EXPECTED

    def test_the_repo_pyproject_matches_the_expectations_these_tests_encode(self) -> None:
        """Guards the tests themselves against drifting from the real project."""
        pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
        assert expected_from_pyproject(pyproject.read_text(encoding="utf-8")) == EXPECTED


class TestArchiveReading:
    """Reading is zip/tar only — no installation, no network."""

    def test_read_wheel_surfaces_members_metadata_and_entry_points(self, tmp_path: Path) -> None:
        contents = read_wheel(write_wheel(tmp_path))
        assert "beam_agents/py.typed" in contents.names
        assert "Metadata-Version: 2.4" in contents.metadata
        assert "beam-agents-effector" in contents.entry_points

    def test_read_sdist_strips_the_root_directory(self, tmp_path: Path) -> None:
        names = read_sdist(write_sdist(tmp_path))
        assert "pyproject.toml" in names
        assert not any(name.startswith("beam_agents-") for name in names)

    def test_a_wheel_without_dist_info_fails_loudly(self, tmp_path: Path) -> None:
        path = tmp_path / "broken-0.1.0-py3-none-any.whl"
        with zipfile.ZipFile(path, "w") as zf:
            zf.writestr("beam_agents/__init__.py", "")
        with pytest.raises(ValueError, match="dist-info"):
            read_wheel(path)


class TestMainDispatch:
    def test_main_rejects_a_directory_with_no_distributions(self, tmp_path: Path) -> None:
        assert main([str(tmp_path)]) == 1

    def test_main_reports_every_problem_not_just_the_first(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        members = [
            m for m in COMPLIANT_MEMBERS if m != "beam_agents/py.typed" and "_pb2.py" not in m
        ]
        wheel = write_wheel(tmp_path, members=members)
        assert main([str(wheel), "--requires-python", ">=3.11,<3.13", *_extras_args()]) == 1
        err = capsys.readouterr().err
        assert "py.typed" in err
        assert "_pb2.py" in err
