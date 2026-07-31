"""Unit tests for the tag/version/lock agreement and versioning-policy check.

`scripts/check_release.py` is the gate between a pushed tag and a build:
it proves the tag, `[project].version`, and `uv.lock`'s recorded
`beam-agents` version all agree, that the tagged commit is an ancestor of
`main` (so it passed the required merge gates), and that the pending
changelog fragments are compatible with the version component the tag moves.
The decision logic is a pure function over injected inputs, so every failure
mode is exercised offline here rather than by cutting a release.

Spec: openspec/changes/add-0-1-0-release/specs/release-process —
"The project version is single-sourced and consistent across tag, metadata,
and lockfile" and "Pre-1.0 versioning policy is documented and machine-checked
at release time".
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pytest
from scripts.check_release import (
    FRAGMENT_TYPES,
    MINOR_REQUIRING_TYPES,
    Fragment,
    main,
    project_version,
    read_fragments,
    release_problems,
    uv_lock_version,
    version_from_tag,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def _problems(
    *,
    tag: str = "v0.1.0",
    metadata_version: str = "0.1.0",
    lock_version: str = "0.1.0",
    fragments: Sequence[Fragment] = (),
    tagged_commit_on_main: bool = True,
) -> list[str]:
    return release_problems(
        tag=tag,
        metadata_version=metadata_version,
        lock_version=lock_version,
        fragments=fragments,
        tagged_commit_on_main=tagged_commit_on_main,
    )


class TestConsistentRelease:
    def test_a_fully_consistent_minor_tag_has_no_problems(self) -> None:
        assert _problems() == []

    def test_a_consistent_patch_tag_with_only_patch_fragments_passes(self) -> None:
        problems = _problems(
            tag="v0.1.1",
            metadata_version="0.1.1",
            lock_version="0.1.1",
            fragments=[Fragment("fix-timer-drift", "fixed"), Fragment("docs-tidy", "docs")],
        )
        assert problems == []

    def test_internal_fragments_never_constrain_the_version_component(self) -> None:
        problems = _problems(
            tag="v0.1.2",
            metadata_version="0.1.2",
            lock_version="0.1.2",
            fragments=[Fragment("refactor-bridge", "internal")],
        )
        assert problems == []


class TestTagAndVersionDisagree:
    """From scenario: Tag and version disagree."""

    def test_mismatch_is_reported_naming_both_values(self) -> None:
        problems = _problems(tag="v0.2.0", metadata_version="0.1.0", lock_version="0.1.0")
        assert len(problems) == 1
        assert "0.2.0" in problems[0]
        assert "0.1.0" in problems[0]

    def test_an_unparseable_tag_is_rejected(self) -> None:
        problems = _problems(tag="release-0.1.0")
        assert any("release-0.1.0" in problem for problem in problems)

    @pytest.mark.parametrize("tag", ["v0.1.0", "v1.2.3", "v0.1.0rc1", "v0.2.0b2"])
    def test_accepted_tag_shapes(self, tag: str) -> None:
        assert version_from_tag(tag) == tag.removeprefix("v")

    @pytest.mark.parametrize("tag", ["0.1.0", "v0.1", "v0.1.0.1", "vX.Y.Z", "v0.1.0-rc1"])
    def test_rejected_tag_shapes(self, tag: str) -> None:
        with pytest.raises(ValueError, match="tag"):
            version_from_tag(tag)


class TestLockfileLags:
    """From scenario: Lockfile lags the version bump."""

    def test_stale_lock_version_is_reported(self) -> None:
        problems = _problems(tag="v0.1.0", metadata_version="0.1.0", lock_version="0.0.0")
        assert len(problems) == 1
        assert "uv.lock" in problems[0]
        assert "0.0.0" in problems[0]

    def test_the_message_names_the_fix(self) -> None:
        problems = _problems(lock_version="0.0.0")
        assert any("uv lock" in problem for problem in problems)


class TestTagNotOnMain:
    """From scenario: Tag on a commit not on main."""

    def test_non_ancestor_commit_is_reported_as_ungated(self) -> None:
        problems = _problems(tagged_commit_on_main=False)
        assert len(problems) == 1
        assert "main" in problems[0]

    def test_the_message_explains_which_gates_were_skipped(self) -> None:
        problems = _problems(tagged_commit_on_main=False)
        assert any("gates" in problem for problem in problems)


class TestPatchTagPolicy:
    """From scenario: Patch tag with a breaking fragment is rejected."""

    @pytest.mark.parametrize("fragment_type", sorted(MINOR_REQUIRING_TYPES))
    def test_patch_tag_with_a_minor_requiring_fragment_fails(self, fragment_type: str) -> None:
        problems = _problems(
            tag="v0.1.1",
            metadata_version="0.1.1",
            lock_version="0.1.1",
            fragments=[Fragment("add-thing", fragment_type)],
        )
        assert len(problems) == 1
        assert fragment_type in problems[0]
        assert "MINOR" in problems[0]

    def test_the_offending_fragment_is_named(self) -> None:
        problems = _problems(
            tag="v0.1.1",
            metadata_version="0.1.1",
            lock_version="0.1.1",
            fragments=[Fragment("drop-python-3-10", "breaking"), Fragment("tidy", "fixed")],
        )
        assert any("drop-python-3-10" in problem for problem in problems)
        assert not any("tidy" in problem for problem in problems)


class TestMinorTagPolicy:
    """From scenario: Minor tag accepts feature and breaking fragments."""

    def test_minor_tag_accepts_added_and_breaking_fragments(self) -> None:
        problems = _problems(
            tag="v0.2.0",
            metadata_version="0.2.0",
            lock_version="0.2.0",
            fragments=[Fragment("add-spark", "added"), Fragment("drop-x", "breaking")],
        )
        assert problems == []

    def test_a_major_tag_also_accepts_them(self) -> None:
        problems = _problems(
            tag="v1.0.0",
            metadata_version="1.0.0",
            lock_version="1.0.0",
            fragments=[Fragment("stabilize", "breaking")],
        )
        assert problems == []


class TestClosedFragmentRegistry:
    """From scenario: Unregistered fragment type fails assembly (verify half)."""

    def test_unregistered_type_is_reported_rather_than_ignored(self) -> None:
        problems = _problems(fragments=[Fragment("add-thing", "feature")])
        assert any("feature" in problem for problem in problems)

    def test_the_registry_is_exactly_the_six_documented_types(self) -> None:
        assert FRAGMENT_TYPES == ("breaking", "added", "changed", "fixed", "docs", "internal")

    def test_minor_requiring_types_are_the_first_three(self) -> None:
        assert set(MINOR_REQUIRING_TYPES) == {"breaking", "added", "changed"}


class TestProblemsAccumulate:
    def test_every_independent_failure_is_reported_at_once(self) -> None:
        problems = _problems(
            tag="v0.1.1",
            metadata_version="0.2.0",
            lock_version="0.0.0",
            fragments=[Fragment("add-thing", "added")],
            tagged_commit_on_main=False,
        )
        assert len(problems) == 4


class TestReadingRealInputs:
    def test_project_version_is_read_from_the_project_table_only(self) -> None:
        text = (
            '[project]\nname = "beam-agents"\nversion = "0.1.0"\n\n'
            '[tool.other]\nversion = "9.9.9"\n'
        )
        assert project_version(text) == "0.1.0"

    def test_uv_lock_version_is_read_from_the_beam_agents_package_entry(self) -> None:
        text = (
            '[[package]]\nname = "attrs"\nversion = "25.0.0"\n\n'
            '[[package]]\nname = "beam-agents"\nversion = "0.1.0"\n'
            'source = { editable = "." }\n'
        )
        assert uv_lock_version(text) == "0.1.0"

    def test_a_lock_without_the_project_entry_fails_loudly(self) -> None:
        with pytest.raises(ValueError, match="beam-agents"):
            uv_lock_version('[[package]]\nname = "attrs"\nversion = "25.0.0"\n')

    def test_read_fragments_parses_name_and_type_and_skips_the_readme(self, tmp_path: Path) -> None:
        (tmp_path / "README.md").write_text("how to write fragments\n")
        (tmp_path / ".gitkeep").write_text("")
        (tmp_path / "add-spark-runner.added.md").write_text("Spark is supported.\n")
        (tmp_path / "fix-timer.fixed.md").write_text("Timers fire once.\n")
        assert sorted(read_fragments(tmp_path)) == [
            Fragment("add-spark-runner", "added"),
            Fragment("fix-timer", "fixed"),
        ]

    def test_read_fragments_keeps_unregistered_types_for_the_checker_to_reject(
        self, tmp_path: Path
    ) -> None:
        (tmp_path / "add-thing.feature.md").write_text("nope\n")
        assert read_fragments(tmp_path) == [Fragment("add-thing", "feature")]

    def test_read_fragments_on_a_missing_directory_is_empty(self, tmp_path: Path) -> None:
        assert read_fragments(tmp_path / "nope") == []


class TestRepositoryStateIsSelfConsistent:
    """The repo's own version/lock agreement, checked in the unit lane.

    A version bump that forgets `uv lock` breaks every `uv sync --locked` job;
    catching it here makes the failure legible instead of a resolution error.
    """

    def test_pyproject_and_uv_lock_agree_today(self) -> None:
        metadata = project_version((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        locked = uv_lock_version((REPO_ROOT / "uv.lock").read_text(encoding="utf-8"))
        assert metadata == locked

    def test_the_changelog_documents_the_current_version(self) -> None:
        metadata = project_version((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        changelog = (REPO_ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
        assert f"## {metadata}" in changelog

    def test_every_pending_fragment_uses_a_registered_type(self) -> None:
        for fragment in read_fragments(REPO_ROOT / "changelog.d"):
            assert fragment.type in FRAGMENT_TYPES, fragment


class TestMainWiring:
    def test_main_passes_on_the_repository_with_ancestry_checking_disabled(self) -> None:
        version = project_version((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        assert main([f"v{version}", "--repo-root", str(REPO_ROOT), "--no-ancestry-check"]) == 0

    def test_main_reports_failures_on_stderr(self, capsys: pytest.CaptureFixture[str]) -> None:
        code = main(["v9.9.9", "--repo-root", str(REPO_ROOT), "--no-ancestry-check"])
        assert code == 1
        assert "9.9.9" in capsys.readouterr().err

    def test_fragments_only_mode_ignores_the_tag_entirely(self, tmp_path: Path) -> None:
        (tmp_path / "add-thing.added.md").write_text("A thing.\n")
        assert main(["--fragments-only", "--changelog-dir", str(tmp_path)]) == 0

    def test_fragments_only_mode_rejects_an_unregistered_type(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        (tmp_path / "add-thing.feature.md").write_text("A thing.\n")
        assert main(["--fragments-only", "--changelog-dir", str(tmp_path)]) == 1
        assert "feature" in capsys.readouterr().err
