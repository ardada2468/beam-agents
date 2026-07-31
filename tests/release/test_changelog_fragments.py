"""Contract tests for the changelog-fragment hook and the assembly command.

Two mechanisms are pinned here:

* `scripts/check_changelog_fragment.sh` — the local pre-commit hook that
  blocks a `src/` commit carrying no fragment, mirroring the shape (and the
  escape-hatch discipline) of `scripts/check_openspec_change.sh`. Driven
  against throwaway git repositories so the staged-diff behaviour is real.
* `make changelog` — assembly, which runs the closed-registry check before
  towncrier so an unregistered fragment type fails loudly instead of being
  silently dropped, and whose draft variant is side-effect free.

Spec: openspec/changes/add-0-1-0-release/specs/changelog-automation.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path

import pytest
from scripts.check_release import FRAGMENT_TYPES, consume_internal, main

REPO_ROOT = Path(__file__).resolve().parents[2]
HOOK = REPO_ROOT / "scripts" / "check_changelog_fragment.sh"
MAKEFILE = REPO_ROOT / "Makefile"


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        env={**os.environ, "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_SYSTEM": "/dev/null"},
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A throwaway repo with the hook script and an empty `changelog.d/`."""
    work = tmp_path / "repo"
    (work / "src" / "beam_agents").mkdir(parents=True)
    (work / "changelog.d").mkdir()
    (work / "scripts").mkdir()
    shutil.copy(HOOK, work / "scripts" / HOOK.name)
    (work / "changelog.d" / "README.md").write_text("fragment naming\n")
    (work / "README.md").write_text("repo\n")
    _git(work, "init", "-q", "-b", "main")
    _git(work, "config", "user.email", "t@example.com")
    _git(work, "config", "user.name", "t")
    _git(work, "add", "-A")
    _git(work, "commit", "-qm", "initial")
    return work


def run_hook(repo: Path, env: Mapping[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    base = {k: v for k, v in os.environ.items() if k != "CI"}
    return subprocess.run(
        ["bash", "scripts/check_changelog_fragment.sh"],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
        env={**base, **(env or {})},
    )


def stage_src_edit(repo: Path) -> None:
    target = repo / "src" / "beam_agents" / "widget.py"
    target.write_text("VALUE = 1\n")
    _git(repo, "add", "src/beam_agents/widget.py")


class TestFragmentRequiredForSrcChanges:
    """From scenario: src/ commit without a fragment is blocked."""

    def test_staged_src_edit_without_a_fragment_is_rejected(self, repo: Path) -> None:
        stage_src_edit(repo)
        result = run_hook(repo)
        assert result.returncode == 1

    def test_the_rejection_points_at_the_fragment_documentation(self, repo: Path) -> None:
        stage_src_edit(repo)
        result = run_hook(repo)
        assert "changelog.d/" in result.stderr
        assert "CONTRIBUTING.md" in result.stderr or "docs/releasing.md" in result.stderr

    def test_a_readme_only_changelog_dir_does_not_count_as_a_fragment(self, repo: Path) -> None:
        stage_src_edit(repo)
        assert (repo / "changelog.d" / "README.md").exists()
        assert run_hook(repo).returncode == 1

    def test_a_fragment_unblocks_the_commit(self, repo: Path) -> None:
        stage_src_edit(repo)
        (repo / "changelog.d" / "add-widget.added.md").write_text("Widgets are supported.\n")
        assert run_hook(repo).returncode == 0

    def test_a_commit_touching_nothing_under_src_is_never_blocked(self, repo: Path) -> None:
        (repo / "docs.md").write_text("prose\n")
        _git(repo, "add", "docs.md")
        assert run_hook(repo).returncode == 0


class TestInternalFragmentSatisfiesTheHook:
    """From scenario: Internal-only change passes with an unrendered fragment."""

    def test_internal_type_satisfies_the_requirement(self, repo: Path) -> None:
        stage_src_edit(repo)
        (repo / "changelog.d" / "refactor-bridge.internal.md").write_text("No user effect.\n")
        assert run_hook(repo).returncode == 0

    @pytest.mark.parametrize("fragment_type", FRAGMENT_TYPES)
    def test_every_registered_type_satisfies_the_requirement(
        self, repo: Path, fragment_type: str
    ) -> None:
        stage_src_edit(repo)
        (repo / "changelog.d" / f"a-change.{fragment_type}.md").write_text("text\n")
        assert run_hook(repo).returncode == 0

    def test_an_unregistered_type_does_not_satisfy_the_requirement(self, repo: Path) -> None:
        stage_src_edit(repo)
        (repo / "changelog.d" / "a-change.feature.md").write_text("text\n")
        result = run_hook(repo)
        assert result.returncode == 1
        assert "feature" in result.stderr


class TestEscapeHatch:
    """From scenario: Escape hatch bypasses only the fragment hook."""

    def test_the_documented_variable_bypasses_the_hook(self, repo: Path) -> None:
        stage_src_edit(repo)
        result = run_hook(repo, env={"BEAM_AGENTS_ALLOW_NO_FRAGMENT": "1"})
        assert result.returncode == 0

    def test_the_openspec_escape_hatch_does_not_bypass_this_hook(self, repo: Path) -> None:
        """Each hook has its own opt-out; one bypass must not disable the other."""
        stage_src_edit(repo)
        result = run_hook(repo, env={"BEAM_AGENTS_ALLOW_NO_CHANGE": "1"})
        assert result.returncode == 1

    def test_any_value_other_than_one_does_not_bypass(self, repo: Path) -> None:
        stage_src_edit(repo)
        result = run_hook(repo, env={"BEAM_AGENTS_ALLOW_NO_FRAGMENT": "true"})
        assert result.returncode == 1


class TestCiMode:
    """In CI nothing is staged, so the hook compares against the PR base."""

    def test_a_src_diff_against_origin_main_without_a_fragment_is_rejected(
        self, repo: Path
    ) -> None:
        base = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        _git(repo, "update-ref", "refs/remotes/origin/main", base)
        stage_src_edit(repo)
        _git(repo, "commit", "-qm", "add widget")
        result = run_hook(repo, env={"CI": "true"})
        assert result.returncode == 1

    def test_the_same_diff_with_a_committed_fragment_passes(self, repo: Path) -> None:
        base = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        _git(repo, "update-ref", "refs/remotes/origin/main", base)
        stage_src_edit(repo)
        (repo / "changelog.d" / "add-widget.added.md").write_text("Widgets.\n")
        _git(repo, "add", "-A")
        _git(repo, "commit", "-qm", "add widget")
        assert run_hook(repo, env={"CI": "true"}).returncode == 0


class TestAssemblyCommandShape:
    """From scenarios: Unregistered fragment type fails assembly / Draft mode.

    The assembly command is `make changelog`; the registry gate runs *before*
    towncrier so an unregistered type fails deterministically without needing
    the `release` group installed (see `TestClosedFragmentRegistry` in
    test_check_release.py for the gate's own behaviour).
    """

    def _target(self, name: str) -> str:
        text = MAKEFILE.read_text(encoding="utf-8")
        start = text.index(f"\n{name}:")
        rest = text[start + 1 :]
        end = rest.find("\n\n")
        return rest if end == -1 else rest[:end]

    def test_changelog_target_gates_on_the_fragment_registry_before_building(self) -> None:
        body = self._target("changelog")
        assert "check_release.py --fragments-only" in body
        assert body.index("check_release.py") < body.index("towncrier")

    def test_changelog_target_consumes_internal_fragments_after_building(self) -> None:
        body = self._target("changelog")
        assert "--consume-internal" in body
        assert body.index("towncrier") < body.index("--consume-internal")

    def test_changelog_target_builds_with_an_explicit_version_and_no_prompt(self) -> None:
        body = self._target("changelog")
        assert "towncrier build" in body
        assert "--version" in body
        assert "--yes" in body

    def test_draft_target_never_writes(self) -> None:
        body = self._target("changelog-draft")
        assert "--draft" in body
        assert ">" not in body.split("towncrier", 1)[1]

    def test_build_target_cleans_dist_before_building(self) -> None:
        body = self._target("build")
        assert "dist" in body
        assert body.index("rm -rf") < body.index("uv build")


CHANGELOG_HEAD = (
    "# Changelog\n\nintro\n\n<!-- towncrier release notes start -->\n\n"
    "## 0.1.0 - 2026-07-30\n\nSeeded.\n"
)


@pytest.fixture
def assembly_tree(tmp_path: Path) -> Path:
    """A miniature repo carrying the *real* `[tool.towncrier]` configuration."""
    pytest.importorskip("towncrier")
    work = tmp_path / "proj"
    (work / "changelog.d").mkdir(parents=True)
    (work / "src" / "beam_agents").mkdir(parents=True)
    (work / "src" / "beam_agents" / "__init__.py").write_text("")
    shutil.copy(REPO_ROOT / "pyproject.toml", work / "pyproject.toml")
    (work / "CHANGELOG.md").write_text(CHANGELOG_HEAD)
    return work


def run_towncrier(tree: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "towncrier", "build", *args],
        cwd=tree,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.slow
class TestRealAssembly:
    """Real towncrier behaviour, wherever the `release` group is installed.

    Skipped in the unit lane (which syncs lint/typecheck/test only);
    `TestAssemblyCommandShape` pins the command shape unconditionally, and
    the release workflow installs the group so these run there.
    """

    def test_draft_leaves_the_tree_byte_identical(self, assembly_tree: Path) -> None:
        """From scenario: Draft mode is side-effect free."""
        (assembly_tree / "changelog.d" / "add-thing.added.md").write_text("A thing exists.\n")
        before = {
            p.relative_to(assembly_tree).as_posix(): p.read_bytes()
            for p in sorted(assembly_tree.rglob("*"))
            if p.is_file()
        }

        result = run_towncrier(assembly_tree, "--draft", "--version", "0.2.0")

        after = {
            p.relative_to(assembly_tree).as_posix(): p.read_bytes()
            for p in sorted(assembly_tree.rglob("*"))
            if p.is_file()
        }
        assert result.returncode == 0, result.stderr
        assert "A thing exists." in result.stdout
        assert before == after

    def test_build_consumes_each_fragment_exactly_once(self, assembly_tree: Path) -> None:
        """From scenario: Assembly consumes fragments exactly once."""
        (assembly_tree / "changelog.d" / "add-thing.added.md").write_text("A thing exists.\n")
        (assembly_tree / "changelog.d" / "fix-thing.fixed.md").write_text("A thing works.\n")

        assert run_towncrier(assembly_tree, "--version", "0.2.0", "--yes").returncode == 0

        changelog = (assembly_tree / "CHANGELOG.md").read_text()
        assert "## 0.2.0" in changelog
        assert "A thing exists." in changelog
        assert "A thing works." in changelog
        assert list((assembly_tree / "changelog.d").glob("*.*.md")) == []
        # The seeded, hand-curated section survives underneath.
        assert "## 0.1.0 - 2026-07-30" in changelog

        assert run_towncrier(assembly_tree, "--version", "0.3.0", "--yes").returncode == 0
        rerendered = (assembly_tree / "CHANGELOG.md").read_text()
        assert rerendered.count("A thing exists.") == 1

    def test_breaking_changes_are_listed_first(self, assembly_tree: Path) -> None:
        """From scenario: Types render under their own headings."""
        (assembly_tree / "changelog.d" / "drop-x.breaking.md").write_text("X is gone.\n")
        (assembly_tree / "changelog.d" / "add-y.added.md").write_text("Y exists.\n")
        (assembly_tree / "changelog.d" / "fix-z.fixed.md").write_text("Z works.\n")

        result = run_towncrier(assembly_tree, "--draft", "--version", "0.2.0")

        assert result.returncode == 0, result.stderr
        assert (
            result.stdout.index("Breaking changes")
            < result.stdout.index("Added")
            < result.stdout.index("Fixed")
        )

    def test_internal_fragments_render_nowhere(self, assembly_tree: Path) -> None:
        """From scenario: Internal-only change passes with an unrendered fragment."""
        (assembly_tree / "changelog.d" / "refactor-bridge.internal.md").write_text(
            "Reworked the async bridge.\n"
        )
        (assembly_tree / "changelog.d" / "add-y.added.md").write_text("Y exists.\n")

        assert run_towncrier(assembly_tree, "--version", "0.2.0", "--yes").returncode == 0

        changelog = (assembly_tree / "CHANGELOG.md").read_text()
        assert "Y exists." in changelog
        assert "Reworked the async bridge." not in changelog
        assert "Internal" not in changelog

    def test_towncrier_leaves_internal_fragments_for_the_consume_step(
        self, assembly_tree: Path
    ) -> None:
        """Pins WHY `make changelog` has a third step.

        towncrier skips a fragment whose type it does not know — which is how
        `internal` renders nowhere — but it also never deletes what it skips,
        so without `--consume-internal` the fragment would stay pending after
        every release.
        """
        internal = assembly_tree / "changelog.d" / "refactor-bridge.internal.md"
        internal.write_text("Reworked the async bridge.\n")
        (assembly_tree / "changelog.d" / "add-y.added.md").write_text("Y exists.\n")

        run_towncrier(assembly_tree, "--version", "0.2.0", "--yes")
        assert internal.exists()

        removed = consume_internal(assembly_tree / "changelog.d")
        assert removed == ["refactor-bridge.internal.md"]
        assert not internal.exists()
        assert list((assembly_tree / "changelog.d").glob("*.*.md")) == []


class TestConsumeInternal:
    """From scenario: Assembly consumes fragments exactly once (internal half)."""

    def test_only_internal_fragments_are_removed(self, tmp_path: Path) -> None:
        (tmp_path / "keep.added.md").write_text("kept\n")
        (tmp_path / "README.md").write_text("docs\n")
        (tmp_path / "drop.internal.md").write_text("dropped\n")

        assert consume_internal(tmp_path) == ["drop.internal.md"]
        assert sorted(p.name for p in tmp_path.iterdir()) == ["README.md", "keep.added.md"]

    def test_consuming_twice_is_a_no_op(self, tmp_path: Path) -> None:
        (tmp_path / "drop.internal.md").write_text("dropped\n")
        assert consume_internal(tmp_path) == ["drop.internal.md"]
        assert consume_internal(tmp_path) == []

    def test_main_exposes_it_as_a_mode(self, tmp_path: Path) -> None:
        (tmp_path / "drop.internal.md").write_text("dropped\n")
        assert main(["--consume-internal", "--changelog-dir", str(tmp_path)]) == 0
        assert list(tmp_path.iterdir()) == []
