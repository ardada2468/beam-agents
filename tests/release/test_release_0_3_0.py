"""Release-artifact consistency gates for the 0.3.0 milestone (C35).

`scripts/check_release.py` already proves the *mechanical* release properties
(tag == `pyproject.toml` == `uv.lock`, fragment policy, ancestry). What it
cannot know is what this particular milestone promised to contain. The
`release-0-3` capability makes three checkable claims about the shipped
artifacts, and each has a quiet failure mode:

* the version bump lands but the changelog section does not enumerate the M2
  batch, so a user cannot tell what 0.3.0 is;
* an M2 dependency is named in the release notes that does not exist as an
  OpenSpec change at all (or one exists and was silently dropped from the
  notes);
* the benchmark comparison report is promised by the spec and never linked, so
  the release ships a claim with no artifact behind it.

The nine M2 change names are **sourced from the change's own proposal.md**,
not restated here, for the same reason
``tests/docs/test_upstream_design_doc.py`` sources its invariant phrases from
``openspec/project.md``: a list transcribed into a test drifts from the
document it is supposed to be checking, and the drift is invisible.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

from scripts.check_release import read_fragments

REPO_ROOT = Path(__file__).resolve().parents[2]
CHANGELOG = REPO_ROOT / "CHANGELOG.md"
PYPROJECT = REPO_ROOT / "pyproject.toml"
CHANGE_DIR = REPO_ROOT / "openspec" / "changes" / "add-0-3-0-release"
PROPOSAL = CHANGE_DIR / "proposal.md"
CHANGES_ROOT = REPO_ROOT / "openspec" / "changes"

RELEASE_VERSION = "0.3.0"
REPORT_RELPATH = "docs/benchmarks/0.3.0-vs-flink-agents.md"

#: `C26 `add-vllm-provider`` and friends, as the proposal writes them.
_M2_CITATION = re.compile(r"C(?P<id>2[6-9]|3[0-4])\s+`(?P<name>[a-z0-9-]+)`")


def _read(path: Path) -> str:
    assert path.is_file(), f"{path.relative_to(REPO_ROOT)} does not exist"
    return path.read_text(encoding="utf-8")


def m2_changes() -> dict[str, str]:
    """``{change-name: roadmap-id}`` for the nine M2 dependencies.

    Read out of ``proposal.md``'s prose so the set cannot drift from what the
    release promised. Nine is asserted, not assumed: a tenth citation (or a
    dropped one) is a proposal edit that must be noticed here.
    """
    found = {m.group("name"): f"C{m.group('id')}" for m in _M2_CITATION.finditer(_read(PROPOSAL))}
    assert len(found) == 9, (
        f"expected nine M2 dependencies cited in {PROPOSAL.name} as "
        f"``C<n> `change-name```, found {len(found)}: {sorted(found)}"
    )
    return found


def changelog_section(version: str) -> str:
    """The body of the ``## <version> - <date>`` section, up to the next one."""
    text = _read(CHANGELOG)
    match = re.search(
        rf"(?m)^##\s+{re.escape(version)}\s+-\s+\d{{4}}-\d{{2}}-\d{{2}}\s*$(?P<body>.*?)(?=^##\s|\Z)",
        text,
        re.DOTALL,
    )
    assert match is not None, (
        f"CHANGELOG.md has no dated '## {version} - YYYY-MM-DD' section; "
        "`make changelog VERSION=X.Y.Z` assembles it from changelog.d/"
    )
    return match.group("body")


# --- Requirement: the shipped version and changelog match the milestone --------


def _parts(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def _project_version() -> str:
    return str(tomllib.loads(_read(PYPROJECT))["project"]["version"])


def test_project_version_is_at_least_the_release_version() -> None:
    # Scenario: The shipped version and changelog match the milestone (the
    # metadata half). check_release.py compares the tag against this string;
    # this asserts the string is at least the one 0.3.0 promised.
    #
    # Amended by add-0-5-0-release (C43), which bumped the tree to 0.5.0 and
    # turned the original `== "0.3.0"` assertion red: a milestone's equality
    # with the *current* version is only true between its own bump and the
    # next one, so it was never the durable property. The floor is: the tree
    # never regresses below a milestone whose release notes are already
    # assembled. C43's own test carries the same shape, one milestone up.
    assert _parts(_project_version()) >= _parts(RELEASE_VERSION), (
        f"pyproject.toml [project].version reads {_project_version()!r}, below the "
        f"{RELEASE_VERSION} milestone this repository has already assembled release notes for"
    )


def test_lockfile_agrees_with_the_project_version() -> None:
    # The bump is only half-done without `uv lock`: uv.lock records the
    # project's own version and every CI job installs with `uv sync --locked`.
    # Stated as agreement with pyproject.toml rather than equality with 0.3.0
    # (same C43 amendment) — agreement is what `scripts/check_release.py`
    # enforces at tag time and what stays true across later bumps.
    lock = tomllib.loads(_read(REPO_ROOT / "uv.lock"))
    versions = [p.get("version") for p in lock["package"] if p.get("name") == "beam-agents"]
    assert versions == [_project_version()], (
        f"uv.lock records beam-agents {versions} but pyproject.toml declares "
        f"{_project_version()!r} — refresh the lockfile with `uv lock`"
    )


def test_changelog_section_enumerates_every_m2_change() -> None:
    # Scenario: The shipped version and changelog match the milestone (the
    # enumeration half). Every one of the nine M2 changes the release closes
    # out must be named in the section a reader of the release notes sees.
    section = changelog_section(RELEASE_VERSION)
    missing = sorted(name for name in m2_changes() if name not in section)
    assert not missing, (
        f"the {RELEASE_VERSION} changelog section does not name these M2 "
        f"changes: {missing} — the milestone section must enumerate all nine"
    )


def test_every_named_m2_change_exists_as_an_openspec_change() -> None:
    # Scenario: An unarchived M2 dependency blocks the release — the half a
    # test can render. A name in the release notes with no change folder behind
    # it (typo, renamed change) is a gate condition that can never be evidenced.
    # Accepts either state, live or archived, so this does not go red the day
    # the batch is archived.
    archived = {path.name for path in (CHANGES_ROOT / "archive").iterdir() if path.is_dir()}
    missing: list[str] = []
    for name in sorted(m2_changes()):
        if (CHANGES_ROOT / name).is_dir():
            continue
        if any(entry.endswith(f"-{name}") for entry in archived):
            continue
        missing.append(name)
    assert not missing, (
        f"M2 changes named by the release notes with no OpenSpec change folder "
        f"(live or archived): {missing}"
    )


def test_changelog_section_links_the_benchmark_comparison_report() -> None:
    # Scenario: The report ships with the release (the linkage half). The
    # `benchmark-comparison-report` capability requires the report be linked
    # from the 0.3.0 changelog section, and the target must resolve.
    section = changelog_section(RELEASE_VERSION)
    assert REPORT_RELPATH in section, (
        f"the {RELEASE_VERSION} changelog section must link {REPORT_RELPATH}; "
        "a published comparison that the release notes do not reference is a "
        "claim with no artifact behind it"
    )
    assert (REPO_ROOT / REPORT_RELPATH).is_file(), f"{REPORT_RELPATH} does not exist"


# --- Requirement: feedback dispositions are recorded, never omitted ------------


def test_changelog_section_records_the_feedback_dispositions() -> None:
    # Scenario: Zero feedback is recorded, not omitted. An absent disposition
    # table is a process failure even when the intake list is empty, so the
    # section must carry the heading either way.
    section = changelog_section(RELEASE_VERSION)
    assert re.search(r"(?mi)^###\s+Design-partner feedback\b", section), (
        f"the {RELEASE_VERSION} section must carry a '### Design-partner "
        "feedback' subsection recording every item's disposition — an empty "
        "table is a disposition, an absent table is a process failure"
    )
    assert re.search(r"(?i)release-blocking", section), (
        "the disposition record must name the release-blocking bucket of the "
        "triage rubric so the bar a reader is being told about is the specced one"
    )


def test_changelog_section_records_the_release_gate_checklist() -> None:
    # Scenario: A fully green gate opens the release. Design D5 requires the
    # evaluated checklist — every condition with its evidence — to be recorded
    # in the release notes, not merely asserted to have passed.
    section = changelog_section(RELEASE_VERSION)
    assert re.search(r"(?mi)^###\s+Release gate\b", section), (
        f"the {RELEASE_VERSION} section must record the evaluated release-gate "
        "checklist (condition, evidence, verdict) per design D5"
    )
    for condition in ("conformance", "benchmark", "M2"):
        assert re.search(condition, section, re.IGNORECASE), (
            f"the recorded gate checklist does not mention the {condition!r} "
            "condition; the gate is evaluated and recorded as a whole"
        )


def test_no_published_fragment_is_still_pending() -> None:
    # `make changelog` consumes every pending fragment, including the
    # unrendered `internal` ones (add-0-1-0-release Revision 2). A fragment
    # that was rendered into the section *and* is still sitting in changelog.d/
    # would be published twice. Stated as "published implies consumed" rather
    # than "changelog.d/ is empty": fragments accumulate again the moment the
    # next change lands, and a test that goes red on the first post-release
    # commit is a test that gets deleted.
    section = changelog_section(RELEASE_VERSION)
    republished = sorted(
        f"{fragment.name}.{fragment.type}.md"
        for fragment in read_fragments(REPO_ROOT / "changelog.d")
        if f"({fragment.name})" in section
    )
    assert republished == [], (
        f"these fragments were rendered into the {RELEASE_VERSION} section and "
        f"are still pending in changelog.d/: {republished} — assembly consumes "
        "each exactly once"
    )
