"""Release-artifact consistency gates for the 0.5.0 milestone (C43).

`scripts/check_release.py` proves the *mechanical* release properties (tag ==
`pyproject.toml` == `uv.lock`, fragment policy, ancestry) and
`tests/release/test_release_0_3_0.py` does the same job for the previous
milestone. What neither can know is what *this* milestone promised to contain.
The `release-0-5` capability makes three checkable claims, and each has a quiet
failure mode:

* the version bump lands but the changelog section does not enumerate the M3
  adoption surface, so a user cannot tell what 0.5.0 is;
* an M3 dependency is named in the release notes that does not exist as an
  OpenSpec change at all (a typo, or a renamed change), so the gate condition
  it stands for can never be evidenced;
* the recorded gate verdict drifts from the repository state it describes —
  the notes say the archival condition is satisfied while change folders are
  still live, which is the one failure mode a release gate must not have.

The seven M3 change names are **sourced from the change's own proposal.md**,
not restated here, for the same reason ``test_release_0_3_0.py`` sources the M2
names from its proposal: a list transcribed into a test drifts from the
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
CHANGE_DIR = REPO_ROOT / "openspec" / "changes" / "add-0-5-0-release"
PROPOSAL = CHANGE_DIR / "proposal.md"
CHANGES_ROOT = REPO_ROOT / "openspec" / "changes"

RELEASE_VERSION = "0.5.0"

#: The proposal's Impact section states the batch as a single sentence:
#: ``**Depends on** — … before this change can complete: `a`, `b`, … `g`.``
#: Everything up to that sentence's terminating period is the batch; the
#: `add-0-1-0-release` reference in the *next* sentence deliberately is not.
_DEPENDS_ON = re.compile(r"\*\*Depends on\*\*[^:]*:(?P<names>[^.]*)\.", re.DOTALL)
_CHANGE_NAME = re.compile(r"`(?P<name>[a-z0-9-]+)`")

#: The verdict marker the gate table uses when the archival condition is unmet.
_PENDING_ARCHIVAL = "pending (archival)"


def _read(path: Path) -> str:
    assert path.is_file(), f"{path.relative_to(REPO_ROOT)} does not exist"
    return path.read_text(encoding="utf-8")


def m3_changes() -> list[str]:
    """The seven M3 dependency change names, read out of ``proposal.md``.

    Seven is asserted, not assumed: an eighth name (or a dropped one) is a
    proposal edit that changes what the release is, and must be noticed here.
    """
    sentence = _DEPENDS_ON.search(_read(PROPOSAL))
    assert sentence is not None, (
        f"{PROPOSAL.name} must state the batch as a '**Depends on** — …: `a`, `b`, …' "
        "sentence; the release gate's dependency list is sourced from it"
    )
    found = sorted({m.group("name") for m in _CHANGE_NAME.finditer(sentence.group("names"))})
    assert len(found) == 7, (
        f"expected seven M3 dependencies cited in {PROPOSAL.name}'s "
        f"'Depends on' sentence, found {len(found)}: {found}"
    )
    return found


def archived_names() -> set[str]:
    """Change names that have an ``openspec/changes/archive/<date>-<name>`` entry."""
    archive = CHANGES_ROOT / "archive"
    return {path.name for path in archive.iterdir() if path.is_dir()}


def is_archived(name: str) -> bool:
    return any(entry.endswith(f"-{name}") for entry in archived_names())


def verdicts(section: str) -> list[str]:
    """The verdict cell of every ``| condition | evidence | verdict |`` row.

    Read from the rows themselves, not by substring: the section's own prose
    names the pending marker while explaining the check, and a bare ``in``
    test would let a row silently flip to ``pass`` and still be satisfied by
    that sentence (verified — the substring form had no teeth).
    """
    found: list[str] = []
    for line in section.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if len(cells) == 3 and not set(cells[2]) <= set("- :"):
            found.append(cells[2])
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


# --- Requirement: the released artifact carries 0.5.0 and a complete changelog ---


def _parts(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def project_version() -> str:
    return str(tomllib.loads(_read(PYPROJECT))["project"]["version"])


def test_project_version_is_at_least_the_release_version() -> None:
    # Scenario: Version and changelog agree at tag time (the metadata half).
    # check_release.py compares the tag against this string; this asserts the
    # string is at least the one 0.5.0 promised.
    #
    # A milestone test asserting *equality* with its own version is only true
    # between its bump and the next one — `test_release_0_3_0.py` shipped that
    # form and went red the moment this change bumped past it (tasks.md
    # Revision 2). The durable property is a floor: the tree never regresses
    # below a milestone it has already assembled release notes for, and each
    # later milestone's own test raises the floor again.
    assert _parts(project_version()) >= _parts(RELEASE_VERSION), (
        f"pyproject.toml [project].version reads {project_version()!r}, below the "
        f"{RELEASE_VERSION} milestone this repository has already assembled release notes for"
    )


def test_lockfile_agrees_with_the_project_version() -> None:
    # The bump is only half-done without `uv lock`: uv.lock records the
    # project's own version and every CI job installs with `uv sync --locked`,
    # so a bump without a lock refresh fails every job with a resolution error.
    # Stated as agreement with pyproject.toml rather than equality with 0.5.0,
    # for the same durability reason as above — this is the invariant
    # `scripts/check_release.py` actually enforces at tag time.
    lock = tomllib.loads(_read(REPO_ROOT / "uv.lock"))
    versions = [p.get("version") for p in lock["package"] if p.get("name") == "beam-agents"]
    assert versions == [project_version()], (
        f"uv.lock records beam-agents {versions} but pyproject.toml declares "
        f"{project_version()!r} — refresh the lockfile with `uv lock`"
    )


def test_changelog_section_enumerates_every_m3_change() -> None:
    # Scenario: Version and changelog agree at tag time (the enumeration half).
    # Every one of the seven M3 changes the release closes out must be named in
    # the section a reader of the release notes sees. Three of the seven landed
    # with no changelog fragment at all, so mechanical assembly alone cannot
    # satisfy this — which is exactly why it is asserted.
    section = changelog_section(RELEASE_VERSION)
    missing = [name for name in m3_changes() if name not in section]
    assert not missing, (
        f"the {RELEASE_VERSION} changelog section does not name these M3 "
        f"changes: {missing} — the milestone section must enumerate all seven"
    )


def test_every_named_m3_change_exists_as_an_openspec_change() -> None:
    # Scenario: A dependency change is still pending — the half a test can
    # render. A name in the release notes with no change folder behind it
    # (typo, renamed change) is a gate condition that can never be evidenced.
    # Accepts either state, live or archived, so this does not go red the day
    # the batch is archived.
    missing = [
        name
        for name in m3_changes()
        if not (CHANGES_ROOT / name).is_dir() and not is_archived(name)
    ]
    assert not missing, (
        f"M3 changes named by the release notes with no OpenSpec change folder "
        f"(live or archived): {missing}"
    )


# --- Requirement: release-blocking gates are green on the release commit -------


def test_changelog_section_records_the_release_gate_checklist() -> None:
    # Scenarios: A dependency change is still pending / Conformance matrix is
    # green on the release commit / Benchmark gate is green on the release
    # commit. The gate is evaluated as a whole and recorded with its evidence,
    # not merely asserted to have passed.
    section = changelog_section(RELEASE_VERSION)
    assert re.search(r"(?mi)^###\s+Release gate\b", section), (
        f"the {RELEASE_VERSION} section must record the evaluated release-gate "
        "checklist (condition, evidence, verdict)"
    )
    for condition in ("archiv", "conformance", "benchmark", "M3"):
        assert re.search(condition, section, re.IGNORECASE), (
            f"the recorded gate checklist does not mention the {condition!r} "
            "condition; the gate is evaluated and recorded as a whole"
        )


def test_recorded_archival_verdict_matches_the_repository() -> None:
    # Scenario: A dependency change is still pending → the gate fails and no
    # tag is created. The one failure mode a release gate must not have is
    # notes that claim a condition is met while the tree says otherwise, so the
    # recorded verdict is checked against the archive directory in both
    # directions: unarchived means the section says so *and* says the tag was
    # not cut; fully archived means the pending marker is gone.
    section = changelog_section(RELEASE_VERSION)
    recorded = verdicts(section)
    assert recorded, f"the {RELEASE_VERSION} gate table has no verdict rows to check"
    unarchived = [name for name in m3_changes() if not is_archived(name)]
    if unarchived:
        assert _PENDING_ARCHIVAL in recorded, (
            f"these M3 changes are not archived: {unarchived} — the gate table "
            f"must record the archival condition as {_PENDING_ARCHIVAL!r}, not as met; "
            f"recorded verdicts are {recorded}"
        )
        assert re.search(rf"`v{re.escape(RELEASE_VERSION)}`\s+is not tagged", section), (
            "an unmet archival condition means the release slips; the section "
            f"must state that `v{RELEASE_VERSION}` is not tagged"
        )
    else:
        assert _PENDING_ARCHIVAL not in recorded, (
            "every M3 change is archived now — the gate table still records "
            f"{_PENDING_ARCHIVAL!r} and must be re-evaluated before tagging"
        )


def test_no_published_fragment_is_still_pending() -> None:
    # `make changelog` consumes every pending fragment, including the
    # unrendered `internal` ones. A fragment rendered into the section *and*
    # still sitting in changelog.d/ would be published twice. Stated as
    # "published implies consumed" rather than "changelog.d/ is empty":
    # fragments accumulate again the moment the next change lands.
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
