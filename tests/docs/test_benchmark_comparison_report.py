"""Structural gates for the published beam-agents vs. Apache Flink Agents report.

`docs/benchmarks/0.3.0-vs-flink-agents.md` is the one artifact of this release
whose failure mode is *credibility*, not breakage. The `benchmark-comparison-
report` capability's requirements are mostly prose obligations a reviewer must
judge, but four of them are mechanizable, and each corresponds to a way the
report can quietly become dishonest:

* the beam-agents leg stops naming a real C33 harness scenario, so the workload
  is one invented for the comparison (``test_report_names_a_real_c33_scenario``);
* the methodology stops enumerating a non-equivalence, or stops saying which
  side it favors, so the reader is handed a comparison that looks like-for-like
  and is not (``test_methodology_enumerates_every_non_equivalence``);
* a number appears in the report that no artifact in this repository backs —
  the single most damaging thing a competitive benchmark can do, and the exact
  hazard `docs/benchmarks.md` warns about when it forbids seeding baselines
  from developer hardware (``test_no_measurement_is_stated_while_the_runs_are_
  pending``, ``test_no_figure_in_the_report_is_invented``);
* the frozen-at-publication rule (design D4) stops being stated, so a later
  favorable optimization can be silently edited in
  (``test_report_states_the_freeze_rule``).

The number rule is modeled on ``tests/docs/test_upstream_design_doc.py``: a
figure carrying a unit is a measurement claim unless the repository already
states it. Because `benchmark-baseline.toml`'s ``[medians_ms]`` is deliberately
unseeded (no CI-hardware run exists yet), the report ships with its measurement
tables marked pending, and this module is what keeps a placeholder from
hardening into a published figure.
"""

from __future__ import annotations

import re
from pathlib import Path

from scripts.bench_gate import EXPECTED_RESULTS, GATED_BENCHMARK

REPO_ROOT = Path(__file__).resolve().parents[2]
REPORT = REPO_ROOT / "docs" / "benchmarks" / "0.3.0-vs-flink-agents.md"

#: The marker every not-yet-measured cell carries. One spelling, so a table
#: cell either says this or is a published number — there is no third state.
PENDING = "pending (CI hardware)"


def _read(path: Path) -> str:
    assert path.is_file(), f"{path.relative_to(REPO_ROOT)} does not exist"
    return path.read_text(encoding="utf-8")


def _normalize(text: str) -> str:
    stripped = text.replace("`", "").replace("*", "").replace("\u00a0", " ")
    return re.sub(r"\s+", " ", stripped).strip().lower()


def _section(text: str, heading: str) -> str:
    """The body of the ``##``-level section whose title is exactly ``heading``.

    Exact, not substring: "## Status: why the results tables are empty" would
    otherwise shadow "## Results" and the pending-cell assertion would pass by
    reading the wrong section.
    """
    sections = re.split(r"(?m)^##\s+", text)
    matches = [
        section
        for section in sections[1:]
        if section.splitlines()[0].strip().lower() == heading.lower()
    ]
    assert matches, f"no '## {heading}' section found in the report"
    return matches[0]


def _bench_names() -> set[str]:
    """Every benchmark name `scripts/bench_gate.py` declares — the C33 scenario set."""
    return {name for names in EXPECTED_RESULTS.values() for name in names}


# --- Requirement: the report ships and is anchored to a real C33 scenario ------


def test_report_exists_and_names_the_release_it_belongs_to() -> None:
    # Scenario: The report ships with the release. Design D4 makes the file a
    # versioned, release-named artifact rather than a living page.
    report = _read(REPORT)
    assert "0.3.0" in report, "the report must name the release it is frozen against"


def test_report_names_a_real_c33_scenario() -> None:
    # Scenario: The workload comes from the C33 scenario set. The named
    # scenario is checked against what `scripts/bench_gate.py` actually
    # declares, so renaming a benchmark out from under the report fails here
    # rather than leaving the report pointing at nothing.
    report = _read(REPORT)
    named = sorted(name for name in _bench_names() if name in report)
    assert named, (
        "the report names no benchmark from the C33 harness; the beam-agents "
        "leg must run a scenario declared in scripts/bench_gate.py, identified "
        f"by name (declared: {sorted(_bench_names())})"
    )
    assert GATED_BENCHMARK in named, (
        f"the primary leg must be the gated scenario {GATED_BENCHMARK!r} — the "
        "one the release budget is rendered on — not an ungated neighbour; "
        f"named: {named}"
    )
    assert "benchmarks/bench_overhead_tiers.py" in report, (
        "the report must cite the harness module its primary scenario lives in "
        "so a third party can locate the code that produced the numbers"
    )


def test_report_states_the_equal_cost_fake_model_rule() -> None:
    # Scenario: Model latency is excluded from the comparison.
    report = _normalize(_read(REPORT))
    for phrase in ("excluding LLM and tool time", "FakeLLM"):
        assert _normalize(phrase) in report, (
            f"the report must state {phrase!r}: both legs run a scripted fake "
            "model of equal cost, so the figures read as runtime overhead"
        )


# --- Requirement: the methodology discloses its limits and is reproducible -----

#: (label, phrase the methodology must carry). Sourced from the
#: `benchmark-comparison-report` spec's "at minimum" list, which is itself
#: design D3's enumeration.
NON_EQUIVALENCES: tuple[tuple[str, str], ...] = (
    ("language runtime", "language runtime"),
    ("effect model", "effect model"),
    ("state backend / checkpointing", "state backend"),
)


def test_methodology_enumerates_every_non_equivalence() -> None:
    # Scenario: The methodology section enumerates non-equivalences. Each entry
    # must also say which side it favors — an enumeration without a direction
    # is a disclosure the reader cannot use.
    methodology = _normalize(_section(_read(REPORT), "Methodology"))
    missing = [label for label, phrase in NON_EQUIVALENCES if _normalize(phrase) not in methodology]
    assert not missing, (
        f"the methodology section does not enumerate these non-equivalences: {missing}"
    )
    assert methodology.count("favors") >= len(NON_EQUIVALENCES), (
        "every enumerated non-equivalence must state which side it structurally "
        f"favors; found {methodology.count('favors')} 'favors' statements for "
        f"{len(NON_EQUIVALENCES)} required dimensions"
    )


def test_methodology_pins_the_measured_versions() -> None:
    # Scenario: The methodology section enumerates non-equivalences (the
    # reproducibility half): every framework in the comparison is pinned, and
    # anything not yet pinnable says so rather than being omitted.
    methodology = _read(REPORT)
    for component in (
        "beam-agents",
        "Apache Flink Agents",
        "Apache Flink",
        "Apache Beam",
        "Python",
    ):
        assert component in methodology, (
            f"the version table must pin {component!r}; an unpinned component "
            "makes the run unreproducible"
        )


def test_every_relative_link_in_the_report_resolves() -> None:
    broken: list[str] = []
    for target in re.findall(r"\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)", _read(REPORT)):
        if target.startswith(("http://", "https://", "mailto:", "#")):
            continue
        if not (REPORT.parent / target.split("#", 1)[0]).resolve().exists():
            broken.append(target)
    assert not broken, f"relative links in the report resolving to nothing: {broken}"


def test_report_states_the_freeze_rule() -> None:
    # Scenario: The published report is frozen. Without the rule written into
    # the artifact itself, a later release's optimization can be edited in and
    # every published number stops being attributable to one configuration.
    report = _normalize(_read(REPORT))
    assert _normalize("frozen") in report and _normalize("never") in report, (
        "the report must state that it is frozen at publication and that later "
        "performance changes appear only in a later release's report (design D4)"
    )


# --- Requirement: no number without a run --------------------------------------

# Same closed unit list as tests/docs/test_upstream_design_doc.py: `s` and `h`
# would match ordinary prose.
_MEASUREMENT = re.compile(r"(?<![\w.])(\d+(?:\.\d+)?)\s*(ms|KiB|MiB|GiB|%|\u00d7)(?![\w])")

# The standing budget and the harness's own pinned constants are *declarations*
# the repository already makes, not measurements this report produced.
GROUNDING_SOURCES: tuple[Path, ...] = (
    REPO_ROOT / "openspec" / "project.md",
    REPO_ROOT / "docs" / "benchmarks.md",
    REPO_ROOT / "benchmark-baseline.toml",
    REPO_ROOT / "benchmarks" / "_harness.py",
)


def test_no_figure_in_the_report_is_invented() -> None:
    # Every figure carrying a unit must already be stated by an artifact in this
    # repository. `benchmark-baseline.toml`'s [medians_ms] is deliberately
    # unseeded and `docs/benchmarks.md` forbids seeding from developer hardware,
    # so a measured figure appearing here could only have come from a machine
    # whose numbers this project has already ruled inadmissible.
    corpus = _normalize("".join(path.read_text(encoding="utf-8") for path in GROUNDING_SOURCES))
    ungrounded = [
        f"{value} {unit}"
        for value, unit in _MEASUREMENT.findall(_read(REPORT))
        if _normalize(f"{value} {unit}") not in corpus
        and _normalize(f"{value}{unit}") not in corpus
    ]
    assert not ungrounded, (
        "figures stated in the comparison report that no artifact in this "
        f"repository backs: {ungrounded}. Until a CI-hardware run seeds "
        "benchmark-baseline.toml, every measurement cell must read "
        f"{PENDING!r}."
    )


def test_results_tables_are_marked_pending_not_filled_with_placeholders() -> None:
    # Scenario: An unfavorable result is published unfiltered — enforced from
    # the other end. Before any run exists, the honest table has no numbers at
    # all: every measurement cell of every results table says so explicitly, so
    # a placeholder can never be mistaken for (or quietly promoted to) a result.
    results = _section(_read(REPORT), "Results")
    tables: list[list[str]] = []
    for line in results.splitlines():
        if line.strip().startswith("|"):
            if not tables or not tables[-1]:
                tables.append([])
            tables[-1].append(line.strip())
        elif tables and tables[-1]:
            tables.append([])
    tables = [table for table in tables if table]
    assert tables, "the report has no results table"
    unmarked: list[str] = []
    for table in tables:
        # [0] is the header row, [1] the alignment rule; the rest are data.
        for row in table[2:]:
            if PENDING not in row:
                unmarked.append(row)
    assert not unmarked, (
        f"every results-table data row must carry {PENDING!r} until a "
        f"CI-hardware run exists; unmarked rows: {unmarked}"
    )


def test_report_states_why_the_tables_are_empty() -> None:
    # The pending marker without its reason is indistinguishable from an
    # unfinished draft. The report must say what unblocks it.
    report = _normalize(_read(REPORT))
    for phrase in ("benchmark-baseline.toml", "developer hardware"):
        assert _normalize(phrase) in report, (
            f"the report must state {phrase!r} when explaining why its "
            "measurement tables are unpopulated"
        )
