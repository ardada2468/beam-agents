"""Consistency gates for the upstreaming artifacts under ``docs/design/``.

The two documents this module guards — the `apache_beam.ml.agents` design
document and the dev@beam.apache.org thread plan — make claims *about this
repository* to an audience that cannot check them against it. Prose quality is
a review concern; what is mechanizable is exactly the set of ways those claims
can quietly stop being true:

* an invariant is dropped or paraphrased into something weaker than
  ``openspec/project.md`` says;
* a relative link rots when a file moves;
* a quantitative figure appears with no artifact behind it, while the
  thread-ready checklist still says the artifact is pending;
* the move/stay decision record silently omits a module the runtime grew;
* the objections register shrinks below the minimum set design D4 fixed.

Every phrase this module requires the design document to carry is first
asserted to occur in ``openspec/project.md`` (``test_invariant_phrases_are_
sourced_from_the_constitution``), so the phrase list cannot drift into being a
restatement of whatever the document happens to say. That assertion is what
makes this a consistency test rather than a tautology.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DESIGN_DIR = REPO_ROOT / "docs" / "design"
DESIGN_DOC = DESIGN_DIR / "apache-beam-ml-agents.md"
THREAD_PLAN = DESIGN_DIR / "apache-beam-ml-agents-thread-plan.md"
PROJECT_MD = REPO_ROOT / "openspec" / "project.md"

UPSTREAM_DOCS = (DESIGN_DOC, THREAD_PLAN)


def _normalize(text: str) -> str:
    """Markdown emphasis and code spans out, whitespace collapsed, lowercased.

    Both corpora are prose about the same statements written for different
    audiences, so a comparison has to be insensitive to ``**bold**`` and
    ``` `code` ``` decoration and to line wrapping — and to nothing else. In
    particular no word is dropped or stemmed: a weakened restatement must still
    fail.
    """
    stripped = text.replace("`", "").replace("*", "").replace("\u00a0", " ")
    return re.sub(r"\s+", " ", stripped).strip().lower()


# --- The seven correctness invariants ------------------------------------------
#
# Sourced by reading `openspec/project.md` §Architecture → "Correctness
# invariants" (seven numbered entries), NOT by reading the design document.
# Each tuple is (invariant name, load-bearing phrases the document must carry).
# The phrases are the ones whose removal would weaken the claim: an invariant
# restated without them is a different, softer promise.

INVARIANT_PHRASES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "atomic commit",
        (
            "commit atomically with the Beam bundle",
            "A failed/timed-out activation mutates nothing",
        ),
    ),
    (
        "deterministic intent IDs",
        (
            "uuid5(NAMESPACE, key + seq + step_index)",
            "byte-identical intents",
            "the effector dedups on intent_id",
        ),
    ),
    (
        "replay cache",
        (
            "sha256(model_id, canonical_json(messages), tools_schema, sampling_params, key, seq)",
            "Bundle retries must incur ZERO additional provider calls on the cached path",
        ),
    ),
    (
        "per-key serialization",
        (
            "process one element at a time per key",
            "Memory is race-free by construction",
        ),
    ),
    (
        "side effects only via intents",
        (
            "calling a side_effect=True tool directly raises",
            "ctx.act(...) is the only effect path",
        ),
    ),
    (
        "fail-closed timeouts",
        (
            "Timeouts fail closed at both layers",
            "effector refuses expired intents",
            "Late results are dropped as orphaned_result",
        ),
    ),
    (
        "protobuf-only state",
        (
            "State is protobuf, never pickle",
            "additive proto changes only",
            "state_schema_version",
        ),
    ),
)

# The standing latency budget is a *threshold* declared in the constitution, not
# a measurement, so the evidence section may name it while its benchmark
# artifact is still pending. Pinned to the constitution's exact wording (and
# asserted to occur there) so the carve-out cannot be widened by rewording.
BUDGET_PHRASES: tuple[str, ...] = ("p50 < 15 ms", "p99 < 60 ms")

# A figure with a unit is a measurement claim unless it is one of the design
# constants the constitution already fixes. `_UNITS` is deliberately a closed,
# unambiguous list: `s` and `h` would match ordinary prose.
_MEASUREMENT = re.compile(r"(?<![\w.])(\d+(?:\.\d+)?)\s*(ms|KiB|MiB|GiB|%|\u00d7)(?![\w])")


def _read(path: Path) -> str:
    assert path.is_file(), f"{path.relative_to(REPO_ROOT)} does not exist"
    return path.read_text(encoding="utf-8")


def _section(text: str, heading_contains: str) -> str:
    """The body of the first ``##``-level section whose title matches."""
    sections = re.split(r"(?m)^##\s+", text)
    matches = [
        section
        for section in sections[1:]
        if heading_contains.lower() in section.splitlines()[0].lower()
    ]
    assert matches, f"no '## ...{heading_contains}...' section found"
    return matches[0]


# --- Requirement: the design document states the invariants without weakening --


def test_invariant_phrases_are_sourced_from_the_constitution() -> None:
    # The guard that keeps the rest of this module honest: every phrase the
    # design document is held to must be one `openspec/project.md` actually
    # makes. A phrase that drifted out of the constitution fails here first,
    # naming itself, rather than silently relaxing the document's bar.
    constitution = _normalize(_read(PROJECT_MD))
    missing = [
        f"{name}: {phrase!r}"
        for name, phrases in INVARIANT_PHRASES
        for phrase in phrases
        if _normalize(phrase) not in constitution
    ]
    missing += [
        f"budget: {phrase!r}" for phrase in BUDGET_PHRASES if _normalize(phrase) not in constitution
    ]
    assert not missing, (
        "phrases required of the design document no longer appear in "
        f"openspec/project.md; re-source them from the constitution: {missing}"
    )


def test_design_document_carries_all_seven_correctness_invariants() -> None:
    # Scenario: Dropping an invariant from the doc fails the build.
    doc = _normalize(_read(DESIGN_DOC))
    missing: list[str] = []
    for name, phrases in INVARIANT_PHRASES:
        absent = [phrase for phrase in phrases if _normalize(phrase) not in doc]
        if absent:
            missing.append(f"{name} (missing {absent})")
    assert not missing, (
        "the design document must state all seven correctness invariants at "
        f"no less than the constitution's force; weakened or absent: {missing}"
    )


def test_design_document_states_the_effectively_once_boundary_honestly() -> None:
    # Scenario: The effectively-once boundary is stated honestly. An
    # unconditional exactly-once claim is the failure mode; the boundary is the
    # crash window plus the intent_id-idempotency precondition.
    doc = _normalize(_read(DESIGN_DOC))
    for phrase in (
        "idempotent on intent_id",
        "crash window",
    ):
        assert _normalize(phrase) in doc, (
            f"the effectively-once section must state {phrase!r}; without it the "
            "document claims a stronger guarantee than the runtime delivers"
        )


# --- Requirement: links between the documents and the repo resolve -------------

_LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")


def test_every_relative_link_in_the_upstream_documents_resolves() -> None:
    # A design document sent to a mailing list is read at the commit it was
    # sent from; a link that rotted before then is a correction on a public
    # thread. Absolute URLs are out of scope (nothing in-repo can check them).
    broken: list[str] = []
    for doc in UPSTREAM_DOCS:
        for target in _LINK.findall(_read(doc)):
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            resolved = (doc.parent / target.split("#", 1)[0]).resolve()
            if not resolved.exists():
                broken.append(f"{doc.name} -> {target}")
    assert not broken, f"relative links resolving to nothing: {broken}"


# --- Requirement: no number without an artifact --------------------------------


def test_evidence_section_states_no_measurement_while_artifacts_are_pending() -> None:
    # Scenario: A placeholder number cannot ride to thread-readiness.
    evidence = _section(_read(DESIGN_DOC), "Evidence")
    pending = re.findall(r"(?m)^\s*-\s*\[ \]\s*(.+)$", evidence)
    scanned = evidence
    for phrase in BUDGET_PHRASES:
        scanned = scanned.replace(phrase, "<standing budget>")
    claims = [f"{value} {unit}" for value, unit in _MEASUREMENT.findall(scanned)]
    if pending:
        assert not claims, (
            f"the evidence section states measurements {claims} while its "
            f"thread-ready checklist still marks {len(pending)} artifact(s) "
            f"pending ({pending}); a figure with no artifact behind it must not "
            "reach the dev@ thread"
        )


def test_no_figure_in_the_upstream_documents_is_invented() -> None:
    # The doc-wide half of the same rule: every figure carrying a unit must
    # already be stated somewhere in the repository it describes. This is what
    # makes "distillation" checkable — a number that appears only in the
    # distillation was invented by it.
    corpus = _normalize(
        _read(PROJECT_MD)
        + _read(REPO_ROOT / "pyproject.toml")
        + "".join(path.read_text(encoding="utf-8") for path in sorted(REPO_ROOT.glob("docs/*.md")))
    )
    ungrounded: list[str] = []
    for doc in UPSTREAM_DOCS:
        for value, unit in _MEASUREMENT.findall(_read(doc)):
            figure = f"{value} {unit}"
            if _normalize(figure) not in corpus and _normalize(f"{value}{unit}") not in corpus:
                ungrounded.append(f"{doc.name}: {figure}")
    assert not ungrounded, (
        "figures stated in the upstreaming documents that appear nowhere in "
        f"openspec/project.md, pyproject.toml or docs/*.md: {ungrounded}"
    )


# --- Requirement: the move/stay decision record dispositions every module ------


def _top_level_modules() -> set[str]:
    package = REPO_ROOT / "src" / "beam_agents"
    names: set[str] = set()
    for entry in sorted(package.iterdir()):
        if entry.name in {"__init__.py", "py.typed", "__pycache__"}:
            continue
        if entry.is_dir() and (entry / "__init__.py").is_file():
            names.add(entry.name)
        elif entry.suffix == ".py":
            names.add(entry.stem)
    assert names, "no top-level modules discovered under src/beam_agents"
    return names


def test_decision_record_dispositions_every_top_level_module() -> None:
    # Scenario: Every module has an explicit disposition. Sourced from the
    # filesystem, so a module added after the document was written fails here
    # instead of being silently excluded from what is being offered to Beam.
    record = _normalize(_section(_read(DESIGN_DOC), "would move"))
    omitted = sorted(name for name in _top_level_modules() if name.lower() not in record)
    assert not omitted, (
        f"the move/stay decision record omits {omitted}; every top-level module "
        "of src/beam_agents must carry an explicit disposition and rationale"
    )


def test_decision_record_keeps_the_effector_outside_the_donation() -> None:
    # Scenario: The effector boundary is a recorded decision.
    record = _normalize(_section(_read(DESIGN_DOC), "would move"))
    for phrase in ("stays external", "intent/result protobuf contract"):
        assert _normalize(phrase) in record, (
            f"the effector entry must state {phrase!r}: the decision is that "
            "Beam standardizes the contract, not the service"
        )


# --- Requirement: the thread plan pairs an announcement with a register --------

_REGISTER_ENTRY = re.compile(r"(?m)^###\s+(O\d+\s*[—-]\s*.+)$")

# Design D4's minimum register population, as (topic label, marker that must
# appear in some entry's heading). Decided in design.md so drafting cannot
# quietly shrink the register.
REQUIRED_OBJECTIONS: tuple[tuple[str, str], ...] = (
    ("stateful DoFn vs. SDF", "SDF"),
    ("RunInference overlap", "RunInference"),
    ("inline durable execution", "durable execution"),
    ("cross-language scope", "Python-only"),
    ("dependency policy", "dependenc"),
    ("pipeline update / state compatibility", "--update"),
    ("maintainership", "maintain"),
    ("governance and donation mechanics", "donation"),
)

ANSWER_MARKERS = ("**Answer.**", "**Open — asking the thread.**")


def test_objections_register_covers_the_required_minimum_topics() -> None:
    # Scenario: The minimum objection set is present.
    headings = _REGISTER_ENTRY.findall(_read(THREAD_PLAN))
    assert headings, "no '### O<n> — ...' objection entries found in the thread plan"
    joined = " ".join(headings).lower()
    missing = [label for label, marker in REQUIRED_OBJECTIONS if marker.lower() not in joined]
    assert not missing, f"objections register is missing entries for: {missing}"


def test_every_objection_entry_is_answered_or_explicitly_open() -> None:
    # Scenario: Every register entry is answered or honestly open. "Filler" is
    # approximated by length: an entry too short to be an argument is one.
    text = _read(THREAD_PLAN)
    bodies = re.split(r"(?m)^###\s+", text)[1:]
    unanswered: list[str] = []
    for body in bodies:
        heading = body.splitlines()[0]
        if not heading.startswith("O") or not heading[1:2].isdigit():
            continue
        if not any(marker in body for marker in ANSWER_MARKERS) or len(body) < 200:
            unanswered.append(heading)
    assert not unanswered, (
        "every objection must carry '**Answer.**' or '**Open — asking the "
        f"thread.**' with substance behind it; blank or filler: {unanswered}"
    )


def test_thread_plan_blocks_the_announcement_on_the_evidence_checklist() -> None:
    # Scenario: Announcement readiness is gated on evidence — the thread plan
    # half. Sending before the 0.3 artifacts land is the failure this prevents.
    plan = _normalize(_read(THREAD_PLAN))
    assert _normalize("thread-ready checklist") in plan, (
        "the thread plan must name the design document's thread-ready checklist "
        "as the precondition for sending the announcement"
    )
