"""Tripwire over `docs/state-compat.md`: the published contract keeps its shape.

The compatibility table *is* the doc's contract (design D2 of
add-state-guarantees), so a table that silently loses a row — or flips a
verdict — would weaken a published promise with no test going red. This module
is the cheap guard: it parses the table out of the markdown and asserts one row
per spec-enumerated change class, with the verdict each row is required to
carry.

Derived from the `state-guarantees` scenarios "The table classifies a safe
change and a breaking change differently" and "A graph-shape change is
classified even though no bytes change", plus the requirement's explicit
not-promised list.

Offline and dependency-free: it reads a file and matches text. Nothing here
touches Beam, GCP, or the harness.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DOC = REPO_ROOT / "docs" / "state-compat.md"

# The eight change classes the spec enumerates ("at minimum"), each keyed by a
# case-insensitive pattern matched against the table's first column, with the
# verdict its `readable` and `--update` columns must carry. `via migration` is
# spelled out rather than collapsed to yes/no: a bumped blob is readable only
# because a registered migration makes it so, and flattening that to "yes"
# would hide the obligation.
REQUIRED_ROWS: dict[str, tuple[str, str, str]] = {
    "additive field": (r"add .*optional .*field", "yes", "yes"),
    "enum value": (r"add .*enum value", "yes", "yes"),
    "new state spec": (r"add .*new state spec", "yes", "yes"),
    "remove/renumber/retype": (r"remove ?/ ?renumber ?/ ?retype", "no", "no"),
    "coder encoding": (r"chang\w* .*encoding", "no", "no"),
    "graph-shape rename": (r"rename .*transform .*state spec id", "yes", "no"),
    "coder type": (r"chang\w* .*coder type", "no", "no"),
    "versioned migration": (r"state_schema_version.*bump", "via migration", "yes"),
}


def _read_doc() -> str:
    assert DOC.is_file(), f"the published compatibility policy is missing: {DOC}"
    return DOC.read_text(encoding="utf-8")


def _parse_table(text: str, *, header_cell: str) -> list[list[str]]:
    """Return the body rows of the first markdown table whose header starts with
    ``header_cell``, as lists of stripped cells.
    """
    rows: list[list[str]] = []
    in_table = False
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            if in_table:
                break
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if not in_table:
            if cells and cells[0].lower() == header_cell.lower():
                in_table = True
                rows.append(cells)
            continue
        if all(set(cell) <= {"-", ":", " "} for cell in cells):
            continue  # the header separator row
        rows.append(cells)
    assert in_table, f"no markdown table with a {header_cell!r} header column in {DOC}"
    return rows


@pytest.fixture(scope="module")
def doc_text() -> str:
    return _read_doc()


@pytest.fixture(scope="module")
def table(doc_text: str) -> list[list[str]]:
    return _parse_table(doc_text, header_cell="Change class")


def test_the_table_has_the_three_contract_columns(table: list[list[str]]) -> None:
    header = [cell.lower() for cell in table[0]]
    assert len(header) == 4, f"expected 4 columns (class + 3 verdicts), got {table[0]}"
    assert "readable" in header[1]
    assert "update" in header[2]
    assert "action" in header[3]


@pytest.mark.parametrize(("change_class", "spec"), REQUIRED_ROWS.items(), ids=list(REQUIRED_ROWS))
def test_every_state_affecting_change_class_has_a_row(
    change_class: str, spec: tuple[str, str, str], table: list[list[str]]
) -> None:
    """One row per enumerated class, carrying the verdict the spec requires."""
    pattern, readable, updatable = spec
    matches = [row for row in table[1:] if re.search(pattern, row[0], re.IGNORECASE)]
    assert len(matches) == 1, (
        f"expected exactly one {change_class!r} row matching {pattern!r} in the "
        f"compatibility table; found {len(matches)}: {[row[0] for row in matches]}"
    )
    row = matches[0]
    assert readable in row[1].lower(), f"{change_class!r}: readable column is {row[1]!r}"
    assert updatable in row[2].lower(), f"{change_class!r}: --update column is {row[2]!r}"
    assert row[3].strip(), f"{change_class!r}: required-action column is empty"


def test_a_graph_shape_rename_is_readable_but_not_updatable(table: list[list[str]]) -> None:
    """The row nobody writes down: bytes are fine, step matching is not.

    Scenario: "A graph-shape change is classified even though no bytes change".
    """
    (row,) = [
        row
        for row in table[1:]
        if re.search(r"rename .*transform .*state spec id", row[0], re.IGNORECASE)
    ]
    assert "transform_name_mapping" in row[3], (
        "the rename row must prescribe --transform_name_mapping as the escape hatch; "
        f"got {row[3]!r}"
    )


def test_the_forbidden_rows_say_forbidden(table: list[list[str]]) -> None:
    """Coder encoding and coder type are the two "no bump can buy this back" rows.

    Scenario: "The table classifies a safe change and a breaking change
    differently" — the coder-encoding row states the change is forbidden
    because the raw-proto wire format is the contract.
    """
    forbidden = [
        row
        for row in table[1:]
        if re.search(r"chang\w* .*(encoding|coder type)", row[0], re.IGNORECASE)
    ]
    assert len(forbidden) == 2, [row[0] for row in forbidden]
    for row in forbidden:
        assert "forbidden" in row[3].lower(), f"{row[0]!r}: required action is {row[3]!r}"


def test_the_versioned_row_cites_the_migration_policy(table: list[list[str]]) -> None:
    (row,) = [
        row for row in table[1:] if re.search(r"state_schema_version.*bump", row[0], re.IGNORECASE)
    ]
    assert "state-migration.md" in row[3], (
        "the versioned-migration row must cite the migration policy that defines "
        f"the obligations; got {row[3]!r}"
    )


def test_the_promise_is_stated_in_rfc_2119_terms(doc_text: str) -> None:
    """Scenario: "The promise separates guaranteed from best-effort" (the SHALL half)."""
    assert re.search(
        r"release N\b.*SHALL be readable by\b.*N\s*\+\s*1", doc_text, re.IGNORECASE | re.DOTALL
    ), "the adjacent-release readability promise must be stated with SHALL"
    assert re.search(r"`?--update`?.*SHALL succeed", doc_text, re.IGNORECASE)


def test_the_wire_format_statement_pins_the_raw_proto_encoding(doc_text: str) -> None:
    assert "SerializeToString(deterministic=True)" in doc_text
    assert re.search(r"no (additional )?framing", doc_text, re.IGNORECASE)


@pytest.mark.parametrize(
    ("topic", "pattern"),
    [
        ("skip-level", r"N\s*\+\s*k.*best.effort|best.effort.*N\s*\+\s*k"),
        ("downgrade", r"[Dd]owngrade.*(unsupported|NOT supported)"),
        ("byte-identity", r"[Bb]yte.identity.*not promised|not promised.*byte.identity"),
        ("flink savepoints", r"[Ff]link savepoint"),
        ("cross-runner", r"[Cc]ross.runner"),
    ],
)
def test_the_document_classifies_what_is_not_promised(
    topic: str, pattern: str, doc_text: str
) -> None:
    """Scenario: "The promise separates guaranteed from best-effort" (the other half)."""
    assert re.search(pattern, doc_text, re.DOTALL), f"no explicit non-promise for {topic}"


def test_the_release_procedure_makes_a_red_gate_block_the_tag(doc_text: str) -> None:
    """Scenario: "A red gate stops the tag"."""
    assert re.search(r"##+ .*[Rr]elease procedure", doc_text), (
        "the document must carry the release-procedure section that makes the gate release-blocking"
    )
    assert re.search(r"nightly.*`?dataflow`?.*green|green.*nightly.*`?dataflow`?", doc_text)
    assert re.search(r"never by weakening the gate", doc_text, re.IGNORECASE)
