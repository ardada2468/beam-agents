"""The fidelity checks are only worth having if they actually fail.

Each test below builds a tiny content tree, points a check at it, and asserts
the check rejects it — one case per rule. A checker with no tests is a checker
that silently stops catching things, and this repository's whole argument is
that guarantees should be enforced rather than remembered.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import check_docs_prose  # noqa: E402
import verify_docs_claims  # noqa: E402
from _docs_content import ContentPage, load_pages  # noqa: E402


def make_page(body: str, tmp_path: Path, **frontmatter: object) -> ContentPage:
    """A content page in memory, with sane defaults for what is not under test."""
    data: dict[str, object] = {
        "title": "A page",
        "summary": "What it says.",
        "status": "stable",
    }
    data.update(frontmatter)
    path = tmp_path / "page.mdx"
    lines = ["---"]
    for key, value in data.items():
        lines.append(f"{key}: {value!r}" if isinstance(value, str) else f"{key}: {value}")
    lines.append("---")
    path.write_text("\n".join(lines) + "\n" + body, encoding="utf-8")
    return ContentPage(
        path=path,
        rel="website/content/learn/page.mdx",
        frontmatter=data,
        body=body,
        body_offset=len(lines) + 1,
    )


# --- Requirement: claims are declared as machine-checkable assertions ----------


def test_symbol_assertion_resolves_by_import() -> None:
    # Scenario: symbol assertions resolve by import, not by grep.
    assert verify_docs_claims.resolve_symbol("beam_agents.RunAgent") is None


def test_symbol_that_only_appears_in_source_text_fails() -> None:
    # Scenario: a name that is not importable is not API.
    error = verify_docs_claims.resolve_symbol("beam_agents.NotARealSymbol")
    assert error is not None
    assert "does not resolve" in error


def test_symbol_outside_the_package_is_rejected() -> None:
    error = verify_docs_claims.resolve_symbol("os.path.join")
    assert error is not None
    assert "rooted at `beam_agents`" in error


def test_unresolvable_module_assertion_fails(tmp_path: Path) -> None:
    # Scenario: unresolvable module assertion fails verification.
    page = make_page("body", tmp_path, verifies=[{"module": "src/beam_agents/memory/stores.py"}])
    findings = verify_docs_claims.check_assertions(page)
    assert len(findings) == 1
    assert "does not exist" in findings[0].message


def test_unknown_assertion_type_fails(tmp_path: Path) -> None:
    # Scenario: unknown assertion type fails verification.
    page = make_page("body", tmp_path, verifies=[{"symbl": "beam_agents.RunAgent"}])
    findings = verify_docs_claims.check_assertions(page)
    assert len(findings) == 1
    assert "unrecognized assertion key" in findings[0].message


def test_entry_with_two_assertions_fails(tmp_path: Path) -> None:
    page = make_page(
        "body",
        tmp_path,
        verifies=[{"symbol": "beam_agents.RunAgent", "module": "pyproject.toml"}],
    )
    findings = verify_docs_claims.check_assertions(page)
    assert len(findings) == 1
    assert "exactly one key" in findings[0].message


# --- Requirement: status semantics are enforced, in both directions -----------


def test_stable_page_without_a_test_assertion_fails(tmp_path: Path) -> None:
    # Scenario: stable page without a test assertion fails.
    page = make_page("body", tmp_path, verifies=[{"symbol": "beam_agents.RunAgent"}])
    findings = verify_docs_claims.check_status_semantics(page)
    assert any("one `spec:` and one `test:`" in f.message for f in findings)


def test_experimental_page_requires_a_test(tmp_path: Path) -> None:
    page = make_page(
        "body", tmp_path, status="experimental", verifies=[{"symbol": "beam_agents.RunAgent"}]
    )
    findings = verify_docs_claims.check_status_semantics(page)
    assert any("requires at least one `test:`" in f.message for f in findings)


def test_partial_page_must_state_what_is_missing(tmp_path: Path) -> None:
    # Scenario: partial page must state what is missing.
    page = make_page("Some prose with no such section.", tmp_path, status="partial", verifies=[])
    findings = verify_docs_claims.check_status_semantics(page)
    assert any("NOT implemented" in f.message for f in findings)


def test_partial_page_with_the_section_passes(tmp_path: Path) -> None:
    body = "Some prose.\n\n## Not yet implemented\n\nThe store does not exist.\n"
    page = make_page(body, tmp_path, status="partial", verifies=[])
    assert verify_docs_claims.check_status_semantics(page) == []


def test_planned_page_fails_once_the_feature_ships(tmp_path: Path) -> None:
    # Scenario: planned page fails once the feature ships. This is the check
    # that keeps status honest in the *other* direction — a roadmap entry
    # cannot quietly understate the project either.
    body = '<Callout kind="not-implemented">Not built.</Callout>'
    page = make_page(
        "\n" + body, tmp_path, status="planned", verifies=[{"module": "pyproject.toml"}]
    )
    findings = verify_docs_claims.check_status_semantics(page)
    assert any("now exists" in f.message for f in findings)


def test_planned_page_naming_a_live_symbol_fails(tmp_path: Path) -> None:
    body = '<Callout kind="not-implemented">Not built.</Callout>'
    page = make_page(
        "\n" + body, tmp_path, status="planned", verifies=[{"symbol": "beam_agents.RunAgent"}]
    )
    findings = verify_docs_claims.check_status_semantics(page)
    assert any("now resolves" in f.message for f in findings)


def test_planned_page_needs_the_not_implemented_callout(tmp_path: Path) -> None:
    page = make_page("\nJust prose.", tmp_path, status="planned", verifies=[])
    findings = verify_docs_claims.check_status_semantics(page)
    assert any("not-implemented" in f.message for f in findings)


def test_status_outside_the_closed_set_fails(tmp_path: Path) -> None:
    page = make_page("body", tmp_path, status="beta")
    findings = verify_docs_claims.check_status_semantics(page)
    assert len(findings) == 1
    assert "is not one of" in findings[0].message


# --- Requirement: distribution claims match the package's real state ----------


def test_unqualified_registry_install_fails_while_unreleased(tmp_path: Path) -> None:
    # Scenario: unqualified registry install fails while unreleased.
    assert verify_docs_claims.package_version() == "0.0.0", (
        "this test encodes the current pre-release state; update it when a release exists"
    )
    page = make_page("\nInstall it:\n\n    pip install beam-agents\n", tmp_path)
    findings = verify_docs_claims.check_release_state(page)
    assert len(findings) == 1
    assert "not published" in findings[0].message


def test_registry_install_under_a_when_released_heading_passes(tmp_path: Path) -> None:
    body = "\n## When released\n\nOnce published:\n\n    pip install beam-agents\n"
    page = make_page(body, tmp_path)
    assert verify_docs_claims.check_release_state(page) == []


# --- Requirement: citations -------------------------------------------------


def test_uncited_comparison_claim_fails(tmp_path: Path) -> None:
    # Scenario: uncited comparative claim fails verification.
    page = make_page("\nApache Flink Agents does X.\n", tmp_path)
    object.__setattr__(page, "rel", "website/content/comparison/page.mdx")
    page.path = tmp_path / "comparison" / "page.mdx"
    page.path.parent.mkdir(parents=True, exist_ok=True)
    page.path.write_text("---\n---\nApache Flink Agents does X.\n", encoding="utf-8")
    findings = verify_docs_claims.check_citations(page)
    assert any("dated citation" in f.message for f in findings)


def test_cell_citing_a_missing_source_fails(tmp_path: Path) -> None:
    body = "\n<ClaimTable rows={[{cells: [{text: 'x', source: 'https://a.invalid'}]}]} />\n"
    page = make_page(body, tmp_path, sources=[])
    findings = verify_docs_claims.check_citations(page)
    assert any("no matching `sources` entry" in f.message for f in findings)


def test_malformed_source_entry_fails(tmp_path: Path) -> None:
    page = make_page("body", tmp_path, sources=[{"claim": "x", "url": "https://a.invalid"}])
    findings = verify_docs_claims.check_citations(page)
    assert any("exactly `claim`, `url`, `retrieved`" in f.message for f in findings)


# --- Requirement: prohibited content is rejected by a lexical check -----------


@pytest.mark.parametrize(
    ("line", "rule"),
    [
        ("It is trusted by teams in production.", "social-proof"),
        ("The fastest agent runtime available.", "superlative"),
        ("beam-agents is an Apache project.", "asf-governance"),
        ("Over 50,000 downloads to date.", "download-counts"),
        ("It is 3x faster than the alternative.", "performance-claim"),
    ],
)
def test_prohibited_prose_is_rejected(line: str, rule: str, tmp_path: Path) -> None:
    # Scenario: fabricated social proof, superlatives, ASF phrasing, and
    # unsourced performance numbers all fail the check.
    page = make_page("\n" + line + "\n", tmp_path)
    findings = check_docs_prose.check_page(page)
    assert any(rule in f.message for f in findings), f"{line!r} was not caught by {rule}"


def test_the_asf_disclaimer_itself_is_allowed(tmp_path: Path) -> None:
    # The sentence this project is obliged to print must not be flagged; a
    # check that forbids the disclaimer is worse than no check.
    page = make_page("\nIt is **not** an Apache Software Foundation project.\n", tmp_path)
    assert check_docs_prose.check_page(page) == []


def test_precise_internal_claims_are_not_superlatives(tmp_path: Path) -> None:
    # "the only effect path" is a spec-backed claim, not marketing.
    page = make_page("\n`ctx.act` is the only effect path.\n", tmp_path)
    assert check_docs_prose.check_page(page) == []


def test_numbers_inside_code_fences_are_ignored(tmp_path: Path) -> None:
    page = make_page("\n```python\nlatency_ms = 15\n```\n", tmp_path)
    assert check_docs_prose.check_page(page) == []


def test_labelled_budget_is_allowed(tmp_path: Path) -> None:
    page = make_page("\nThe design budget is p99 under 60 ms; it is not measured.\n", tmp_path)
    assert check_docs_prose.check_page(page) == []


def test_escape_without_a_reason_is_rejected(tmp_path: Path) -> None:
    # Scenario: escape comments require a reason.
    page = make_page("\nIt is the fastest. <!-- prose-check: ok -->\n", tmp_path)
    findings = check_docs_prose.check_page(page)
    assert any("carries no reason" in f.message for f in findings)


def test_escape_with_a_reason_exempts_the_line(tmp_path: Path) -> None:
    page = make_page(
        "\nIt is the fastest. <!-- prose-check: ok quoted from a cited benchmark -->\n", tmp_path
    )
    assert check_docs_prose.check_page(page) == []


# --- Requirement: the real content tree passes -------------------------------


def test_the_published_content_tree_passes_every_check() -> None:
    """Scenario: the site's own content satisfies the checks it ships with.

    A checker that only passes on fixtures proves nothing about the site.
    """
    pages = load_pages()
    assert pages, "no content pages found"

    findings = []
    for page in pages:
        findings.extend(verify_docs_claims.check_required_frontmatter(page))
        findings.extend(verify_docs_claims.check_status_semantics(page))
        findings.extend(verify_docs_claims.check_release_state(page))
        findings.extend(verify_docs_claims.check_citations(page))
        findings.extend(check_docs_prose.check_page(page))

    assert not findings, "\n".join(f.render() for f in findings)
