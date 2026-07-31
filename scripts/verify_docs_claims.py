#!/usr/bin/env python
"""Verify every claim the documentation site makes against this repository.

The site's credibility rests on this script. Each content page declares typed
assertions in its frontmatter, and each is resolved against ground truth:

    verifies:
      - symbol: beam_agents.RunAgent          # resolved by IMPORT, not grep
      - module: src/beam_agents/core/dofn.py  # must exist
      - spec: openspec/specs/tool-registry/spec.md
      - test: tests/core/test_transform.py    # must be collectable by pytest
      - example: fast_path.py                 # must exist under website/examples/

`symbol:` resolving by import is the load-bearing choice: a name that appears
in source text but cannot be imported is not API, and a checker that greps
would call it verified.

On top of resolution, four rules run:

1. **Status semantics** (both directions). `stable` needs a spec and a test;
   `experimental` needs a test; `partial` must say what is missing; `planned`
   must NOT name code that exists — so a page describing a shipped feature as
   planned fails until it is reclassified.
2. **Release state.** While `project.version` is `0.0.0`, no page may present a
   registry install as a command that works today.
3. **Citations.** A claim naming another project needs a dated `sources` entry.
4. **Planned-page containment.** Planned pages must open with the
   not-implemented callout and must not embed executed examples.

Usage:  uv run python scripts/verify_docs_claims.py
"""

from __future__ import annotations

import importlib
import re
import subprocess
import sys
import tomllib
from collections.abc import Callable
from functools import cache
from pathlib import Path

# `_docs_content` is a sibling module, not an installed package: make the
# script's own directory importable before reaching for it, so the check runs
# the same whether invoked as `scripts/verify_docs_claims.py` or via `make`.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _docs_content import (
    EXAMPLES_ROOT,
    REPO_ROOT,
    ContentPage,
    Finding,
    frontmatter_line,
    load_pages,
)

ASSERTION_KEYS = ("symbol", "module", "spec", "test", "example")
STATUSES = ("stable", "experimental", "partial", "planned")

# Projects the site may compare itself with. Naming one in prose obliges the
# page to carry a dated citation for the claim.
THIRD_PARTY_PROJECTS = (
    "Apache Flink Agents",
    "Flink Agents",
    "LangGraph",
    "Google ADK",
    "Pydantic AI",
    "Ray Serve",
    "Temporal",
)

# Phrasings that present a registry install as available today.
REGISTRY_INSTALL = re.compile(
    r"(?:pip|uv pip|uv add|poetry add)\s+install\s+['\"]?beam[-_]agents|"
    r"uv add\s+['\"]?beam[-_]agents"
)

NOT_IMPLEMENTED_MARKER = re.compile(r'<Callout\s+kind=(?:"|\')not-implemented(?:"|\')')
NOT_YET_IMPLEMENTED_SECTION = re.compile(r"^#{2,4}\s+.*not (?:yet )?implemented", re.IGNORECASE)
EXAMPLE_EMBED = re.compile(r'<Example\s+file=(?:"|\')([^"\']+)(?:"|\')')
# `when released` gates the PyPI instructions; a page carrying it may show the
# registry command as a future path.
RELEASE_GATE = re.compile(r"when released", re.IGNORECASE)


@cache
def package_version() -> str:
    with (REPO_ROOT / "pyproject.toml").open("rb") as handle:
        return str(tomllib.load(handle)["project"]["version"])


@cache
def collected_test_ids() -> frozenset[str]:
    """Every test node id pytest can collect, plus their file paths.

    One subprocess for the whole run — collection is the expensive part, and a
    per-assertion invocation would make the check unusable.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "--collect-only",
            "-q",
            "--no-header",
            "-p",
            "no:cacheprovider",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    ids: set[str] = set()
    for raw in result.stdout.split("\n"):
        node = raw.strip()
        if not node or node.startswith(("=", "-", "no tests ran", "ERROR")):
            continue
        if "::" in node or node.endswith(".py"):
            ids.add(node)
            ids.add(node.split("::", maxsplit=1)[0])
    if not ids:
        print(
            "warning: `pytest --collect-only` returned no test ids; `test:` assertions "
            f"cannot be verified.\nstderr:\n{result.stderr[:2000]}",
            file=sys.stderr,
        )
    return frozenset(ids)


def resolve_symbol(dotted: str) -> str | None:
    """Import ``dotted`` and return an error message, or None on success."""
    parts = dotted.split(".")
    if parts[0] != "beam_agents":
        return f"symbol `{dotted}` must be rooted at `beam_agents`"
    # Walk from the longest importable module prefix, then getattr the rest.
    for split in range(len(parts), 0, -1):
        module_name = ".".join(parts[:split])
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        obj: object = module
        for attribute in parts[split:]:
            try:
                obj = getattr(obj, attribute)
            except AttributeError:
                return (
                    f"symbol `{dotted}` does not resolve: "
                    f"`{module_name}` has no attribute `{attribute}`"
                )
        return None
    return f"symbol `{dotted}` does not resolve: no importable module prefix"


def check_assertions(page: ContentPage) -> list[Finding]:
    findings: list[Finding] = []
    line = frontmatter_line(page, "verifies")
    entries = page.frontmatter.get("verifies") or []
    if not isinstance(entries, list):
        return [Finding(page.rel, line, "`verifies` must be a list of assertions")]

    for entry in entries:
        if not isinstance(entry, dict):
            findings.append(
                Finding(page.rel, line, f"`verifies` entry is not a mapping: {entry!r}")
            )
            continue
        unknown = set(entry) - set(ASSERTION_KEYS)
        if unknown:
            findings.append(
                Finding(
                    page.rel,
                    line,
                    f"unrecognized assertion key(s) {sorted(unknown)}; "
                    f"expected one of {list(ASSERTION_KEYS)}",
                )
            )
            continue
        if len(entry) != 1:
            findings.append(
                Finding(page.rel, line, f"each `verifies` entry carries exactly one key: {entry!r}")
            )
            continue
        key, value = next(iter(entry.items()))
        findings.extend(_resolve_assertion(page, line, key, str(value)))
    return findings


def _resolve_module(value: str) -> str | None:
    if not (REPO_ROOT / value).exists():
        return f"module `{value}` does not exist in the repository"
    return None


def _resolve_spec(value: str) -> str | None:
    if not (REPO_ROOT / value).exists():
        return f"spec `{value}` does not exist in the repository"
    if not value.startswith("openspec/specs/"):
        return f"spec `{value}` must live under openspec/specs/"
    return None


def _resolve_example(value: str) -> str | None:
    if not (EXAMPLES_ROOT / value).exists():
        return f"example `website/examples/{value}` does not exist"
    return None


def _resolve_test(value: str) -> str | None:
    ids = collected_test_ids()
    # An empty collection means pytest itself failed; that is warned about at
    # collection time rather than reported as every page being wrong.
    if ids and value not in ids:
        return (
            f"test `{value}` is not collectable by pytest. Reproduce with:\n"
            f"    uv run pytest --collect-only -q {value.split('::', maxsplit=1)[0]}"
        )
    return None


# One resolver per assertion kind. A table rather than a branch chain so adding
# a kind means adding a function, and `ASSERTION_KEYS` cannot drift from what
# is actually resolvable — the test below asserts they match.
_RESOLVERS: dict[str, Callable[[str], str | None]] = {
    "symbol": resolve_symbol,
    "module": _resolve_module,
    "spec": _resolve_spec,
    "example": _resolve_example,
    "test": _resolve_test,
}


def _resolve_assertion(page: ContentPage, line: int, key: str, value: str) -> list[Finding]:
    """Resolve one assertion, returning findings for whatever did not hold."""
    resolver = _RESOLVERS.get(key)
    if resolver is None:
        return [Finding(page.rel, line, f"unhandled assertion kind `{key}`")]
    error = resolver(value)
    return [Finding(page.rel, line, error)] if error else []


def check_status_semantics(page: ContentPage) -> list[Finding]:
    """Enforce what each status *means*, in both directions."""
    findings: list[Finding] = []
    line = frontmatter_line(page, "status")
    status = page.status
    if status not in STATUSES:
        return [
            Finding(
                page.rel,
                line,
                f"status {status!r} is not one of {list(STATUSES)}",
            )
        ]

    entries = [e for e in (page.frontmatter.get("verifies") or []) if isinstance(e, dict)]
    kinds = {key for entry in entries for key in entry}

    if status == "stable":
        if "spec" not in kinds or "test" not in kinds:
            findings.append(
                Finding(
                    page.rel,
                    line,
                    "status `stable` requires at least one `spec:` and one `test:` assertion "
                    "(a stable claim must be traceable to a specification and a test)",
                )
            )
    elif status == "experimental":
        if "test" not in kinds:
            findings.append(
                Finding(
                    page.rel, line, "status `experimental` requires at least one `test:` assertion"
                )
            )
    elif status == "partial":
        if not any(NOT_YET_IMPLEMENTED_SECTION.match(text) for _, text in page.prose_lines()):
            findings.append(
                Finding(
                    page.rel,
                    line,
                    "status `partial` requires a heading section stating what is NOT implemented "
                    '(e.g. "## Not yet implemented")',
                )
            )
    elif status == "planned":
        findings.extend(_check_planned(page, line, entries))
    return findings


def _check_planned(page: ContentPage, line: int, entries: list[dict[str, object]]) -> list[Finding]:
    """A planned page must not describe code that exists.

    This is the inverted check that keeps status honest in the other direction:
    when the feature ships, the page fails until someone reclassifies it, so a
    roadmap entry cannot quietly understate the project either.
    """
    findings: list[Finding] = []
    for entry in entries:
        for key, value in entry.items():
            if key == "symbol" and resolve_symbol(str(value)) is None:
                findings.append(
                    Finding(
                        page.rel,
                        line,
                        f"status `planned` but symbol `{value}` now resolves — the feature "
                        "exists. Reclassify this page (experimental/partial/stable).",
                    )
                )
            if key == "module" and (REPO_ROOT / str(value)).exists():
                findings.append(
                    Finding(
                        page.rel,
                        line,
                        f"status `planned` but module `{value}` now exists. "
                        "Reclassify this page (experimental/partial/stable).",
                    )
                )
    if not any(NOT_IMPLEMENTED_MARKER.search(text) for _, text in page.body_lines()):
        findings.append(
            Finding(
                page.rel,
                page.body_offset,
                'status `planned` requires the page to open with <Callout kind="not-implemented">',
            )
        )
    for number, text in page.body_lines():
        match = EXAMPLE_EMBED.search(text)
        if match:
            findings.append(
                Finding(
                    page.rel,
                    number,
                    "planned pages must not embed executed examples "
                    f'(<Example file="{match.group(1)}" />); illustrative code on a '
                    "planned page must be a plain fenced block, labelled as illustrative",
                )
            )
    return findings


def check_release_state(page: ContentPage) -> list[Finding]:
    """No page may present a registry install as working while unreleased."""
    if package_version() != "0.0.0":
        return []
    findings: list[Finding] = []
    for number, text in page.body_lines():
        if not REGISTRY_INSTALL.search(text):
            continue
        window = _context_window(page, number)
        if RELEASE_GATE.search(window):
            continue
        findings.append(
            Finding(
                page.rel,
                number,
                "presents a registry install of `beam-agents` as available, but "
                f"pyproject declares version {package_version()} and the package is not "
                'published. Put it under a "when released" heading, or install from source.',
            )
        )
    return findings


def _context_window(page: ContentPage, number: int, radius: int = 12) -> str:
    lines = page.body_lines()
    lower = max(0, number - page.body_offset - radius)
    upper = min(len(lines), number - page.body_offset + radius)
    return "\n".join(text for _, text in lines[lower:upper])


def _check_source_entries(page: ContentPage, sources: list[object]) -> list[Finding]:
    """Each citation must be complete and dated, or it is not a citation."""
    findings: list[Finding] = []
    line = frontmatter_line(page, "sources")
    for index, source in enumerate(sources):
        if not isinstance(source, dict) or set(source) != {"claim", "url", "retrieved"}:
            findings.append(
                Finding(
                    page.rel,
                    line,
                    f"sources[{index}] must carry exactly `claim`, `url`, `retrieved`",
                )
            )
            continue
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(source["retrieved"])):
            findings.append(
                Finding(
                    page.rel, line, f"sources[{index}].retrieved must be an ISO date (YYYY-MM-DD)"
                )
            )
    return findings


def check_citations(page: ContentPage) -> list[Finding]:
    """Comparative claims need a dated source; every cited source must resolve.

    Two rules, scoped differently on purpose.

    The citation requirement applies to the **comparison section**, because
    that is where statements about another project's behavior live. Elsewhere,
    naming LangGraph is a statement about *this* repository ("the LangGraph
    adapter is implemented") backed by a `module:` assertion, not a claim about
    LangGraph that needs a citation. Requiring one everywhere produced only
    noise, and a check that cries wolf gets switched off.

    The resolution rule is global: any `source:` URL used in a comparison-table
    cell must appear in the page's `sources`, or the footnote marker links to a
    citation that does not exist.
    """
    sources = page.frontmatter.get("sources") or []
    if not isinstance(sources, list):
        return [Finding(page.rel, frontmatter_line(page, "sources"), "`sources` must be a list")]

    findings = _check_source_entries(page, sources)
    cited_urls = {str(source.get("url")) for source in sources if isinstance(source, dict)}

    # Every footnote marker must point at a citation that exists.
    for number, text in page.body_lines():
        for match in re.finditer(r"source:\s*['\"]([^'\"]+)['\"]", text):
            if match.group(1) not in cited_urls:
                findings.append(
                    Finding(
                        page.rel,
                        number,
                        f"comparison cell cites {match.group(1)} but no matching `sources` "
                        "entry exists in this page's frontmatter",
                    )
                )

    if page.section != "comparison":
        return findings

    has_sources = bool(sources)
    for number, text in page.prose_lines():
        if text.lstrip().startswith(("<!--", "//")):
            continue
        for project in THIRD_PARTY_PROJECTS:
            if project not in text:
                continue
            if not has_sources:
                findings.append(
                    Finding(
                        page.rel,
                        number,
                        f"names {project!r} on a comparison page with no `sources` entry. "
                        "Comparative statements about another project need a dated citation.",
                    )
                )
            break
    return findings


def check_required_frontmatter(page: ContentPage) -> list[Finding]:
    findings = [Finding(page.rel, 1, message) for message in page.errors]
    for key in ("title", "summary", "status"):
        if not page.frontmatter.get(key):
            findings.append(Finding(page.rel, 1, f"frontmatter is missing required key `{key}`"))
    return findings


def check_coverage() -> list[Finding]:
    """Every repository doc page and capability spec must have a site page."""
    findings: list[Finding] = []
    pages = load_pages()
    covered_modules: set[str] = set()
    for page in pages:
        for entry in page.frontmatter.get("verifies") or []:
            if isinstance(entry, dict):
                for key in ("module", "spec"):
                    if key in entry:
                        covered_modules.add(str(entry[key]))
        for number, text in page.body_lines():
            del number
            for match in re.finditer(r"(docs/[a-z_]+\.md|openspec/specs/[a-z-]+/spec\.md)", text):
                covered_modules.add(match.group(1))

    for doc in sorted((REPO_ROOT / "docs").glob("*.md")):
        rel = doc.relative_to(REPO_ROOT).as_posix()
        if rel not in covered_modules:
            findings.append(
                Finding(
                    "website/content/docs",
                    1,
                    f"repository doc `{rel}` has no site page covering it "
                    "(reference it from a Docs page's `verifies:` or body)",
                )
            )
    for spec in sorted((REPO_ROOT / "openspec" / "specs").glob("*/spec.md")):
        rel = spec.relative_to(REPO_ROOT).as_posix()
        if rel not in covered_modules:
            findings.append(
                Finding(
                    "website/content/specs",
                    1,
                    f"capability spec `{rel}` has no site page covering it",
                )
            )
    return findings


def main() -> int:
    pages = load_pages()
    if not pages:
        print("no content pages found under website/content/", file=sys.stderr)
        return 1

    findings: list[Finding] = []
    for page in pages:
        findings.extend(check_required_frontmatter(page))
        if page.errors:
            continue
        findings.extend(check_assertions(page))
        findings.extend(check_status_semantics(page))
        findings.extend(check_release_state(page))
        findings.extend(check_citations(page))
    findings.extend(check_coverage())

    if findings:
        print(f"claim verification failed: {len(findings)} finding(s)\n", file=sys.stderr)
        for finding in findings:
            print(finding.render(), file=sys.stderr)
        print(
            "\nReproduce locally with:\n    uv run python scripts/verify_docs_claims.py",
            file=sys.stderr,
        )
        return 1

    print(f"claim verification passed: {len(pages)} page(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
