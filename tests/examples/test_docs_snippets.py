"""Offline snippet-integrity and self-containment checks for the example pages.

`docs/examples/*.md` render their example modules by `pymdownx.snippets`
inclusion, so the bytes the site shows are the bytes the example tests execute.
The strict docs build (`make docs`) catches a broken inclusion at build time;
these tests catch the same breakage — plus an example leaning on test helpers —
in the offline unit lane, without installing the docs toolchain.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCS_EXAMPLES_DIR = REPO_ROOT / "docs" / "examples"
EXAMPLES_DIR = REPO_ROOT / "examples"

# The inclusion directive the pages must carry: --8<-- "examples/<module>.py"
_SNIPPET_DIRECTIVE = re.compile(r'--8<--\s+"(?P<path>examples/[a-z0-9_]+\.py)"')


# --- Requirement: example pages render the runnable source by inclusion --------


def test_every_example_page_includes_its_module_by_path() -> None:
    # Scenario: A moved example file cannot publish silently (offline half).
    pages = sorted(DOCS_EXAMPLES_DIR.glob("*.md"))
    assert pages, f"no example pages found under {DOCS_EXAMPLES_DIR}"
    for page in pages:
        match = _SNIPPET_DIRECTIVE.search(page.read_text(encoding="utf-8"))
        assert match is not None, (
            f"{page.name} has no --8<-- snippet directive naming an examples/ module"
        )
        included = REPO_ROOT / match["path"]
        assert included.is_file(), f"{page.name} includes {match['path']!r}, which does not exist"
        # The page's name and its module's name must agree (hello-world.md ->
        # hello_world.py), so a page cannot silently render the wrong example.
        expected_module = page.stem.replace("-", "_") + ".py"
        assert Path(match["path"]).name == expected_module, (
            f"{page.name} includes {match['path']!r}; expected examples/{expected_module}"
        )


def test_every_example_module_is_rendered_by_a_page() -> None:
    # The inverse direction: an example module nothing renders is dead docs
    # weight, and a deleted page would silently unpublish a tested example.
    modules = {path.name for path in EXAMPLES_DIR.glob("*.py") if path.name != "__init__.py"}
    rendered = set()
    for page in DOCS_EXAMPLES_DIR.glob("*.md"):
        match = _SNIPPET_DIRECTIVE.search(page.read_text(encoding="utf-8"))
        if match is not None:
            rendered.add(Path(match["path"]).name)
    assert modules == rendered, (
        f"example modules and rendered pages disagree: modules={sorted(modules)} "
        f"rendered={sorted(rendered)}"
    )


# --- Requirement: examples are self-contained, offline, runnable modules -------


def test_no_example_imports_anything_under_tests() -> None:
    # Scenario: An example importing test helpers fails the unit lane — a user
    # copying the example out of the repository could not run it.
    offenders: list[str] = []
    for module in sorted(EXAMPLES_DIR.glob("*.py")):
        tree = ast.parse(module.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            offenders.extend(
                f"{module.name} imports {name!r}"
                for name in names
                if name == "tests" or name.startswith("tests.")
            )
    assert not offenders, f"examples must never import from tests/: {offenders}"
