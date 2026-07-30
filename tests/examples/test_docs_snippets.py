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

# The inclusion directive the pages must carry, for either example shape:
#   --8<-- "examples/<module>.py"            a single-module example
#   --8<-- "examples/<package>/<module>.py"  a package-shaped example
_SNIPPET_DIRECTIVE = re.compile(r'--8<--\s+"(?P<path>examples/[a-z0-9_]+(?:/[a-z0-9_]+)?\.py)"')


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
        # The page's name and its example's name must agree, so a page cannot
        # silently render the wrong example: hello-world.md -> hello_world.py
        # for a single-module example, slack-approval.md -> any module under
        # examples/slack_approval/ for a package-shaped one.
        expected_stem = page.stem.replace("-", "_")
        included_relative = Path(match["path"]).relative_to("examples")
        owner = (
            included_relative.parts[0]
            if len(included_relative.parts) > 1
            else included_relative.stem
        )
        assert owner == expected_stem, (
            f"{page.name} includes {match['path']!r}; expected an examples/{expected_stem} module"
        )


def test_every_example_module_is_rendered_by_a_page() -> None:
    # The inverse direction: an example module nothing renders is dead docs
    # weight, and a deleted page would silently unpublish a tested example.
    # The unit of publication is the example, not the file: a single-module
    # example is its stem, a package-shaped one is its directory name (one page
    # renders one of its modules and describes the rest).
    modules = {path.stem for path in EXAMPLES_DIR.glob("*.py") if path.name != "__init__.py"}
    modules |= {path.name for path in EXAMPLES_DIR.iterdir() if (path / "__init__.py").is_file()}
    rendered = set()
    for page in DOCS_EXAMPLES_DIR.glob("*.md"):
        match = _SNIPPET_DIRECTIVE.search(page.read_text(encoding="utf-8"))
        if match is not None:
            relative = Path(match["path"]).relative_to("examples")
            rendered.add(relative.parts[0] if len(relative.parts) > 1 else relative.stem)
    assert modules == rendered, (
        f"examples and rendered pages disagree: examples={sorted(modules)} "
        f"rendered={sorted(rendered)}"
    )


# --- Requirement: examples are self-contained, offline, runnable modules -------


def test_no_example_imports_anything_under_tests() -> None:
    # Scenario: An example importing test helpers fails the unit lane — a user
    # copying the example out of the repository could not run it.
    offenders: list[str] = []
    for module in sorted(EXAMPLES_DIR.rglob("*.py")):
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
