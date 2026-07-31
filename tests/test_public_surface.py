"""The frozen public surface: ``public-surface.toml`` versus the real source tree.

After 1.0 every public name in ``beam_agents`` is a compatibility promise, so
the set of them is a reviewed artifact rather than whatever the tree happens to
contain. ``public-surface.toml`` records, per public module, the sorted public
top-level names and the module's declared ``__all__``; this module re-derives
both from the sources and compares for **exact equality in both directions** —
an unreviewed addition and an unreviewed removal are equally failures, because
one is accidental API and the other is a silent break.

Three deliberate choices, each of which has bitten this repo before:

* **AST, not ``dir()``.** Runtime introspection observes imported submodules and
  instrumentation artifacts (mutmut injects a ``MutantDict``; see the comment in
  ``tests/test_import.py``), and importing every module would couple the freeze
  to which optional extras happen to be installed. The AST sees the *declared*
  surface, including names behind ``TYPE_CHECKING`` and lazy ``__getattr__``.
* **Underscore-prefixed paths are outside the surface.** A module under a
  ``_``-prefixed package, or a ``_``-prefixed module, is internal machinery by
  its path: nothing in it can be public contract, so it is not snapshotted and
  its contents need no ``__all__``.
* **``__all__`` is the contract.** Every public name a public module *declares*
  must be listed in that module's ``__all__``; anything else must carry a
  leading underscore. That is what keeps the snapshot a statement of intent
  rather than a transcript of accidents.

The module lives at the ``tests/`` root, outside the ``tests/core`` selection
``[tool.mutmut]`` uses, for the same reason ``tests/test_import.py`` does: it
introspects source that mutmut has instrumented.

Regenerate the snapshot after an intentional surface change with::

    uv run python tests/test_public_surface.py

and review the resulting diff — that diff *is* the API review.
"""

from __future__ import annotations

import ast
import re
import sys
import tomllib
from pathlib import Path
from typing import cast

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
PACKAGE_ROOT = SRC_ROOT / "beam_agents"
SNAPSHOT_PATH = REPO_ROOT / "public-surface.toml"
API_REFERENCE_PATH = REPO_ROOT / "docs" / "api.md"

SNAPSHOT_HEADER = """\
# The frozen public API surface of `beam_agents` (OpenSpec change
# `add-1-0-api-freeze`, roadmap C45).
#
# One table per public module, keyed by its path relative to `src/`, recording:
#   names = every public (non-underscore) top-level name the module *declares*
#           -- functions, classes, and module-level assignments -- sorted.
#   all   = the module's declared `__all__`, verbatim, when it has one. For the
#           package `__init__` modules this is the whole contract, since they
#           re-export rather than declare.
#
# Modules under a `_`-prefixed package or with a `_`-prefixed name are internal
# machinery by their path and are deliberately absent: `_protos/`,
# `adapters/_transport.py`, `model/_http.py`, `yaml/_config.py`, `yaml/_refs.py`.
# Generated protobuf bindings are never snapshotted.
#
# This file is GENERATED, never hand-edited:
#
#     uv run python tests/test_public_surface.py
#
# `tests/test_public_surface.py` compares it against the tree by exact equality
# in both directions, so an unreviewed addition and an unreviewed removal both
# fail `make test-unit`. Regenerating is not a way to make the test pass -- the
# resulting diff is the API review, and after 1.0 a removal in that diff needs
# the deprecation window CONTRIBUTING.md defines.
"""


# --- Derivation ---------------------------------------------------------------


def _is_public_path(relative: Path) -> bool:
    """True when no component of ``relative`` is a single-underscore private name.

    ``__init__.py`` and ``__main__.py`` are dunder names, not private ones, so a
    package's entry modules stay in the surface while ``_protos/`` and
    ``model/_http.py`` stay out.
    """
    return not any(part.startswith("_") and not part.startswith("__") for part in relative.parts)


def _string_list(node: ast.expr) -> list[str] | None:
    """The literal ``str`` elements of a list/tuple expression, else ``None``."""
    if not isinstance(node, (ast.List, ast.Tuple)):
        return None
    values: list[str] = []
    for element in node.elts:
        if not isinstance(element, ast.Constant) or not isinstance(element.value, str):
            return None
        values.append(element.value)
    return values


def _module_surface(source: str) -> dict[str, list[str]]:
    """The declared public names and ``__all__`` of one module's source text.

    Returns a mapping with a sorted, de-duplicated ``names`` entry (``@overload``
    stubs declare the same name repeatedly) and an ``all`` entry present only
    when the module assigns a literal ``__all__``.
    """
    names: set[str] = set()
    declared_all: list[str] | None = None
    for node in ast.parse(source).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            if not node.name.startswith("_"):
                names.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if not isinstance(target, ast.Name):
                    continue
                if target.id == "__all__":
                    declared_all = _string_list(node.value)
                elif not target.id.startswith("_"):
                    names.add(target.id)
        elif isinstance(node, ast.AnnAssign):
            target = node.target
            if isinstance(target, ast.Name) and not target.id.startswith("_"):
                names.add(target.id)
    surface: dict[str, list[str]] = {"names": sorted(names)}
    if declared_all is not None:
        surface["all"] = declared_all
    return surface


def _derive(package_root: Path, source_root: Path) -> dict[str, dict[str, list[str]]]:
    """Derive the public surface of every public module under ``package_root``."""
    derived: dict[str, dict[str, list[str]]] = {}
    for path in sorted(package_root.rglob("*.py")):
        relative = path.relative_to(source_root)
        if not _is_public_path(relative) or "_pb2" in path.name:
            continue
        derived[relative.as_posix()] = _module_surface(path.read_text())
    return derived


def _differences(
    derived: dict[str, dict[str, list[str]]],
    snapshot: dict[str, dict[str, list[str]]],
) -> list[str]:
    """Human-readable descriptions of every way the tree and the snapshot disagree.

    Each entry names the module, the offending name, and the direction, so the
    failure message tells a contributor what to review rather than dumping two
    sets side by side. An empty list means the surface is exactly as frozen.
    """
    problems: list[str] = []
    fix = "regenerate with `uv run python tests/test_public_surface.py` and review the diff"
    for module in sorted(set(derived) | set(snapshot)):
        if module not in snapshot:
            problems.append(f"{module}: module is not in the snapshot at all ({fix})")
            continue
        if module not in derived:
            problems.append(f"{module}: snapshot records a module that no longer exists ({fix})")
            continue
        actual, frozen = derived[module], snapshot[module]
        actual_names, frozen_names = set(actual["names"]), set(frozen.get("names", []))
        for name in sorted(actual_names - frozen_names):
            problems.append(f"{module}: public name {name!r} is not in the frozen surface ({fix})")
        for name in sorted(frozen_names - actual_names):
            problems.append(f"{module}: frozen public name {name!r} has disappeared ({fix})")
        if actual.get("all") != frozen.get("all"):
            problems.append(
                f"{module}: __all__ is {actual.get('all')!r} but the snapshot froze "
                f"{frozen.get('all')!r} ({fix})"
            )
    return problems


def _load_snapshot() -> dict[str, dict[str, list[str]]]:
    """Parse the committed snapshot file."""
    return cast("dict[str, dict[str, list[str]]]", tomllib.loads(SNAPSHOT_PATH.read_text()))


def _frozen_public_names(snapshot: dict[str, dict[str, list[str]]]) -> set[str]:
    """Every public name the snapshot freezes, across declarations and ``__all__``."""
    names: set[str] = set()
    for record in snapshot.values():
        names.update(record.get("names", []))
        names.update(name for name in record.get("all", []) if not name.startswith("_"))
    return names


# --- The gate -----------------------------------------------------------------


def test_committed_snapshot_matches_the_source_tree() -> None:
    problems = _differences(_derive(PACKAGE_ROOT, SRC_ROOT), _load_snapshot())
    assert problems == [], "public surface drifted from public-surface.toml:\n" + "\n".join(
        problems
    )


def test_every_declared_public_name_is_listed_in_its_module_all() -> None:
    # The underscore-privacy rule, checked at its source: a public module may
    # declare a public name only by putting it in `__all__`. Anything else is
    # internal and must carry a leading underscore. Without this the snapshot
    # would faithfully freeze accidents.
    offenders: list[str] = []
    for module, record in sorted(_derive(PACKAGE_ROOT, SRC_ROOT).items()):
        declared = record["names"]
        if not declared:
            continue
        exported = set(record.get("all", []))
        if not exported:
            offenders.append(f"{module}: declares {declared} but has no __all__")
            continue
        missing = sorted(set(declared) - exported)
        if missing:
            offenders.append(f"{module}: {missing} declared publicly but absent from __all__")
    assert offenders == [], (
        "every public name in a public module must be in that module's __all__, "
        "or carry a leading underscore:\n" + "\n".join(offenders)
    )


# --- The gate's own failure modes ---------------------------------------------
#
# A gate never observed failing is not known to work. These drive `_derive` and
# `_differences` over a synthetic package so both directions of the exact-
# equality comparison, the underscore exemption, and `__all__` drift are proven
# without perturbing the real tree.


def _synthetic_package(root: Path, source: str) -> tuple[Path, Path]:
    """Write ``source`` as ``pkg/mod.py`` under ``root``; return (package, source root)."""
    package = root / "pkg"
    package.mkdir(parents=True, exist_ok=True)
    (package / "mod.py").write_text(source)
    return package, root


def test_an_unreviewed_public_addition_fails(tmp_path: Path) -> None:
    package, source_root = _synthetic_package(
        tmp_path, '__all__ = ["kept", "added"]\n\n\ndef kept(): ...\n\n\ndef added(): ...\n'
    )
    snapshot = {"pkg/mod.py": {"names": ["kept"], "all": ["kept"]}}
    problems = _differences(_derive(package, source_root), snapshot)
    assert any("'added' is not in the frozen surface" in problem for problem in problems)
    assert any("pkg/mod.py" in problem for problem in problems)


def test_an_unreviewed_removal_fails(tmp_path: Path) -> None:
    package, source_root = _synthetic_package(tmp_path, '__all__ = ["kept"]\n\n\ndef kept(): ...\n')
    snapshot = {"pkg/mod.py": {"names": ["kept", "dropped"], "all": ["kept"]}}
    problems = _differences(_derive(package, source_root), snapshot)
    assert any("frozen public name 'dropped' has disappeared" in problem for problem in problems)


def test_an_underscore_rename_reads_as_a_removal(tmp_path: Path) -> None:
    # The same event as above seen from the audit's side: privatizing a frozen
    # name is a contract change, so it must go through the snapshot diff too.
    package, source_root = _synthetic_package(
        tmp_path, '__all__ = ["kept"]\n\n\ndef kept(): ...\n\n\ndef _was_public(): ...\n'
    )
    snapshot = {"pkg/mod.py": {"names": ["kept", "was_public"], "all": ["kept"]}}
    problems = _differences(_derive(package, source_root), snapshot)
    assert any("frozen public name 'was_public' has disappeared" in problem for problem in problems)


def test_a_new_underscore_name_needs_no_snapshot_update(tmp_path: Path) -> None:
    package, source_root = _synthetic_package(
        tmp_path,
        '__all__ = ["kept"]\n\n\ndef kept(): ...\n\n\ndef _helper(): ...\n\n\n_CONST = 1\n',
    )
    snapshot = {"pkg/mod.py": {"names": ["kept"], "all": ["kept"]}}
    assert _differences(_derive(package, source_root), snapshot) == []


def test_all_drift_fails(tmp_path: Path) -> None:
    package, source_root = _synthetic_package(
        tmp_path, '__all__ = ["kept", "Extra"]\n\n\ndef kept(): ...\n'
    )
    snapshot = {"pkg/mod.py": {"names": ["kept"], "all": ["kept"]}}
    problems = _differences(_derive(package, source_root), snapshot)
    assert any("__all__ is" in problem and "pkg/mod.py" in problem for problem in problems)


def test_overload_stubs_collapse_to_one_name(tmp_path: Path) -> None:
    # `tools/registry.py` declares `tool` three times (two @overload stubs plus
    # the implementation); the surface has one name, not three.
    package, source_root = _synthetic_package(
        tmp_path,
        '__all__ = ["tool"]\n\n\ndef tool(fn): ...\n\n\ndef tool(fn): ...\n\n\ndef tool(fn): ...\n',
    )
    assert _derive(package, source_root)["pkg/mod.py"]["names"] == ["tool"]


def test_private_modules_are_outside_the_surface(tmp_path: Path) -> None:
    package = tmp_path / "pkg"
    (package / "_internal").mkdir(parents=True)
    (package / "mod.py").write_text('__all__ = ["kept"]\n\n\ndef kept(): ...\n')
    (package / "_private.py").write_text("def helper(): ...\n")
    (package / "_internal" / "thing.py").write_text("def helper(): ...\n")
    derived = _derive(package, tmp_path)
    assert sorted(derived) == ["pkg/mod.py"]


# --- Documentation drift ------------------------------------------------------


def _documented_names() -> set[str]:
    """Every identifier appearing inside a code span on the API reference page.

    Code spans rather than free prose: the word "tool" occurs in sentences that
    document nothing, and a reference page that mentions a name only by accident
    has not documented it.
    """
    text = API_REFERENCE_PATH.read_text()
    tokens: set[str] = set()
    for span in re.findall(r"`([^`\n]+)`", text):
        tokens.update(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", span))
    return tokens


def test_api_reference_documents_every_frozen_name() -> None:
    frozen = _frozen_public_names(_load_snapshot())
    undocumented = sorted(frozen - _documented_names())
    assert undocumented == [], (
        f"{len(undocumented)} frozen public name(s) are missing from docs/api.md: "
        f"{undocumented}. Every name the snapshot freezes is a compatibility "
        "promise and must carry a line of contract on the reference page."
    )


def test_the_reference_page_does_not_document_names_outside_the_surface() -> None:
    # The other direction, scoped to names the page presents as API: a heading
    # naming a module that is no longer public, or a leftover entry for a
    # privatized name, is stale documentation the drift test above cannot see.
    frozen = _frozen_public_names(_load_snapshot())
    entries = re.findall(r"^\| `([A-Za-z_][A-Za-z0-9_]*)` \|", API_REFERENCE_PATH.read_text(), re.M)
    stale = sorted({entry for entry in entries} - frozen)
    assert stale == [], f"docs/api.md documents names outside the frozen surface: {stale}"


# --- Regeneration -------------------------------------------------------------


def _render_snapshot(derived: dict[str, dict[str, list[str]]]) -> str:
    """Render the derived surface as the committed TOML document."""

    def _array(values: list[str]) -> str:
        if not values:
            return "[]"
        body = "".join(f'  "{value}",\n' for value in values)
        return f"[\n{body}]"

    chunks = [SNAPSHOT_HEADER]
    for module, record in derived.items():
        lines = [f'\n["{module}"]', f"names = {_array(record['names'])}"]
        if "all" in record:
            lines.append(f"all = {_array(record['all'])}")
        chunks.append("\n".join(lines) + "\n")
    return "".join(chunks)


if __name__ == "__main__":  # pragma: no cover - developer entry point
    SNAPSHOT_PATH.write_text(_render_snapshot(_derive(PACKAGE_ROOT, SRC_ROOT)))
    sys.stdout.write(f"wrote {SNAPSHOT_PATH.relative_to(REPO_ROOT)}\n")
