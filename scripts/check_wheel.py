#!/usr/bin/env python3
"""Verify built distributions before they are published.

Run by `.github/workflows/release.yml` between `make build` and the publish
job. Reads the wheel as a zip and the sdist as a tarball — no installation, no
network, no import of the package under test — and fails on anything that
would make the published artifact wrong in a way `pip install` only reveals
later:

* a missing `beam_agents/py.typed` (the package advertises full type hints;
  without the marker every downstream `mypy` silently ignores them);
* missing generated `_protos/*_pb2.py` bindings (a wheel built from a tree
  where gen output was stripped imports fine to a linter and dies at runtime);
* test, docker, or CI content leaking into the wheel;
* a missing or retargeted `beam-agents-effector` console script;
* metadata drift on `Requires-Python` or the published extras.

Expected metadata is derived from `pyproject.toml` rather than hardcoded here,
so the check compares the *built artifact* against the *source declaration*
and cannot go stale when an extra is added.
"""

from __future__ import annotations

import argparse
import configparser
import fnmatch
import sys
import tarfile
import tomllib
import zipfile
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from email import message_from_string
from pathlib import Path

PACKAGE = "beam_agents"
TYPING_MARKER = f"{PACKAGE}/py.typed"
PROTO_GLOB = f"{PACKAGE}/_protos/*_pb2.py"
CONSOLE_SCRIPT = "beam-agents-effector"
CONSOLE_TARGET = "beam_agents.effector.__main__:main"

#: Path segments that must never appear in the wheel. `testing` (the shipped
#: chaos helpers) is deliberately absent: it is runtime code, not test content.
FORBIDDEN_WHEEL_SEGMENTS = frozenset({"tests", "test", "docker", ".github", "openspec"})

#: The sdist ships the proto *sources* alongside the generated bindings, so a
#: downstream consumer can regenerate them (design open question, resolved
#: against `uv build`'s actual default collection).
REQUIRED_SDIST_MEMBERS = (
    "pyproject.toml",
    "PKG-INFO",
    "protos/beam_agents.proto",
    f"src/{PACKAGE}/py.typed",
)
REQUIRED_SDIST_GLOBS = (f"src/{PACKAGE}/_protos/*_pb2.py",)


@dataclass(frozen=True)
class Expected:
    """What the built metadata must say, per `pyproject.toml`."""

    requires_python: str
    extras: frozenset[str]


@dataclass(frozen=True)
class WheelContents:
    """Everything the checks need from a wheel, read once."""

    names: frozenset[str]
    metadata: str
    entry_points: str


def expected_from_pyproject(pyproject_text: str) -> Expected:
    data = tomllib.loads(pyproject_text)
    project = data.get("project", {})
    return Expected(
        requires_python=str(project.get("requires-python", "")),
        extras=frozenset(project.get("optional-dependencies", {})),
    )


def _specifier_set(spec: str) -> frozenset[str]:
    """`'<3.13,>=3.11'` and `'>=3.11, <3.13'` compare equal.

    Build backends normalize and reorder specifiers, so a literal string
    comparison would report drift on every release for no reason.
    """
    return frozenset(part.strip() for part in spec.split(",") if part.strip())


def read_wheel(path: Path) -> WheelContents:
    with zipfile.ZipFile(path) as zf:
        names = frozenset(zf.namelist())
        dist_infos = sorted({n.split("/", 1)[0] for n in names if ".dist-info/" in n})
        if not dist_infos:
            raise ValueError(f"{path.name}: no .dist-info directory — not a wheel?")
        dist_info = dist_infos[0]
        metadata = zf.read(f"{dist_info}/METADATA").decode("utf-8")
        entry_points_name = f"{dist_info}/entry_points.txt"
        entry_points = (
            zf.read(entry_points_name).decode("utf-8") if entry_points_name in names else ""
        )
    return WheelContents(names=names, metadata=metadata, entry_points=entry_points)


def read_sdist(path: Path) -> frozenset[str]:
    """Member paths with the `<name>-<version>/` root directory stripped."""
    with tarfile.open(path) as tf:
        names = tf.getnames()
    stripped = set()
    for name in names:
        _, _, rest = name.partition("/")
        if rest:
            stripped.add(rest)
    return frozenset(stripped)


def _is_forbidden(name: str) -> bool:
    return any(segment in FORBIDDEN_WHEEL_SEGMENTS for segment in name.split("/"))


def _console_scripts(entry_points: str) -> dict[str, str]:
    parser = configparser.ConfigParser()
    parser.read_string(entry_points or "")
    if not parser.has_section("console_scripts"):
        return {}
    return dict(parser["console_scripts"])


def wheel_problems(contents: WheelContents, expected: Expected) -> list[str]:
    """Pure list of wheel-content/metadata failures; [] means publishable."""
    problems: list[str] = []
    names = contents.names

    if TYPING_MARKER not in names:
        problems.append(
            f"wheel is missing the typing marker {TYPING_MARKER} — downstream "
            "type checkers would ignore the package's annotations"
        )

    if not any(fnmatch.fnmatch(name, PROTO_GLOB) for name in names):
        problems.append(
            f"wheel is missing the generated proto bindings ({PROTO_GLOB}); the "
            f"{PACKAGE}/_protos package would fail to import at runtime"
        )

    if leaked := sorted(name for name in names if _is_forbidden(name)):
        problems.append(
            "wheel contains test/docker/CI content that must not ship: " + ", ".join(leaked[:10])
        )

    scripts = _console_scripts(contents.entry_points)
    if CONSOLE_SCRIPT not in scripts:
        problems.append(
            f"wheel declares no {CONSOLE_SCRIPT!r} console script "
            f"(found: {sorted(scripts) or 'none'})"
        )
    elif scripts[CONSOLE_SCRIPT] != CONSOLE_TARGET:
        problems.append(
            f"console script {CONSOLE_SCRIPT!r} points at "
            f"{scripts[CONSOLE_SCRIPT]!r}, expected {CONSOLE_TARGET!r}"
        )

    metadata = message_from_string(contents.metadata)
    declared_python = metadata.get("Requires-Python", "")
    if _specifier_set(declared_python) != _specifier_set(expected.requires_python):
        problems.append(
            f"metadata drift: Requires-Python is {declared_python!r}, "
            f"pyproject.toml declares {expected.requires_python!r}"
        )

    declared_extras = frozenset(metadata.get_all("Provides-Extra") or ())
    if declared_extras != expected.extras:
        missing = sorted(expected.extras - declared_extras)
        unexpected = sorted(declared_extras - expected.extras)
        detail = []
        if missing:
            detail.append(f"missing {', '.join(missing)}")
        if unexpected:
            detail.append(f"unexpected {', '.join(unexpected)}")
        problems.append("metadata drift in Provides-Extra: " + "; ".join(detail))

    return problems


def sdist_problems(names: Iterable[str]) -> list[str]:
    """Pure list of sdist-content failures; [] means publishable."""
    members = frozenset(names)
    problems = [
        f"sdist is missing {required}"
        for required in REQUIRED_SDIST_MEMBERS
        if required not in members
    ]
    problems += [
        f"sdist is missing {glob}"
        for glob in REQUIRED_SDIST_GLOBS
        if not any(fnmatch.fnmatch(name, glob) for name in members)
    ]
    if not any(name.startswith(f"src/{PACKAGE}/") for name in members):
        problems.append(f"sdist ships no src/{PACKAGE} sources")
    return problems


def _collect(paths: Sequence[Path]) -> tuple[list[Path], list[Path]]:
    wheels: list[Path] = []
    sdists: list[Path] = []
    for path in paths:
        candidates = sorted(path.iterdir()) if path.is_dir() else [path]
        for candidate in candidates:
            if candidate.name.endswith(".whl"):
                wheels.append(candidate)
            elif candidate.name.endswith(".tar.gz"):
                sdists.append(candidate)
    return wheels, sdists


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        default=[Path("dist")],
        help="wheel/sdist files, or a directory containing them (default: dist/)",
    )
    parser.add_argument(
        "--pyproject",
        type=Path,
        default=Path("pyproject.toml"),
        help="source of the expected metadata (default: ./pyproject.toml)",
    )
    parser.add_argument(
        "--requires-python",
        default=None,
        help="override the expected Requires-Python instead of reading pyproject",
    )
    parser.add_argument(
        "--extra",
        dest="extras",
        action="append",
        default=None,
        help="override an expected extra (repeatable) instead of reading pyproject",
    )
    args = parser.parse_args(argv)

    if args.requires_python is not None or args.extras is not None:
        expected = Expected(
            requires_python=args.requires_python or "",
            extras=frozenset(args.extras or ()),
        )
    else:
        expected = expected_from_pyproject(args.pyproject.read_text(encoding="utf-8"))

    wheels, sdists = _collect(args.paths)
    problems: list[str] = []
    if not wheels and not sdists:
        problems.append(
            "no distributions found — `make build` must produce a wheel and an "
            f"sdist under {', '.join(str(p) for p in args.paths)}"
        )
    for wheel in wheels:
        problems += [f"{wheel.name}: {p}" for p in wheel_problems(read_wheel(wheel), expected)]
    for sdist in sdists:
        problems += [f"{sdist.name}: {p}" for p in sdist_problems(read_sdist(sdist))]

    if problems:
        print("distribution verification FAILED:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1

    checked = ", ".join(p.name for p in [*wheels, *sdists])
    print(f"distribution verification OK: {checked}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
