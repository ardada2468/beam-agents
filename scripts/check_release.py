#!/usr/bin/env python3
"""Verify a release tag against the project's metadata, lockfile, and policy.

Run by `.github/workflows/release.yml` before anything is built for publishing,
and by `make changelog` (in ``--fragments-only`` mode) before assembly. Four
independent properties, all release-gating:

1. **Tag == metadata.** The `vX.Y.Z` tag's version equals `[project].version`
   in `pyproject.toml`. The version is single-sourced there and never derived
   from git state, so this is what converts "tagged without bumping" from a
   published mistake into a failed workflow run.
2. **Metadata == lockfile.** `uv.lock` records the project's own version. A
   bump without `uv lock` makes every `uv sync --locked` job fail with a
   resolution error; catching it here names the actual fix.
3. **Tagged commit is an ancestor of `main`.** Any commit on `main` has passed
   the required `ci`/`integration`/`quality` checks, which is what lets the
   release workflow trust the docker-backed gates instead of re-running them.
4. **Pending fragment types fit the version component.** The closed registry
   below is the versioning policy's input (docs/releasing.md): a PATCH tag
   (`vX.Y.Z` with `Z > 0`) may not ship `breaking`, `added`, or `changed`
   fragments. An unregistered type fails outright rather than being silently
   dropped by assembly.

Every check is a pure function over injected inputs so the whole matrix runs
offline in the unit lane (tests/release/test_check_release.py) — the same
stance as scripts/coverage_ratchet.py and scripts/check_semantics_partition.py.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tomllib
from collections.abc import Sequence
from pathlib import Path
from typing import NamedTuple

#: The closed fragment-type registry. Order is rendering order: breaking first.
FRAGMENT_TYPES: tuple[str, ...] = ("breaking", "added", "changed", "fixed", "docs", "internal")

#: Types whose presence forbids a PATCH release (docs/releasing.md).
MINOR_REQUIRING_TYPES: frozenset[str] = frozenset({"breaking", "added", "changed"})

#: `vX.Y.Z`, optionally with a PEP 440 pre-release suffix for rehearsals.
_TAG_RE = re.compile(r"^v(?P<version>(?P<x>\d+)\.(?P<y>\d+)\.(?P<z>\d+)(?:(?:a|b|rc)\d+)?)$")

_FRAGMENT_RE = re.compile(r"^(?P<name>.+)\.(?P<type>[a-z]+)\.md$")

PROJECT_DISTRIBUTION = "beam-agents"


class Fragment(NamedTuple):
    """One pending changelog entry: the OpenSpec change name and its type."""

    name: str
    type: str


def version_from_tag(tag: str) -> str:
    """``v0.1.0`` -> ``0.1.0``. Raises ``ValueError`` on any other shape."""
    match = _TAG_RE.match(tag)
    if match is None:
        raise ValueError(
            f"release tag {tag!r} is not of the form vX.Y.Z "
            "(optionally with an aN/bN/rcN pre-release suffix)"
        )
    return match.group("version")


def patch_component(tag: str) -> int:
    """The Z of ``vX.Y.Z``. Raises ``ValueError`` on an unparseable tag."""
    match = _TAG_RE.match(tag)
    if match is None:
        raise ValueError(f"release tag {tag!r} is not of the form vX.Y.Z")
    return int(match.group("z"))


def project_version(pyproject_text: str) -> str:
    """`[project].version` — the one authoritative version string."""
    data = tomllib.loads(pyproject_text)
    version = data.get("project", {}).get("version")
    if not isinstance(version, str):
        raise ValueError("pyproject.toml declares no static [project].version")
    return version


def uv_lock_version(lock_text: str) -> str:
    """The version `uv.lock` records for the project's own package entry."""
    data = tomllib.loads(lock_text)
    for package in data.get("package", []):
        if package.get("name") == PROJECT_DISTRIBUTION:
            version = package.get("version")
            if isinstance(version, str):
                return version
    raise ValueError(f"uv.lock has no [[package]] entry for {PROJECT_DISTRIBUTION!r}")


def read_fragments(directory: Path) -> list[Fragment]:
    """Parse `changelog.d/<name>.<type>.md` files; a missing directory is empty.

    Unregistered types are returned as-is rather than filtered out — rejecting
    them is the caller's job, and silently dropping one is the exact failure
    mode the closed registry exists to prevent.
    """
    if not directory.is_dir():
        return []
    fragments: list[Fragment] = []
    for path in sorted(directory.iterdir()):
        if not path.is_file():
            continue
        match = _FRAGMENT_RE.match(path.name)
        if match is None:
            # README.md, .gitkeep and the like: documentation for the format,
            # not entries in it.
            continue
        fragments.append(Fragment(match.group("name"), match.group("type")))
    return fragments


def fragment_problems(fragments: Sequence[Fragment]) -> list[str]:
    """Closed-registry violations; [] means every pending type is registered."""
    unregistered = [f for f in fragments if f.type not in FRAGMENT_TYPES]
    if not unregistered:
        return []
    listed = ", ".join(f"{f.name}.{f.type}.md" for f in unregistered)
    return [
        f"unregistered changelog fragment type(s): {listed} — the registry is "
        f"{', '.join(FRAGMENT_TYPES)} (see docs/releasing.md)"
    ]


def release_problems(
    *,
    tag: str,
    metadata_version: str,
    lock_version: str,
    fragments: Sequence[Fragment],
    tagged_commit_on_main: bool,
) -> list[str]:
    """Pure set of release-verification failures; [] means the tag may build."""
    problems: list[str] = []

    try:
        tag_version: str | None = version_from_tag(tag)
    except ValueError as exc:
        problems.append(str(exc))
        tag_version = None

    if tag_version is not None and tag_version != metadata_version:
        problems.append(
            f"tag {tag} declares version {tag_version} but pyproject.toml "
            f"[project].version is {metadata_version} — the release PR must bump "
            "the version and the tag must land on that merged commit"
        )

    if lock_version != metadata_version:
        problems.append(
            f"uv.lock records {PROJECT_DISTRIBUTION} {lock_version} but "
            f"pyproject.toml [project].version is {metadata_version} — refresh "
            "the lockfile with `uv lock` (every CI job installs with "
            "`uv sync --locked`)"
        )

    if not tagged_commit_on_main:
        problems.append(
            "the tagged commit is not an ancestor of main — it has not passed "
            "the required merge gates (ci, integration, quality), which the "
            "release process trusts instead of re-running the docker tiers"
        )

    problems.extend(fragment_problems(fragments))

    if tag_version is not None and patch_component(tag) > 0:
        offending = [f for f in fragments if f.type in MINOR_REQUIRING_TYPES]
        if offending:
            listed = ", ".join(f"{f.name} ({f.type})" for f in sorted(offending))
            problems.append(
                f"PATCH tag {tag} but pending changelog fragments require a MINOR "
                f"release: {listed} — PATCH releases contain fixes and docs only "
                "(docs/releasing.md)"
            )

    return problems


def is_ancestor_of_main(repo_root: Path, commit: str = "HEAD") -> bool:
    """True if ``commit`` is reachable from `origin/main` (or local `main`).

    Fetch depth matters: the release workflow checks out with full history so
    the ancestry is decidable. An unresolvable main ref is reported as *not* an
    ancestor — failing closed, since the whole point is to prove the commit was
    merged through the required checks.
    """
    for ref in ("origin/main", "main"):
        resolved = subprocess.run(
            ["git", "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
        )
        if resolved.returncode != 0:
            continue
        return (
            subprocess.run(
                ["git", "merge-base", "--is-ancestor", commit, ref],
                cwd=repo_root,
                capture_output=True,
                check=False,
            ).returncode
            == 0
        )
    return False


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "tag",
        nargs="?",
        default=os.environ.get("GITHUB_REF_NAME"),
        help="the release tag (default: $GITHUB_REF_NAME)",
    )
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--pyproject", type=Path, default=None)
    parser.add_argument("--lock", type=Path, default=None)
    parser.add_argument("--changelog-dir", type=Path, default=None)
    parser.add_argument(
        "--no-ancestry-check",
        dest="ancestry",
        action="store_false",
        help="skip the ancestor-of-main check (local inspection only)",
    )
    parser.add_argument(
        "--fragments-only",
        action="store_true",
        help="validate the pending fragments against the closed registry and exit "
        "(what `make changelog` runs before assembly)",
    )
    parser.add_argument(
        "--consume-internal",
        action="store_true",
        help="delete the pending `internal` fragments and exit (what `make "
        "changelog` runs after assembly; towncrier does not know the type and "
        "would otherwise leave them pending forever)",
    )
    return parser.parse_args(argv)


def consume_internal(directory: Path) -> list[str]:
    """Delete `internal` fragments, returning the filenames removed.

    `internal` is deliberately not a registered towncrier type — that is what
    makes it render nowhere — but towncrier consequently never removes it
    either, so `changelog.d/` would accumulate every internal fragment ever
    written and the "a fragment is published in exactly one release" property
    would be false for them. Assembly deletes them here instead, after
    towncrier has succeeded.
    """
    removed: list[str] = []
    for fragment in read_fragments(directory):
        if fragment.type != "internal":
            continue
        path = directory / f"{fragment.name}.{fragment.type}.md"
        path.unlink()
        removed.append(path.name)
    return removed


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    root: Path = args.repo_root
    changelog_dir: Path = args.changelog_dir or root / "changelog.d"
    fragments = read_fragments(changelog_dir)

    if args.consume_internal:
        removed = consume_internal(changelog_dir)
        print(f"consumed {len(removed)} internal fragment(s): {', '.join(removed) or 'none'}")
        return 0

    if args.fragments_only:
        problems = fragment_problems(fragments)
    else:
        if not args.tag:
            print(
                "check_release: no tag given and $GITHUB_REF_NAME is unset",
                file=sys.stderr,
            )
            return 1
        pyproject: Path = args.pyproject or root / "pyproject.toml"
        lock: Path = args.lock or root / "uv.lock"
        problems = release_problems(
            tag=args.tag,
            metadata_version=project_version(pyproject.read_text(encoding="utf-8")),
            lock_version=uv_lock_version(lock.read_text(encoding="utf-8")),
            fragments=fragments,
            tagged_commit_on_main=(not args.ancestry) or is_ancestor_of_main(root),
        )

    if problems:
        print("release verification FAILED:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1

    if args.fragments_only:
        print(f"changelog fragments OK: {len(fragments)} pending, all registered types")
    else:
        print(
            f"release verification OK: {args.tag} == pyproject == uv.lock, "
            f"{len(fragments)} pending fragment(s) compatible with the tag"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
