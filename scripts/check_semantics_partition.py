"""Assert the two semantics selections exactly partition the semantics tier.

The tier is release-gating and split across two CI lanes: offline gates run
as a required `ci` check under ``-m "semantics and not integration"``, and
docker-backed gates run in the `integration` workflow under
``-m "semantics and integration"``. A test carrying the ``semantics`` marker
that neither selection picks up cannot exist by construction of those two
expressions — what CAN go wrong is a selection being run with a stale or
mistyped expression, or a gate losing its marker entirely. This script fails
the build if the union of the two selections differs from the bare
``-m semantics`` selection, or if either selection is empty.

Runs offline (collection only, no docker).
"""

from __future__ import annotations

import subprocess
import sys


def collect(marker_expr: str) -> set[str]:
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-m",
            marker_expr,
            "--collect-only",
            "-q",
            "-p",
            "no:cacheprovider",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    # Exit 5 = nothing collected: legal here, the emptiness check is ours.
    if proc.returncode not in (0, 5):
        print(proc.stdout, proc.stderr, sep="\n", file=sys.stderr)
        raise SystemExit(f"collection failed for -m {marker_expr!r}: {proc.returncode}")
    return {
        line.strip()
        for line in proc.stdout.splitlines()
        if "::" in line and not line.startswith(" ")
    }


def main() -> int:
    offline = collect("semantics and not integration")
    docker = collect("semantics and integration")
    everything = collect("semantics")

    problems: list[str] = []
    if not offline:
        problems.append("the offline semantics selection is EMPTY")
    if not docker:
        problems.append("the docker semantics selection is EMPTY")
    if overlap := offline & docker:
        problems.append(f"selections overlap: {sorted(overlap)[:5]}")
    if uncovered := everything - (offline | docker):
        problems.append(f"semantics tests covered by NEITHER lane: {sorted(uncovered)[:5]}")
    if phantom := (offline | docker) - everything:
        problems.append(f"selected tests without the semantics marker?! {sorted(phantom)[:5]}")

    if problems:
        print("semantics tier partition check FAILED:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1

    print(
        f"semantics tier partition OK: {len(offline)} offline + {len(docker)} docker "
        f"= {len(everything)} total"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
