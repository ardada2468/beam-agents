"""Assert the semantics tier's CI lanes both partition and cover the tier.

Two properties, both release-gating:

1. **Marker partition across lanes.** The tier is split across two CI lanes:
   offline gates run as a required `ci` check under
   ``-m "semantics and not integration"``, and docker-backed gates run in the
   `integration` workflow's `flink-minicluster` job under
   ``-m "semantics and integration"``. A test carrying the ``semantics``
   marker that neither selection picks up cannot exist by construction of
   those two expressions — what CAN go wrong is a selection being run with a
   stale or mistyped expression, or a gate losing its marker entirely. This
   script fails the build if the union of the two selections differs from the
   bare ``-m semantics`` selection, or if either selection is empty.

2. **Path coverage within the docker lane.** The docker lane's make targets
   are path-scoped: `make test-semantics` runs the docker selection over
   ``tests/semantics`` only, and `make test-conformance-flink` over
   ``tests/conformance`` only (kept separate so an e2e-gate timeout and a
   conformance failure stay distinguishable). A ``semantics and integration``
   test added anywhere else would satisfy property 1 yet be executed by
   NEITHER target — green partition, gate never runs. This script fails the
   build if the two path-scoped selections are empty, overlap, or their union
   differs from the repo-wide docker selection, naming any escaped test.

Runs offline (collection only, no docker).
"""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Sequence


def collect(marker_expr: str, paths: Sequence[str] = ()) -> set[str]:
    """Collect the nodeids ``pytest -m marker_expr [paths...]`` selects.

    ``paths`` mirrors the docker-lane make targets' invocations (e.g.
    ``("tests/semantics",)``); empty means repo-wide, exactly as before.
    """
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
            *paths,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    # Exit 5 = nothing collected: legal here, the emptiness check is ours.
    if proc.returncode not in (0, 5):
        print(proc.stdout, proc.stderr, sep="\n", file=sys.stderr)
        scope = f" over {list(paths)}" if paths else ""
        raise SystemExit(f"collection failed for -m {marker_expr!r}{scope}: {proc.returncode}")
    return {
        line.strip()
        for line in proc.stdout.splitlines()
        if "::" in line and not line.startswith(" ")
    }


def partition_problems(
    *,
    offline: set[str],
    docker: set[str],
    everything: set[str],
    docker_semantics: set[str],
    docker_conformance: set[str],
) -> list[str]:
    """Pure set algebra over the five collected selections; [] means healthy.

    ``offline``/``docker``/``everything`` are the repo-wide marker selections
    (property 1); ``docker_semantics``/``docker_conformance`` are the docker
    marker selection scoped to the paths `make test-semantics` and
    `make test-conformance-flink` actually run (property 2).
    """
    problems: list[str] = []

    # Property 1: the marker selections partition the tier across lanes.
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

    # Property 2: the path-scoped make targets cover the docker lane.
    if not docker_semantics:
        problems.append(
            "the tests/semantics docker selection is EMPTY "
            "(`make test-semantics` would run nothing — the e2e gate is deselected)"
        )
    if not docker_conformance:
        problems.append(
            "the tests/conformance docker selection is EMPTY "
            "(`make test-conformance-flink` would run nothing — the Flink leg is deselected)"
        )
    if path_overlap := docker_semantics & docker_conformance:
        problems.append(
            f"the path-scoped docker selections are not disjoint: {sorted(path_overlap)[:5]}"
        )
    if escaped := docker - (docker_semantics | docker_conformance):
        problems.append(
            "docker-backed semantics tests OUTSIDE both path-scoped make targets — "
            "neither `make test-semantics` (tests/semantics) nor "
            "`make test-conformance-flink` (tests/conformance) ever runs them: "
            f"{sorted(escaped)[:5]}"
        )
    if stray := (docker_semantics | docker_conformance) - docker:
        problems.append(
            "path-scoped selections collected tests the repo-wide docker "
            f"selection does not?! {sorted(stray)[:5]}"
        )

    return problems


def main() -> int:
    offline = collect("semantics and not integration")
    docker = collect("semantics and integration")
    everything = collect("semantics")
    # Mirror the docker-lane make targets' exact invocations (Makefile:
    # test-semantics and test-conformance-flink).
    docker_semantics = collect("semantics and integration", paths=("tests/semantics",))
    docker_conformance = collect("semantics and integration", paths=("tests/conformance",))

    problems = partition_problems(
        offline=offline,
        docker=docker,
        everything=everything,
        docker_semantics=docker_semantics,
        docker_conformance=docker_conformance,
    )

    if problems:
        print("semantics tier partition check FAILED:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1

    print(
        f"semantics tier partition OK: {len(offline)} offline + {len(docker)} docker "
        f"= {len(everything)} total; docker lane covered by "
        f"{len(docker_semantics)} tests/semantics + {len(docker_conformance)} tests/conformance"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
