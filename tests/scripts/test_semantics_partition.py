"""Unit tests for the semantics-partition check's pure set algebra.

`scripts/check_semantics_partition.py` guards two properties of the
release-gating semantics tier: the offline and docker *marker* selections
partition the tier across the two CI lanes, and the docker lane's two
*path-scoped* make-target selections (`tests/semantics`, `tests/conformance`)
together cover every docker-backed semantics test in the repo. The set logic
is a pure function, tested here with hand-built nodeid sets; the final test
pins the coverage property against the real repo layout via the script's own
``collect()`` (recorded from ``--collect-only``, no docker services).

Spec: openspec/changes/add-flink-minicluster-ci —
"No docker-backed semantics test can escape the Flink lane's path-scoped
targets".
"""

from __future__ import annotations

import pytest
from scripts.check_semantics_partition import collect, partition_problems

# A current-shape layout in miniature: offline gates in tests/semantics and
# the conformance DirectRunner leg; docker gates split between the e2e gate
# (tests/semantics) and the conformance Flink leg (tests/conformance).
OFFLINE = {
    "tests/semantics/test_retry_determinism.py::test_no_extra_llm_calls",
    "tests/conformance/test_direct.py::test_cell[protocol-fast_path]",
}
DOCKER_SEMANTICS = {
    "tests/semantics/test_effectively_once_e2e.py::test_effectively_once",
}
DOCKER_CONFORMANCE = {
    "tests/conformance/test_flink.py::test_cell[protocol-fast_path]",
}
DOCKER = DOCKER_SEMANTICS | DOCKER_CONFORMANCE
EVERYTHING = OFFLINE | DOCKER


def _problems(
    *,
    offline: set[str] = OFFLINE,
    docker: set[str] = DOCKER,
    everything: set[str] = EVERYTHING,
    docker_semantics: set[str] = DOCKER_SEMANTICS,
    docker_conformance: set[str] = DOCKER_CONFORMANCE,
) -> list[str]:
    return partition_problems(
        offline=offline,
        docker=docker,
        everything=everything,
        docker_semantics=docker_semantics,
        docker_conformance=docker_conformance,
    )


class TestCurrentLayoutShape:
    """From scenario: The current layout passes."""

    def test_current_shape_inputs_return_no_problems(self) -> None:
        assert _problems() == []


class TestEscapedDockerSemanticsTest:
    """From scenario: An escaped docker-semantics test fails the required check."""

    def test_escaped_test_is_reported_by_nodeid(self) -> None:
        escaped = "tests/effector/test_chaos_docker.py::test_new_gate"
        problems = _problems(
            docker=DOCKER | {escaped},
            everything=EVERYTHING | {escaped},
        )
        assert any(escaped in problem for problem in problems)

    def test_escape_message_names_neither_make_target_runs_it(self) -> None:
        escaped = "tests/effector/test_chaos_docker.py::test_new_gate"
        problems = _problems(
            docker=DOCKER | {escaped},
            everything=EVERYTHING | {escaped},
        )
        assert any("path-scoped" in problem or "make target" in problem for problem in problems)


class TestEmptiedPathScopedSelection:
    """From scenario: An emptied path-scoped selection fails."""

    def test_empty_tests_semantics_selection_is_reported(self) -> None:
        problems = _problems(
            docker=DOCKER_CONFORMANCE,
            everything=OFFLINE | DOCKER_CONFORMANCE,
            docker_semantics=set(),
        )
        assert any("tests/semantics" in problem and "EMPTY" in problem for problem in problems)

    def test_empty_tests_conformance_selection_is_reported(self) -> None:
        problems = _problems(
            docker=DOCKER_SEMANTICS,
            everything=OFFLINE | DOCKER_SEMANTICS,
            docker_conformance=set(),
        )
        assert any("tests/conformance" in problem and "EMPTY" in problem for problem in problems)


class TestPreexistingPartitionProblemsStillSurface:
    """The original marker-partition assertions survive the refactor unchanged."""

    def test_empty_offline_selection(self) -> None:
        problems = _problems(offline=set(), everything=DOCKER)
        assert any("offline" in problem and "EMPTY" in problem for problem in problems)

    def test_empty_docker_selection(self) -> None:
        problems = _problems(
            docker=set(),
            everything=OFFLINE,
            docker_semantics=set(),
            docker_conformance=set(),
        )
        assert any("docker" in problem and "EMPTY" in problem for problem in problems)

    def test_lane_overlap(self) -> None:
        stowaway = next(iter(DOCKER_SEMANTICS))
        problems = _problems(offline=OFFLINE | {stowaway})
        assert any("overlap" in problem for problem in problems)

    def test_uncovered_semantics_test(self) -> None:
        orphan = "tests/core/test_orphan.py::test_unselected_gate"
        problems = _problems(everything=EVERYTHING | {orphan})
        assert any("NEITHER" in problem and orphan in problem for problem in problems)

    def test_phantom_selection(self) -> None:
        phantom = "tests/core/test_phantom.py::test_not_semantics"
        problems = _problems(offline=OFFLINE | {phantom})
        assert any("without the semantics marker" in problem for problem in problems)


class TestPathScopedConsistency:
    def test_overlapping_path_selections_are_reported(self) -> None:
        shared = next(iter(DOCKER_SEMANTICS))
        problems = _problems(docker_conformance=DOCKER_CONFORMANCE | {shared})
        assert any("disjoint" in problem or "overlap" in problem for problem in problems)

    def test_path_selection_outside_docker_selection_is_reported(self) -> None:
        stray = "tests/semantics/test_stray.py::test_missing_marker"
        problems = _problems(docker_semantics=DOCKER_SEMANTICS | {stray})
        assert any(stray in problem for problem in problems)


@pytest.mark.slow
@pytest.mark.timeout(300)
def test_repo_docker_semantics_population_is_covered_by_the_path_scoped_selections() -> None:
    """From scenario: The current layout passes (against the real repo).

    Pins, before any workflow change depends on it, that today's repo-wide
    ``semantics and integration`` population is exactly the union of the two
    path-scoped selections the docker-lane make targets run. Uses the script's
    own ``collect()`` — the same ``--collect-only`` recording ``main()`` uses —
    so a drift between the Makefile's path scoping and the test layout fails
    here and in the required `ci` step alike. Offline: collection only.
    """
    docker = collect("semantics and integration")
    docker_semantics = collect("semantics and integration", paths=("tests/semantics",))
    docker_conformance = collect("semantics and integration", paths=("tests/conformance",))

    assert docker_semantics, "the tests/semantics docker selection collected nothing"
    assert docker_conformance, "the tests/conformance docker selection collected nothing"
    assert docker_semantics | docker_conformance == docker
    assert docker_semantics & docker_conformance == set()
