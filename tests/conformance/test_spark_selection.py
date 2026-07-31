"""Marker-taxonomy pinning for the weekly spark leg.

Spec scenarios (`spark-runner-support`): *Pull-request workflows are
unchanged* and *Spark cells are counted but stay out of the semantics
partition*.

The spark leg's whole containment story is a marker expression: cells carry
``integration + spark`` and never ``semantics`` (design D4), and the four
per-PR selections each exclude them. Nothing in the type system or the test
bodies enforces that — a single edit to a ``-m`` expression or a stray
``pytest.mark.semantics`` would leak a best-effort runner's leg into a
required check, or orphan the leg entirely, and both failures are silent.
This module makes them loud by collecting the real selections.

Offline: ``--collect-only`` through the same ``collect()`` helper
``scripts/check_semantics_partition.py`` uses, so no docker service is
started and no Spark container is required.
"""

from __future__ import annotations

import pytest
from scripts.check_semantics_partition import collect

from tests.conformance._registry import ADAPTERS
from tests.conformance._spec import SCENARIOS

#: Every spark cell lives here — one module per leg, exactly as the Flink leg
#: lives in test_flink.py.
SPARK_CELL_MODULE = "tests/conformance/test_spark.py"

#: The selections a pull request can reach: the base integration lane, the
#: required offline semantics lane, the docker-backed semantics lane, and the
#: Flink conformance lane (Makefile: test-integration, test-semantics-offline,
#: test-semantics, test-conformance-flink).
PER_PR_SELECTIONS = (
    "integration and not semantics and not spark",
    "semantics and not integration",
    "semantics and integration",
)

#: What the weekly workflow runs (Makefile: test-conformance-spark).
SPARK_SELECTION = "integration and spark"


def _spark_cells(nodeids: set[str]) -> set[str]:
    return {nodeid for nodeid in nodeids if nodeid.startswith(SPARK_CELL_MODULE)}


@pytest.mark.slow
@pytest.mark.timeout(600)
def test_no_per_pr_selection_collects_a_spark_cell() -> None:
    # Scenario: Pull-request workflows are unchanged.
    for selection in PER_PR_SELECTIONS:
        leaked = _spark_cells(collect(selection))
        assert not leaked, (
            f"the per-PR selection -m {selection!r} collected spark-leg cells "
            f"{sorted(leaked)[:5]} — a best-effort runner's leg must never run on a "
            f"pull request (promote-spark-runner design D3)"
        )


@pytest.mark.slow
@pytest.mark.timeout(600)
def test_spark_selection_collects_exactly_the_declared_spark_cells() -> None:
    # Scenario: Spark cells are counted but stay out of the semantics partition
    # — the counted half. The expected size is derived from the registry and
    # the scenario list (declared skips are cells too), never a literal, so a
    # fifth adapter grows this assertion by itself.
    collected = collect(SPARK_SELECTION, paths=("tests/conformance",))
    assert collected == _spark_cells(collected), (
        f"-m {SPARK_SELECTION!r} collected non-spark tests: "
        f"{sorted(collected - _spark_cells(collected))[:5]}"
    )
    assert len(collected) == len(ADAPTERS) * len(SCENARIOS), (
        f"-m {SPARK_SELECTION!r} collected {len(collected)} cells; the matrix declares "
        f"{len(ADAPTERS)} adapters x {len(SCENARIOS)} scenarios on the spark leg"
    )


@pytest.mark.slow
@pytest.mark.timeout(600)
def test_spark_cells_carry_no_semantics_marker() -> None:
    # Scenario: Spark cells are counted but stay out of the semantics partition
    # — the partition half. Checked against the bare `semantics` selection
    # rather than the two lane selections, so a spark cell that acquired the
    # marker fails here even if some future lane expression happened to
    # exclude it anyway.
    leaked = _spark_cells(collect("semantics"))
    assert not leaked, (
        f"spark-leg cells carry the semantics marker: {sorted(leaked)[:5]} — the "
        f"semantics tier is release gates that never skip, which a best-effort leg "
        f"cannot promise (promote-spark-runner design D4)"
    )


@pytest.mark.slow
@pytest.mark.timeout(600)
def test_semantics_partition_selections_are_unchanged_by_the_spark_leg() -> None:
    # The `scripts/check_semantics_partition.py` invariant, restated where the
    # spark leg could break it: both lane selections stay non-empty and
    # disjoint, and their union is still the whole semantics tier.
    offline = collect("semantics and not integration")
    docker = collect("semantics and integration")
    everything = collect("semantics")
    assert offline and docker
    assert offline & docker == set()
    assert offline | docker == everything
