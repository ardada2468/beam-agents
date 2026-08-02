"""The spark leg: the conformance matrix on the Spark job server.

**Every scenario on this leg is currently a declared skip, so this module
submits no job.** Seven cells per adapter are still collected and reported as
skips carrying their declared reasons, which is what keeps the matrix
meta-test's registry x scenario x leg accounting balanced.

Three of the seven are structural constraints of the overlay and harness
(``restart_mid_suspension`` — the embedded ``local[4]`` master has no separate
worker container to restart; ``bundle_retry_cache`` — the in-process chaos
monkeypatch cannot reach the spark-scoped SDK-harness container;
``ttl_expiry`` — no idle-partition watermark control). The other four
(``single_shot``, ``multi_tool_inline``, ``suspension_resume``,
``approval_timeout_fallback``) were provisional ``Run()`` declarations until
the first real job-server execution on 2026-07-31, which failed all sixteen
runnable cells identically: Beam's Spark portable runner registers no bundle
checkpoint handler, and the leg's spool ingest is an ``unbounded_per_element``
SDF that self-checkpoints via ``tracker.defer_remainder``. See
``openspec/changes/record-spark-sdf-checkpoint-gap/``.

The per-adapter run machinery that drove those four cells was removed with
them rather than left unreachable — ``tests/conformance/_spark/harness.py`` is
untouched and still provides ``run_adapter_leg``, so restoring a cell is
re-adding its assertions, not rebuilding the leg. Do that only alongside
evidence that the gap is closed (a runner that registers the handler, or a
non-SDF ingest for this leg), and flip the scenario's declaration in
``_spec.py`` in the same change.

Markers, per design D4: ``integration + spark``, deliberately **not**
``semantics``. Spark is best-effort, and the semantics tier is defined as
release gates that never get skipped or marked flaky — a promise a
best-effort leg cannot make. The consequence is that no per-PR selection can
reach these cells; only `make test-conformance-spark`, which the weekly
`spark-weekly` workflow runs, does.
"""

from __future__ import annotations

import pytest

from tests.conformance._cells import adapter_params
from tests.conformance._spec import (
    APPROVAL_TIMEOUT_FALLBACK,
    BUNDLE_RETRY_CACHE,
    MULTI_TOOL_INLINE,
    RESTART_MID_SUSPENSION,
    SINGLE_SHOT,
    SPARK,
    SUSPENSION_RESUME,
    TTL_EXPIRY,
    ScenarioSpec,
    Skip,
)

pytestmark = [pytest.mark.integration, pytest.mark.spark, pytest.mark.slow]


def _declared_skip(spec: ScenarioSpec) -> str:
    """The scenario's declared spark skip reason (spec scenario: *A
    spark-inexpressible scenario is an explicit skip with a reason*).

    The assertion is the tripwire that keeps this module honest: if a scenario
    is ever re-declared ``Run()`` on spark, its cell here must run it rather
    than report a skip that no longer has a reason to exist.
    """
    declaration = spec.legs[SPARK]
    assert isinstance(declaration, Skip), (
        f"{spec.name!r} is declared runnable on spark but has no cell test that runs it"
    )
    return declaration.reason


# ---------------------------------------------------------------------------------


@pytest.mark.parametrize("adapter_name", adapter_params(SINGLE_SHOT, SPARK))
def test_spark_single_shot(adapter_name: str) -> None:
    pytest.skip(_declared_skip(SINGLE_SHOT))


@pytest.mark.parametrize("adapter_name", adapter_params(MULTI_TOOL_INLINE, SPARK))
def test_spark_multi_tool_inline(adapter_name: str) -> None:
    pytest.skip(_declared_skip(MULTI_TOOL_INLINE))


@pytest.mark.parametrize("adapter_name", adapter_params(SUSPENSION_RESUME, SPARK))
def test_spark_suspension_resume(adapter_name: str) -> None:
    pytest.skip(_declared_skip(SUSPENSION_RESUME))


@pytest.mark.parametrize("adapter_name", adapter_params(APPROVAL_TIMEOUT_FALLBACK, SPARK))
def test_spark_approval_timeout_fallback(adapter_name: str) -> None:
    pytest.skip(_declared_skip(APPROVAL_TIMEOUT_FALLBACK))


@pytest.mark.parametrize("adapter_name", adapter_params(RESTART_MID_SUSPENSION, SPARK))
def test_spark_restart_mid_suspension(adapter_name: str) -> None:
    pytest.skip(_declared_skip(RESTART_MID_SUSPENSION))


@pytest.mark.parametrize("adapter_name", adapter_params(BUNDLE_RETRY_CACHE, SPARK))
def test_spark_bundle_retry_cache(adapter_name: str) -> None:
    pytest.skip(_declared_skip(BUNDLE_RETRY_CACHE))


@pytest.mark.parametrize("adapter_name", adapter_params(TTL_EXPIRY, SPARK))
def test_spark_ttl_expiry(adapter_name: str) -> None:
    pytest.skip(_declared_skip(TTL_EXPIRY))
