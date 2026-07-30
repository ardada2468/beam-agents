"""The Flink leg: the conformance matrix on the compose mini-cluster.

One multiplexed job per adapter (design D5) drives all Flink-runnable
scenarios at once; the first cell touching an adapter pays for its whole run,
and every cell of that adapter asserts against the captured results. A
scenario failure is therefore diagnosed from per-key observations — every
assertion names its scenario key prefix. Stack problems surface as
``InfraFailure``, never as an adapter verdict.

``bundle_retry_cache`` and ``ttl_expiry`` are declared skips on this leg (the
reasons live on their specs); the meta-test counts them as cells.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from beam_agents._protos import ToolIntent
from beam_agents.core.agent import intent_id_for
from beam_agents.core.dofn import REASON_ORPHANED
from tests.conformance._cells import adapter_params, require_framework
from tests.conformance._flink.harness import FlinkLegResults, run_adapter_leg
from tests.conformance._registry import validated_bundle
from tests.conformance._spec import (
    APPROVAL_TIMEOUT_FALLBACK,
    BUNDLE_RETRY_CACHE,
    FLINK,
    MULTI_TOOL_INLINE,
    RESTART_MID_SUSPENSION,
    SINGLE_SHOT,
    SUSPENSION_RESUME,
    TTL_EXPIRY,
    ScenarioSpec,
    Skip,
)

pytestmark = [pytest.mark.semantics, pytest.mark.integration, pytest.mark.slow]

# The first cell per adapter runs the whole leg (freshen + submit + scenarios
# + TaskManager restart + real-time HITL deadline); later cells are reads.
_LEG_TIMEOUT_S = 1200

#: Memoized per-adapter leg results (or the failure that produced none), so
#: one job serves every cell of its adapter.
_RESULTS: dict[str, FlinkLegResults | BaseException] = {}


def _results_for(adapter_name: str) -> FlinkLegResults:
    if adapter_name not in _RESULTS:
        try:
            _RESULTS[adapter_name] = asyncio.run(run_adapter_leg(adapter_name))
        except BaseException as exc:  # re-raised for every cell of the adapter
            _RESULTS[adapter_name] = exc
    result = _RESULTS[adapter_name]
    if isinstance(result, BaseException):
        raise result
    return result


def _cell(adapter_name: str, spec: ScenarioSpec) -> FlinkLegResults:
    require_framework(adapter_name)
    # Equivalence-check the adapter's bundle for this scenario before reading
    # any results (task 1.3's guarantee, host-side for the Flink leg).
    validated_bundle(adapter_name, spec.name)
    return _results_for(adapter_name)


def _expected_intent_id(results: FlinkLegResults, spec: ScenarioSpec) -> str:
    expectation = spec.expected_intents[0]
    return intent_id_for(results.keys[spec.name], expectation.seq, expectation.step_index)


def _assert_single_deterministic_intent(results: FlinkLegResults, spec: ScenarioSpec) -> None:
    blobs = results.intents_for(spec.name)
    assert len(blobs) == 1, (
        f"[{spec.name}] expected exactly one distinct committed intent for the "
        f"scenario's key, got {len(blobs)} — at-least-once duplicates collapse by "
        f"identity, so >1 means replay minted different bytes"
    )
    intent = ToolIntent.FromString(next(iter(blobs)))
    expectation = spec.expected_intents[0]
    assert intent.intent_id == _expected_intent_id(results, spec), (
        f"[{spec.name}] intent_id diverged from the deterministic formula"
    )
    assert intent.step_index == expectation.step_index
    assert intent.seq == expectation.seq
    assert intent.tool_name == expectation.tool_name
    assert intent.kind == expectation.kind


# ---------------------------------------------------------------------------------


@pytest.mark.timeout(_LEG_TIMEOUT_S)
@pytest.mark.parametrize("adapter_name", adapter_params(SINGLE_SHOT, FLINK))
def test_flink_single_shot(adapter_name: str) -> None:
    spec = SINGLE_SHOT
    results = _cell(adapter_name, spec)
    assert spec.expected_outputs[0] in results.outputs, (
        f"[{spec.name}] terminal missing from the output topic: {sorted(results.outputs)!r}"
    )
    assert not results.intents_for(spec.name), f"[{spec.name}] fast path staged intents"
    assert not results.errors_for(spec.name), f"[{spec.name}] unexpected errors"


@pytest.mark.timeout(_LEG_TIMEOUT_S)
@pytest.mark.parametrize("adapter_name", adapter_params(MULTI_TOOL_INLINE, FLINK))
def test_flink_multi_tool_inline(adapter_name: str) -> None:
    spec = MULTI_TOOL_INLINE
    results = _cell(adapter_name, spec)
    # The terminal embeds both read-only tool results: inline execution and
    # its ordering, observed over Kafka.
    assert spec.expected_outputs[0] in results.outputs, (
        f"[{spec.name}] terminal missing from the output topic: {sorted(results.outputs)!r}"
    )
    assert not results.intents_for(spec.name), f"[{spec.name}] inline tools staged intents"
    assert not results.errors_for(spec.name), f"[{spec.name}] unexpected errors"


@pytest.mark.timeout(_LEG_TIMEOUT_S)
@pytest.mark.parametrize("adapter_name", adapter_params(SUSPENSION_RESUME, FLINK))
def test_flink_suspension_resume(adapter_name: str) -> None:
    spec = SUSPENSION_RESUME
    results = _cell(adapter_name, spec)
    assert spec.expected_outputs[0] in results.outputs, (
        f"[{spec.name}] resumed terminal missing: {sorted(results.outputs)!r}"
    )
    _assert_single_deterministic_intent(results, spec)
    assert not results.errors_for(spec.name), f"[{spec.name}] unexpected errors"


@pytest.mark.timeout(_LEG_TIMEOUT_S)
@pytest.mark.parametrize("adapter_name", adapter_params(APPROVAL_TIMEOUT_FALLBACK, FLINK))
def test_flink_approval_timeout_fallback(adapter_name: str) -> None:
    spec = APPROVAL_TIMEOUT_FALLBACK
    results = _cell(adapter_name, spec)
    assert spec.expected_outputs[0] in results.outputs, (
        f"[{spec.name}] fail-closed timeout terminal missing: {sorted(results.outputs)!r}"
    )
    _assert_single_deterministic_intent(results, spec)
    reasons = {value.split(b"|")[0] for value in results.errors_for(spec.name)}
    assert REASON_ORPHANED.encode() in reasons, (
        f"[{spec.name}] the late decision vanished instead of surfacing as "
        f"orphaned_result (got {reasons!r})"
    )


@pytest.mark.timeout(_LEG_TIMEOUT_S)
@pytest.mark.parametrize("adapter_name", adapter_params(RESTART_MID_SUSPENSION, FLINK))
def test_flink_restart_mid_suspension(adapter_name: str) -> None:
    # Scenario: Flink leg survives a TaskManager restart mid-suspension. The
    # harness restarted the TaskManager between the observed suspend intent
    # and the result publish; the terminal below was therefore produced by an
    # activation resumed from checkpointed state on a fresh TaskManager.
    spec = RESTART_MID_SUSPENSION
    results = _cell(adapter_name, spec)
    assert spec.expected_outputs[0] in results.outputs, (
        f"[{spec.name}] post-restart resumed terminal missing: {sorted(results.outputs)!r}"
    )
    _assert_single_deterministic_intent(results, spec)
    intent = ToolIntent.FromString(next(iter(results.intents_for(spec.name))))
    assert json.loads(intent.args_json) == dict(spec.turns[0].args)


@pytest.mark.parametrize("adapter_name", adapter_params(BUNDLE_RETRY_CACHE, FLINK))
def test_flink_bundle_retry_cache(adapter_name: str) -> None:
    declaration = BUNDLE_RETRY_CACHE.legs[FLINK]
    assert isinstance(declaration, Skip)
    pytest.skip(declaration.reason)


@pytest.mark.parametrize("adapter_name", adapter_params(TTL_EXPIRY, FLINK))
def test_flink_ttl_expiry(adapter_name: str) -> None:
    declaration = TTL_EXPIRY.legs[FLINK]
    assert isinstance(declaration, Skip)
    pytest.skip(declaration.reason)
