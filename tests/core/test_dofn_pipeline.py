"""Pipeline tests for single-activation routing and state topology.

Covers the bounded (order-insensitive) scenarios: the state/timer topology is
protobuf-coded and pickle-free, a fresh key seeds seq 0, an event starts an
activation, and an unmatched resume routes to ``.errors``. Ordered multi-element
scenarios (resume, seq progression, timeouts, timers, interleaving) live in
test_dofn_streaming.
"""

from __future__ import annotations

import apache_beam as beam
import pytest
from apache_beam.coders.coders import VarIntCoder
from apache_beam.coders.typecoders import registry as coder_registry

# Aliased: a bare "TestPipeline" name would be mis-collected by pytest.
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to

from beam_agents._protos import AgentEnvelope, ToolResult
from beam_agents.core.coders import DeterministicProtoCoder, register_coders
from beam_agents.core.dofn import REASON_ORPHANED, ActivationError, _AgentDoFn
from beam_agents.core.transform import AgentConfig, RunAgent
from tests.core._dofn_helpers import append_agent, keyed, make_pong_provider, seq_agent


def _event(key: bytes, payload: bytes, t_ms: int = 1000) -> AgentEnvelope:
    return AgentEnvelope(entity_key=key, event_time_ms=t_ms, external_event=payload)


# --- Requirement: keyed state and timer topology -------------------------------


def test_proto_state_specs_use_deterministic_coders() -> None:
    # Scenario: state specs are protobuf-backed and pickle-free.
    for spec in (_AgentDoFn.MEMORY, _AgentDoFn.CONTINUATION, _AgentDoFn.LLM_CACHE):
        assert isinstance(spec.coder, DeterministicProtoCoder)
    assert isinstance(_AgentDoFn.PENDING.coder, DeterministicProtoCoder)
    # SEQ is an integer counter, coded as a varint (not pickle).
    assert isinstance(_AgentDoFn.SEQ.coder, VarIntCoder)


def test_run_agent_registers_deterministic_envelope_coder() -> None:
    register_coders()
    resolved = coder_registry.get_coder(AgentEnvelope)
    assert isinstance(resolved, DeterministicProtoCoder)
    assert resolved.is_deterministic() is True


# --- Requirement: element routing by envelope kind -----------------------------


def test_fresh_key_reads_seq_zero() -> None:
    # Scenario: a fresh key reads versioned-empty facades and zero seq.
    with BeamTestPipeline() as p:
        envs = p | beam.Create([_event(b"k", b"go")])
        out = keyed(envs) | RunAgent(
            seq_agent, config=AgentConfig(provider_factory=make_pong_provider)
        )
        assert_that(out.output, equal_to([b"0"]))


def test_event_starts_activation() -> None:
    # Scenario: event starts an activation (and its memory write commits).
    with BeamTestPipeline() as p:
        envs = p | beam.Create([_event(b"k", b"hello")])
        out = keyed(envs) | RunAgent(
            append_agent, config=AgentConfig(provider_factory=make_pong_provider)
        )
        assert_that(out.output, equal_to([b"hello#0"]))


def test_orphaned_result_routes_to_errors() -> None:
    # Scenario: orphaned resume mutates nothing (no continuation to match).
    envelope = AgentEnvelope(entity_key=b"k", event_time_ms=1000)
    envelope.tool_result.intent_id = "ghost"
    envelope.tool_result.status = ToolResult.OK

    with BeamTestPipeline() as p:
        envs = p | beam.Create([envelope])
        out = keyed(envs) | RunAgent(
            seq_agent, config=AgentConfig(provider_factory=make_pong_provider)
        )
        assert_that(out.output, equal_to([]), label="no-output")
        assert_that(
            out.errors,
            equal_to([ActivationError(b"k", REASON_ORPHANED, "ghost")]),
            label="orphaned-error",
        )


def test_orphaned_approval_routes_to_errors() -> None:
    # Scenario: an approval-kind element with no matching continuation is also
    # routed as orphaned (exercises the approval routing branch).
    envelope = AgentEnvelope(entity_key=b"k", event_time_ms=1000)
    envelope.approval.intent_id = "ghost"
    envelope.approval.approved = True

    with BeamTestPipeline() as p:
        envs = p | beam.Create([envelope])
        out = keyed(envs) | RunAgent(
            seq_agent, config=AgentConfig(provider_factory=make_pong_provider)
        )
        assert_that(out.output, equal_to([]), label="no-output")
        assert_that(
            out.errors,
            equal_to([ActivationError(b"k", REASON_ORPHANED, "ghost")]),
            label="orphaned-approval",
        )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
