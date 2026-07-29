"""Trace/span identity tests for the `trace-events` capability.

Covers: the wire widths, reproducibility across processes, collision-freedom
across roles and indices, and the absence of any ambient-state read in the
derivation.
"""

from __future__ import annotations

import random
import subprocess
import sys
import time

import pytest

from beam_agents._protos import TraceEvent
from beam_agents.observability import (
    ROLE_ACTIVATION,
    ROLE_TIMER,
    span_id_for,
    trace_id_for,
)
from beam_agents.observability.traces import role_for_event_type

# --- Requirement: Deterministic trace and span identity ----------------------


def test_trace_id_is_sixteen_bytes_and_span_id_is_eight() -> None:
    # Widths match W3C trace-context / OTel, so an exporter passes them through
    # untranslated.
    assert len(trace_id_for(b"key-1", 3)) == 16
    assert len(span_id_for(b"key-1", 3, ROLE_ACTIVATION, 0)) == 8


def test_identifiers_are_reproducible_across_processes() -> None:
    # Scenario: Identifiers are reproducible across processes.
    # A fresh interpreter is the honest test: a per-process seed or a hash
    # randomization dependency would show up here and nowhere else.
    source = (
        "from beam_agents.observability import span_id_for, trace_id_for, ROLE_ACTIVATION;"
        "print(trace_id_for(b'key-1', 3).hex());"
        "print(span_id_for(b'key-1', 3, ROLE_ACTIVATION, 0).hex())"
    )
    out = subprocess.run(
        [sys.executable, "-c", source],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()

    assert out == [
        trace_id_for(b"key-1", 3).hex(),
        span_id_for(b"key-1", 3, ROLE_ACTIVATION, 0).hex(),
    ]


def test_trace_id_is_scoped_to_entity_key_and_seq() -> None:
    # Scenario: A new seq starts a new trace.
    assert trace_id_for(b"key-1", 7) != trace_id_for(b"key-1", 8)
    assert trace_id_for(b"key-1", 7) != trace_id_for(b"key-2", 7)
    assert trace_id_for(b"key-1", 7) == trace_id_for(b"key-1", 7)


def test_span_ids_do_not_collide_across_roles_or_indices() -> None:
    # The role component is what makes a global counter unnecessary.
    ids = {
        span_id_for(b"key-1", 3, role, index)
        for role in (ROLE_ACTIVATION, ROLE_TIMER, "LLM_CALL", "INTENT_EMITTED")
        for index in range(4)
    }
    assert len(ids) == 16


def test_different_event_kinds_at_one_step_get_different_span_ids() -> None:
    # Scenario: Different event kinds at the same step do not share a span id.
    # `AgentContext` lets the agent pick the step_index it passes to the
    # facade, drawn from the same space as intent step indices, so this is a
    # real collision without the role component.
    llm = span_id_for(b"key-1", 3, role_for_event_type(TraceEvent.LLM_CALL), 2)
    intent = span_id_for(b"key-1", 3, role_for_event_type(TraceEvent.INTENT_EMITTED), 2)
    assert llm != intent


def test_derivation_reads_no_clock_or_randomness(monkeypatch: pytest.MonkeyPatch) -> None:
    # Scenario: Identity derivation reads no ambient state.
    def _boom(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("identity derivation must not read ambient state")

    monkeypatch.setattr(time, "time", _boom)
    monkeypatch.setattr(time, "monotonic", _boom)
    monkeypatch.setattr(random, "random", _boom)
    monkeypatch.setattr(random, "getrandbits", _boom)

    assert len(trace_id_for(b"key-1", 3)) == 16
    assert len(span_id_for(b"key-1", 3, ROLE_ACTIVATION, 0)) == 8
