"""The downstream failure-streak alarm from `docs/errors.md`, exercised.

The point of encoding `.errors` as `AgentEnvelope`-wrapped records is that the
errors topic is consumable by an ordinary downstream Beam pipeline — no adapter,
no runtime internals. This test proves that claim by decoding with nothing but
the public proto bindings and asserting the documented alarm behavior.

`FailureStreak` below is the DoFn from `docs/errors.md`, copied verbatim.
Changing one without the other is a defect: the doc is the contract this test
holds the runtime to. Keep them in sync.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec

from beam_agents._protos import ActivationErrorRecord, AgentEnvelope
from beam_agents.core.dofn import REASON_ERROR, ActivationError
from beam_agents.core.error_records import serialize_error_envelope

# --- begin docs/errors.md example (keep in sync) ------------------------------


def parse_error_record(payload: bytes) -> ActivationErrorRecord:
    """Decode one errors-topic value: an AgentEnvelope carrying the record."""
    envelope = AgentEnvelope()
    envelope.ParseFromString(payload)
    record = ActivationErrorRecord()
    record.ParseFromString(envelope.external_event)
    return record


class FailureStreak(beam.DoFn):
    """Alarms when one key accumulates `threshold` dead letters.

    Per-key state, so Beam serializes the counting for us — the same
    per-key-serialization property `RunAgent` itself relies on. The count
    resets on alarm: the streak is a fresh count of failures since the last
    page, not a running total that would re-alarm on every later error.
    """

    COUNT = ReadModifyWriteStateSpec("count", beam.coders.VarIntCoder())

    def __init__(self, threshold: int) -> None:
        super().__init__()
        self._threshold = threshold

    def process(
        self,
        element: tuple[bytes, ActivationErrorRecord],
        count: Any = beam.DoFn.StateParam(COUNT),
    ) -> Iterator[tuple[bytes, int]]:
        key, _record = element
        streak = (count.read() or 0) + 1
        if streak < self._threshold:
            count.write(streak)
            return
        count.clear()
        yield key, streak


# --- end docs/errors.md example -----------------------------------------------


def _encoded(key: bytes, i: int) -> bytes:
    """One errors-topic value, produced by the runtime's own encoder."""
    _, payload = serialize_error_envelope(
        ActivationError(key, REASON_ERROR, f"RuntimeError('boom {i}')", 1_000 + i)
    )
    return payload


def _alarms(
    values: list[bytes], threshold: int, label: str
) -> tuple[BeamTestPipeline, beam.pvalue.PCollection]:
    """The documented consumer pipeline over raw errors-topic values."""
    p = BeamTestPipeline()
    alarms = (
        p
        | f"read-{label}" >> beam.Create(values)
        | f"parse-{label}" >> beam.Map(parse_error_record)
        | f"key-{label}"
        >> beam.WithKeys(lambda r: r.entity_key).with_output_types(
            tuple[bytes, ActivationErrorRecord]
        )
        | f"streak-{label}" >> beam.ParDo(FailureStreak(threshold))
    )
    return p, alarms


# --- Requirement: downstream failure-streak alarm is documented and tested ----


def test_the_alarm_fires_once_at_the_threshold() -> None:
    # Scenario: The alarm fires once at the threshold.
    p, alarms = _alarms([_encoded(b"k", i) for i in range(3)], 3, "at")
    with p:
        assert_that(alarms, equal_to([(b"k", 3)]))


def test_a_below_threshold_key_stays_silent() -> None:
    # Scenario: Below-threshold keys stay silent and counts reset after
    # alarming -- the silent half.
    p, alarms = _alarms([_encoded(b"quiet", i) for i in range(2)], 3, "below")
    with p:
        assert_that(alarms, equal_to([]))


def test_a_key_past_the_threshold_alarms_once_until_the_count_resets() -> None:
    # Scenario: Below-threshold keys stay silent and counts reset after
    # alarming. 2N-1 records: one alarm at N, then N-1 more -- not enough to
    # re-alarm, which is exactly what the reset buys.
    values = [_encoded(b"loud", i) for i in range(5)] + [_encoded(b"quiet", 0)]
    p, alarms = _alarms(values, 3, "reset")
    with p:
        assert_that(alarms, equal_to([(b"loud", 3)]))


def test_the_example_consumes_the_documented_wire_format() -> None:
    # Scenario: The example consumes the documented wire format. Decoding uses
    # only the public bindings -- no runtime import, no internal helper.
    record = parse_error_record(_encoded(b"k", 7))

    assert record.entity_key == b"k"
    assert record.reason == REASON_ERROR
    assert record.detail == "RuntimeError('boom 7')"
    assert record.event_time_ms == 1_007
