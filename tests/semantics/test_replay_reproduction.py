"""Semantics gate: an exported activation replays to a byte-identical outcome.

The whole capability, end to end and offline: run an activation in a real
pipeline, export its committed state in band with an `export_request`, collect
`.traces`, then replay it locally through `beam_agents.replay` against a
provider that cannot serve anything — and require the re-run to reproduce the
traced record byte for byte with zero provider calls.

Offline by construction (no docker, no network): it carries `semantics` and not
`integration`, so it rides the required `ci` offline semantics selection.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Aliased: a bare "TestPipeline" name would be mis-collected by pytest.
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue

from beam_agents._protos import AgentEnvelope, StateSnapshot, TraceEvent
from beam_agents.core.transform import AgentConfig, RunAgent
from beam_agents.replay.__main__ import EXIT_REPRODUCED, main
from beam_agents.replay.bundle import (
    build_bundle,
    frame_trace_events,
    load_snapshot,
    parse_trace_stream,
    run_replay,
)
from beam_agents.replay.diff import compare
from tests.core._dofn_helpers import keyed
from tests.replay._fixtures import exact_replay_agent, failing_agent, make_provider

pytestmark = pytest.mark.semantics

_KEY = b"entity-1"
_EVENT_MS = 1_700_000_000_000
_EXPORT_MS = _EVENT_MS + 5_000
_BIG_TTL_MS = 1_000_000_000

_AGENT_PATH = "tests.replay._fixtures:exact_replay_agent"
_FAILING_AGENT_PATH = "tests.replay._fixtures:failing_agent"


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


class _AppendFramed:
    """Picklable sink: append each element's proto bytes, varint-length-framed.

    A file rather than an in-memory list because the DirectRunner is free to
    execute bundles outside this thread; the framing is the same interchange
    the CLI reads.
    """

    def __init__(self, path: str) -> None:
        self._path = path

    def __call__(self, message: Any) -> Any:
        payload = message.SerializeToString(deterministic=True)
        with open(self._path, "ab") as handle:
            handle.write(_varint(len(payload)) + payload)
        return message


def _event(payload: bytes) -> TimestampedValue[AgentEnvelope]:
    envelope = AgentEnvelope(entity_key=_KEY, event_time_ms=_EVENT_MS, external_event=payload)
    return TimestampedValue(envelope, _EVENT_MS / 1000)


def _export() -> TimestampedValue[AgentEnvelope]:
    envelope = AgentEnvelope(
        entity_key=_KEY,
        event_time_ms=_EXPORT_MS,
        export_request=AgentEnvelope.StateExportRequest(request_id="req-1"),
    )
    return TimestampedValue(envelope, _EXPORT_MS / 1000)


def _run_pipeline(agent: Any, tmp_path: Path) -> tuple[StateSnapshot, list[TraceEvent]]:
    """Activate, then export the committed state on the same key, in order."""
    traces_path = tmp_path / "traces.pb"
    snapshots_path = tmp_path / "snapshots.pb"
    traces_path.write_bytes(b"")
    snapshots_path.write_bytes(b"")
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"go")])
        .add_elements([_export()])
        .advance_watermark_to_infinity()
    )
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    with BeamTestPipeline(options=options) as p:
        outputs = keyed(p | stream) | RunAgent(
            agent,
            config=AgentConfig(provider_factory=make_provider, ttl_ms=_BIG_TTL_MS),
        )
        outputs.traces | "collect-traces" >> beam.Map(_AppendFramed(str(traces_path)))
        outputs.snapshots | "collect-snapshots" >> beam.Map(_AppendFramed(str(snapshots_path)))

    snapshots = [_parse_snapshot(payload) for payload in _frames(snapshots_path.read_bytes())]
    assert len(snapshots) == 1, "the export request must yield exactly one snapshot"
    return snapshots[0], parse_trace_stream(traces_path.read_bytes())


def _frames(data: bytes) -> list[bytes]:
    frames: list[bytes] = []
    offset = 0
    while offset < len(data):
        length = 0
        shift = 0
        while True:
            byte = data[offset]
            offset += 1
            length |= (byte & 0x7F) << shift
            if not byte & 0x80:
                break
            shift += 7
        frames.append(data[offset : offset + length])
        offset += length
    return frames


def _parse_snapshot(payload: bytes) -> StateSnapshot:
    snapshot = StateSnapshot()
    snapshot.ParseFromString(payload)
    return snapshot


def _write_cli_inputs(
    tmp_path: Path, snapshot: StateSnapshot, traces: list[TraceEvent], envelope: AgentEnvelope
) -> list[str]:
    snapshot_path = tmp_path / "cli-snapshot.pb"
    traces_path = tmp_path / "cli-traces.pb"
    event_path = tmp_path / "cli-event.pb"
    snapshot_path.write_bytes(snapshot.SerializeToString(deterministic=True))
    traces_path.write_bytes(frame_trace_events(traces))
    event_path.write_bytes(envelope.SerializeToString(deterministic=True))
    return [
        "--snapshot",
        str(snapshot_path),
        "--traces",
        str(traces_path),
        "--event",
        str(event_path),
    ]


# --- Requirement: a replayed activation reproduces the traced outcome ---------


def test_a_replayed_activation_reproduces_the_traced_outcome(tmp_path: Path) -> None:
    # Scenario: A replayed activation reproduces the traced outcome.
    snapshot, traces = _run_pipeline(exact_replay_agent, tmp_path)
    envelope = AgentEnvelope(entity_key=_KEY, event_time_ms=_EVENT_MS, external_event=b"go")

    assert snapshot.entity_key == _KEY
    assert snapshot.snapshot_at_ms == _EXPORT_MS
    assert snapshot.seq == 1  # the counter after the activation committed
    assert snapshot.llm_cache.entries, "the committed cache must carry the served response"

    bundle = build_bundle(
        snapshot=load_snapshot(snapshot.SerializeToString(deterministic=True)),
        traces=traces,
        envelope=envelope,
    )
    outcome = run_replay(bundle, exact_replay_agent)
    report = compare(bundle, outcome)

    # Zero provider calls: every request was served from the exported blob.
    assert outcome.provider_calls == 0
    # Byte-identical trace events, after the closed cache-hit normalization.
    assert report.reproduced is True, report.render()
    assert [e.event_type for e in outcome.traces] == [e.event_type for e in bundle.traced]
    # ...and the intents carry the traced IDs, which is the effectively-once
    # argument surviving a local re-run.
    traced_ids = [
        e.attributes["beam_agents.intent_id"]
        for e in bundle.traced
        if e.event_type == TraceEvent.INTENT_EMITTED
    ]
    assert [i.intent_id for i in outcome.intents] == traced_ids
    assert traced_ids, "the agent stages an intent, so there is one to compare"

    # The same thing through the console script an operator actually runs.
    assert (
        main([*_write_cli_inputs(tmp_path, snapshot, traces, envelope), "--agent", _AGENT_PATH])
        == EXIT_REPRODUCED
    )


def test_a_failed_activation_replays_to_its_traced_failure_position(tmp_path: Path) -> None:
    # Scenario: A failed activation replays to its traced failure position. The
    # attempt committed nothing (invariant 1), so the snapshot taken after it
    # is the exact pre-image, and `.traces` carries only the ERROR event.
    snapshot, traces = _run_pipeline(failing_agent, tmp_path)
    envelope = AgentEnvelope(entity_key=_KEY, event_time_ms=_EVENT_MS, external_event=b"go")

    assert snapshot.seq == 0, "a failed activation must not advance SEQ"
    assert [e.event_type for e in traces] == [TraceEvent.ERROR]

    bundle = build_bundle(
        snapshot=load_snapshot(snapshot.SerializeToString(deterministic=True)),
        traces=traces,
        envelope=envelope,
    )
    outcome = run_replay(bundle, failing_agent)
    report = compare(bundle, outcome)

    assert outcome.status == "failed"
    assert outcome.provider_calls == 0
    assert report.reproduced is True, report.render()

    assert (
        main(
            [
                *_write_cli_inputs(tmp_path, snapshot, traces, envelope),
                "--agent",
                _FAILING_AGENT_PATH,
            ]
        )
        == EXIT_REPRODUCED
    )
