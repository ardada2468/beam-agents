"""The `beam-agents-replay` console script: wiring and the exit-code contract.

0 reproduced / 1 diverged / 2 usage or version refusal / 3 irreproducible —
asserted end to end over files on disk, which is how an operator runs it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from beam_agents._protos import TraceEvent
from beam_agents.core.migration import CURRENT_STATE_SCHEMA_VERSION
from beam_agents.observability.traces import INTENT_ID
from beam_agents.replay.__main__ import (
    EXIT_DIVERGED,
    EXIT_IRREPRODUCIBLE,
    EXIT_REPRODUCED,
    EXIT_USAGE,
    build_parser,
    import_object,
    main,
)
from beam_agents.replay.bundle import frame_trace_events
from tests.replay._fixtures import run_original, run_original_failure, run_original_resume

_AGENT = "tests.replay._fixtures:exact_replay_agent"
_FAILING_AGENT = "tests.replay._fixtures:failing_agent"
_SUSPENDING_AGENT = "tests.replay._fixtures:suspending_agent"


def _write(tmp_path: Path, original: Any, *, traces: Any = None) -> list[str]:
    """Write the three inputs to disk and return the CLI arguments naming them."""
    snapshot = tmp_path / "snapshot.pb"
    trace_stream = tmp_path / "traces.pb"
    envelope = tmp_path / "event.pb"
    snapshot.write_bytes(original.snapshot.SerializeToString(deterministic=True))
    trace_stream.write_bytes(frame_trace_events(traces if traces is not None else original.traces))
    envelope.write_bytes(original.envelope.SerializeToString(deterministic=True))
    return [
        "--snapshot",
        str(snapshot),
        "--traces",
        str(trace_stream),
        "--event",
        str(envelope),
    ]


# --- Requirement: the CLI reconstructs an activation and re-runs it locally ----


def test_a_reproduced_replay_exits_zero(tmp_path: Path, capsys: Any) -> None:
    original = run_original()

    code = main([*_write(tmp_path, original), "--agent", _AGENT])

    out = capsys.readouterr().out
    assert code == EXIT_REPRODUCED
    assert "reproduced" in out
    # The diff header names what actually ran, so version skew is visible.
    assert _AGENT in out


def test_a_resume_replay_exits_zero(tmp_path: Path) -> None:
    original = run_original_resume()

    code = main([*_write(tmp_path, original), "--agent", _SUSPENDING_AGENT])

    assert code == EXIT_REPRODUCED


def test_a_failed_activation_replays_to_its_traced_failure_position(tmp_path: Path) -> None:
    original = run_original_failure()

    code = main([*_write(tmp_path, original), "--agent", _FAILING_AGENT])

    assert code == EXIT_REPRODUCED


def test_a_divergent_re_run_produces_a_diff_and_exit_code_1(tmp_path: Path, capsys: Any) -> None:
    # Scenario: A divergent re-run produces a diff and exit code 1.
    original = run_original()
    traced = []
    for event in original.traces:
        copy = TraceEvent()
        copy.CopyFrom(event)
        if copy.event_type == TraceEvent.INTENT_EMITTED:
            copy.attributes[INTENT_ID] = "00000000-0000-5000-8000-000000000000"
        traced.append(copy)

    code = main([*_write(tmp_path, original, traces=traced), "--agent", _AGENT])

    out = capsys.readouterr().out
    assert code == EXIT_DIVERGED
    assert "diverged" in out
    assert "00000000-0000-5000-8000-000000000000" in out


def test_a_cache_miss_exits_three(tmp_path: Path, capsys: Any) -> None:
    # Scenario: A cache miss aborts loudly instead of calling a provider.
    original = run_original()
    del original.snapshot.llm_cache.entries[:]

    code = main([*_write(tmp_path, original), "--agent", _AGENT])

    assert code == EXIT_IRREPRODUCIBLE
    assert "irreproducible" in capsys.readouterr().err


def test_a_newer_schema_snapshot_exits_two(tmp_path: Path, capsys: Any) -> None:
    # Scenario: A newer-schema snapshot fails closed.
    original = run_original()
    original.snapshot.state_schema_version = CURRENT_STATE_SCHEMA_VERSION + 1

    code = main([*_write(tmp_path, original), "--agent", _AGENT])

    assert code == EXIT_USAGE
    assert "upgrade beam-agents" in capsys.readouterr().err


def test_a_mismatched_envelope_exits_two(tmp_path: Path, capsys: Any) -> None:
    # Scenario: A mismatched envelope is refused.
    original = run_original()
    original.envelope.entity_key = b"other-key"

    code = main([*_write(tmp_path, original), "--agent", _AGENT])

    assert code == EXIT_USAGE
    assert b"other-key".hex() in capsys.readouterr().err


def test_a_missing_file_exits_two(tmp_path: Path, capsys: Any) -> None:
    original = run_original()
    args = _write(tmp_path, original)

    code = main(["--snapshot", str(tmp_path / "absent.pb"), *args[2:], "--agent", _AGENT])

    assert code == EXIT_USAGE
    assert "absent.pb" in capsys.readouterr().err


def test_a_bad_agent_path_exits_two(tmp_path: Path, capsys: Any) -> None:
    original = run_original()

    code = main([*_write(tmp_path, original), "--agent", "tests.replay._fixtures"])

    assert code == EXIT_USAGE
    assert "module:attribute" in capsys.readouterr().err


def test_import_object_reports_a_missing_attribute() -> None:
    with pytest.raises(ValueError, match="no attribute"):
        import_object("tests.replay._fixtures:nope", flag="--agent")


def test_the_parser_exposes_the_documented_flags() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "--snapshot",
            "s",
            "--traces",
            "t",
            "--event",
            "e",
            "--agent",
            "m:a",
            "--registry",
            "m:R",
            "--decode",
            "m:d",
            "--seq",
            "7",
        ]
    )

    assert (args.snapshot, args.traces, args.event) == ("s", "t", "e")
    assert (args.agent, args.registry, args.decode, args.seq) == ("m:a", "m:R", "m:d", 7)
    assert parser.prog == "beam-agents-replay"
