"""Golden-blob compat tests: committed v1 bytes must decode with current bindings.

Guards the additive-only evolution rule (design D6). Each committed
``tests/core/golden/*.bin`` blob is decoded with the current bindings and
asserted field-equal to the expected value in ``generate.GOLDEN`` — the same
builders that produced the bytes. These tests deliberately do NOT assert
byte-identical re-encoding: a protobuf library upgrade may change serialization
details while remaining wire-compatible.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from beam_agents._protos import Continuation, ToolIntent
from tests.core.golden.generate import GOLDEN

GOLDEN_DIR = Path(__file__).parent / "golden"


@pytest.mark.parametrize("name", sorted(GOLDEN))
def test_golden_blob_decodes_to_expected_fields(name: str) -> None:
    # Scenario: Golden blobs decode with current bindings.
    expected = GOLDEN[name]
    blob_path = GOLDEN_DIR / f"{name}.bin"
    assert blob_path.exists(), f"missing committed golden fixture: {blob_path}"

    decoded = type(expected)()
    decoded.ParseFromString(blob_path.read_bytes())  # must not raise

    assert decoded == expected


def test_every_message_type_has_a_golden_fixture() -> None:
    # All seven message types must have a committed baseline. A type may have
    # more than one fixture: a field added after the v1 baseline gets its own
    # blob, so the original keeps proving pre-field bytes still decode.
    committed = {p.stem for p in GOLDEN_DIR.glob("*.bin")}
    assert committed == set(GOLDEN)
    assert len({type(message) for message in GOLDEN.values()}) == 7


def test_pre_v1_baseline_blobs_decode_with_fields_added_later() -> None:
    # Scenario: An intent written before kind existed reads as a tool call.
    # Scenario: Escalation count defaults to zero.
    # The committed `tool_intent`/`continuation` blobs were serialized before
    # `ToolIntent.kind` and `Continuation.escalations` existed, so they are the
    # real pre-field bytes, not a reconstruction.
    intent = ToolIntent()
    intent.ParseFromString((GOLDEN_DIR / "tool_intent.bin").read_bytes())
    assert intent.kind == ToolIntent.TOOL_KIND_UNSPECIFIED

    cont = Continuation()
    cont.ParseFromString((GOLDEN_DIR / "continuation.bin").read_bytes())
    assert cont.escalations == 0
