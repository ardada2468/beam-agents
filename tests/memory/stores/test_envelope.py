"""Envelope encoding for the memory-stores capability.

Pins the two byte-level properties the cross-backend contract rests on: the
`LongTermRecord` envelope encoding (golden bytes — every backend stores this
value verbatim, so backend migration is a verbatim row copy) and the
order-preserving big-endian seq encoding (what lets Bigtable's ValueRangeFilter
and Redis's Lua byte compare evaluate the numeric guard lexicographically,
mirroring the dedup store's `encode_lease_expiry` property test).
"""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from beam_agents.memory.stores.base import (
    _decode_envelope,
    _decode_seq,
    _encode_envelope,
    _encode_seq,
)

from ._conformance import ENTITY_A, a_record

# The committed golden encoding of the fixed record below. Changing the
# envelope encoding is a cross-backend migration event, not a refactor; this
# constant is the tripwire.
_GOLDEN_RECORD = a_record("profile", value=b"\x01\x02", seq=7, updated_at_ms=1_700_000_000_000)
_GOLDEN_HEX = "0801120770726f66696c651a02010220072880d095ffbc31"


def test_envelope_bytes_are_pinned_by_a_golden_test() -> None:
    # Scenario: Envelope bytes are pinned by a golden test.
    encoded = _encode_envelope(_GOLDEN_RECORD)

    assert encoded.hex() == _GOLDEN_HEX
    assert _decode_envelope(ENTITY_A, bytes.fromhex(_GOLDEN_HEX)) == _GOLDEN_RECORD


def test_encoding_is_deterministic_across_calls() -> None:
    record = a_record("k", value=b"v" * 100, seq=123, updated_at_ms=456)
    assert _encode_envelope(record) == _encode_envelope(record)


_MAX_SEQ = 2**63 - 1


@given(
    a=st.integers(min_value=0, max_value=_MAX_SEQ),
    b=st.integers(min_value=0, max_value=_MAX_SEQ),
)
def test_big_endian_seq_encoding_preserves_numeric_order(a: int, b: int) -> None:
    # Scenario: Big-endian seq encoding preserves numeric order.
    ea, eb = _encode_seq(a), _encode_seq(b)

    assert (ea < eb) == (a < b)
    assert (ea == eb) == (a == b)
    assert len(ea) == len(eb) == 8


@given(seq=st.integers(min_value=0, max_value=_MAX_SEQ))
def test_seq_encoding_round_trips(seq: int) -> None:
    assert _decode_seq(_encode_seq(seq)) == seq


def test_a_negative_seq_is_unencodable() -> None:
    with pytest.raises(ValueError, match="seq"):
        _encode_seq(-1)
