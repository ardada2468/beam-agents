"""Property-based size accounting for the memory-facade capability.

Covers the "Size accounting is incremental and exact" requirement: after any
sequence of mutations, ``size_bytes`` equals a from-scratch recomputation over
the stored entries, and ``to_blob().total_value_bytes`` agrees.
"""

from __future__ import annotations

import enum

from hypothesis import given
from hypothesis import strategies as st

from beam_agents.memory import Memory

NOW = 9_000


class _Op(enum.Enum):
    SET = "set"
    DELETE = "delete"
    APPEND = "append"


_keys = st.sampled_from(["a", "b", "c"])
# Keep values small so the sequence stays well under the hard cap.
_values = st.binary(min_size=0, max_size=8)
_ops = st.tuples(st.sampled_from(list(_Op)), _keys, _values)


def _recompute(mem: Memory) -> int:
    return sum(len(e.value) for e in mem.to_blob().entries)


@given(st.lists(_ops, max_size=40))
def test_accounting_matches_recomputation_under_mixed_operations(
    ops: list[tuple[_Op, str, bytes]],
) -> None:
    # Scenario: Accounting matches recomputation under mixed operations.
    mem = Memory(now_ms=NOW)
    # Track which keys are rings so we never mix kinds (a spec-level error, not
    # an accounting concern) while still exercising set/delete/append.
    ring_keys: set[str] = set()
    scalar_keys: set[str] = set()

    for op, key, value in ops:
        if op is _Op.APPEND and key not in scalar_keys:
            mem.append(key, value)
            ring_keys.add(key)
        elif op is _Op.SET and key not in ring_keys:
            mem.set(key, value)
            scalar_keys.add(key)
        elif op is _Op.DELETE:
            mem.delete(key)
            ring_keys.discard(key)
            scalar_keys.discard(key)
        else:
            continue
        assert mem.size_bytes == _recompute(mem)
        # The compaction enumeration surface is part of the same accounting:
        # a strategy that sums `entry_size` over `keys()` to decide what to
        # evict must reach exactly `size_bytes`, after any operation sequence.
        stored = mem.keys()
        assert sum(mem.entry_size(key) for key in stored) == mem.size_bytes

    assert mem.to_blob().total_value_bytes == mem.size_bytes
