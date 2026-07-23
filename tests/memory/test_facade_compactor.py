"""Compaction hook protocol behavior for the memory-facade capability.

Covers the "Compaction hook is a stable protocol with a safe default"
requirement: compactor mutates through the facade with exact accounting and no
re-entrant enforcement, and compactor exceptions propagate unmodified.
"""

from __future__ import annotations

from unittest import mock

import pytest

from beam_agents._protos import MemoryBlob
from beam_agents.memory import Compactor, Memory, MemoryOverflow
from beam_agents.memory.facade import _SOFT_CAP_BYTES, HARD_CAP_BYTES

NOW = 13_000


def _scalar_blob(*pairs: tuple[str, bytes]) -> MemoryBlob:
    blob = MemoryBlob(state_schema_version=1)
    total = 0
    for key, value in pairs:
        encoded = b"\x00" + value
        blob.entries.add(key=key, value=encoded, last_access_ms=0)
        total += len(encoded)
    blob.total_value_bytes = total
    return blob


class _AccountingCompactor:
    """Deletes one key and rewrites another through the facade API."""

    def __init__(self) -> None:
        self.calls = 0

    def compact(self, memory: Memory) -> None:
        self.calls += 1
        memory.delete("a")
        memory.set("b", b"small")


class _Boom(Exception):
    pass


class _RaisingCompactor:
    def compact(self, memory: Memory) -> None:
        raise _Boom("compaction failed")


def test_compactor_mutates_through_the_facade_with_correct_accounting() -> None:
    # Scenario: Compactor mutates through the facade with correct accounting.
    compactor = _AccountingCompactor()
    mem = Memory(now_ms=NOW, compactor=compactor)

    with mock.patch("beam_agents.memory.facade.Metrics"):
        mem.set("a", b"x" * (_SOFT_CAP_BYTES - 1))  # crosses soft cap → compaction

    recomputed = sum(len(e.value) for e in mem.to_blob().entries)
    assert mem.size_bytes == recomputed
    assert mem.get("a") is None
    assert mem.get("b") == b"small"
    # The compactor's own set("b") must not re-trigger compaction.
    assert compactor.calls == 1


def test_compactor_exceptions_propagate() -> None:
    # Scenario: Compactor exceptions propagate.
    blob = _scalar_blob(("bulk", b"b" * (HARD_CAP_BYTES - 100)))
    mem = Memory(blob, now_ms=NOW, compactor=_RaisingCompactor())

    with pytest.raises(_Boom):
        mem.set("payload", b"y" * 500)  # overflow → hard-cap path invokes compactor


def test_compactor_protocol_is_runtime_checkable() -> None:
    # Requirement: Compactor is an exported protocol satisfied structurally.
    assert isinstance(_AccountingCompactor(), Compactor)


def test_missing_compactor_behaves_as_no_op() -> None:
    # Requirement: a facade without a compactor behaves as if a no-op is set;
    # an overflowing write with no compactor simply raises MemoryOverflow.
    mem = Memory(now_ms=NOW)
    with pytest.raises(MemoryOverflow):
        mem.set("k", b"x" * HARD_CAP_BYTES)
