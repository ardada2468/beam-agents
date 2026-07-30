"""In-memory working-memory facade with size accounting and cap enforcement.

See :mod:`beam_agents.memory` for the capability overview and the change design
(``openspec/changes/add-memory-facade/design.md``) for the load-bearing
decisions: injected-and-frozen clock (D1), self-describing ring encoding inside
opaque entry bytes (D2), per-call ring bound (D3), incremental accounting (D4),
check->compact->reject cap order (D5), and read-updates-LRU (D6).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from apache_beam.metrics.metric import Metrics

from beam_agents._protos import MemoryBlob

_LOGGER = logging.getLogger(__name__)

# Working-memory hard cap per key (project constraint, release-gating).
HARD_CAP_BYTES = 1_048_576
# Soft cap: warn + compact at 75% utilization while writes still succeed.
_SOFT_CAP_BYTES = HARD_CAP_BYTES * 3 // 4

# Beam metrics counter incremented once per activation that crosses the soft cap.
_METRIC_NAMESPACE = "beam_agents.memory"
_SOFT_CAP_COUNTER = "soft_cap_warnings"

# First byte of every stored entry value tags its kind, so a bounded ring and a
# scalar coexist in the same opaque ``bytes`` field with no proto schema change.
_KIND_SCALAR = 0x00
_KIND_RING = 0x01
# Ring items are length-prefixed with a fixed-width big-endian u32.
_RING_LEN_PREFIX = 4


class MemoryOverflow(Exception):
    """Raised when a mutation would push working memory past the hard cap.

    A runtime capacity condition (routed to the errors output), not a
    construction-time misconfiguration — hence ``Exception``, not ``ValueError``.
    """

    def __init__(self, key: str, attempted_bytes: int, cap_bytes: int) -> None:
        super().__init__(
            f"working memory for key {key!r} would reach {attempted_bytes} bytes, "
            f"over the {cap_bytes}-byte hard cap"
        )
        self.key = key
        self.attempted_bytes = attempted_bytes
        self.cap_bytes = cap_bytes


@runtime_checkable
class Compactor(Protocol):
    """Hook invoked at the soft-cap crossing and before hard-cap rejection.

    Receives the facade itself so strategies mutate memory only through the
    guarded API; cap enforcement is suspended for the duration of the call.
    """

    def compact(self, memory: Memory) -> None: ...


@dataclass(slots=True)
class _Entry:
    """One stored entry: the kind-tagged value bytes and its access stamp."""

    value: bytes
    last_access_ms: int


def _encode_scalar(value: bytes) -> bytes:
    return bytes([_KIND_SCALAR]) + value


def _encode_ring(items: Sequence[bytes]) -> bytes:
    out = bytearray([_KIND_RING])
    for item in items:
        out += len(item).to_bytes(_RING_LEN_PREFIX, "big")
        out += item
    return bytes(out)


def _decode_ring(encoded: bytes) -> list[bytes]:
    """Decode ring items from a tag-``0x01`` value; raise on truncation."""
    items: list[bytes] = []
    pos = 1  # skip the kind tag
    n = len(encoded)
    while pos < n:
        if pos + _RING_LEN_PREFIX > n:
            raise ValueError("corrupt ring entry: truncated length prefix")
        length = int.from_bytes(encoded[pos : pos + _RING_LEN_PREFIX], "big")
        pos += _RING_LEN_PREFIX
        if pos + length > n:
            raise ValueError("corrupt ring entry: truncated item")
        items.append(encoded[pos : pos + length])
        pos += length
    return items


class Memory:
    """Facade over a single keyed ``MemoryBlob``, staging mutations in memory."""

    def __init__(
        self,
        blob: MemoryBlob | None = None,
        *,
        now_ms: int,
        compactor: Compactor | None = None,
    ) -> None:
        self._now_ms = now_ms
        self._compactor = compactor
        self._entries: dict[str, _Entry] = {}
        self._total = 0
        self._dirty = False
        self._soft_cap_warned = False
        # Suspended (set False) while a compactor runs, to disable re-entrant cap
        # enforcement and soft-cap handling triggered by the compactor's writes.
        self._enforcing = True
        if blob is not None:
            for entry in blob.entries:
                self._entries[entry.key] = _Entry(entry.value, entry.last_access_ms)
                self._total += len(entry.value)

    # -- properties -----------------------------------------------------------

    @property
    def size_bytes(self) -> int:
        return self._total

    @property
    def dirty(self) -> bool:
        return self._dirty

    # -- scalar access --------------------------------------------------------

    def get(self, key: str) -> bytes | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        if entry.value[0] != _KIND_SCALAR:
            raise TypeError(f"key {key!r} holds a ring; use ring() instead of get()")
        value = entry.value[1:]
        self._touch(key)
        return value

    def set(self, key: str, value: bytes) -> None:
        # A scalar value is independent of current state; overwriting a ring is
        # an explicit, permitted replace, so no kind check here.
        self._guarded_write(key, lambda: _encode_scalar(value))

    def delete(self, key: str) -> None:
        entry = self._entries.pop(key, None)
        if entry is None:
            return  # idempotent on absent keys; no state change, stays clean
        self._total -= len(entry.value)
        self._dirty = True

    # -- ring access ----------------------------------------------------------

    def append(self, key: str, item: bytes, *, max_items: int = 64) -> None:
        existing = self._entries.get(key)
        if existing is not None and existing.value[0] != _KIND_RING:
            raise TypeError(f"key {key!r} holds a scalar; append() requires a ring")

        def compute() -> bytes:
            # Re-read state on each call so a compactor that dropped this key
            # between the first evaluation and a retry starts from an empty ring.
            entry = self._entries.get(key)
            items = _decode_ring(entry.value) if entry is not None else []
            items.append(item)
            items = items[-max_items:] if max_items > 0 else []
            return _encode_ring(items)

        self._guarded_write(key, compute)

    def ring(self, key: str) -> tuple[bytes, ...]:
        entry = self._entries.get(key)
        if entry is None:
            return ()
        if entry.value[0] != _KIND_RING:
            raise TypeError(f"key {key!r} holds a scalar; use get() instead of ring()")
        items = tuple(_decode_ring(entry.value))
        self._touch(key)
        return items

    # -- serialization --------------------------------------------------------

    def to_blob(self) -> MemoryBlob:
        # Imported lazily: `beam_agents.core`'s package init imports the
        # context, which imports this package — a top-level import here would
        # make `import beam_agents.memory` order-dependent. The call-time read
        # also means a version bump in core/migration.py moves this stamp with
        # no edit here.
        from beam_agents.core import migration

        blob = MemoryBlob(state_schema_version=migration.CURRENT_STATE_SCHEMA_VERSION)
        for key, entry in self._entries.items():
            blob.entries.add(key=key, value=entry.value, last_access_ms=entry.last_access_ms)
        blob.total_value_bytes = self._total
        return blob

    # -- internals ------------------------------------------------------------

    def _touch(self, key: str) -> None:
        """Re-stamp access time and move the entry to most-recently-used."""
        entry = self._entries.pop(key)
        entry.last_access_ms = self._now_ms
        self._entries[key] = entry
        self._dirty = True

    def _guarded_write(self, key: str, compute: Callable[[], bytes]) -> None:
        new_encoded = compute()
        prospective = self._prospective_total(key, new_encoded)
        if self._enforcing and prospective > HARD_CAP_BYTES:
            if self._compactor is not None:
                self._run_compactor()
                new_encoded = compute()  # re-evaluate against post-compaction state
                prospective = self._prospective_total(key, new_encoded)
            if prospective > HARD_CAP_BYTES:
                raise MemoryOverflow(key, prospective, HARD_CAP_BYTES)
        self._commit(key, new_encoded)
        if self._enforcing:
            self._maybe_soft_cap()

    def _prospective_total(self, key: str, new_encoded: bytes) -> int:
        existing = self._entries.get(key)
        old_len = len(existing.value) if existing is not None else 0
        return self._total + len(new_encoded) - old_len

    def _commit(self, key: str, new_encoded: bytes) -> None:
        existing = self._entries.pop(key, None)
        if existing is not None:
            self._total -= len(existing.value)
        self._total += len(new_encoded)
        # Reinsert at the end so written keys become most-recently-used.
        self._entries[key] = _Entry(new_encoded, self._now_ms)
        self._dirty = True

    def _run_compactor(self) -> None:
        assert self._compactor is not None
        self._enforcing = False
        try:
            self._compactor.compact(self)
        finally:
            self._enforcing = True

    def _maybe_soft_cap(self) -> None:
        if self._soft_cap_warned or self._total < _SOFT_CAP_BYTES:
            return
        self._soft_cap_warned = True
        _LOGGER.warning(
            "working memory crossed soft cap: %d/%d bytes",
            self._total,
            HARD_CAP_BYTES,
        )
        Metrics.counter(_METRIC_NAMESPACE, _SOFT_CAP_COUNTER).inc()
        if self._compactor is not None:
            self._run_compactor()
