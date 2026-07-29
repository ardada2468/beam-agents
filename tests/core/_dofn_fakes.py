"""Fake Beam state/timer handles for driving ``_AgentDoFn`` without a runner.

Beam's StateParam/TimerParam handles are dynamic objects the runner injects at
call time, so a test can supply its own: these implement the same read/write/
add/clear surface over plain Python values. Driving the DoFn this way keeps its
element path inside the mutation gate's test selection, which the pipeline
suites (deselected under mutmut) cannot do.

Not collected by pytest (module name doesn't match ``test_*``).
"""

from __future__ import annotations

from typing import Any

from apache_beam.utils.timestamp import Timestamp

from beam_agents._protos import ToolIntent


class FakeValue:
    """Stand-in for a ``ReadModifyWriteStateSpec`` handle."""

    def __init__(self, value: Any = None) -> None:
        self.value = value
        self.cleared = False

    def read(self) -> Any:
        return self.value

    def write(self, value: Any) -> None:
        self.value = value

    def clear(self) -> None:
        self.value = None
        self.cleared = True


class FakeBag:
    """Stand-in for a ``BagStateSpec`` handle."""

    def __init__(self, items: list[ToolIntent] | None = None) -> None:
        self.items = list(items or [])
        self.cleared = False

    def read(self) -> list[ToolIntent]:
        return list(self.items)

    def add(self, item: ToolIntent) -> None:
        self.items.append(item)

    def clear(self) -> None:
        self.items = []
        self.cleared = True


class FakeSum:
    """Stand-in for the ``SEQ`` combining-value handle (sum accumulator)."""

    def __init__(self, value: int = 0) -> None:
        self.value = value
        self.cleared = False

    def read(self) -> int:
        return self.value

    def add(self, n: int) -> None:
        self.value += n

    def clear(self) -> None:
        self.value = 0
        self.cleared = True


class FakeTimer:
    """Stand-in for a ``TimerParam`` handle; records the mark it was set to."""

    def __init__(self) -> None:
        self.set_to: Timestamp | None = None
        self.cleared = False

    def set(self, ts: Timestamp) -> None:
        self.set_to = ts

    def clear(self) -> None:
        self.cleared = True


class RecordingMetrics:
    """``MetricsSink`` double: counter totals and every distribution sample."""

    def __init__(self) -> None:
        self.counters: dict[str, int] = {}
        self.samples: dict[str, list[int]] = {}

    def incr(self, name: str, n: int = 1) -> None:
        self.counters[name] = self.counters.get(name, 0) + n

    def observe(self, name: str, value: int) -> None:
        self.samples.setdefault(name, []).append(value)


def scripted_clock(*readings_ns: int) -> Any:
    """Monotonic double returning ``readings_ns`` in order, then zero.

    Falling back to zero rather than raising lets a test pin the one duration it
    cares about without scripting every internal reading.
    """
    remaining = list(readings_ns)

    def read() -> int:
        return remaining.pop(0) if remaining else 0

    return read
