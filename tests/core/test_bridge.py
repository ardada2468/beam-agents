"""Unit tests for the async bridge: single loop thread, timeout cancellation.

Fast, Beam-free coverage of the bridge in isolation. Pipeline-level timeout
behavior (route-to-errors, no state mutation) is covered in test_dofn_timeout.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from collections.abc import Iterator
from typing import Any

import pytest

from beam_agents.core.bridge import ActivationTimeout, AsyncBridge


@pytest.fixture
def bridge() -> Iterator[AsyncBridge]:
    b = AsyncBridge(cancel_grace_s=2.0)
    b.start()
    try:
        yield b
    finally:
        b.stop()


async def _thread_ident() -> int:
    return threading.get_ident()


def test_run_returns_coroutine_result(bridge: AsyncBridge) -> None:
    async def add() -> int:
        return 40 + 2

    assert bridge.run(add, timeout_s=5.0) == 42


def test_single_loop_thread_reused_across_calls(bridge: AsyncBridge) -> None:
    # Scenario: one event loop thread spans the DoFn lifetime.
    first = bridge.run(_thread_ident, timeout_s=5.0)
    second = bridge.run(_thread_ident, timeout_s=5.0)
    assert first == second
    assert first != threading.get_ident()  # not the calling thread


def test_timeout_raises_activation_timeout(bridge: AsyncBridge) -> None:
    async def slow() -> int:
        await asyncio.sleep(30)
        return 1  # pragma: no cover - cancelled first

    with pytest.raises(ActivationTimeout):
        bridge.run(slow, timeout_s=0.2)


def test_loop_healthy_after_timeout(bridge: AsyncBridge) -> None:
    # Scenario: cancellation does not leak into the next element.
    ident = bridge.run(_thread_ident, timeout_s=5.0)

    async def slow() -> int:
        await asyncio.sleep(30)
        return 1  # pragma: no cover

    with pytest.raises(ActivationTimeout):
        bridge.run(slow, timeout_s=0.2)

    # Same loop thread still serves subsequent work.
    assert bridge.run(_thread_ident, timeout_s=5.0) == ident


def test_timeout_actually_cancels_coroutine(bridge: AsyncBridge) -> None:
    completed = threading.Event()

    async def slow() -> None:
        await asyncio.sleep(30)
        completed.set()  # reached only if the sleep was NOT cancelled

    with pytest.raises(ActivationTimeout):
        bridge.run(slow, timeout_s=0.2)
    # Give any (incorrectly) surviving coroutine a chance to set the flag.
    assert not completed.wait(timeout=0.5)


def test_non_timeout_exception_propagates(bridge: AsyncBridge) -> None:
    async def boom() -> None:
        raise ValueError("kaboom")

    with pytest.raises(ValueError, match="kaboom"):
        bridge.run(boom, timeout_s=5.0)


def test_constructor_preserves_cancel_grace_and_empty_state() -> None:
    bridge = AsyncBridge(cancel_grace_s=1.25)

    assert bridge._cancel_grace_s == 1.25
    assert bridge._loop is None
    assert bridge._thread is None


def test_start_creates_named_daemon_thread_for_the_new_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = object()
    started: list[bool] = []
    captured: dict[str, object] = {}

    class FakeThread:
        def start(self) -> None:
            started.append(True)

    def make_thread(**kwargs: object) -> FakeThread:
        captured.update(kwargs)
        return FakeThread()

    monkeypatch.setattr(asyncio, "new_event_loop", lambda: loop)
    monkeypatch.setattr(threading, "Thread", make_thread)
    bridge = AsyncBridge()

    bridge.start()

    assert bridge._loop is loop
    assert bridge._thread is not None
    assert captured == {
        "target": bridge._run_loop,
        "name": "beam-agents-bridge",
        "daemon": True,
    }
    assert started == [True]


def test_run_loop_installs_and_runs_its_owned_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []

    class FakeLoop:
        def run_forever(self) -> None:
            events.append("run_forever")

    loop = FakeLoop()
    monkeypatch.setattr(
        asyncio,
        "set_event_loop",
        lambda installed: events.append(("set_event_loop", installed)),
    )
    bridge = AsyncBridge()
    bridge._loop = loop  # type: ignore[assignment]

    bridge._run_loop()

    assert events == [("set_event_loop", loop), "run_forever"]


def test_run_before_start_has_exact_diagnostic() -> None:
    bridge = AsyncBridge()

    with pytest.raises(
        AssertionError,
        match=r"^AsyncBridge\.run called before start\(\)$",
    ):
        bridge.run(lambda: asyncio.sleep(0), timeout_s=1.0)


def test_timeout_cancels_created_task_and_waits_with_configured_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []

    class FakeTask:
        def cancel(self) -> None:
            events.append("task.cancel")

        def __await__(self) -> Any:
            yield "pending"

    class FakeLoop:
        def call_soon_threadsafe(self, callback: object) -> None:
            events.append(("call_soon_threadsafe", callback))

    class FakeFuture:
        def result(self, timeout: float) -> None:
            events.append(("result", timeout))
            raise concurrent.futures.TimeoutError

        def cancel(self) -> None:
            events.append("future.cancel")

    task = FakeTask()
    loop = FakeLoop()
    future = FakeFuture()

    def submit(coro: Any, submitted_loop: object) -> FakeFuture:
        assert submitted_loop is loop
        try:
            coro.send(None)
        finally:
            coro.close()
        return future

    def ensure_future(coro: Any) -> FakeTask:
        coro.close()
        return task

    monkeypatch.setattr(asyncio, "ensure_future", ensure_future)
    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", submit)
    bridge = AsyncBridge(cancel_grace_s=1.25)
    bridge._loop = loop  # type: ignore[assignment]

    with pytest.raises(ActivationTimeout) as exc_info:
        bridge.run(lambda: asyncio.sleep(0), timeout_s=0.5)

    assert exc_info.value.__cause__ is None
    assert events == [
        ("result", 0.5),
        ("call_soon_threadsafe", task.cancel),
        ("result", 1.25),
    ]


def test_timeout_cancels_wrapper_when_task_was_not_created(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    loop = object()

    class FakeFuture:
        def result(self, timeout: float) -> None:
            events.append(("result", timeout))
            raise concurrent.futures.TimeoutError

        def cancel(self) -> None:
            events.append("future.cancel")

    future = FakeFuture()

    def submit(coro: Any, submitted_loop: object) -> FakeFuture:
        assert submitted_loop is loop
        coro.close()
        return future

    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", submit)
    bridge = AsyncBridge(cancel_grace_s=2.5)
    bridge._loop = loop  # type: ignore[assignment]

    with pytest.raises(ActivationTimeout):
        bridge.run(lambda: asyncio.sleep(0), timeout_s=0.75)

    assert events == [
        ("result", 0.75),
        "future.cancel",
        ("result", 2.5),
    ]


def test_stop_stops_joins_closes_and_clears_owned_resources() -> None:
    events: list[object] = []

    class FakeLoop:
        def stop(self) -> None:
            events.append("stop")

        def call_soon_threadsafe(self, callback: object) -> None:
            events.append(("call_soon_threadsafe", callback))

        def close(self) -> None:
            events.append("close")

    class FakeThread:
        def join(self, *, timeout: float) -> None:
            events.append(("join", timeout))

    loop = FakeLoop()
    thread = FakeThread()
    bridge = AsyncBridge()
    bridge._loop = loop  # type: ignore[assignment]
    bridge._thread = thread  # type: ignore[assignment]

    bridge.stop(join_timeout_s=3.5)

    assert events == [
        ("call_soon_threadsafe", loop.stop),
        ("join", 3.5),
        "close",
    ]
    assert bridge._loop is None
    assert bridge._thread is None


def test_stop_uses_five_second_join_timeout_by_default() -> None:
    timeouts: list[float] = []

    class FakeThread:
        def join(self, *, timeout: float) -> None:
            timeouts.append(timeout)

    bridge = AsyncBridge()
    bridge._thread = FakeThread()  # type: ignore[assignment]

    bridge.stop()

    assert timeouts == [5.0]
