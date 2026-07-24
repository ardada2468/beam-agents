"""Unit tests for the async bridge: single loop thread, timeout cancellation.

Fast, Beam-free coverage of the bridge in isolation. Pipeline-level timeout
behavior (route-to-errors, no state mutation) is covered in test_dofn_timeout.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Iterator

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
