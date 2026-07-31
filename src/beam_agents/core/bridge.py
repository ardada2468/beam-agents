"""The async bridge: one background asyncio loop per DoFn instance.

Beam calls ``process()`` synchronously; the model client is async. The bridge
owns a single daemon thread running a dedicated event loop (with worker-local
shared httpx pools living on whatever the provider holds). ``process()`` submits
the activation coroutine and blocks up to ``activation_timeout``. On timeout the
bridge cancels the in-flight coroutine and raises :class:`ActivationTimeout`, so
the DoFn routes the element to ``.errors`` having committed nothing (correctness
invariant 6). One loop spans the DoFn lifetime, so httpx pools are reused across
elements rather than rebuilt per call.

Importing this module has no side effects.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import threading
from collections.abc import Callable, Coroutine
from typing import TypeVar

__all__ = [
    "ActivationTimeout",
    "AsyncBridge",
]

_T = TypeVar("_T")

# Grace period to let a cancelled coroutine unwind after a timeout before the
# DoFn moves on. Bounded so a coroutine ignoring cancellation cannot wedge the
# worker; the loop thread is a daemon and is torn down at teardown regardless.
_CANCEL_GRACE_S = 5.0


class ActivationTimeout(Exception):
    """Raised by :meth:`AsyncBridge.run` when a coroutine exceeds its budget."""


class AsyncBridge:
    """A per-instance event-loop thread that runs one activation at a time."""

    def __init__(self, *, cancel_grace_s: float = _CANCEL_GRACE_S) -> None:
        self._cancel_grace_s = cancel_grace_s
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the loop thread. Idempotent per instance is not required: the
        DoFn calls this once from ``setup()``.
        """
        loop = asyncio.new_event_loop()
        self._loop = loop
        self._thread = threading.Thread(
            target=self._run_loop, name="beam-agents-bridge", daemon=True
        )
        self._thread.start()

    def _run_loop(self) -> None:
        assert self._loop is not None
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def run(
        self, coro_factory: Callable[[], Coroutine[object, object, _T]], timeout_s: float
    ) -> _T:
        """Run ``coro_factory()`` on the loop thread, blocking up to ``timeout_s``.

        The coroutine is created on the loop thread and held as a task so a
        timeout can cancel it even mid-await. On timeout the task is cancelled,
        drained for up to the cancel grace, and :class:`ActivationTimeout` is
        raised. Any other exception from the coroutine propagates unchanged.
        """
        loop = self._loop
        assert loop is not None, "AsyncBridge.run called before start()"

        holder: dict[str, asyncio.Task[_T]] = {}

        async def _await_task() -> _T:
            task: asyncio.Task[_T] = asyncio.ensure_future(coro_factory())
            holder["task"] = task
            return await task

        future = asyncio.run_coroutine_threadsafe(_await_task(), loop)
        try:
            return future.result(timeout=timeout_s)
        except concurrent.futures.TimeoutError:
            task = holder.get("task")
            if task is not None:
                loop.call_soon_threadsafe(task.cancel)
            else:
                # The wrapper had not created the task yet; cancelling the
                # concurrent future stops it before it starts.
                future.cancel()
            # Drain the cancellation; expect CancelledError (cancellation took)
            # or another TimeoutError if the coroutine ignored cancellation.
            with contextlib.suppress(BaseException):
                future.result(timeout=self._cancel_grace_s)
            raise ActivationTimeout() from None

    def stop(self, *, join_timeout_s: float = 5.0) -> None:
        """Stop the loop and join the thread. Safe to call once from ``teardown()``."""
        loop = self._loop
        thread = self._thread
        if loop is not None:
            loop.call_soon_threadsafe(loop.stop)
        if thread is not None:
            thread.join(timeout=join_timeout_s)
        if loop is not None:
            loop.close()
        self._loop = None
        self._thread = None
