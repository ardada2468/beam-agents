"""The live feed: one broadcast fan-out from ingest to connected browsers.

A console is most useful while a pipeline is running, and polling a list
endpoint fast enough to feel live is the wrong trade — it multiplies query cost
by connected tabs to deliver mostly-unchanged pages.

The contract that matters is that **a reader can never affect a writer**. Each
subscriber holds a bounded queue; when it fills, that subscriber's events are
dropped and it is marked lagged, rather than the publisher blocking. A slow
laptop with a stale tab open must not become backpressure on ingest, for the
same reason ``WriteToConsole`` drops rather than blocks the pipeline.

Events carry identity, not payload — the activation an ingest touched, not its
contents. The client refetches what it needs, which keeps the broadcast small
and means a dropped event costs a refresh rather than a hole in the data.

Three implementation decisions follow from that contract.

**A ``deque`` under a ``threading.Lock``, not an ``asyncio.Queue``.** Publishing
happens wherever ingest happens: on the event loop for the HTTP endpoints, and
on a plain worker thread for the Kafka and BigQuery sources. ``asyncio.Queue``
is not thread-safe, so a publisher would have to know which thread it is on;
here it does not, and :meth:`Broadcaster.publish` is callable from anywhere.

**The newest event is dropped, not the oldest.** Both are lossy, but discarding
the arrival keeps the backlog a contiguous prefix and keeps the drop O(1). What
the client actually needs is not the missing events — it refetches — but to know
that it missed some, which is what the ``lagged`` marker is for.

**Registration is eager.** ``subscribe()`` adds the queue before the caller has
iterated anything, because the gap between a route calling it and the SSE
response pulling its first frame is real, and an event published inside that gap
is exactly the first event of the run someone opened the console to watch.

Importing this module has no side effects.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import threading
import weakref
from collections import deque
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from beam_agents.console._records import RecordBatch

__all__ = ["Broadcaster", "LiveEvent"]

# Default per-subscriber queue depth. Deep enough to ride out a render pause,
# shallow enough that a tab left open on a sleeping machine is dropped rather
# than accumulating an unbounded backlog.
DEFAULT_QUEUE_DEPTH = 256

# What changed. `frontend/src/lib/live.ts` keys its error-list invalidation off
# `KIND_ERROR` by name; the rest are opaque to it, and every kind invalidates the
# lists and aggregates.
KIND_TRACE = "trace"
KIND_ERROR = "error"
KIND_SNAPSHOT = "snapshot"
# Not a record: the marker that this subscriber missed `count` events. The UI
# treats it like any other event, which means a full refetch — exactly the
# recovery a gap calls for.
KIND_LAGGED = "lagged"


class LiveEvent:
    """One notification that the store changed.

    Identity only: which activation, which kind of record, how many. The client
    refetches, so a dropped event costs a refresh rather than a gap.
    """

    __slots__ = ("count", "entity_key", "kind", "seq", "trace_id")

    def __init__(
        self,
        *,
        kind: str,
        entity_key: str = "",
        seq: int | None = None,
        trace_id: str = "",
        count: int = 1,
    ) -> None:
        """Record what changed."""
        self.kind = kind
        self.entity_key = entity_key
        self.seq = seq
        self.trace_id = trace_id
        self.count = count

    def __repr__(self) -> str:
        return (
            f"LiveEvent(kind={self.kind!r}, entity_key={self.entity_key!r}, "
            f"seq={self.seq!r}, trace_id={self.trace_id!r}, count={self.count!r})"
        )

    def to_sse(self) -> str:
        """Render as a server-sent-events frame.

        No ``event:`` line, deliberately: the UI reads ``EventSource.onmessage``,
        which fires only for the default event type. Named events would be
        silently ignored there.
        """
        return f"data: {json.dumps(self.to_dict(), separators=(',', ':'))}\n\n"

    def to_dict(self) -> dict[str, Any]:
        """Render as the JSON payload carried in the frame.

        Field for field the ``LiveEvent`` interface in
        ``frontend/src/lib/api-types.ts``. ``seq`` is ``None`` — JSON ``null`` —
        for a record with no activation, which several error reasons are: they
        fire from timer callbacks that never entered one.
        """
        return {
            "kind": self.kind,
            "entity_key": self.entity_key,
            "seq": self.seq,
            "trace_id": self.trace_id,
            "count": self.count,
        }


class _Subscriber:
    """One connected client's bounded mailbox.

    ``loop`` is captured at subscribe time so a publisher on another thread can
    wake the waiter without knowing anything about it.
    """

    __slots__ = ("depth", "dropped", "loop", "queue", "wakeup")

    def __init__(self, *, depth: int, loop: asyncio.AbstractEventLoop) -> None:
        self.depth = depth
        self.dropped = 0
        self.loop = loop
        self.queue: deque[LiveEvent] = deque()
        self.wakeup = asyncio.Event()


def _wake(subscriber: _Subscriber) -> None:
    """Signal the subscriber's reader, from whichever thread is publishing."""
    try:
        running: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
    except RuntimeError:
        running = None
    if running is subscriber.loop:
        subscriber.wakeup.set()
        return
    # A closed reader loop raises here. Its `finally` has already unregistered
    # it, or is about to; either way this event has nowhere to go, and dropping
    # it must not fail the publisher.
    with contextlib.suppress(RuntimeError):
        subscriber.loop.call_soon_threadsafe(subscriber.wakeup.set)


class Broadcaster:
    """Fan-out from ingest to every connected subscriber.

    Publishing is non-blocking by construction: a subscriber whose queue is full
    loses events and is marked lagged. Nothing a client does can slow ingest.
    """

    def __init__(self, *, queue_depth: int = DEFAULT_QUEUE_DEPTH) -> None:
        """Create a broadcaster with the given per-subscriber queue depth."""
        if queue_depth < 1:
            raise ValueError(f"queue_depth must be at least 1, got {queue_depth}")
        self._queue_depth = queue_depth
        # A plain lock rather than an asyncio one: publishers arrive from the
        # event loop and from source threads alike, and the critical section is
        # a few deque operations.
        self._lock = threading.Lock()
        self._subscribers: set[_Subscriber] = set()

    @property
    def subscribers(self) -> int:
        """The number of currently connected subscribers."""
        with self._lock:
            return len(self._subscribers)

    def publish(self, event: LiveEvent) -> None:
        """Deliver ``event`` to every subscriber that has room for it."""
        self._fan_out((event,))

    def publish_batch(self, batch: RecordBatch) -> None:
        """Publish one event per activation the batch touched, coalesced.

        A batch of two hundred spans for one activation is one refresh for the
        client, not two hundred, so it is one frame on the wire.
        """
        self._fan_out(_coalesce(batch))

    def subscribe(self) -> AsyncIterator[LiveEvent]:
        """Yield events until the consumer stops iterating.

        Cleans up its queue on exit, including when the client disconnects
        mid-stream, so a closed tab does not leak a subscriber.
        """
        subscriber = _Subscriber(depth=self._queue_depth, loop=asyncio.get_running_loop())
        with self._lock:
            self._subscribers.add(subscriber)
        stream = self._drain(subscriber)
        # `_drain`'s `finally` covers every generator that was started. This
        # covers the one that never was: registration is eager, so a caller that
        # drops the stream without iterating it would otherwise leak the queue.
        weakref.finalize(stream, self._release, subscriber)
        return stream

    # -- internals -------------------------------------------------------------

    def _fan_out(self, events: tuple[LiveEvent, ...]) -> None:
        if not events:
            return
        with self._lock:
            for subscriber in self._subscribers:
                for event in events:
                    self._offer(subscriber, event)

    def _offer(self, subscriber: _Subscriber, event: LiveEvent) -> None:
        """Enqueue for one subscriber, or count the drop. Caller holds the lock."""
        if len(subscriber.queue) >= subscriber.depth:
            subscriber.dropped += 1
            return
        subscriber.queue.append(event)
        _wake(subscriber)

    async def _drain(self, subscriber: _Subscriber) -> AsyncIterator[LiveEvent]:
        try:
            while True:
                with self._lock:
                    event = self._take(subscriber)
                if event is None:
                    # `wakeup` was cleared under the same lock a publisher takes
                    # to set it, so an event arriving in this window is not lost.
                    await subscriber.wakeup.wait()
                    continue
                yield event
        finally:
            self._release(subscriber)

    def _take(self, subscriber: _Subscriber) -> LiveEvent | None:
        """Pop the next event to deliver, or ``None``. Caller holds the lock."""
        if subscriber.dropped:
            # Report the gap before the backlog: the client's recovery is a full
            # refetch, and the sooner it starts the less stale the screen is.
            dropped, subscriber.dropped = subscriber.dropped, 0
            return LiveEvent(kind=KIND_LAGGED, count=dropped)
        if subscriber.queue:
            return subscriber.queue.popleft()
        subscriber.wakeup.clear()
        return None

    def _release(self, subscriber: _Subscriber) -> None:
        with self._lock:
            self._subscribers.discard(subscriber)
            subscriber.queue.clear()


def _coalesce(batch: RecordBatch) -> tuple[LiveEvent, ...]:
    """Collapse a batch into one event per (kind, activation), in arrival order."""
    counts: dict[tuple[str, str, int | None, str], int] = {}
    for event_row in batch.events:
        key = (KIND_TRACE, event_row.entity_key, event_row.seq, event_row.trace_id)
        counts[key] = counts.get(key, 0) + 1
    for error_row in batch.errors:
        # `seq` is absent for the reasons that fire from a timer callback, which
        # never entered an activation.
        error_key = (KIND_ERROR, error_row.entity_key, error_row.seq, "")
        counts[error_key] = counts.get(error_key, 0) + 1
    for snapshot_row in batch.snapshots:
        snapshot_key = (KIND_SNAPSHOT, snapshot_row.entity_key, snapshot_row.seq, "")
        counts[snapshot_key] = counts.get(snapshot_key, 0) + 1
    return tuple(
        LiveEvent(kind=kind, entity_key=entity_key, seq=seq, trace_id=trace_id, count=count)
        for (kind, entity_key, seq, trace_id), count in counts.items()
    )
