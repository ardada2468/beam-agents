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

Importing this module has no side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from beam_agents.console._records import RecordBatch

__all__ = ["Broadcaster", "LiveEvent"]

# Default per-subscriber queue depth. Deep enough to ride out a render pause,
# shallow enough that a tab left open on a sleeping machine is dropped rather
# than accumulating an unbounded backlog.
DEFAULT_QUEUE_DEPTH = 256


class LiveEvent:
    """One notification that the store changed.

    Identity only: which activation, which kind of record, how many. The client
    refetches, so a dropped event costs a refresh rather than a gap.
    """

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
        raise NotImplementedError

    def to_sse(self) -> str:
        """Render as a server-sent-events frame."""
        raise NotImplementedError

    def to_dict(self) -> dict[str, Any]:
        """Render as the JSON payload carried in the frame."""
        raise NotImplementedError


class Broadcaster:
    """Fan-out from ingest to every connected subscriber.

    Publishing is non-blocking by construction: a subscriber whose queue is full
    loses events and is marked lagged. Nothing a client does can slow ingest.
    """

    def __init__(self, *, queue_depth: int = DEFAULT_QUEUE_DEPTH) -> None:
        """Create a broadcaster with the given per-subscriber queue depth."""
        raise NotImplementedError

    @property
    def subscribers(self) -> int:
        """The number of currently connected subscribers."""
        raise NotImplementedError

    def publish(self, event: LiveEvent) -> None:
        """Deliver ``event`` to every subscriber that has room for it."""
        raise NotImplementedError

    def publish_batch(self, batch: RecordBatch) -> None:
        """Publish one event per activation the batch touched, coalesced."""
        raise NotImplementedError

    def subscribe(self) -> AsyncIterator[LiveEvent]:
        """Yield events until the consumer stops iterating.

        Cleans up its queue on exit, including when the client disconnects
        mid-stream, so a closed tab does not leak a subscriber.
        """
        raise NotImplementedError
