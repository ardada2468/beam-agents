"""The live feed's contract: a reader can never affect a writer.

Every test here is about the one asymmetry the broadcaster exists to enforce.
Ingest publishes; browsers subscribe; nothing a subscriber does — being slow,
being asleep, going away mid-frame — is allowed to reach back and cost the
publisher a blocked call or a raised exception.
"""

from __future__ import annotations

import asyncio
import gc
import json
import threading
from typing import TYPE_CHECKING, cast

import pytest

from beam_agents.console._records import ErrorRow, EventRow, RecordBatch, SnapshotRow
from beam_agents.console._sse import (
    KIND_ERROR,
    KIND_LAGGED,
    KIND_SNAPSHOT,
    KIND_TRACE,
    Broadcaster,
    LiveEvent,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

# Every await on the stream is bounded: the suite's pytest-timeout is 30s, and a
# broadcaster bug has to fail as an assertion rather than as a hung worker.
_AWAIT_TIMEOUT_S = 5.0


def _subscribe(broadcaster: Broadcaster) -> AsyncGenerator[LiveEvent, None]:
    """Subscribe, narrowed to the generator type so a test can close it."""
    return cast("AsyncGenerator[LiveEvent, None]", broadcaster.subscribe())


async def _next(stream: AsyncGenerator[LiveEvent, None]) -> LiveEvent:
    """Pull one event, failing the test rather than hanging if none arrives."""
    return await asyncio.wait_for(anext(stream), _AWAIT_TIMEOUT_S)


def _event_row(*, entity_key: str = "k", seq: int = 1, span_id: str = "aa") -> EventRow:
    return EventRow(
        trace_id="t" * 32,
        span_id=span_id,
        parent_span_id="",
        entity_key=entity_key,
        seq=seq,
        step_index=0,
        event_type="LLM_CALL",
        start_ms=1_000,
        end_ms=1_000,
    )


# --- LiveEvent: identity only, in the frame the UI already parses -------------


def test_a_live_event_carries_identity_and_not_payload() -> None:
    event = LiveEvent(kind=KIND_TRACE, entity_key="orders/7", seq=3, trace_id="ab" * 16, count=4)

    # Exactly the fields `frontend/src/lib/api-types.ts` declares on LiveEvent.
    assert event.to_dict() == {
        "kind": KIND_TRACE,
        "entity_key": "orders/7",
        "seq": 3,
        "trace_id": "ab" * 16,
        "count": 4,
    }


def test_an_event_with_no_activation_reports_a_null_seq() -> None:
    # `hitl_timeout` and the TTL wipes fire from timer callbacks that have no
    # activation, so the UI's `event.seq !== null` guard needs something to
    # guard against.
    assert LiveEvent(kind=KIND_ERROR, entity_key="orders/7").to_dict()["seq"] is None


def test_a_frame_is_a_default_message_event_the_ui_can_parse() -> None:
    event = LiveEvent(kind=KIND_TRACE, entity_key="k", seq=1)

    frame = event.to_sse()

    # `useLiveStream` reads `source.onmessage`, which fires only for frames
    # carrying no `event:` line.
    assert "event:" not in frame
    assert frame.endswith("\n\n")
    assert json.loads(frame.removeprefix("data: ").strip()) == event.to_dict()


# --- Delivery -----------------------------------------------------------------


async def test_an_ingested_record_reaches_an_open_stream() -> None:
    broadcaster = Broadcaster()
    stream = _subscribe(broadcaster)

    broadcaster.publish_batch(RecordBatch(events=(_event_row(entity_key="orders/7", seq=3),)))

    event = await _next(stream)
    assert (event.kind, event.entity_key, event.seq) == (KIND_TRACE, "orders/7", 3)
    await stream.aclose()


async def test_a_subscriber_registered_before_it_is_iterated_loses_nothing() -> None:
    # The window between the route calling `subscribe()` and sse_starlette
    # pulling the first frame is real, and an event published inside it is
    # exactly the first event of a run someone opened the console to watch.
    broadcaster = Broadcaster()
    stream = _subscribe(broadcaster)

    broadcaster.publish(LiveEvent(kind=KIND_TRACE, entity_key="k", seq=1))

    assert (await _next(stream)).entity_key == "k"
    await stream.aclose()


async def test_one_event_per_activation_the_batch_touched() -> None:
    broadcaster = Broadcaster()
    stream = _subscribe(broadcaster)
    batch = RecordBatch(
        events=(
            _event_row(entity_key="a", seq=1, span_id="01"),
            _event_row(entity_key="a", seq=1, span_id="02"),
            _event_row(entity_key="b", seq=2, span_id="03"),
        ),
        errors=(ErrorRow(entity_key="a", reason="activation_error", detail="", event_time_ms=5),),
        snapshots=(SnapshotRow(entity_key="b", seq=2, snapshot_at_ms=5, state_schema_version=1),),
    )

    broadcaster.publish_batch(batch)

    received = [await _next(stream) for _ in range(4)]
    assert [(e.kind, e.entity_key, e.seq, e.count) for e in received] == [
        (KIND_TRACE, "a", 1, 2),
        (KIND_TRACE, "b", 2, 1),
        (KIND_ERROR, "a", None, 1),
        (KIND_SNAPSHOT, "b", 2, 1),
    ]
    await stream.aclose()


async def test_an_empty_batch_publishes_nothing() -> None:
    broadcaster = Broadcaster()
    stream = _subscribe(broadcaster)

    broadcaster.publish_batch(RecordBatch())

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(anext(stream), 0.05)
    await stream.aclose()


def test_publishing_with_no_subscribers_is_a_no_op() -> None:
    broadcaster = Broadcaster()

    broadcaster.publish(LiveEvent(kind=KIND_TRACE))

    assert broadcaster.subscribers == 0


async def test_every_subscriber_receives_every_event() -> None:
    broadcaster = Broadcaster()
    first, second = _subscribe(broadcaster), _subscribe(broadcaster)
    assert broadcaster.subscribers == 2

    broadcaster.publish(LiveEvent(kind=KIND_TRACE, entity_key="k", seq=9))

    assert (await _next(first)).seq == 9
    assert (await _next(second)).seq == 9
    await first.aclose()
    await second.aclose()


# --- A reader can never affect a writer ---------------------------------------


async def test_a_slow_client_is_dropped_rather_than_allowed_to_block_a_writer() -> None:
    broadcaster = Broadcaster(queue_depth=2)
    stream = _subscribe(broadcaster)

    # Nothing consumes; the queue fills after two, and every later publish must
    # still return — not block, not raise.
    for seq in range(50):
        broadcaster.publish(LiveEvent(kind=KIND_TRACE, entity_key="k", seq=seq))

    # The gap is reported rather than hidden: the client is told to refetch.
    lagged = await _next(stream)
    assert (lagged.kind, lagged.count) == (KIND_LAGGED, 48)
    assert [(await _next(stream)).seq for _ in range(2)] == [0, 1]
    await stream.aclose()


async def test_a_disconnected_client_does_not_block_ingest() -> None:
    broadcaster = Broadcaster()
    leaving, staying = _subscribe(broadcaster), _subscribe(broadcaster)
    broadcaster.publish(LiveEvent(kind=KIND_TRACE, entity_key="before", seq=1))
    assert (await _next(leaving)).entity_key == "before"

    await leaving.aclose()
    assert broadcaster.subscribers == 1

    broadcaster.publish(LiveEvent(kind=KIND_TRACE, entity_key="after", seq=2))

    assert (await _next(staying)).entity_key == "before"
    assert (await _next(staying)).entity_key == "after"
    await staying.aclose()


async def test_subscribe_cleans_up_its_queue_on_exit() -> None:
    broadcaster = Broadcaster()
    stream = _subscribe(broadcaster)
    broadcaster.publish(LiveEvent(kind=KIND_TRACE))
    await _next(stream)

    await stream.aclose()

    assert broadcaster.subscribers == 0


async def test_a_stream_abandoned_without_being_iterated_leaks_no_subscriber() -> None:
    broadcaster = Broadcaster()
    stream = _subscribe(broadcaster)  # never iterated, never closed
    assert broadcaster.subscribers == 1

    del stream
    gc.collect()

    assert broadcaster.subscribers == 0


async def test_a_publish_from_another_thread_reaches_a_subscriber() -> None:
    # The Kafka and BigQuery sources publish from their own threads; the
    # broadcaster is the boundary where that has to be safe.
    broadcaster = Broadcaster()
    stream = _subscribe(broadcaster)

    thread = threading.Thread(
        target=broadcaster.publish,
        args=(LiveEvent(kind=KIND_TRACE, entity_key="from-thread", seq=1),),
    )
    thread.start()
    thread.join()

    assert (await _next(stream)).entity_key == "from-thread"
    await stream.aclose()


def test_a_queue_depth_below_one_is_rejected() -> None:
    with pytest.raises(ValueError, match="queue_depth"):
        Broadcaster(queue_depth=0)
