"""The BigQuery trace source: reversing the published row encoding, incrementally.

Covers the two scenarios of "The console reads an existing BigQuery trace table"
— rows reversing into the records that produced them, and an overlapping re-read
changing nothing — plus the import-boundary half of task 1.14 (constructing a
source whose client is missing names the extra).

Every row these tests feed the source is produced by the **real**
``observability.exporters.trace_event_to_row``, so the reversal is anchored to
the encoder rather than to an assumption about it: if the published encoding
changes, these tests fail rather than agreeing with a stale copy of it.

The whole suite runs offline. ``google-cloud-bigquery`` is not in the unit lane,
and the ``client`` constructor argument is the seam that makes that possible: a
fake standing in for a BigQuery ``Client``, holding rows and answering the
source's SQL by parsing the window out of it, exercises the real windowing.

``_ingest.normalize`` and ``ConsoleStore`` belong to other units and still raise
``NotImplementedError``; both are replaced here by stand-ins that record what
they were handed. The stand-in store dedups on the published
``(trace_id, span_id, event_type)`` key, which is what makes "the store's
contents are unchanged" a real assertion rather than a restatement of the call
count.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime
import importlib
import re
import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

import pytest

from beam_agents._protos import TraceEvent
from beam_agents.console import _ingest
from beam_agents.console._records import PROVENANCE_BIGQUERY, EventRow, RecordBatch
from beam_agents.console._sources._bigquery import EXTRA_NAME, BigQueryTraceSource
from beam_agents.observability.exporters import trace_event_to_row

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from beam_agents.console._store import ConsoleStore

_URI = "bigquery://demo-project/agent_telemetry/traces"

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)

# 2024-05-01T00:00:00Z, so the windows in these tests are readable as dates.
_BASE_MS = 1714521600000
_HOUR_MS = 3_600_000


def _ms(iso: str) -> int:
    return int(datetime.datetime.fromisoformat(iso).timestamp() * 1000)


def _event(
    *,
    trace: int,
    span: int,
    event_type: TraceEvent.EventType,
    start_ms: int,
    seq: int = 7,
    step_index: int = 3,
    parent: int = 0,
    attributes: Mapping[str, str] | None = None,
) -> TraceEvent:
    """Build a TraceEvent shaped like the ones ``observability/traces.py`` emits."""
    return TraceEvent(
        trace_id=trace.to_bytes(16, "big"),
        span_id=span.to_bytes(8, "big"),
        parent_span_id=parent.to_bytes(8, "big"),
        entity_key=b"order-42",
        seq=seq,
        step_index=step_index,
        event_type=event_type,
        start_ms=start_ms,
        end_ms=start_ms,  # spans are zero-width by design (add-trace-events D7)
        attributes=dict(attributes or {}),
    )


# --------------------------------------------------------------------------
# Stand-ins for the units this source depends on.
# --------------------------------------------------------------------------


_TIMESTAMP_LITERAL = re.compile(r"TIMESTAMP\s+'([^']+)'")


@dataclass
class _FakeQueryJob:
    """What ``Client.query`` returns: a job whose ``result()`` yields rows."""

    rows: tuple[dict[str, Any], ...]

    def result(self) -> Iterator[dict[str, Any]]:
        """Yield the rows this job selected."""
        return iter(self.rows)


class FakeBigQueryClient:
    """An offline stand-in for ``google.cloud.bigquery.Client``.

    Holds the table's rows and answers a query by parsing the two ``TIMESTAMP``
    literals out of the SQL and returning the rows whose ``event_time`` falls in
    ``[start, end)`` — the same partition pruning the real table does, so a test
    that asserts on the returned rows is asserting on the window the source asked
    for.
    """

    def __init__(self, rows: Sequence[Mapping[str, Any]] = ()) -> None:
        """Seed the table with ``rows`` (as produced by ``trace_event_to_row``)."""
        self.rows: list[dict[str, Any]] = [dict(row) for row in rows]
        self.queries: list[str] = []
        self.close_calls = 0

    def query(self, sql: str) -> _FakeQueryJob:
        """Record the SQL and return the rows inside its event-time window."""
        self.queries.append(sql)
        bounds = _TIMESTAMP_LITERAL.findall(sql)
        assert len(bounds) == 2, f"expected a two-sided window, got {bounds!r} in {sql!r}"
        start, end = (datetime.datetime.fromisoformat(bound) for bound in bounds)
        selected = tuple(
            {key: value for key, value in row.items() if key != "event_time"}
            for row in self.rows
            if start <= datetime.datetime.fromisoformat(row["event_time"]) < end
        )
        return _FakeQueryJob(selected)

    def close(self) -> None:
        """Count the release so the source's ``stop`` can be shown to be idempotent."""
        self.close_calls += 1

    @property
    def windows(self) -> list[tuple[int, int]]:
        """Each query's ``(start_ms, end_ms)`` window, in the order it was issued."""
        out: list[tuple[int, int]] = []
        for sql in self.queries:
            start, end = _TIMESTAMP_LITERAL.findall(sql)
            out.append((_ms(start), _ms(end)))
        return out


@dataclass
class FakeStore:
    """A stand-in for ``ConsoleStore`` that upserts on the published dedup key.

    Idempotency is the point: design D5 keys the event table on
    ``(trace_id, span_id, event_type)``, so re-writing a batch the store already
    holds must leave it exactly as it was. ``write`` returns rows *changed*, which
    is how the source's callers see that a re-read cost nothing.
    """

    batches: list[RecordBatch] = field(default_factory=list)
    events: dict[tuple[str, str, str], EventRow] = field(default_factory=dict)

    def write(self, batch: RecordBatch) -> int:
        """Upsert ``batch``'s events; return how many rows it actually changed."""
        self.batches.append(batch)
        changed = 0
        for row in batch.events:
            key = (row.trace_id, row.span_id, row.event_type)
            if self.events.get(key) != row:
                self.events[key] = row
                changed += 1
        return changed


def _normalize_stub(
    *,
    events: Sequence[TraceEvent] = (),
    errors: Sequence[Any] = (),
    snapshots: Sequence[Any] = (),
    provenance: str,
) -> RecordBatch:
    """Stand in for ``_ingest.normalize`` (Unit 2), which still raises."""
    assert not errors and not snapshots, "a trace source produces trace events only"
    return RecordBatch(
        events=tuple(
            EventRow(
                trace_id=event.trace_id.hex(),
                span_id=event.span_id.hex(),
                parent_span_id=event.parent_span_id.hex(),
                entity_key=event.entity_key.hex(),
                seq=event.seq,
                step_index=event.step_index,
                event_type=TraceEvent.EventType.Name(event.event_type),
                start_ms=event.start_ms,
                end_ms=event.end_ms,
                attributes=dict(event.attributes),
                provenance=provenance,
            )
            for event in events
        )
    )


@pytest.fixture
def normalized(monkeypatch: pytest.MonkeyPatch) -> list[tuple[TraceEvent, ...]]:
    """Install the ``normalize`` stand-in and collect the protos it is handed."""
    seen: list[tuple[TraceEvent, ...]] = []

    def _capture(
        *,
        events: Sequence[TraceEvent] = (),
        errors: Sequence[Any] = (),
        snapshots: Sequence[Any] = (),
        provenance: str,
    ) -> RecordBatch:
        seen.append(tuple(events))
        return _normalize_stub(
            events=events, errors=errors, snapshots=snapshots, provenance=provenance
        )

    monkeypatch.setattr(_ingest, "normalize", _capture)
    return seen


def _source(
    client: FakeBigQueryClient,
    store: FakeStore,
    **kwargs: Any,
) -> BigQueryTraceSource:
    return BigQueryTraceSource(
        _URI,
        cast("ConsoleStore", store),
        client=client,
        **kwargs,
    )


# --------------------------------------------------------------------------
# Scenario: Rows are reversed into the records they encoded.
# --------------------------------------------------------------------------


def test_rows_are_reversed_into_the_records_they_encoded(
    normalized: list[tuple[TraceEvent, ...]],
) -> None:
    originals = (
        _event(
            trace=0xA1,
            span=0xB1,
            event_type=TraceEvent.ACTIVATION_START,
            start_ms=_BASE_MS,
            attributes={"kind": "start", "attempt": "1"},
        ),
        _event(
            trace=0xA1,
            span=0xB2,
            parent=0xB1,
            event_type=TraceEvent.LLM_CALL,
            start_ms=_BASE_MS + 5,
            seq=8,
            step_index=1,
            attributes={"model": "claude", "input_tokens": "120", "cache_hit": "false"},
        ),
        _event(
            trace=0xA1,
            span=0xB3,
            parent=0xB1,
            event_type=TraceEvent.ACTIVATION_END,
            start_ms=_BASE_MS + 9,
            attributes={},
        ),
    )
    client = FakeBigQueryClient([trace_event_to_row(event) for event in originals])
    store = FakeStore()
    source = _source(client, store)

    stored = source.pull_once(now_ms=_BASE_MS + _HOUR_MS)

    assert stored == len(originals)
    assert source.records_stored == len(originals)
    decoded = normalized[0]
    assert len(decoded) == len(originals)
    for original, event in zip(originals, decoded, strict=True):
        assert event.trace_id == original.trace_id
        assert event.span_id == original.span_id
        assert event.parent_span_id == original.parent_span_id
        assert event.entity_key == original.entity_key
        assert event.seq == original.seq
        assert event.step_index == original.step_index
        assert event.event_type == original.event_type
        assert event.start_ms == original.start_ms
        assert event.end_ms == original.end_ms
        assert dict(event.attributes) == dict(original.attributes)
    # The strongest form of the claim: the decoded protos are equal to the ones
    # that produced the rows, field for field, including defaults.
    assert list(decoded) == list(originals)


def test_every_event_type_in_the_vocabulary_survives_the_round_trip(
    normalized: list[tuple[TraceEvent, ...]],
) -> None:
    # Scenario: Rows are reversed into the records they encoded — the enum half.
    # `trace_event_to_row` publishes the enum *name*, so the reversal must go
    # through `EventType.Value`, not through the number.
    originals = tuple(
        _event(
            trace=0xC0,
            span=index,
            event_type=cast("TraceEvent.EventType", number),
            start_ms=_BASE_MS + index,
        )
        for index, number in enumerate(TraceEvent.EventType.values())
    )
    client = FakeBigQueryClient([trace_event_to_row(event) for event in originals])
    store = FakeStore()
    source = _source(client, store)

    source.pull_once(now_ms=_BASE_MS + _HOUR_MS)

    assert [event.event_type for event in normalized[0]] == list(TraceEvent.EventType.values())


def test_the_reader_never_reads_event_time_back(normalized: list[tuple[TraceEvent, ...]]) -> None:
    # Scenario: Rows are reversed into the records they encoded — `event_time` is
    # a pure derivation of `start_ms`, so reading it would be reading the same
    # number twice. It is a partition filter on the way in and nothing on the way
    # out: the projection must not even select it.
    original = _event(
        trace=0xD1, span=0xD2, event_type=TraceEvent.TOOL_CALL, start_ms=_BASE_MS + 11
    )
    client = FakeBigQueryClient([trace_event_to_row(original)])
    source = _source(client, FakeStore())

    source.pull_once(now_ms=_BASE_MS + _HOUR_MS)

    projection = client.queries[0].split("FROM")[0]
    assert "event_time" not in projection
    assert "event_time" in client.queries[0]  # still the WHERE clause's column
    assert normalized[0][0].start_ms == original.start_ms


def test_stored_records_carry_bigquery_provenance(monkeypatch: pytest.MonkeyPatch) -> None:
    # Records normalized by this source must be attributable to the path they
    # arrived by; `provenance` is what the UI keys completeness warnings off.
    monkeypatch.setattr(_ingest, "normalize", _normalize_stub)
    original = _event(trace=1, span=2, event_type=TraceEvent.ERROR, start_ms=_BASE_MS)
    store = FakeStore()
    source = _source(FakeBigQueryClient([trace_event_to_row(original)]), store)

    source.pull_once(now_ms=_BASE_MS + _HOUR_MS)

    assert [row.provenance for row in store.batches[0].events] == [PROVENANCE_BIGQUERY]


def test_an_undecodable_row_is_counted_and_skipped(
    normalized: list[tuple[TraceEvent, ...]],
) -> None:
    # The third rule of `_sources/`: one malformed record is not a reason for a
    # viewer to stop viewing.
    good = _event(trace=0xE1, span=0xE2, event_type=TraceEvent.LLM_CALL, start_ms=_BASE_MS + 1)
    bad = trace_event_to_row(
        _event(trace=0xE1, span=0xE3, event_type=TraceEvent.LLM_CALL, start_ms=_BASE_MS + 2)
    )
    bad["event_type"] = "NOT_AN_EVENT_TYPE"
    client = FakeBigQueryClient([trace_event_to_row(good), bad])
    source = _source(client, FakeStore())

    stored = source.pull_once(now_ms=_BASE_MS + _HOUR_MS)

    assert stored == 1
    assert source.decode_failures == 1
    assert list(normalized[0]) == [good]


# --------------------------------------------------------------------------
# Scenario: Re-reading an overlapping window changes nothing.
# --------------------------------------------------------------------------


def test_re_reading_an_overlapping_window_changes_nothing(
    normalized: list[tuple[TraceEvent, ...]],
) -> None:
    now_ms = _BASE_MS + _HOUR_MS
    # Placed just inside the window's trailing edge, which is where the overlap
    # reaches back to and therefore where a re-read actually re-reads.
    originals = tuple(
        _event(
            trace=0xF0,
            span=index,
            event_type=TraceEvent.LLM_CALL,
            start_ms=now_ms - 1000 + index,
            attributes={"model": "claude"},
        )
        for index in range(1, 4)
    )
    client = FakeBigQueryClient([trace_event_to_row(event) for event in originals])
    store = FakeStore()
    source = _source(client, store)

    first = source.pull_once(now_ms=now_ms)
    contents = dict(store.events)
    # A second pull whose window deliberately reaches back over the first one.
    second = source.pull_once(now_ms=now_ms)

    assert first == len(originals)
    # The overlap is real: the second window starts before the first one ended.
    assert client.windows[1][0] < client.windows[0][1]
    # Every row is read a second time...
    assert second == len(originals)
    assert len(normalized[1]) == len(originals)
    # ...and changes nothing, because the store upserts on the dedup key.
    assert store.events == contents
    assert store.write(store.batches[-1]) == 0


def test_a_window_with_no_rows_leaves_the_store_untouched(
    normalized: list[tuple[TraceEvent, ...]],
) -> None:
    # An empty window must not cost a write: the store is on the other side of a
    # single-writer SQLite file, and a poll loop that writes nothing every 30s
    # forever is a poll loop that is not writing nothing.
    client = FakeBigQueryClient()
    store = FakeStore()
    source = _source(client, store)

    assert source.pull_once(now_ms=_BASE_MS) == 0
    assert store.batches == []
    assert normalized == []


# --------------------------------------------------------------------------
# Incremental reads: the watermark and the window it produces.
# --------------------------------------------------------------------------


def test_the_first_pull_reads_back_only_as_far_as_the_lookback(
    normalized: list[tuple[TraceEvent, ...]],
) -> None:
    old = _event(trace=1, span=1, event_type=TraceEvent.LLM_CALL, start_ms=_BASE_MS)
    recent = _event(
        trace=1, span=2, event_type=TraceEvent.LLM_CALL, start_ms=_BASE_MS + 3 * _HOUR_MS
    )
    client = FakeBigQueryClient([trace_event_to_row(old), trace_event_to_row(recent)])
    source = _source(client, FakeStore(), lookback_hours=2.0)

    now_ms = _BASE_MS + 4 * _HOUR_MS
    stored = source.pull_once(now_ms=now_ms)

    assert client.windows == [(now_ms - 2 * _HOUR_MS, now_ms)]
    assert stored == 1
    assert list(normalized[0]) == [recent]


def test_there_is_no_watermark_before_the_first_pull() -> None:
    assert _source(FakeBigQueryClient(), FakeStore()).watermark_ms is None


def test_the_watermark_advances_so_a_pull_prunes_instead_of_scanning() -> None:
    # Incremental by `event_time` — the table's partition column — is the whole
    # reason the console can read a table it does not own.
    client = FakeBigQueryClient()
    source = _source(client, FakeStore(), lookback_hours=1.0)

    first_now = _BASE_MS + _HOUR_MS
    source.pull_once(now_ms=first_now)
    assert source.watermark_ms == first_now

    second_now = first_now + _HOUR_MS
    source.pull_once(now_ms=second_now)
    assert source.watermark_ms == second_now

    first_window, second_window = client.windows
    assert first_window == (first_now - _HOUR_MS, first_now)
    # The second window resumes from the watermark, not from the lookback, and
    # overlaps the first one rather than abutting it.
    assert second_window[1] == second_now
    assert first_window[1] - _HOUR_MS < second_window[0] < first_window[1]


def test_a_pull_that_has_not_moved_forward_queries_nothing() -> None:
    # `now_ms` at or behind the watermark is an empty window, not a backwards one.
    client = FakeBigQueryClient()
    source = _source(client, FakeStore())

    source.pull_once(now_ms=_BASE_MS)
    assert source.pull_once(now_ms=_BASE_MS - _HOUR_MS) == 0
    assert len(client.queries) == 1


def test_the_query_names_the_table_the_uri_names() -> None:
    client = FakeBigQueryClient()
    source = _source(client, FakeStore())

    source.pull_once(now_ms=_BASE_MS)

    assert "`demo-project.agent_telemetry.traces`" in client.queries[0]


# --------------------------------------------------------------------------
# Configuration.
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "uri",
    [
        "bigquery://demo-project/traces",
        "bigquery:///agent_telemetry/traces",
        "bigquery://demo-project/agent_telemetry/traces/extra",
        "kafka://localhost:9092/traces",
        "not-a-uri",
    ],
)
def test_a_malformed_uri_is_rejected_at_construction(uri: str) -> None:
    # The grammar is the runtime's, reused rather than reinvented, so a URI the
    # pipeline's `traces_to` accepts is a URI this accepts and vice versa.
    with pytest.raises(ValueError, match="bigquery"):
        BigQueryTraceSource(uri, cast("ConsoleStore", FakeStore()), client=FakeBigQueryClient())


@pytest.mark.parametrize(
    ("field_name", "value"), [("lookback_hours", 0.0), ("poll_interval_s", -1)]
)
def test_a_non_positive_interval_is_rejected_at_construction(field_name: str, value: float) -> None:
    with pytest.raises(ValueError, match=field_name):
        _source(FakeBigQueryClient(), FakeStore(), **{field_name: value})


def test_a_table_identifier_that_could_escape_its_quoting_is_rejected() -> None:
    # The table path is interpolated into the SQL — BigQuery cannot parameterize
    # an identifier — so the characters that could end the quoting are refused
    # before any query is built.
    with pytest.raises(ValueError, match="identifier"):
        BigQueryTraceSource(
            "bigquery://demo-project/agent_telemetry/tra`ces",
            cast("ConsoleStore", FakeStore()),
            client=FakeBigQueryClient(),
        )


# --------------------------------------------------------------------------
# The import boundary (task 1.14).
# --------------------------------------------------------------------------


class _BlockBigQuery:
    """A meta-path finder that makes ``google.cloud.bigquery`` unimportable.

    The unit lane does not install ``google-cloud-bigquery``, but
    ``apache-beam[gcp]`` drags it into some environments; blocking it explicitly
    makes this test prove the same thing in both.
    """

    def find_spec(self, name: str, path: object = None, target: object = None) -> None:
        """Refuse the BigQuery client package; returning ``None`` defers the rest."""
        if name == "google.cloud.bigquery" or name.startswith("google.cloud.bigquery."):
            raise ImportError(f"blocked for this test: {name}")


@contextlib.contextmanager
def _bigquery_uninstalled() -> Iterator[None]:
    blocker = _BlockBigQuery()
    cached = {
        name: sys.modules.pop(name)
        for name in list(sys.modules)
        if name == "google.cloud.bigquery" or name.startswith("google.cloud.bigquery.")
    }
    cloud = sys.modules.get("google.cloud")
    had_attr = hasattr(cloud, "bigquery")
    if had_attr:
        delattr(cloud, "bigquery")
    sys.meta_path.insert(0, blocker)
    try:
        yield
    finally:
        sys.meta_path.remove(blocker)
        sys.modules.update(cached)
        if had_attr and cloud is not None:
            cloud.bigquery = cached.get("google.cloud.bigquery")  # type: ignore[attr-defined]


def test_constructing_without_the_client_names_the_console_ingest_extra() -> None:
    # Task 1.14: a missing client is an actionable error naming the extra, not a
    # transitive ImportError from somewhere inside google.cloud.
    with _bigquery_uninstalled(), pytest.raises(ImportError) as excinfo:
        BigQueryTraceSource(_URI, cast("ConsoleStore", FakeStore()))

    message = str(excinfo.value)
    assert EXTRA_NAME in message
    assert "google-cloud-bigquery" in message
    assert "BigQueryTraceSource" in message


def test_the_injected_client_is_used_without_importing_the_library() -> None:
    # The seam that lets the whole suite run offline: with a client injected, the
    # constructor never reaches for `google.cloud.bigquery` at all.
    with _bigquery_uninstalled():
        source = _source(FakeBigQueryClient(), FakeStore())
        assert source.pull_once(now_ms=_BASE_MS) == 0


def test_the_module_imports_with_the_client_absent() -> None:
    # Task 1.14: the module itself never reaches for the client at import time.
    with _bigquery_uninstalled():
        importlib.reload(sys.modules["beam_agents.console._sources._bigquery"])


# --------------------------------------------------------------------------
# The polling loop.
# --------------------------------------------------------------------------


async def test_run_polls_until_stopped(normalized: list[tuple[TraceEvent, ...]]) -> None:
    original = _event(trace=9, span=9, event_type=TraceEvent.LLM_CALL, start_ms=_now_ms() - 1000)
    client = FakeBigQueryClient([trace_event_to_row(original)])
    store = FakeStore()
    source = _source(client, store, poll_interval_s=0.01)

    task = asyncio.create_task(source.run())
    for _ in range(200):
        await asyncio.sleep(0.01)
        if len(client.queries) >= 2:
            break
    await source.stop()
    await task

    assert len(client.queries) >= 2
    assert source.records_stored >= 1
    assert store.events


async def test_stop_is_idempotent_and_releases_the_client() -> None:
    client = FakeBigQueryClient()
    source = _source(client, FakeStore())

    await source.stop()
    await source.stop()

    assert client.close_calls == 1


def _now_ms() -> int:
    return int((datetime.datetime.now(tz=datetime.UTC) - _EPOCH).total_seconds() * 1000)
