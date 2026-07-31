"""The ASGI assembly: ingest in, API, stream, and UI out.

Everything here runs in-process — ``httpx.ASGITransport`` for the finite
responses, and a hand-driven ASGI call for the stream, because
``ASGITransport`` buffers a whole response before returning and a live stream
never ends. No socket, no server process.

The store, the read router, and the decoders belong to other modules and are
faked: what is under test is the assembly — which route exists, what it decodes
with, what it normalizes through, what it writes, what it broadcasts, and what
it refuses.

The one fake that is not a stub is ``normalize``. "No partial write" and "the
same path for every endpoint" are only meaningful if real rows come out the far
end, so the fake here builds real ``_records`` rows out of real protos.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import json
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import httpx
import pytest
import uvicorn
from fastapi import APIRouter
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2

from beam_agents._protos import ActivationErrorRecord, StateSnapshot, TraceEvent
from beam_agents.console import _api, _app, _ingest
from beam_agents.console._records import (
    PROVENANCE_NATIVE,
    PROVENANCE_OTLP,
    ErrorRow,
    EventRow,
    RecordBatch,
    SnapshotRow,
)
from beam_agents.console._schema import SCHEMA_VERSION
from beam_agents.console._sse import KIND_TRACE, Broadcaster
from beam_agents.observability.otlp import _encode_batch, _event_to_span
from beam_agents.replay.bundle import ReplayUsageError, frame_trace_events, parse_trace_stream

if TYPE_CHECKING:
    from collections.abc import Iterator, MutableMapping, Sequence

    from fastapi import FastAPI

_AWAIT_TIMEOUT_S = 5.0

# A path that cannot exist, so "is there a packaged bundle in this tree?" has
# the same answer on a clean checkout and on a machine where someone has run
# `make console-build`.
_NO_PACKAGED_BUNDLE = "/nonexistent/beam-agents-console-static"


# --- fakes for the units built in parallel ------------------------------------


class FakeStore:
    """Records what was written, and nothing else."""

    def __init__(self, path: str | Path = "", *, retention_hours: float | None = None) -> None:
        self.path = path
        self.retention_hours = retention_hours
        self.batches: list[RecordBatch] = []
        self.closed = False

    def write(self, batch: RecordBatch) -> int:
        self.batches.append(batch)
        return len(batch)

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> FakeStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @property
    def rows(self) -> list[EventRow]:
        return [row for batch in self.batches for row in batch.events]


def _normalize(
    *,
    events: Sequence[TraceEvent] = (),
    errors: Sequence[ActivationErrorRecord] = (),
    snapshots: Sequence[StateSnapshot] = (),
    provenance: str,
) -> RecordBatch:
    """A real-enough ``_ingest.normalize``: protos in, rows out, provenance stamped."""
    return RecordBatch(
        events=tuple(
            EventRow(
                trace_id=event.trace_id.hex(),
                span_id=event.span_id.hex(),
                parent_span_id=event.parent_span_id.hex(),
                entity_key=event.entity_key.decode(),
                seq=event.seq,
                step_index=event.step_index,
                event_type=str(TraceEvent.EventType.Name(event.event_type)),
                start_ms=event.start_ms,
                end_ms=event.end_ms,
                attributes=dict(event.attributes),
                provenance=provenance,
            )
            for event in events
        ),
        errors=tuple(
            ErrorRow(
                entity_key=error.entity_key.decode(),
                reason=error.reason,
                detail=error.detail,
                event_time_ms=error.event_time_ms,
                provenance=provenance,
            )
            for error in errors
        ),
        snapshots=tuple(
            SnapshotRow(
                entity_key=snapshot.entity_key.decode(),
                seq=snapshot.seq,
                snapshot_at_ms=snapshot.snapshot_at_ms,
                state_schema_version=snapshot.state_schema_version,
                raw=snapshot.SerializeToString(deterministic=True),
                provenance=provenance,
            )
            for snapshot in snapshots
        ),
    )


def _decode_trace_stream(payload: bytes) -> tuple[TraceEvent, ...]:
    try:
        return tuple(parse_trace_stream(payload))
    except ReplayUsageError as exc:
        # `_ingest.decode_trace_stream` raises the console's own ValueError
        # subclass; the framing parser underneath it raises the replay CLI's.
        raise _ingest.TruncatedStreamError(str(exc), records_read=0) from exc


def _decode_otlp_request(payload: bytes) -> tuple[TraceEvent, ...]:
    request = trace_service_pb2.ExportTraceServiceRequest()
    request.ParseFromString(payload)
    return tuple(
        TraceEvent(
            trace_id=span.trace_id,
            span_id=span.span_id,
            entity_key=b"orders/7",
            seq=3,
            event_type=cast("Any", TraceEvent.EventType.Value(span.name.upper())),
            start_ms=span.start_time_unix_nano // 1_000_000,
            end_ms=span.end_time_unix_nano // 1_000_000,
        )
        for resource_spans in request.resource_spans
        for scope_spans in resource_spans.scope_spans
        for span in scope_spans.spans
    )


def _decode_error_payload(payload: bytes) -> tuple[ActivationErrorRecord, ...]:
    record = ActivationErrorRecord()
    record.ParseFromString(payload)
    return (record,)


def _decode_snapshot_payload(payload: bytes) -> tuple[StateSnapshot, ...]:
    snapshot = StateSnapshot()
    snapshot.ParseFromString(payload)
    return (snapshot,)


def _build_router(store: object, **options: Any) -> Any:
    # The real `build_router` carries its own `/api` prefix, so this stand-in
    # must too. A fake that drops it lets `create_app` double-prefix without any
    # test noticing — which is exactly what happened: every read route went to
    # `/api/api/...` and the SPA fallback answered `/api/...` with HTML.
    router = APIRouter(prefix="/api")

    @router.get("/overview")
    async def overview() -> dict[str, str]:
        return {"from": "the read router"}

    @router.get("/store")
    async def store_status() -> dict[str, str]:
        return {"from": "the read router"}

    return router


@pytest.fixture(autouse=True)
def wired(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Stand in for the decoders and the read router, which other units own.

    The decoders are the real proto parsers rather than stubs: a decode that
    cannot fail would make the malformed-payload tests vacuous.
    """
    monkeypatch.setattr(_ingest, "decode_trace_stream", _decode_trace_stream)
    monkeypatch.setattr(_ingest, "decode_otlp_request", _decode_otlp_request)
    monkeypatch.setattr(_ingest, "decode_error_payload", _decode_error_payload)
    monkeypatch.setattr(_ingest, "decode_snapshot_payload", _decode_snapshot_payload)
    monkeypatch.setattr(_ingest, "normalize", _normalize)
    monkeypatch.setattr(_api, "build_router", _build_router)
    monkeypatch.setattr(_app, "PACKAGED_STATIC", Path(_NO_PACKAGED_BUNDLE))
    yield


# --- payloads and clients -----------------------------------------------------


def _trace_event(*, entity_key: bytes = b"orders/7", seq: int = 3) -> TraceEvent:
    return TraceEvent(
        trace_id=bytes(range(16)),
        span_id=bytes(range(8)),
        entity_key=entity_key,
        seq=seq,
        step_index=0,
        event_type=TraceEvent.LLM_CALL,
        start_ms=1_700_000_000_000,
        end_ms=1_700_000_000_000,
        attributes={"gen_ai.request.model": "fake/model"},
    )


def _otlp_payload(event: TraceEvent) -> bytes:
    # Exactly the bytes `WriteTracesToOtlp` puts on the wire.
    span = _event_to_span(event)
    assert span is not None
    return _encode_batch([span], service_name="beam-agents")


def _client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://console.test")


def _build(store: FakeStore | None = None, **options: Any) -> FastAPI:
    return _app.create_app(cast("Any", store if store is not None else FakeStore()), **options)


class RawStream:
    """One SSE client, driven straight against the ASGI callable.

    ``httpx.ASGITransport`` buffers a response to completion before it returns,
    so it cannot read a stream that never ends. Calling the app directly also
    makes the disconnect explicit, which is the thing the stream has to survive.
    """

    def __init__(self, app: FastAPI, path: str = "/api/stream") -> None:
        self._app = app
        self._path = path
        self._sent: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._disconnect = asyncio.Event()
        self._body_read = False
        self._task: asyncio.Task[None] | None = None
        self.status: int | None = None
        self.headers: dict[str, str] = {}

    async def __aenter__(self) -> RawStream:
        self._task = asyncio.create_task(self._run())
        start = await self._next_message("http.response.start")
        self.status = int(start["status"])
        self.headers = {k.decode().lower(): v.decode() for k, v in start.get("headers", [])}
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.disconnect()

    async def _run(self) -> None:
        async def receive() -> dict[str, Any]:
            if not self._body_read:
                self._body_read = True
                return {"type": "http.request", "body": b"", "more_body": False}
            await self._disconnect.wait()
            return {"type": "http.disconnect"}

        async def send(message: MutableMapping[str, Any]) -> None:
            await self._sent.put(dict(message))

        scope: dict[str, Any] = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": self._path,
            "raw_path": self._path.encode(),
            "query_string": b"",
            "root_path": "",
            "headers": [(b"host", b"console.test"), (b"accept", b"text/event-stream")],
            "client": ("127.0.0.1", 5000),
            "server": ("console.test", 80),
        }
        await self._app(scope, receive, send)

    async def _next_message(self, kind: str) -> dict[str, Any]:
        while True:
            message = await asyncio.wait_for(self._sent.get(), _AWAIT_TIMEOUT_S)
            if message["type"] == kind:
                return message

    async def next_event(self) -> dict[str, Any]:
        """Return the JSON of the next `data:` frame, ignoring keep-alive comments."""
        while True:
            message = await self._next_message("http.response.body")
            for line in message.get("body", b"").decode().splitlines():
                if line.startswith("data:"):
                    return cast("dict[str, Any]", json.loads(line.removeprefix("data:").strip()))

    async def disconnect(self) -> None:
        """Hang up, the way a closed browser tab does."""
        self._disconnect.set()
        if self._task is not None:
            await asyncio.wait_for(self._task, _AWAIT_TIMEOUT_S)
            self._task = None


# --- liveness -----------------------------------------------------------------


async def test_the_liveness_endpoint_reports_healthy_before_any_ingest() -> None:
    async with _client(_build()) as client:
        response = await client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "version": version("beam-agents"),
        "schema_version": SCHEMA_VERSION,
        "ui_bundled": False,
        "sources": [],
    }


async def test_liveness_names_the_configured_ingest_sources() -> None:
    async with _client(_build(sources=("kafka://broker/traces",))) as client:
        assert (await client.get("/healthz")).json()["sources"] == ["kafka://broker/traces"]


# --- the read router ----------------------------------------------------------


async def test_the_read_router_is_mounted_under_api() -> None:
    async with _client(_build()) as client:
        response = await client.get("/api/overview")

    assert response.status_code == 200
    assert response.json() == {"from": "the read router"}


# --- native ingest ------------------------------------------------------------


async def test_a_framed_trace_stream_is_stored_with_native_provenance() -> None:
    store = FakeStore()
    payload = frame_trace_events([_trace_event(), _trace_event(seq=4)])

    async with _client(_build(store)) as client:
        response = await client.post("/ingest/traces", content=payload)

    assert response.status_code == 200
    assert response.json() == {"accepted": 2, "written": 2}
    assert [row.provenance for row in store.rows] == [PROVENANCE_NATIVE] * 2
    assert [row.seq for row in store.rows] == [3, 4]


async def test_an_error_record_is_stored_with_native_provenance() -> None:
    store = FakeStore()
    payload = ActivationErrorRecord(
        entity_key=b"orders/7", reason="activation_timeout", detail="deadline", event_time_ms=7
    ).SerializeToString(deterministic=True)

    async with _client(_build(store)) as client:
        response = await client.post("/ingest/errors", content=payload)

    assert response.status_code == 200
    (batch,) = store.batches
    assert [(row.reason, row.provenance) for row in batch.errors] == [
        ("activation_timeout", PROVENANCE_NATIVE)
    ]


async def test_a_snapshot_is_stored_with_native_provenance() -> None:
    store = FakeStore()
    payload = StateSnapshot(
        entity_key=b"orders/7", seq=3, snapshot_at_ms=9, state_schema_version=1
    ).SerializeToString(deterministic=True)

    async with _client(_build(store)) as client:
        response = await client.post("/ingest/snapshots", content=payload)

    assert response.status_code == 200
    (batch,) = store.batches
    assert [(row.seq, row.provenance) for row in batch.snapshots] == [(3, PROVENANCE_NATIVE)]


async def test_an_empty_payload_is_accepted_as_nothing() -> None:
    # A sender flushing an empty batch is not an error, and it must not be
    # reported as one row written.
    store = FakeStore()

    async with _client(_build(store)) as client:
        response = await client.post("/ingest/traces", content=b"")

    assert response.status_code == 200
    assert response.json() == {"accepted": 0, "written": 0}
    assert store.batches == []


# --- OTLP ingest --------------------------------------------------------------


async def test_an_existing_otlp_exporter_reaches_the_console_unchanged() -> None:
    store = FakeStore()

    async with _client(_build(store)) as client:
        response = await client.post(
            "/v1/traces",
            content=_otlp_payload(_trace_event()),
            headers={"content-type": "application/x-protobuf"},
        )

    assert response.status_code == 200
    assert [row.seq for row in store.rows] == [3]


async def test_otlps_known_loss_is_reported_not_hidden() -> None:
    store = FakeStore()

    async with _client(_build(store)) as client:
        await client.post(
            "/v1/traces",
            content=_otlp_payload(_trace_event()),
            headers={"content-type": "application/x-protobuf"},
        )

    # Provenance is the flag the UI keys its incomplete-record warning off:
    # OTLP carries no ACTIVATION_START, so start-vs-resume is unknowable here.
    assert [row.provenance for row in store.rows] == [PROVENANCE_OTLP]


async def test_the_otlp_response_is_an_empty_export_response() -> None:
    # A real OTLP client parses the body as an ExportTraceServiceResponse, and
    # an empty message is the "everything accepted" encoding.
    async with _client(_build()) as client:
        response = await client.post(
            "/v1/traces",
            content=_otlp_payload(_trace_event()),
            headers={"content-type": "application/x-protobuf"},
        )

    assert response.headers["content-type"] == "application/x-protobuf"
    parsed = trace_service_pb2.ExportTraceServiceResponse()
    parsed.ParseFromString(response.content)
    assert not parsed.HasField("partial_success")


async def test_otlp_json_is_refused_by_naming_the_encoding_it_wants() -> None:
    store = FakeStore()

    async with _client(_build(store)) as client:
        response = await client.post(
            "/v1/traces", content=b"{}", headers={"content-type": "application/json"}
        )

    assert response.status_code == 415
    assert "application/x-protobuf" in response.json()["detail"]
    assert "application/json" in response.json()["detail"]
    assert store.batches == []


# --- malformed payloads -------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "headers"),
    [
        ("/ingest/traces", {}),
        ("/ingest/errors", {}),
        ("/ingest/snapshots", {}),
        ("/v1/traces", {"content-type": "application/x-protobuf"}),
    ],
)
async def test_a_malformed_payload_is_rejected_without_affecting_stored_records(
    path: str, headers: dict[str, str]
) -> None:
    store = FakeStore()

    async with _client(_build(store)) as client:
        response = await client.post(
            path, content=b"\xff\xff not a record \xff\xff", headers=headers
        )

    assert response.status_code == 400
    # Names what was rejected, not just "bad request".
    assert path.rsplit("/", 1)[-1] in response.json()["detail"]
    # No partial write: every payload is decoded before the store is touched.
    assert store.batches == []


async def test_a_malformed_payload_reaches_no_subscriber() -> None:
    broadcaster = Broadcaster()
    stream = broadcaster.subscribe()

    async with _client(_build(broadcaster=broadcaster)) as client:
        assert (await client.post("/ingest/traces", content=b"\xff\xff\xff")).status_code == 400

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(anext(stream), 0.05)


# --- the same path for every endpoint -----------------------------------------


async def test_every_endpoint_normalizes_through_the_one_normalizer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[str] = []

    def recording(**kwargs: Any) -> RecordBatch:
        seen.append(str(kwargs["provenance"]))
        return _normalize(**kwargs)

    monkeypatch.setattr(_ingest, "normalize", recording)
    payloads: list[tuple[str, bytes, dict[str, str]]] = [
        ("/ingest/traces", frame_trace_events([_trace_event()]), {}),
        (
            "/ingest/errors",
            ActivationErrorRecord(entity_key=b"k", reason="activation_error").SerializeToString(),
            {},
        ),
        (
            "/ingest/snapshots",
            StateSnapshot(entity_key=b"k", seq=1, state_schema_version=1).SerializeToString(),
            {},
        ),
        ("/v1/traces", _otlp_payload(_trace_event()), {"content-type": "application/x-protobuf"}),
    ]

    async with _client(_build()) as client:
        for path, content, headers in payloads:
            assert (await client.post(path, content=content, headers=headers)).status_code == 200

    assert seen == [PROVENANCE_NATIVE, PROVENANCE_NATIVE, PROVENANCE_NATIVE, PROVENANCE_OTLP]


# --- the live stream ----------------------------------------------------------


async def test_an_ingested_record_reaches_an_open_stream() -> None:
    app = _build()

    async with _client(app) as client, RawStream(app) as stream:
        assert stream.status == 200
        assert stream.headers["content-type"].startswith("text/event-stream")

        posted = await client.post("/ingest/traces", content=frame_trace_events([_trace_event()]))
        assert posted.status_code == 200
        event = await stream.next_event()

    assert event == {
        "kind": KIND_TRACE,
        "entity_key": "orders/7",
        "seq": 3,
        "trace_id": bytes(range(16)).hex(),
        "count": 1,
    }


async def test_a_disconnected_client_does_not_block_ingest() -> None:
    store = FakeStore()
    broadcaster = Broadcaster()
    app = _build(store, broadcaster=broadcaster)

    async with _client(app) as client:
        leaving = await RawStream(app).__aenter__()
        staying = await RawStream(app).__aenter__()
        await client.post("/ingest/traces", content=frame_trace_events([_trace_event(seq=1)]))
        assert (await leaving.next_event())["seq"] == 1

        await leaving.disconnect()
        assert broadcaster.subscribers == 1

        for seq in (2, 3):
            posted = await client.post(
                "/ingest/traces", content=frame_trace_events([_trace_event(seq=seq)])
            )
            assert posted.status_code == 200

        assert [(await staying.next_event())["seq"] for _ in range(3)] == [1, 2, 3]
        await staying.disconnect()

    assert [row.seq for row in store.rows] == [1, 2, 3]


async def test_a_closed_stream_leaves_no_subscriber_behind() -> None:
    broadcaster = Broadcaster()
    app = _build(broadcaster=broadcaster)

    async with RawStream(app):
        assert broadcaster.subscribers == 1

    assert broadcaster.subscribers == 0


# --- static assets ------------------------------------------------------------


async def test_the_read_api_is_reachable_with_a_bundle_mounted(tmp_path: Path) -> None:
    # Scenario: An empty store answers every endpoint. With a UI bundle mounted
    # at `/`, a read route must still be served by the API — not by the SPA
    # fallback. `include_router(..., prefix="/api")` over a router that already
    # carries `/api` put every read route at `/api/api/...`, and because the
    # fallback answers anything unmatched with `index.html` and a 200, the whole
    # read API silently returned HTML instead of 404ing.
    app = _build(static_dir=_bundle(tmp_path / "ui", "bundle"))

    async with _client(app) as client:
        response = await client.get("/api/store")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")


async def test_an_unknown_api_path_404s_rather_than_serving_the_app(tmp_path: Path) -> None:
    # Scenario: An API path that reaches the static mount is a routing mistake.
    # Answering it with the single-page app hides the mistake behind a 200.
    app = _build(static_dir=_bundle(tmp_path / "ui", "bundle"))

    async with _client(app) as client:
        api = await client.get("/api/no-such-endpoint")
        ui = await client.get("/activations/deadbeef/3")

    assert api.status_code == 404
    # A UI route still falls back to the app, which is what the fallback is for.
    assert ui.status_code == 200
    assert "bundle" in ui.text


def _bundle(root: Path, marker: str) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "index.html").write_text(f"<!doctype html><title>{marker}</title>")
    return root


def test_the_static_dir_argument_wins_over_the_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    argument = _bundle(tmp_path / "argument", "argument")
    monkeypatch.setenv(_app.STATIC_DIR_ENV, str(_bundle(tmp_path / "env", "env")))

    assert _app.resolve_static_dir(argument) == argument


def test_the_environment_is_used_when_no_argument_is_given(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = _bundle(tmp_path / "env", "env")
    monkeypatch.setenv(_app.STATIC_DIR_ENV, str(env))

    assert _app.resolve_static_dir() == env


def test_the_packaged_bundle_is_the_last_resort(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    packaged = _bundle(tmp_path / "packaged", "packaged")
    monkeypatch.delenv(_app.STATIC_DIR_ENV, raising=False)
    monkeypatch.setattr(_app, "PACKAGED_STATIC", packaged)

    assert _app.resolve_static_dir() == packaged


def test_a_directory_without_an_index_is_not_a_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    monkeypatch.delenv(_app.STATIC_DIR_ENV, raising=False)

    assert _app.resolve_static_dir(empty) is None


def test_no_bundle_anywhere_resolves_to_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(_app.STATIC_DIR_ENV, raising=False)

    assert _app.resolve_static_dir() is None


async def test_the_ui_bundle_is_served_at_the_root_when_present(tmp_path: Path) -> None:
    static = _bundle(tmp_path / "ui", "console")

    async with _client(_build(static_dir=static)) as client:
        root = await client.get("/")
        health = await client.get("/healthz")

    assert "console" in root.text
    assert health.json()["ui_bundled"] is True


async def test_a_deep_link_falls_back_to_the_single_page_entry(tmp_path: Path) -> None:
    # The UI routes on the path (`wouter`), so `/activations/orders/3` is a real
    # bookmark and must not 404 out of the static mount.
    static = _bundle(tmp_path / "ui", "console")

    async with _client(_build(static_dir=static)) as client:
        response = await client.get("/activations/orders/3")

    assert response.status_code == 200
    assert "console" in response.text


async def test_the_api_still_answers_when_the_bundle_is_mounted(tmp_path: Path) -> None:
    # The mount is registered last precisely so it cannot shadow these.
    static = _bundle(tmp_path / "ui", "console")

    async with _client(_build(static_dir=static)) as client:
        assert (await client.get("/api/overview")).status_code == 200
        assert (await client.get("/healthz")).status_code == 200
        assert (await client.post("/ingest/traces", content=b"")).status_code == 200


async def test_an_api_only_console_names_the_docker_command() -> None:
    async with _client(_build()) as client:
        root = await client.get("/")
        health = await client.get("/healthz")
        api = await client.get("/api/overview")

    # An API-only console is a supported state, not an error.
    assert root.status_code == 200
    assert "docker compose -f docker/compose.console.yaml up" in root.text
    assert health.json()["ui_bundled"] is False
    assert api.status_code == 200


# --- CORS ---------------------------------------------------------------------


async def test_no_cors_headers_without_configured_origins() -> None:
    async with _client(_build()) as client:
        response = await client.get("/healthz", headers={"origin": "http://localhost:5173"})

    assert "access-control-allow-origin" not in response.headers


async def test_a_configured_origin_is_allowed() -> None:
    # The Vite dev server runs on another port; without this a developer
    # iterating on the UI cannot talk to a locally running console.
    async with _client(_build(cors_origins=("http://localhost:5173",))) as client:
        response = await client.get("/healthz", headers={"origin": "http://localhost:5173"})

    assert response.headers["access-control-allow-origin"] == "http://localhost:5173"


# --- options ------------------------------------------------------------------


def test_an_unknown_option_is_named_rather_than_ignored() -> None:
    with pytest.raises(TypeError, match="retention_hourz"):
        _build(retention_hourz=3)


# --- serve --------------------------------------------------------------------


def test_the_service_starts_with_only_a_database_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stores: list[FakeStore] = []
    ran: list[dict[str, Any]] = []

    def recording_store(path: str | Path, *, retention_hours: float | None = None) -> FakeStore:
        stores.append(FakeStore(path, retention_hours=retention_hours))
        return stores[-1]

    monkeypatch.setattr("beam_agents.console._store.ConsoleStore", recording_store)
    monkeypatch.setattr(uvicorn, "run", lambda app, **kwargs: ran.append({"app": app, **kwargs}))

    _app.serve(database=tmp_path / "console.db")

    assert [(store.path, store.retention_hours) for store in stores] == [
        (tmp_path / "console.db", None)
    ]
    # Loopback by default: the console is a trusted-network tool with no auth.
    assert (ran[0]["host"], ran[0]["port"]) == ("127.0.0.1", 8787)
    assert (_app.DEFAULT_HOST, _app.DEFAULT_PORT) == ("127.0.0.1", 8787)


def test_serve_closes_the_store_on_a_clean_shutdown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stores: list[FakeStore] = []

    def recording_store(path: str | Path, *, retention_hours: float | None = None) -> FakeStore:
        stores.append(FakeStore(path, retention_hours=retention_hours))
        return stores[-1]

    monkeypatch.setattr("beam_agents.console._store.ConsoleStore", recording_store)
    monkeypatch.setattr(uvicorn, "run", lambda *args, **kwargs: None)

    _app.serve(database=tmp_path / "console.db", retention_hours=6.0)

    assert [(store.retention_hours, store.closed) for store in stores] == [(6.0, True)]


def test_serve_rejects_an_unknown_option(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("beam_agents.console._store.ConsoleStore", FakeStore)
    monkeypatch.setattr(uvicorn, "run", lambda *args, **kwargs: None)

    with pytest.raises(TypeError, match="not_a_real_option"):
        _app.serve(database=tmp_path / "console.db", not_a_real_option="x")


def test_serve_accepts_the_options_the_cli_hands_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Scenario: The service starts with a database path. `serve` owns the store,
    # so it also owns the lifetime of everything that needs one — the pull
    # sources and the retention sweep. The CLI resolves configuration and
    # constructs nothing, so every flag it parses has to be a keyword `serve`
    # accepts. It previously accepted none of them and raised TypeError.
    monkeypatch.setattr("beam_agents.console._store.ConsoleStore", FakeStore)
    monkeypatch.setattr(uvicorn, "run", lambda *args, **kwargs: None)

    _app.serve(
        database=tmp_path / "console.db",
        host="0.0.0.0",
        port=8787,
        static_dir=None,
        retention_hours=72.0,
        cors_origins=("http://localhost:5173",),
        kafka_traces_from=None,
        kafka_from_beginning=False,
        bigquery_traces_from=None,
        import_traces=None,
        import_snapshot=None,
        log_level="INFO",
    )


# --- the import boundary ------------------------------------------------------


def test_the_http_stack_is_imported_inside_the_functions_that_need_it() -> None:
    # `fastapi`, `sse_starlette` and `uvicorn` are the `console` extra, and
    # `import beam_agents.console` must work without any of them.
    tree = ast.parse(inspect.getsource(_app))
    top_level = [node for node in tree.body if isinstance(node, ast.Import | ast.ImportFrom)]
    roots = {
        (node.module or "").split(".")[0]
        if isinstance(node, ast.ImportFrom)
        else node.names[0].name.split(".")[0]
        for node in top_level
    }

    assert roots.isdisjoint({"fastapi", "uvicorn", "sse_starlette", "starlette"})
