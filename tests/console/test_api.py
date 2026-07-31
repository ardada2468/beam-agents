"""Tests for the console's read API: the route table over `_queries`.

The router is deliberately thin, so these tests are about the seam rather than
about any answer: that every path and query-parameter name matches
`frontend/src/lib/api.ts` exactly, that request parameters reach the query layer
unaltered, that `limit` cannot escape its ceiling, that a missing record is a
404 with a problem-shaped body rather than a 200 carrying null, and that the
`_dto` models survive serialization with `None` still meaning "not measured".

The query layer is faked throughout. It is built independently (and, in this
tree, still raises `NotImplementedError`), and a route test that needed a
populated SQLite file would be testing the queries instead of the routes. The
fakes return real `_dto` models, so serialization is still exercised end to end.

The app is driven through `httpx.ASGITransport`: no socket, no server process,
no port to collide with a developer's own console.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import httpx
import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute

from beam_agents.console import _queries
from beam_agents.console._api import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, build_router
from beam_agents.console._dto import (
    ActivationDetail,
    ActivationSummary,
    ApprovalSummary,
    BucketPoint,
    EntitySummary,
    ErrorGroup,
    ErrorRecord,
    ModelSummary,
    Overview,
    Page,
    SearchHit,
    StoreStatus,
    ToolSummary,
    TraceDetail,
    TraceSummary,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from fastapi import APIRouter

    from beam_agents.console._store import ConsoleStore

# The router never touches the store; it hands it to a query function. A
# sentinel proves that, and proves the *same* object is handed over each time.
_STORE = cast("ConsoleStore", object())

# The route table, transcribed from `frontend/src/lib/api.ts`. Six frontend
# units call these paths; a rename here breaks all of them at once, which is
# why it is asserted as a set rather than incidentally by the other tests.
_CLIENT_ROUTES = {
    "/api/overview",
    "/api/activations",
    "/api/activations/{entity_key}/{seq}",
    "/api/traces",
    "/api/traces/{trace_id}",
    "/api/errors",
    "/api/errors/groups",
    "/api/models",
    "/api/tools",
    "/api/approvals",
    "/api/entities",
    "/api/search",
    "/api/store",
}

# Every endpoint that answers with a collection or an aggregate. The two detail
# endpoints are excluded on purpose: an empty store has no activation to ask
# for, so their empty-store behaviour is a 404 and is asserted separately.
_COLLECTION_PATHS = [
    "/api/overview",
    "/api/activations",
    "/api/traces",
    "/api/errors",
    "/api/errors/groups",
    "/api/models",
    "/api/tools",
    "/api/approvals",
    "/api/entities",
    "/api/search",
    "/api/store",
]


class _Recorder:
    """Captures the keyword arguments each faked query function was called with."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def record(self, name: str, **kwargs: Any) -> None:
        self.calls.append((name, kwargs))

    def kwargs(self, name: str) -> dict[str, Any]:
        for called, kwargs in self.calls:
            if called == name:
                return kwargs
        raise AssertionError(f"{name} was never called; recorded {[c for c, _ in self.calls]}")


class _FakeFilter:
    """Stands in for `_queries.ActivationFilter`, which is another unit's work."""

    def __init__(self, **fields: Any) -> None:
        self.fields = fields


def _summary(entity_key: str = "agent-1", seq: int = 7) -> ActivationSummary:
    return ActivationSummary(
        entity_key=entity_key,
        seq=seq,
        trace_id="a" * 32,
        status="completed",
        kind="start",
        started_ms=1_700_000_000_000,
    )


def _empty_results() -> dict[str, Any]:
    """The value each faked query returns for a store holding nothing."""
    return {
        "overview": Overview(
            window_ms=86_400_000,
            activations=0,
            completed=0,
            suspended=0,
            in_flight=0,
            errors=0,
        ),
        "activations": Page[ActivationSummary](items=[]),
        "activation_detail": None,
        "traces": Page[TraceSummary](items=[]),
        "trace_detail": None,
        "errors": Page[ErrorRecord](items=[]),
        "error_groups": [],
        "models": [],
        "tools": [],
        "approvals": [],
        "entities": Page[EntitySummary](items=[]),
        "search": [],
        "store_status": StoreStatus(),
    }


def _stub(recorder: _Recorder, name: str, result: Any) -> Callable[..., Any]:
    def call(store: Any, **kwargs: Any) -> Any:
        recorder.record(name, store=store, **kwargs)
        return result

    return call


@pytest.fixture
def recorder(monkeypatch: pytest.MonkeyPatch) -> _Recorder:
    """Replace every query function with a recording fake over an empty store."""
    rec = _Recorder()
    for name, result in _empty_results().items():
        monkeypatch.setattr(_queries, name, _stub(rec, name, result))
    monkeypatch.setattr(_queries, "ActivationFilter", _FakeFilter)
    return rec


def _app(router: APIRouter) -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    return app


@pytest.fixture
async def client(recorder: _Recorder) -> AsyncIterator[httpx.AsyncClient]:
    """An ASGI-transport client over an app carrying only the read router."""
    transport = httpx.ASGITransport(app=_app(build_router(_STORE)))
    async with httpx.AsyncClient(transport=transport, base_url="http://console") as http:
        yield http


async def test_the_route_table_matches_the_frontend_client() -> None:
    router = build_router(_STORE)
    # `path_format` rather than `path`: it is the parameter-name-only form the
    # OpenAPI document and the client both speak, with any Starlette convertor
    # (`:path`) already stripped.
    observed = {route.path_format for route in router.routes if isinstance(route, APIRoute)}
    assert observed == _CLIENT_ROUTES


async def test_an_empty_store_answers_every_endpoint(client: httpx.AsyncClient) -> None:
    for path in _COLLECTION_PATHS:
        response = await client.get(path)
        assert response.status_code == 200, f"{path} -> {response.status_code}"
        assert response.headers["content-type"].startswith("application/json")
        # Well-formed empty: a page envelope, an empty list, or an object of
        # zeroes — never an error, and never a bare `null`. The page envelopes
        # are asserted exactly in the next test.
        assert response.json() is not None


async def test_an_empty_store_answers_the_paged_endpoints_with_a_full_envelope(
    client: httpx.AsyncClient,
) -> None:
    for path in ("/api/activations", "/api/traces", "/api/errors", "/api/entities"):
        body = (await client.get(path)).json()
        assert body == {"items": [], "next_cursor": None, "total": None}


async def test_filters_narrow_the_activation_list(
    client: httpx.AsyncClient, recorder: _Recorder
) -> None:
    response = await client.get(
        "/api/activations",
        params={
            "entity_key": "agent-1",
            "status": "error",
            "kind": "resume",
            "model": "gpt-4o",
            "tool": "search",
            "reason": "activation_timeout",
            "since_ms": 1_700_000_000_000,
            "until_ms": 1_700_000_600_000,
            "query": "needle",
            "cursor": "opaque-1",
            "limit": 25,
        },
    )
    assert response.status_code == 200

    call = recorder.kwargs("activations")
    assert call["store"] is _STORE
    assert call["cursor"] == "opaque-1"
    assert call["limit"] == 25
    assert isinstance(call["filters"], _FakeFilter)
    assert call["filters"].fields == {
        "entity_key": "agent-1",
        "status": "error",
        "kind": "resume",
        "model": "gpt-4o",
        "tool": "search",
        "reason": "activation_timeout",
        "since_ms": 1_700_000_000_000,
        "until_ms": 1_700_000_600_000,
        "query": "needle",
    }


async def test_an_unfiltered_activation_list_constrains_nothing(
    client: httpx.AsyncClient, recorder: _Recorder
) -> None:
    await client.get("/api/activations")
    call = recorder.kwargs("activations")
    assert set(call["filters"].fields.values()) == {None}
    assert call["cursor"] is None
    assert call["limit"] == DEFAULT_PAGE_SIZE


async def test_a_cursor_resumes_the_same_ordering(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, recorder: _Recorder
) -> None:
    page = Page[ActivationSummary](items=[_summary()], next_cursor="cursor-2", total=3)
    monkeypatch.setattr(_queries, "activations", _stub(recorder, "activations", page))

    first = (await client.get("/api/activations")).json()
    assert first["next_cursor"] == "cursor-2"
    assert first["total"] == 3

    await client.get("/api/activations", params={"cursor": first["next_cursor"]})
    assert recorder.calls[-1][1]["cursor"] == "cursor-2"


async def test_limit_is_clamped_to_the_page_size_ceiling(
    client: httpx.AsyncClient, recorder: _Recorder
) -> None:
    paged = {
        "/api/activations": "activations",
        "/api/traces": "traces",
        "/api/errors": "errors",
        "/api/entities": "entities",
        "/api/approvals": "approvals",
    }
    for path, query in paged.items():
        response = await client.get(path, params={"limit": 100_000})
        assert response.status_code == 200
        assert recorder.kwargs(query)["limit"] == MAX_PAGE_SIZE


async def test_a_limit_below_one_is_clamped_up_rather_than_rejected(
    client: httpx.AsyncClient, recorder: _Recorder
) -> None:
    response = await client.get("/api/activations", params={"limit": 0})
    assert response.status_code == 200
    assert recorder.kwargs("activations")["limit"] == 1


async def test_the_page_size_ceiling_is_configurable(recorder: _Recorder) -> None:
    transport = httpx.ASGITransport(app=_app(build_router(_STORE, max_page_size=10)))
    async with httpx.AsyncClient(transport=transport, base_url="http://console") as http:
        await http.get("/api/activations", params={"limit": 999})
    assert recorder.kwargs("activations")["limit"] == 10


async def test_unrelated_options_do_not_reach_the_router(recorder: _Recorder) -> None:
    # `_app.create_app` forwards one options bag to several builders; keys meant
    # for another builder must not fail this one.
    router = build_router(_STORE, retention_hours=6.0, broadcaster=object())
    transport = httpx.ASGITransport(app=_app(router))
    async with httpx.AsyncClient(transport=transport, base_url="http://console") as http:
        assert (await http.get("/api/store")).status_code == 200
    assert recorder.kwargs("store_status")["store"] is _STORE


async def test_a_missing_activation_is_a_problem_shaped_404(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/activations/agent-1/7")
    assert response.status_code == 404
    detail = response.json()["detail"]
    assert detail["status"] == 404
    assert detail["resource"] == "activation"
    assert detail["entity_key"] == "agent-1"
    assert detail["seq"] == 7
    assert detail["title"]


async def test_a_missing_trace_is_a_problem_shaped_404(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/traces/deadbeef")
    assert response.status_code == 404
    detail = response.json()["detail"]
    assert detail["status"] == 404
    assert detail["resource"] == "trace"
    assert detail["trace_id"] == "deadbeef"


async def test_a_present_activation_is_served_whole(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, recorder: _Recorder
) -> None:
    detail = ActivationDetail(
        summary=_summary(entity_key="agent/1", seq=7),
        replay_command="beam-agents-replay --entity-key agent/1 --seq 7",
    )
    monkeypatch.setattr(_queries, "activation_detail", _stub(recorder, "activation_detail", detail))

    # The client percent-encodes the entity key, which is why it may contain
    # characters the path grammar would otherwise split on.
    response = await client.get("/api/activations/agent%2F1/7")
    assert response.status_code == 200
    body = response.json()
    assert body["summary"]["entity_key"] == "agent/1"
    assert body["replay_command"].endswith("--seq 7")
    call = recorder.kwargs("activation_detail")
    assert call == {"store": _STORE, "entity_key": "agent/1", "seq": 7}


async def test_a_present_trace_is_served_with_its_span_tree(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, recorder: _Recorder
) -> None:
    detail = TraceDetail(
        summary=TraceSummary(
            trace_id="b" * 32,
            entity_key="agent-1",
            seq=7,
            events=4,
            spans=2,
            started_ms=1_700_000_000_000,
            status="in_flight",
        )
    )
    monkeypatch.setattr(_queries, "trace_detail", _stub(recorder, "trace_detail", detail))

    body = (await client.get(f"/api/traces/{'b' * 32}")).json()
    assert body["summary"]["status"] == "in_flight"
    assert body["roots"] == []
    assert recorder.kwargs("trace_detail")["trace_id"] == "b" * 32


async def test_a_non_integer_sequence_number_is_rejected(client: httpx.AsyncClient) -> None:
    assert (await client.get("/api/activations/agent-1/not-a-seq")).status_code == 422


async def test_the_overview_window_and_bucket_count_reach_the_query_layer(
    client: httpx.AsyncClient, recorder: _Recorder
) -> None:
    await client.get("/api/overview", params={"window_ms": 3_600_000, "buckets": 12})
    call = recorder.kwargs("overview")
    assert call["window_ms"] == 3_600_000
    assert call["buckets"] == 12


async def test_an_unbounded_bucket_count_is_clamped(
    client: httpx.AsyncClient, recorder: _Recorder
) -> None:
    response = await client.get(
        "/api/overview", params={"window_ms": 60_000, "buckets": 10_000_000}
    )
    assert response.status_code == 200
    assert recorder.kwargs("overview")["buckets"] <= 1024


async def test_the_error_endpoints_carry_the_clients_parameter_names(
    client: httpx.AsyncClient, recorder: _Recorder
) -> None:
    await client.get(
        "/api/errors",
        params={"reason": "hitl_timeout", "entity_key": "agent-1", "since_ms": 1_700_000_000_000},
    )
    assert recorder.kwargs("errors") == {
        "store": _STORE,
        "reason": "hitl_timeout",
        "entity_key": "agent-1",
        "since_ms": 1_700_000_000_000,
        "cursor": None,
        "limit": DEFAULT_PAGE_SIZE,
    }

    await client.get("/api/errors/groups", params={"since_ms": 1_700_000_000_000, "bucket_ms": 6})
    assert recorder.kwargs("error_groups") == {
        "store": _STORE,
        "since_ms": 1_700_000_000_000,
        "bucket_ms": 6,
    }


async def test_the_breakdown_endpoints_carry_the_clients_parameter_names(
    client: httpx.AsyncClient, recorder: _Recorder
) -> None:
    await client.get("/api/models", params={"since_ms": 11})
    await client.get("/api/tools", params={"since_ms": 12})
    await client.get("/api/approvals", params={"pending_only": "true", "limit": 100})
    await client.get("/api/search", params={"q": "needle", "limit": 20})

    assert recorder.kwargs("models") == {"store": _STORE, "since_ms": 11}
    assert recorder.kwargs("tools") == {"store": _STORE, "since_ms": 12}
    assert recorder.kwargs("approvals") == {"store": _STORE, "pending_only": True, "limit": 100}
    assert recorder.kwargs("search") == {"store": _STORE, "query": "needle", "limit": 20}


async def test_a_missing_measurement_serializes_as_null_not_zero(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, recorder: _Recorder
) -> None:
    models = [ModelSummary(model="gpt-4o", calls=3, prompt_tokens=None, cache_hit_ratio=None)]
    monkeypatch.setattr(_queries, "models", _stub(recorder, "models", models))

    body = (await client.get("/api/models")).json()
    assert body[0]["calls"] == 3
    assert body[0]["prompt_tokens"] is None
    assert body[0]["cache_hit_ratio"] is None


async def test_the_populated_aggregates_serialize_through_their_models(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, recorder: _Recorder
) -> None:
    overview = Overview(
        window_ms=86_400_000,
        activations=2,
        completed=1,
        suspended=0,
        in_flight=1,
        errors=1,
        error_ratio=0.5,
        activation_series=[BucketPoint(bucket_ms=1_700_000_000_000, value=2.0)],
        top_tools=[ToolSummary(tool_name="search", calls=4)],
        recent_errors=[
            ErrorRecord(
                entity_key="agent-1",
                seq=7,
                reason="activation_timeout",
                detail="deadline",
                event_time_ms=1_700_000_000_000,
            )
        ],
        store=StoreStatus(row_counts={"events": 9}, schema_version=1),
    )
    monkeypatch.setattr(_queries, "overview", _stub(recorder, "overview", overview))

    body = (await client.get("/api/overview")).json()
    assert body["activation_series"] == [{"bucket_ms": 1_700_000_000_000, "value": 2.0}]
    assert body["recent_errors"][0]["reason"] == "activation_timeout"
    assert body["recent_errors"][0]["failure_step"] is None
    assert body["store"]["row_counts"] == {"events": 9}
    assert body["p95_wall_ms"] is None


async def test_the_list_endpoints_serialize_their_element_models(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch, recorder: _Recorder
) -> None:
    monkeypatch.setattr(
        _queries,
        "error_groups",
        _stub(
            recorder,
            "error_groups",
            [
                ErrorGroup(
                    reason="hitl_timeout",
                    count=2,
                    entities=1,
                    first_seen_ms=1,
                    last_seen_ms=2,
                    series=[BucketPoint(bucket_ms=1, value=2.0)],
                )
            ],
        ),
    )
    monkeypatch.setattr(
        _queries,
        "approvals",
        _stub(
            recorder,
            "approvals",
            [
                ApprovalSummary(
                    intent_id="i-1",
                    entity_key="agent-1",
                    seq=7,
                    tool_name="refund",
                    step_index=2,
                    requested_ms=1_700_000_000_000,
                )
            ],
        ),
    )
    monkeypatch.setattr(
        _queries,
        "search",
        _stub(
            recorder,
            "search",
            [
                SearchHit(
                    kind="event",
                    entity_key="agent-1",
                    label="LLM_CALL",
                    matched_field="gen_ai.request.model",
                    matched_value="gpt-4o",
                    at_ms=1_700_000_000_000,
                )
            ],
        ),
    )
    monkeypatch.setattr(
        _queries,
        "entities",
        _stub(
            recorder,
            "entities",
            Page[EntitySummary](
                items=[
                    EntitySummary(
                        entity_key="agent-1", activations=3, first_seen_ms=1, last_seen_ms=2
                    )
                ]
            ),
        ),
    )

    assert (await client.get("/api/errors/groups")).json()[0]["error_type"] is None
    assert (await client.get("/api/approvals")).json()[0]["decision"] == "pending"
    assert (await client.get("/api/search", params={"q": "gpt"})).json()[0]["kind"] == "event"
    assert (await client.get("/api/entities")).json()["items"][0]["errors"] == 0


async def test_no_read_route_accepts_a_write_method(client: httpx.AsyncClient) -> None:
    # Read-only with respect to agent state: there is no route here that could
    # write to a running pipeline, and the router must not acquire one.
    for path in (*_COLLECTION_PATHS, "/api/activations/agent-1/7", "/api/traces/deadbeef"):
        for method in ("POST", "PUT", "PATCH", "DELETE"):
            response = await client.request(method, path)
            assert response.status_code == 405, f"{method} {path} -> {response.status_code}"
