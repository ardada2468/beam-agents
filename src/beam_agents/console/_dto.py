"""The console's wire shapes: what the API returns and the UI renders.

Written in full up front rather than grown alongside the routes, because this is
the contract between the Python API and the TypeScript client, and the two are
built independently. ``frontend/src/lib/api-types.ts`` mirrors this file field
for field; a field that exists on one side and not the other is a build error on
the TypeScript side rather than a runtime blank.

Two conventions run through every model:

**Milliseconds, always, and always ``*_ms``.** The runtime's protos are int64
unix-epoch milliseconds throughout and nothing here re-expresses them as
datetimes — an ISO string would be a second representation of the same number,
and the two would disagree the first time a timezone got involved.

**A missing measurement is ``None``, never ``0``.** ``usage_attributes`` in
``observability/traces.py`` already omits token counts it does not know rather
than writing zero, because anything summing them would read a real zero-token
call. The same rule holds all the way to the screen: ``None`` renders as "not
recorded", and ``0`` means the runtime measured zero.

Importing this module has no side effects.
"""

from __future__ import annotations

from typing import Any, Generic, Literal, TypeVar

from pydantic import BaseModel, Field

# PEP 695 `class Page[T]` is 3.12+, and the project supports 3.11.
T = TypeVar("T")

__all__ = [
    "ActivationDetail",
    "ActivationSummary",
    "ApprovalSummary",
    "AttemptSummary",
    "BucketPoint",
    "EntitySummary",
    "ErrorGroup",
    "ErrorRecord",
    "EventRecord",
    "Health",
    "IntentSummary",
    "ModelSummary",
    "Overview",
    "Page",
    "SearchHit",
    "SnapshotSummary",
    "SpanNode",
    "StoreStatus",
    "ToolSummary",
    "TraceDetail",
    "TraceSummary",
]

ActivationStatus = Literal["completed", "suspended", "error", "in_flight"]
ActivationKind = Literal["start", "resume", "unknown"]
Provenance = Literal["native", "otlp", "kafka", "bigquery", "bundle"]


class Page(BaseModel, Generic[T]):
    """One page of a keyset-paginated list.

    ``next_cursor`` is opaque and resumes the same total order. It is ``None``
    exactly when the list is exhausted — not when a page happens to come back
    short, which can occur when a filter rejects most of a scanned range.
    """

    items: list[T]
    next_cursor: str | None = None
    total: int | None = Field(
        default=None,
        description="Exact count when cheap to compute, otherwise null. Never an estimate.",
    )


class EventRecord(BaseModel):
    """One trace event, with its complete attribute map.

    ``start_ms`` and ``end_ms`` are equal for every event this runtime produces:
    spans are zero-width by design so the hot path never reads a wall clock.
    Their difference is not a duration and must not be rendered as one.
    """

    trace_id: str
    span_id: str
    parent_span_id: str
    entity_key: str
    seq: int
    step_index: int
    event_type: str
    start_ms: int
    end_ms: int
    attributes: dict[str, str] = Field(default_factory=dict)
    provenance: Provenance = "native"


class SpanNode(BaseModel):
    """A node in an activation's span tree.

    ``depth`` and ``order`` carry the structure the UI draws. There is
    deliberately no width or duration field: the runtime does not measure one,
    and inventing it here would put the fabrication in the API rather than only
    on the screen.
    """

    span_id: str
    parent_span_id: str
    role: str
    step_index: int
    depth: int
    order: int
    events: list[EventRecord] = Field(default_factory=list)


class AttemptSummary(BaseModel):
    """One attempt within an activation.

    A suspend and its resume are one activation with two attempts, because trace
    identity is scoped to ``(entity_key, seq)`` and a resume recomputes the same
    trace ID. ``end_ms`` is ``None`` while the attempt is still open.
    """

    span_id: str
    kind: ActivationKind
    entry_step_index: int
    start_ms: int
    end_ms: int | None = None
    status: ActivationStatus = "in_flight"


class IntentSummary(BaseModel):
    """A tool intent staged by an activation."""

    intent_id: str
    tool_name: str
    intent_kind: str
    step_index: int
    expires_at_ms: int | None = None
    emitted_at_ms: int


class ErrorRecord(BaseModel):
    """One activation error.

    ``failure_*`` are the position scalars ``add-failure-context`` computes for
    the routes that can reach a context; they are ``None`` on the routes that
    cannot, which is a real distinction and not a default.
    """

    entity_key: str
    seq: int | None = None
    reason: str
    detail: str
    error_type: str | None = None
    event_time_ms: int
    failure_step: int | None = None
    failure_last_event: str | None = None
    failure_staged_intents: int | None = None
    failure_llm_calls: int | None = None


class ActivationSummary(BaseModel):
    """An activation's derived rollup — the primary list object.

    Every field is recomputed from the activation's events on each write, so it
    is correct after any subset has arrived. ``status`` is ``in_flight`` when no
    ``ACTIVATION_END`` has been seen, which is honest about a partial arrival
    rather than guessing a terminal state.

    ``wall_ms`` is the only duration here, and it is real: the gap between the
    ``ACTIVATION_START`` and ``ACTIVATION_END`` clock reads. It is ``None``
    while an activation is in flight.
    """

    entity_key: str
    seq: int
    trace_id: str
    status: ActivationStatus
    kind: ActivationKind
    attempts: int = 1
    started_ms: int
    ended_ms: int | None = None
    wall_ms: int | None = None
    model: str | None = None
    llm_calls: int = 0
    tool_calls: int = 0
    intents: int = 0
    errors: int = 0
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    cache_hits: int = 0
    tools: list[str] = Field(default_factory=list)
    reasons: list[str] = Field(default_factory=list)
    provenance: list[Provenance] = Field(default_factory=list)
    complete_provenance: bool = Field(
        default=True,
        description=(
            "False when this activation was assembled only from lossy sources "
            "(OTLP carries no ACTIVATION_START), so start-vs-resume is unknown."
        ),
    )


class SnapshotSummary(BaseModel):
    """A state snapshot's countable metadata.

    The state image itself is opaque by contract, so it stays as bytes behind a
    separate endpoint and only what can be counted is surfaced here.
    """

    entity_key: str
    seq: int
    snapshot_at_ms: int
    state_schema_version: int
    request_id: str = ""
    memory_entries: int = 0
    memory_bytes: int = 0
    llm_cache_entries: int = 0
    pending_intent_ids: list[str] = Field(default_factory=list)
    continuation_step_index: int | None = None
    continuation_deadline_ms: int | None = None
    continuation_adapter: str = ""


class ActivationDetail(BaseModel):
    """Everything recorded about one activation."""

    summary: ActivationSummary
    attempts: list[AttemptSummary] = Field(default_factory=list)
    spans: list[SpanNode] = Field(default_factory=list)
    events: list[EventRecord] = Field(default_factory=list)
    intents: list[IntentSummary] = Field(default_factory=list)
    errors: list[ErrorRecord] = Field(default_factory=list)
    snapshot: SnapshotSummary | None = None
    replay_command: str | None = Field(
        default=None,
        description="A copy-ready `beam-agents-replay` invocation for this activation.",
    )


class TraceSummary(BaseModel):
    """A trace, which is exactly one activation scope."""

    trace_id: str
    entity_key: str
    seq: int
    events: int
    spans: int
    started_ms: int
    ended_ms: int | None = None
    status: ActivationStatus


class TraceDetail(BaseModel):
    """A trace with its assembled span tree."""

    summary: TraceSummary
    roots: list[SpanNode] = Field(default_factory=list)
    attempts: list[AttemptSummary] = Field(default_factory=list)


class BucketPoint(BaseModel):
    """One point in a time-bucketed series.

    ``bucket_ms`` is the bucket's inclusive start. Buckets are contiguous with
    explicit zeros, so a gap in the data is visibly a gap rather than a line
    interpolated across missing time.
    """

    bucket_ms: int
    value: float


class ModelSummary(BaseModel):
    """Per-model usage.

    ``cache_hit_ratio`` is ``None`` rather than ``0.0`` when no call recorded a
    cache-hit attribute at all — "never cached" and "not measured" are different
    answers.
    """

    model: str
    calls: int
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    cache_hits: int = 0
    cache_hit_ratio: float | None = None
    errors: int = 0
    max_attempts: int | None = None
    circuit_states: dict[str, int] = Field(default_factory=dict)


class ToolSummary(BaseModel):
    """Per-tool activity, across both inline calls and staged intents."""

    tool_name: str
    calls: int = 0
    intents: int = 0
    errors: int = 0
    failure_ratio: float | None = None
    last_seen_ms: int | None = None


class ApprovalSummary(BaseModel):
    """A human-approval intent and whatever is known about its resolution."""

    intent_id: str
    entity_key: str
    seq: int
    tool_name: str
    step_index: int
    requested_ms: int
    deadline_ms: int | None = None
    expires_at_ms: int | None = None
    escalations: int = 0
    decision: Literal["approved", "denied", "expired", "pending"] = "pending"
    decided_ms: int | None = None


class EntitySummary(BaseModel):
    """One entity key's activity across every sequence number it has run."""

    entity_key: str
    activations: int
    first_seen_ms: int
    last_seen_ms: int
    errors: int = 0
    total_tokens: int | None = None
    latest_seq: int | None = None
    latest_status: ActivationStatus | None = None


class ErrorGroup(BaseModel):
    """Errors sharing a reason and error type."""

    reason: str
    error_type: str | None = None
    count: int
    entities: int
    first_seen_ms: int
    last_seen_ms: int
    series: list[BucketPoint] = Field(default_factory=list)
    sample_detail: str = ""


class SearchHit(BaseModel):
    """One attribute-search result, pointing at where it was found."""

    kind: Literal["activation", "event", "error", "entity"]
    entity_key: str
    seq: int | None = None
    trace_id: str | None = None
    span_id: str | None = None
    label: str
    matched_field: str
    matched_value: str
    at_ms: int


class StoreStatus(BaseModel):
    """What the store currently holds, and the window it holds it for."""

    row_counts: dict[str, int] = Field(default_factory=dict)
    retention_hours: float | None = None
    database_path: str = ""
    database_bytes: int | None = None
    oldest_record_ms: int | None = None
    newest_record_ms: int | None = None
    schema_version: int = 0


class Overview(BaseModel):
    """The headline figures and series the landing page renders.

    Everything here is derived from stored trace attributes, never from Beam's
    own metrics: those carry no labels and count *attempted* rather than
    committed work, so they disagree with these numbers by construction under
    retry.
    """

    window_ms: int
    activations: int
    completed: int
    suspended: int
    in_flight: int
    errors: int
    error_ratio: float | None = None
    total_tokens: int | None = None
    llm_calls: int = 0
    tool_calls: int = 0
    cache_hit_ratio: float | None = None
    p50_wall_ms: int | None = None
    p95_wall_ms: int | None = None
    activation_series: list[BucketPoint] = Field(default_factory=list)
    error_series: list[BucketPoint] = Field(default_factory=list)
    token_series: list[BucketPoint] = Field(default_factory=list)
    top_models: list[ModelSummary] = Field(default_factory=list)
    top_tools: list[ToolSummary] = Field(default_factory=list)
    recent_errors: list[ErrorRecord] = Field(default_factory=list)
    store: StoreStatus | None = None


class Health(BaseModel):
    """Liveness, answered before any record has been ingested."""

    status: Literal["ok"] = "ok"
    version: str
    schema_version: int
    ui_bundled: bool = False
    sources: list[str] = Field(default_factory=list)


def model_json_schemas() -> dict[str, Any]:
    """Return every exported model's JSON Schema, keyed by name.

    Used to check the TypeScript mirror against this file rather than trusting
    the two to be edited together.
    """
    exported: dict[str, Any] = {}
    for name in __all__:
        model = globals()[name]
        if isinstance(model, type) and issubclass(model, BaseModel):
            exported[name] = model.model_json_schema()
    return exported
