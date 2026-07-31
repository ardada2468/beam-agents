"""Tests for the `console-ingest` capability's decoders and its one normalizer.

Every decoder here reverses an encoder the runtime already ships, so every test
drives the *real* encoder and reverses its output: `frame_trace_events` for the
replay-bundle stream, `trace_event_to_row` for the BigQuery table,
`_event_to_span`/`_encode_batch` for OTLP, `serialize_error_envelope` for the
`.errors` bus form, and `serialize_snapshot` for `.snapshots`. A decoder tested
against a hand-written fixture of what the encoder is assumed to emit proves
nothing about the pairing, which is the only thing these decoders exist for.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from beam_agents._protos import (
    ActivationErrorRecord,
    AgentEnvelope,
    Continuation,
    LlmCacheBlob,
    MemoryBlob,
    StateSnapshot,
    ToolIntent,
    TraceEvent,
)
from beam_agents.console._ingest import (
    TruncatedStreamError,
    _frame_records,
    decode_bigquery_rows,
    decode_error_payload,
    decode_otlp_request,
    decode_snapshot_payload,
    decode_trace_stream,
    normalize,
)
from beam_agents.console._records import (
    PROVENANCE_BIGQUERY,
    PROVENANCE_BUNDLE,
    PROVENANCE_KAFKA,
    PROVENANCE_NATIVE,
    PROVENANCE_OTLP,
)
from beam_agents.core.dofn import (
    REASON_ERROR,
    REASON_HITL_TIMEOUT,
    REASON_TTL_WIPED_SUSPENSION,
    ActivationError,
)
from beam_agents.core.error_records import intent_dead_letter_to_error, serialize_error_envelope
from beam_agents.core.snapshot import build_snapshot, serialize_snapshot
from beam_agents.observability.exporters import trace_event_to_row
from beam_agents.observability.otlp import _encode_batch, _event_to_span
from beam_agents.observability.traces import (
    ACTIVATION_KIND,
    ACTIVATION_STATUS,
    OPERATION_CHAT,
    OPERATION_NAME,
    REQUEST_MODEL,
    USAGE_INPUT_TOKENS,
    USAGE_OUTPUT_TOKENS,
    ActivationTrace,
)
from beam_agents.replay.bundle import frame_trace_events

_ENTITY_KEY = b"entity-1"
_SEQ = 7
_NOW_MS = 1_700_000_000_000


def _activation_events() -> tuple[TraceEvent, ...]:
    """One activation's real trace surface, built by the runtime's own builders."""
    trace = ActivationTrace(entity_key=_ENTITY_KEY, seq=_SEQ, now_ms=_NOW_MS)
    llm_call = trace.stamp(
        TraceEvent(
            entity_key=_ENTITY_KEY,
            seq=_SEQ,
            step_index=1,
            event_type=TraceEvent.LLM_CALL,
            attributes={
                OPERATION_NAME: OPERATION_CHAT,
                REQUEST_MODEL: "m-1",
                USAGE_INPUT_TOKENS: "11",
                USAGE_OUTPUT_TOKENS: "5",
            },
            start_ms=_NOW_MS,
            end_ms=_NOW_MS,
        )
    )
    return (
        trace.activation_start(),
        llm_call,
        trace.tool_call(step_index=1, tool_index=0, tool_name="lookup"),
        trace.intent_emitted(
            step_index=1,
            intent_id="intent-1",
            tool_name="send",
            intent_kind="TOOL",
            expires_at_ms=_NOW_MS + 60_000,
        ),
        trace.activation_end(status="ok", step_index=2),
    )


def _snapshot() -> StateSnapshot:
    """A snapshot of a suspended key, built by the runtime's own builder."""
    memory = MemoryBlob(state_schema_version=1, total_value_bytes=12)
    memory.entries.add(key="a", value=b"12345", last_access_ms=_NOW_MS)
    memory.entries.add(key="b", value=b"1234567", last_access_ms=_NOW_MS)
    cache = LlmCacheBlob(state_schema_version=1)
    cache.entries.add(cache_key="c" * 64, response=b"{}", created_at_ms=_NOW_MS)
    continuation = Continuation(
        state_schema_version=1,
        seq=_SEQ,
        step_index=3,
        pending_intent_ids=["intent-1"],
        adapter="langgraph",
        snapshot=b"checkpoint",
        suspended_at_ms=_NOW_MS,
        deadline_ms=_NOW_MS + 900_000,
    )
    return build_snapshot(
        entity_key=_ENTITY_KEY,
        seq=_SEQ,
        snapshot_at_ms=_NOW_MS,
        request_id="req-1",
        memory_blob=memory,
        cache_blob=cache,
        continuation=continuation,
        pending=[
            ToolIntent(
                intent_id="intent-1",
                entity_key=_ENTITY_KEY,
                seq=_SEQ,
                step_index=1,
                tool_name="send",
                created_at_ms=_NOW_MS,
                expires_at_ms=_NOW_MS + 60_000,
            )
        ],
    )


# --- Requirement: The console imports a captured replay bundle ----------------


def test_a_captured_run_is_inspectable_offline() -> None:
    # Scenario: A captured run is inspectable offline. The stream is exactly
    # what `beam-agents-replay` consumes, so it is decoded by the runtime's own
    # parser rather than by a second reader that could drift from it.
    events = _activation_events()

    decoded = decode_trace_stream(frame_trace_events(events))

    assert decoded == events


def test_a_truncated_stream_reports_what_it_read() -> None:
    # Scenario: A truncated stream reports what it read. A partially-flushed
    # capture is precisely what a crash leaves behind, so the records before the
    # break are still handed back rather than discarded with the tail.
    events = _activation_events()
    payload = frame_trace_events(events)
    complete = len(frame_trace_events(events[:-1]))

    with pytest.raises(TruncatedStreamError) as excinfo:
        decode_trace_stream(payload[: complete + 4])

    assert excinfo.value.records_read == len(events) - 1
    assert excinfo.value.records == events[:-1]


def test_a_stream_truncated_inside_a_length_prefix_reports_what_it_read() -> None:
    # The other way a write can stop: inside the varint framing itself, before
    # any of the record's own bytes were flushed.
    events = _activation_events()
    complete = len(frame_trace_events(events[:-1]))

    with pytest.raises(TruncatedStreamError) as excinfo:
        decode_trace_stream(frame_trace_events(events)[: complete + 1])

    assert excinfo.value.records_read == len(events) - 1
    assert excinfo.value.records == events[:-1]


def test_an_empty_stream_decodes_to_no_records() -> None:
    assert decode_trace_stream(b"") == ()


def test_a_malformed_frame_is_rejected_naming_the_problem() -> None:
    # A complete frame whose body is not a TraceEvent is corruption, not
    # truncation: reporting it as a short read would invite the caller to keep
    # the bytes and try again. The final byte of a framed event closes the
    # `end_ms` varint, so setting its continuation bit corrupts the body while
    # leaving the frame's declared length intact.
    payload = bytearray(frame_trace_events(_activation_events()[:1]))
    payload[-1] = 0xFF

    with pytest.raises(ValueError, match="TraceEvent"):
        decode_trace_stream(bytes(payload))


def test_the_frame_encoder_matches_the_runtimes_own_framing() -> None:
    # The native batch endpoint frames errors and snapshots the same way the
    # replay bundle frames trace events. Asserting byte-identity against
    # `frame_trace_events` is what keeps that claim true: the two framings are
    # one framing, and the splitter below reverses both.
    events = _activation_events()

    assert _frame_records(events) == frame_trace_events(events)


# --- Requirement: The console reads an existing BigQuery trace table ----------


def test_rows_are_reversed_into_the_records_they_encoded() -> None:
    # Scenario: Rows are reversed into the records they encoded.
    events = _activation_events()
    rows = [trace_event_to_row(event) for event in events]

    assert decode_bigquery_rows(rows) == events


def test_re_reading_an_overlapping_window_changes_nothing() -> None:
    # Scenario: Re-reading an overlapping window changes nothing. Decoding is
    # pure, so an overlapping pull produces byte-identical rows and the store's
    # upsert on `(trace_id, span_id, event_type)` collapses them.
    rows = [trace_event_to_row(event) for event in _activation_events()]

    first = normalize(events=decode_bigquery_rows(rows), provenance=PROVENANCE_BIGQUERY)
    second = normalize(events=decode_bigquery_rows(rows + rows[:2]), provenance=PROVENANCE_BIGQUERY)

    assert second.events[: len(first.events)] == first.events
    assert second.events[len(first.events) :] == first.events[:2]


def test_the_derived_event_time_column_is_not_read_back() -> None:
    # `event_time` is `start_ms` re-expressed for BigQuery's partitioning, so
    # reading it would be reading the same number twice — and a table whose
    # partition column was rewritten by a backfill must not shift the record.
    row = trace_event_to_row(_activation_events()[0])
    row["event_time"] = "not-a-timestamp"

    (decoded,) = decode_bigquery_rows([row])

    assert decoded.start_ms == _NOW_MS


def test_a_row_with_an_unrecognized_event_type_is_rejected() -> None:
    # The row encoding carries the enum *name*; a name this package does not
    # know means table/version skew, which is worth a loud failure rather than
    # a silent EVENT_TYPE_UNSPECIFIED that would look like a real record.
    row = trace_event_to_row(_activation_events()[0])
    row["event_type"] = "TELEPORTED"

    with pytest.raises(ValueError, match="TELEPORTED"):
        decode_bigquery_rows([row])


def test_a_row_missing_every_optional_column_still_decodes() -> None:
    # Every column in TRACE_TABLE_SCHEMA is NULLABLE, so a row read back from a
    # table can carry SQL NULLs where the encoder wrote zero-valued defaults.
    (decoded,) = decode_bigquery_rows(
        [
            {
                "trace_id": None,
                "span_id": None,
                "parent_span_id": None,
                "entity_key": None,
                "seq": None,
                "step_index": None,
                "event_type": "ERROR",
                "start_ms": None,
                "end_ms": None,
                "attributes": None,
            }
        ]
    )

    assert decoded == TraceEvent(event_type=TraceEvent.ERROR)


# --- Requirement: The console accepts records over HTTP -----------------------


def test_an_existing_otlp_exporter_reaches_the_console_unchanged() -> None:
    # Scenario: An existing OTLP exporter reaches the console unchanged. The
    # payload is built by the exporter's own mapping and encoder, so this is the
    # request an `otlp://` pipeline actually posts.
    events = _activation_events()
    spans = [span for span in map(_event_to_span, events) if span is not None]

    decoded = decode_otlp_request(_encode_batch(spans, service_name="beam-agents"))

    exported = [event for event in events if event.event_type != TraceEvent.ACTIVATION_START]
    assert [event.trace_id for event in decoded] == [event.trace_id for event in exported]
    assert [event.span_id for event in decoded] == [event.span_id for event in exported]
    assert [event.parent_span_id for event in decoded] == [
        event.parent_span_id for event in exported
    ]
    assert [event.event_type for event in decoded] == [event.event_type for event in exported]
    assert [event.start_ms for event in decoded] == [event.start_ms for event in exported]
    assert [event.end_ms for event in decoded] == [event.end_ms for event in exported]
    assert [dict(event.attributes) for event in decoded] == [
        dict(event.attributes) for event in exported
    ]


def test_otlps_known_loss_is_reported_not_hidden() -> None:
    # Scenario: OTLP's known loss is reported, not hidden. ACTIVATION_START
    # cannot cross the OTLP boundary — it shares a span id with ACTIVATION_END —
    # so the records that do arrive carry the provenance that says so, and
    # `entity_key`/`seq` come back empty because no span attribute carries them.
    events = _activation_events()
    spans = [span for span in map(_event_to_span, events) if span is not None]

    decoded = decode_otlp_request(_encode_batch(spans, service_name="beam-agents"))
    batch = normalize(events=decoded, provenance=PROVENANCE_OTLP)

    assert TraceEvent.ACTIVATION_START not in {event.event_type for event in decoded}
    assert {row.provenance for row in batch.events} == {PROVENANCE_OTLP}
    assert {row.entity_key for row in batch.events} == {""}
    assert {row.seq for row in batch.events} == {0}


def test_an_otlp_span_the_runtime_never_emits_decodes_as_unspecified() -> None:
    # A third-party exporter pointed at the same endpoint sends spans whose
    # names are not this runtime's event vocabulary. Storing them as
    # EVENT_TYPE_UNSPECIFIED keeps them visible; dropping them would make the
    # endpoint silently lossy in a second, undocumented way.
    events = _activation_events()
    span = _event_to_span(events[-1])
    assert span is not None
    span.name = "http_request"

    (decoded,) = decode_otlp_request(_encode_batch([span], service_name="other"))

    assert decoded.event_type == TraceEvent.EVENT_TYPE_UNSPECIFIED


def test_a_malformed_payload_is_rejected_without_affecting_stored_records() -> None:
    # Scenario: A malformed payload is rejected without affecting stored
    # records. Nothing here touches the store, so rejection is total by
    # construction: a decoder either returns every record or none.
    garbage = b"\xff\xfe\xfd\xfc"

    with pytest.raises(ValueError, match="OTLP"):
        decode_otlp_request(garbage)
    with pytest.raises(ValueError, match="error record"):
        decode_error_payload(garbage)
    with pytest.raises(ValueError, match="StateSnapshot"):
        decode_snapshot_payload(garbage)


# --- Requirement: errors arrive in both the bare and the enveloped form -------


def test_the_bus_envelope_form_is_decoded() -> None:
    # `DefaultSinkResolver`'s errors encoding wraps the record in an
    # AgentEnvelope so the errors topic is itself a valid RunAgent input.
    error = ActivationError(
        entity_key=_ENTITY_KEY,
        reason=REASON_ERROR,
        detail="ValueError('boom') failed_at_step=2 after=llm_call",
        event_time_ms=_NOW_MS,
    )
    _, payload = serialize_error_envelope(error)

    (decoded,) = decode_error_payload(payload)

    assert decoded == ActivationErrorRecord(
        entity_key=error.entity_key,
        reason=error.reason,
        detail=error.detail,
        event_time_ms=error.event_time_ms,
    )


def test_the_bare_record_form_is_decoded() -> None:
    # The BigQuery encoding is not enveloped, and the console's own sink writes
    # the record itself; both reach the same endpoint.
    record = ActivationErrorRecord(
        entity_key=_ENTITY_KEY,
        reason=REASON_ERROR,
        detail="ValueError('boom')",
        event_time_ms=_NOW_MS,
    )

    (decoded,) = decode_error_payload(record.SerializeToString(deterministic=True))

    assert decoded == record


def test_a_bare_record_with_no_detail_is_not_mistaken_for_an_envelope() -> None:
    # The two forms overlap on the wire: an ActivationErrorRecord's `detail`
    # occupies the field number an AgentEnvelope uses for `external_event`. A
    # record with no detail is the case where that overlap decodes cleanly as
    # the wrong message, so it is pinned here.
    record = ActivationErrorRecord(
        entity_key=_ENTITY_KEY, reason=REASON_HITL_TIMEOUT, event_time_ms=_NOW_MS
    )

    (decoded,) = decode_error_payload(record.SerializeToString(deterministic=True))

    assert decoded == record


def test_a_batch_of_error_records_is_decoded() -> None:
    # The native ingest endpoint posts a batch, framed exactly as the replay
    # bundle frames trace events.
    records = [
        ActivationErrorRecord(
            entity_key=_ENTITY_KEY, reason=REASON_ERROR, detail=f"e-{index}", event_time_ms=_NOW_MS
        )
        for index in range(3)
    ]

    assert decode_error_payload(_frame_records(records)) == tuple(records)


def test_a_batch_of_error_envelopes_is_decoded() -> None:
    envelopes = []
    for index in range(3):
        _, bus_bytes = serialize_error_envelope(
            ActivationError(
                entity_key=_ENTITY_KEY,
                reason=REASON_ERROR,
                detail=f"e-{index}",
                event_time_ms=_NOW_MS,
            )
        )
        envelope = AgentEnvelope()
        envelope.ParseFromString(bus_bytes)
        envelopes.append(envelope)

    decoded = decode_error_payload(_frame_records(envelopes))

    assert [record.detail for record in decoded] == ["e-0", "e-1", "e-2"]


def test_an_empty_error_payload_decodes_to_no_records() -> None:
    assert decode_error_payload(b"") == ()


def test_a_payload_carrying_no_reason_is_not_an_error_record() -> None:
    # Every dead letter the runtime emits names a reason from the closed
    # vocabulary; a record without one is some other message that happened to
    # parse, and accepting it would put a blank row in the errors view.
    with pytest.raises(ValueError, match="error record"):
        decode_error_payload(ActivationErrorRecord(entity_key=_ENTITY_KEY).SerializeToString())


# --- Requirement: snapshots ---------------------------------------------------


def test_a_snapshot_payload_is_decoded() -> None:
    snapshot = _snapshot()
    _, payload = serialize_snapshot(snapshot)

    assert decode_snapshot_payload(payload) == (snapshot,)


def test_a_batch_of_snapshots_is_decoded() -> None:
    first = _snapshot()
    second = _snapshot()
    second.seq = _SEQ + 1

    assert decode_snapshot_payload(_frame_records([first, second])) == (first, second)


def test_an_empty_snapshot_payload_decodes_to_no_records() -> None:
    assert decode_snapshot_payload(b"") == ()


# --- Requirement: one normalizer behind every source --------------------------


def test_every_route_normalizes_a_record_the_same_way() -> None:
    # Scenario: Records arriving by any endpoint are normalized through the same
    # path, so no field is interpreted differently by delivery route. Only
    # provenance — and the fields OTLP structurally cannot carry — may differ.
    event = _activation_events()[-1]
    native = normalize(events=[event], provenance=PROVENANCE_NATIVE).events[0]
    bundle = normalize(
        events=decode_trace_stream(frame_trace_events([event])), provenance=PROVENANCE_BUNDLE
    ).events[0]
    bigquery = normalize(
        events=decode_bigquery_rows([trace_event_to_row(event)]), provenance=PROVENANCE_BIGQUERY
    ).events[0]
    span = _event_to_span(event)
    assert span is not None
    otlp = normalize(
        events=decode_otlp_request(_encode_batch([span], service_name="beam-agents")),
        provenance=PROVENANCE_OTLP,
    ).events[0]

    assert replace(bundle, provenance=PROVENANCE_NATIVE) == native
    assert replace(bigquery, provenance=PROVENANCE_NATIVE) == native
    assert (otlp.trace_id, otlp.span_id, otlp.event_type) == (
        native.trace_id,
        native.span_id,
        native.event_type,
    )
    assert otlp.attributes == native.attributes


def test_an_event_row_carries_the_identity_tuple_as_hex() -> None:
    event = _activation_events()[-1]

    (row,) = normalize(events=[event], provenance=PROVENANCE_NATIVE).events

    assert row.trace_id == event.trace_id.hex()
    assert row.span_id == event.span_id.hex()
    assert row.parent_span_id == event.parent_span_id.hex()
    assert row.entity_key == _ENTITY_KEY.hex()
    assert row.seq == _SEQ
    assert row.step_index == 2
    assert row.event_type == "ACTIVATION_END"
    assert row.start_ms == _NOW_MS
    assert row.end_ms == _NOW_MS
    assert row.attributes == {ACTIVATION_KIND: "start", ACTIVATION_STATUS: "ok"}


def test_provenance_is_stamped_on_every_row() -> None:
    batch = normalize(
        events=_activation_events(),
        errors=[ActivationErrorRecord(entity_key=_ENTITY_KEY, reason=REASON_ERROR)],
        snapshots=[_snapshot()],
        provenance=PROVENANCE_KAFKA,
    )

    stamped = (
        [row.provenance for row in batch.events]
        + [row.provenance for row in batch.errors]
        + [row.provenance for row in batch.snapshots]
    )
    assert set(stamped) == {PROVENANCE_KAFKA}
    assert len(batch) == len(_activation_events()) + 2


def test_an_unrecognized_provenance_is_rejected() -> None:
    # Provenance is what the UI keys its incomplete-record warning off, so a
    # typo must fail at the decode boundary rather than produce rows nothing
    # knows how to label.
    with pytest.raises(ValueError, match="provenance"):
        normalize(events=_activation_events(), provenance="made-up")


def test_normalizing_nothing_is_an_empty_batch() -> None:
    batch = normalize(provenance=PROVENANCE_NATIVE)

    assert not batch
    assert len(batch) == 0


def test_an_error_seq_is_parsed_from_a_detail_that_carries_it() -> None:
    # `seq` is not a field on the error record: several reasons fire from timer
    # callbacks with no activation. The reasons that *do* know their activation
    # put it in `detail`, in the two shapes `core/dofn.py` writes.
    ttl = ActivationErrorRecord(
        entity_key=_ENTITY_KEY,
        reason=REASON_TTL_WIPED_SUSPENSION,
        detail=f"seq={_SEQ},deadline_ms={_NOW_MS}",
        event_time_ms=_NOW_MS,
    )
    hitl = ActivationErrorRecord(
        entity_key=_ENTITY_KEY,
        reason=REASON_HITL_TIMEOUT,
        detail=f"seq={_SEQ} policy_error=ValueError('x')",
        event_time_ms=_NOW_MS,
    )

    rows = normalize(errors=[ttl, hitl], provenance=PROVENANCE_NATIVE).errors

    assert [row.seq for row in rows] == [_SEQ, _SEQ]


def test_an_intent_dead_letters_seq_is_parsed_from_its_json_detail() -> None:
    # The intent dead letter's detail is JSON, built by the runtime's own
    # encoder — which is what this drives rather than a copy of its shape.
    intent = ToolIntent(
        intent_id="intent-1",
        entity_key=_ENTITY_KEY,
        seq=_SEQ,
        tool_name="send",
        created_at_ms=_NOW_MS,
    )
    error = intent_dead_letter_to_error(((_ENTITY_KEY, intent), "unserializable"))
    record = ActivationErrorRecord(
        entity_key=error.entity_key,
        reason=error.reason,
        detail=error.detail,
        event_time_ms=error.event_time_ms,
    )

    (row,) = normalize(errors=[record], provenance=PROVENANCE_NATIVE).errors

    assert row.seq == _SEQ
    assert row.reason == "intent_dead_letter"


def test_an_error_whose_reason_carries_no_seq_has_none() -> None:
    record = ActivationErrorRecord(
        entity_key=_ENTITY_KEY,
        reason=REASON_ERROR,
        detail="ValueError('seq=99 is bad') failed_at_step=2 after=llm_call",
        event_time_ms=_NOW_MS,
    )

    (row,) = normalize(errors=[record], provenance=PROVENANCE_NATIVE).errors

    assert row.seq is None
    assert row.detail == record.detail
    assert row.entity_key == _ENTITY_KEY.hex()
    assert row.event_time_ms == _NOW_MS


def test_a_snapshot_row_summarizes_what_the_snapshot_holds() -> None:
    snapshot = _snapshot()

    (row,) = normalize(snapshots=[snapshot], provenance=PROVENANCE_NATIVE).snapshots

    assert row.entity_key == _ENTITY_KEY.hex()
    assert row.seq == _SEQ
    assert row.snapshot_at_ms == _NOW_MS
    assert row.request_id == "req-1"
    assert row.memory_entries == 2
    assert row.memory_bytes == 12
    assert row.llm_cache_entries == 1
    assert row.pending_intent_ids == ("intent-1",)
    assert row.continuation_step_index == 3
    assert row.continuation_deadline_ms == _NOW_MS + 900_000
    assert row.continuation_adapter == "langgraph"


def test_a_snapshot_row_keeps_the_bytes_the_replay_cli_needs() -> None:
    # The state image is opaque by contract, so the row keeps it verbatim: what
    # comes back out must be what `beam-agents-replay --snapshot` would read.
    snapshot = _snapshot()
    _, payload = serialize_snapshot(snapshot)

    (row,) = normalize(snapshots=[snapshot], provenance=PROVENANCE_NATIVE).snapshots

    assert row.raw == payload


def test_a_snapshot_of_an_unsuspended_key_has_no_continuation_columns() -> None:
    # Absence of the continuation is what distinguishes "not suspended" from
    # "suspended at step 0", so it must not be flattened into a zero.
    snapshot = build_snapshot(
        entity_key=_ENTITY_KEY,
        seq=_SEQ,
        snapshot_at_ms=_NOW_MS,
        request_id="",
        memory_blob=None,
        cache_blob=None,
        continuation=None,
        pending=[],
    )

    (row,) = normalize(snapshots=[snapshot], provenance=PROVENANCE_NATIVE).snapshots

    assert row.continuation_step_index is None
    assert row.continuation_deadline_ms is None
    assert row.continuation_adapter == ""
    assert row.pending_intent_ids == ()
