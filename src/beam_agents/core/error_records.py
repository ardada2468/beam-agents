"""Encoders that make ``.errors`` deliverable to a configured sink.

``.errors`` is a ``PCollection[ActivationError]`` — a dataclass, not a proto —
and none of the write transforms ``DefaultSinkResolver`` resolves accept one:
Kafka and Pub/Sub want bytes, BigQuery wants a row mapping. These are the two
encodings, kept beside the dead-letter vocabulary rather than in the transform
so the sink wiring stays a one-line ``beam.Map``, exactly as
``observability/exporters.py`` does for ``.traces`` (design D9).

The message-bus encoding wraps the record in an :class:`AgentEnvelope`, with
the serialized :class:`ActivationErrorRecord` as the envelope's opaque
``external_event``. That makes the errors topic a valid ``RunAgent`` input
stream: a downstream pipeline keys it by ``entity_key`` and consumes it like
any other event stream, with no adapter (see ``docs/errors.md``). The wrapping
is a convention of this sink, not a constraint on ``AgentEnvelope`` — the
runtime imposes no schema on ``external_event`` bytes.

Both encodings are deterministic. Dead letters are emitted at-least-once — a
retried bundle re-emits an identical ``ActivationError``, whose
``event_time_ms`` is replay-deterministic by construction — and downstream
dedup only collapses the copies if the encoding is byte-stable too.

Importing this module has no side effects.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from beam_agents._protos import ActivationErrorRecord, AgentEnvelope
from beam_agents.core.dofn import REASON_INTENT_DEAD_LETTER, ActivationError

if TYPE_CHECKING:
    from beam_agents._protos import ToolIntent

__all__ = [
    "activation_error_to_row",
    "intent_dead_letter_to_error",
    "serialize_error_envelope",
]


def _record(error: ActivationError) -> ActivationErrorRecord:
    return ActivationErrorRecord(
        entity_key=error.entity_key,
        reason=error.reason,
        detail=error.detail,
        event_time_ms=error.event_time_ms,
    )


def serialize_error_envelope(error: ActivationError) -> tuple[bytes, bytes]:
    """Encode for a message-bus sink: ``(entity_key, deterministic envelope bytes)``.

    Keyed by ``entity_key`` so one key's dead letters keep their relative order
    through a single partition — the same reason ``WriteIntents`` and
    ``serialize_trace_event`` key their writes.
    """
    envelope = AgentEnvelope(
        entity_key=error.entity_key,
        event_time_ms=error.event_time_ms,
        external_event=_record(error).SerializeToString(deterministic=True),
    )
    return error.entity_key, envelope.SerializeToString(deterministic=True)


def intent_dead_letter_to_error(
    element: tuple[tuple[bytes, ToolIntent], str],
) -> ActivationError:
    """Map a ``WriteIntents`` dead letter onto the shared dead-letter shape.

    ``.dead_letter`` elements are ``((entity_key, ToolIntent), reason)``.
    Mapping them to :class:`ActivationError` is what lets one encoder — and so
    one record schema, for every scheme — cover both error streams; a consumer
    tells them apart by ``reason`` alone.

    The intent's own serialization is what failed, so the identifying fields go
    into ``detail`` as JSON rather than being re-serialized from the proto.
    ``created_at_ms`` is the intent's deterministic stamp (computed from the
    element's event time when it was staged), which keeps the record
    replay-identical.
    """
    (key, intent), reason = element
    detail = json.dumps(
        {
            "reason": reason,
            "intent_id": intent.intent_id,
            "seq": intent.seq,
            "tool_name": intent.tool_name,
        }
    )
    return ActivationError(
        entity_key=key,
        reason=REASON_INTENT_DEAD_LETTER,
        detail=detail,
        event_time_ms=intent.created_at_ms,
    )


def activation_error_to_row(error: ActivationError) -> dict[str, Any]:
    """Encode for a BigQuery sink: a flat row with a hex key.

    ``entity_key`` is hex rather than raw bytes so a table can be clustered and
    joined on it without a decode step, matching ``trace_event_to_row``. The
    remaining fields are carried natively: ``reason`` is the triage dimension a
    query groups by, and ``detail`` stays free-form text.
    """
    return {
        "entity_key": error.entity_key.hex(),
        "reason": error.reason,
        "detail": error.detail,
        "event_time_ms": error.event_time_ms,
    }
