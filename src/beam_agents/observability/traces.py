"""Deterministic trace/span identity and the activation's trace surface.

See the change design (``openspec/changes/add-trace-events/design.md``) for the
load-bearing decisions: ``uuid5`` identity over activation scope (D1), one trace
per ``(entity_key, seq)`` so a suspend/resume cycle is a single trace (D2),
correlation stamped at the staging boundary rather than plumbed through
producers (D3), usage attributes omitted when unknown (D4), and zero-width spans
because every timestamp comes from the injected activation clock (D7).

Nothing here reads a clock, a counter, or a randomness source: every identifier
is a pure function of ``(entity_key, seq, role, index)``, exactly as
``intent_id_for`` derives an intent ID, so a replayed bundle emits byte-identical
trace events.

Importing this module has no side effects.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from beam_agents._protos import TraceEvent

if TYPE_CHECKING:
    from beam_agents.core.loop import FailureContext
    from beam_agents.model.facade import TokenUsage

__all__ = [
    "ACTIVATION_KIND",
    "ACTIVATION_STATUS",
    "ADAPTER",
    "ATTEMPTS",
    "BILLED",
    "CACHE_HIT",
    "CIRCUIT_STATE",
    "DEADLINE_MS",
    "ERROR_TYPE",
    "EXPIRES_AT_MS",
    "FAILURE_LAST_EVENT",
    "FAILURE_LLM_CALLS",
    "FAILURE_STAGED_INTENTS",
    "FAILURE_STEP",
    "INTENT_ID",
    "INTENT_KIND",
    "OPERATION_CHAT",
    "OPERATION_NAME",
    "PENDING_INTENT_IDS",
    "REASON",
    "REQUEST_MODEL",
    "ROLE_ACTIVATION",
    "ROLE_TIMER",
    "TOOL_NAME",
    "USAGE_INPUT_TOKENS",
    "USAGE_OUTPUT_TOKENS",
    "ActivationTrace",
    "role_for_event_type",
    "span_id_for",
    "trace_id_for",
    "usage_attributes",
]

# Fixed namespace for deterministic trace/span IDs. Distinct from the intent
# namespace so the two ID spaces cannot alias; like it, this must never change
# without a state_schema_version bump.
_TRACE_NAMESPACE = uuid.UUID("b1d7c4e2-93a6-5f08-8c31-7e5a9d0f2b46")

# Span roles. `role` separates the ID spaces of the different things that can
# happen at one step index: `AgentContext` lets the agent choose the step_index
# it passes to the model facade, drawn from the same space as intent step
# indices, so without a role an LLM_CALL and an INTENT_EMITTED at step 2 would
# share a span ID.
ROLE_ACTIVATION = "activation"
ROLE_TIMER = "timer"

# -- OTel GenAI semantic-convention attribute names ---------------------------

OPERATION_NAME = "gen_ai.operation.name"
REQUEST_MODEL = "gen_ai.request.model"
USAGE_INPUT_TOKENS = "gen_ai.usage.input_tokens"
USAGE_OUTPUT_TOKENS = "gen_ai.usage.output_tokens"
ERROR_TYPE = "error.type"

# -- Runtime attributes -------------------------------------------------------
#
# `beam_agents.*` rather than `gen_ai.*`: the GenAI semantic conventions do not
# name a cache-hit or a billed attribute, and inventing a `gen_ai.` key the spec
# does not define would be worse than an honestly-namespaced extension.

CACHE_HIT = "beam_agents.cache_hit"
BILLED = "beam_agents.billed"
ATTEMPTS = "beam_agents.attempts"
CIRCUIT_STATE = "beam_agents.circuit_state"
REASON = "beam_agents.reason"
# Failure-position metadata on the `activation_error` route's ERROR event:
# scalars describing where the failed activation was, never what it staged.
# Absent (not defaulted) on routes where no context is reachable — the timeout
# route, failures before context construction, and the non-activation routes.
FAILURE_STEP = "beam_agents.failure.step"
FAILURE_LAST_EVENT = "beam_agents.failure.last_event"
FAILURE_STAGED_INTENTS = "beam_agents.failure.staged_intents"
FAILURE_LLM_CALLS = "beam_agents.failure.llm_calls"
ACTIVATION_STATUS = "beam_agents.activation.status"
ACTIVATION_KIND = "beam_agents.activation.kind"
INTENT_ID = "beam_agents.intent_id"
INTENT_KIND = "beam_agents.intent_kind"
TOOL_NAME = "beam_agents.tool_name"
EXPIRES_AT_MS = "beam_agents.expires_at_ms"
DEADLINE_MS = "beam_agents.deadline_ms"
ADAPTER = "beam_agents.adapter"
PENDING_INTENT_IDS = "beam_agents.pending_intent_ids"

# The one `gen_ai.operation.name` value this runtime produces today.
OPERATION_CHAT = "chat"


def trace_id_for(entity_key: bytes, seq: int) -> bytes:
    """Return the 16-byte trace ID for an activation scope.

    Scoped to ``(entity_key, seq)``, not to a single ``process()`` call: a
    resumed activation runs under its suspended activation's ``seq``, so it
    recomputes the same trace ID with nothing carried on the wire (D2).
    """
    return uuid.uuid5(_TRACE_NAMESPACE, f"{entity_key.hex()}|{seq}").bytes


def span_id_for(entity_key: bytes, seq: int, role: str, index: int) -> bytes:
    """Return the 8-byte span ID for one span within an activation scope."""
    name = f"{entity_key.hex()}|{seq}|{role}|{index}"
    return uuid.uuid5(_TRACE_NAMESPACE, name).bytes[:8]


def role_for_event_type(event_type: TraceEvent.EventType) -> str:
    """Return the span role for an event type: its own enum name.

    ``ACTIVATION_START``/``ACTIVATION_END`` map to :data:`ROLE_ACTIVATION`, so
    they land on the attempt's own span rather than a child span.
    """
    if event_type in (TraceEvent.ACTIVATION_START, TraceEvent.ACTIVATION_END):
        return ROLE_ACTIVATION
    return str(TraceEvent.EventType.Name(event_type))


def usage_attributes(usage: TokenUsage | None, *, billed: bool) -> dict[str, str]:
    """Build the usage attributes, omitting counts that are not known (D4).

    A present attribute is true. ``usage=None`` means no response was decoded —
    a transport failure, an open circuit, or a context with no provider
    ``decode`` — and the counts are left out entirely rather than written as
    ``"0"``, which anything summing them would read as a real zero-token call.
    """
    attributes = {BILLED: _bool(billed)}
    if usage is not None:
        attributes[USAGE_INPUT_TOKENS] = str(usage.prompt_tokens)
        attributes[USAGE_OUTPUT_TOKENS] = str(usage.completion_tokens)
    return attributes


def _bool(value: bool) -> str:
    return "true" if value else "false"


class ActivationTrace:
    """One activation attempt's trace surface: identity, stamping, and builders.

    Constructed per activation from the scope it already has. ``entry_step_index``
    is the step index the attempt started at (``0`` for a fresh activation, the
    resumed ``Continuation``'s cursor otherwise), which is what makes each
    attempt's activation span distinct while all of them stay inside one trace.
    """

    __slots__ = ("_entity_key", "_is_resume", "_now_ms", "_parent_span_id", "_seq", "_span_id")

    def __init__(
        self,
        *,
        entity_key: bytes,
        seq: int,
        now_ms: int,
        entry_step_index: int = 0,
        is_resume: bool = False,
    ) -> None:
        self._entity_key = entity_key
        self._seq = seq
        self._now_ms = now_ms
        self._is_resume = is_resume
        self._span_id = span_id_for(entity_key, seq, ROLE_ACTIVATION, entry_step_index)
        # A resume hangs under the initial attempt (entry step 0); the initial
        # attempt is the trace root and has no parent.
        self._parent_span_id = (
            span_id_for(entity_key, seq, ROLE_ACTIVATION, 0) if is_resume else b""
        )

    @property
    def trace_id(self) -> bytes:
        """The trace id for this activation, derived from ``(entity_key, seq)``.

        Derived, not random, so a replayed activation lands in the same
        trace as the original.
        """
        return trace_id_for(self._entity_key, self._seq)

    @property
    def span_id(self) -> bytes:
        """This attempt's activation span — the parent of every child event."""
        return self._span_id

    @property
    def parent_span_id(self) -> bytes:
        """The span this activation hangs under, or empty bytes at the root."""
        return self._parent_span_id

    # -- stamping -------------------------------------------------------------

    def stamp(self, event: TraceEvent, *, role: str | None = None) -> TraceEvent:
        """Fill in any correlation field the producer left empty, in place.

        Producers (the model facade, the tool path, the loop driver) emit plain
        events and stay ignorant of tracing (D3). Only *empty* fields are
        filled, so a producer that knows better can supply its own parent, and
        re-stamping an already-correlated event is a no-op.
        """
        if not event.trace_id:
            event.trace_id = self.trace_id
        span_role = role if role is not None else role_for_event_type(event.event_type)
        if not event.span_id:
            event.span_id = (
                self._span_id
                if span_role == ROLE_ACTIVATION
                else span_id_for(self._entity_key, self._seq, span_role, event.step_index)
            )
        if not event.parent_span_id:
            event.parent_span_id = (
                self._parent_span_id if span_role == ROLE_ACTIVATION else self._span_id
            )
        return event

    # -- builders -------------------------------------------------------------

    def activation_start(self) -> TraceEvent:
        """Build the ACTIVATION_START event, tagged ``start`` or ``resume``."""
        return self._event(
            TraceEvent.ACTIVATION_START,
            step_index=0,
            attributes={ACTIVATION_KIND: "resume" if self._is_resume else "start"},
        )

    def activation_end(self, *, status: str, step_index: int) -> TraceEvent:
        """Build the ACTIVATION_END event carrying the terminal ``status``."""
        return self._event(
            TraceEvent.ACTIVATION_END,
            step_index=step_index,
            attributes={
                ACTIVATION_STATUS: status,
                ACTIVATION_KIND: "resume" if self._is_resume else "start",
            },
        )

    def suspended(
        self,
        *,
        step_index: int,
        deadline_ms: int,
        adapter: str,
        pending_intent_ids: tuple[str, ...],
    ) -> TraceEvent:
        """Build the SUSPENDED event: the deadline, adapter, and pending intent ids."""
        return self._event(
            TraceEvent.SUSPENDED,
            step_index=step_index,
            attributes={
                DEADLINE_MS: str(deadline_ms),
                ADAPTER: adapter,
                PENDING_INTENT_IDS: ",".join(pending_intent_ids),
            },
        )

    def intent_emitted(
        self,
        *,
        step_index: int,
        intent_id: str,
        tool_name: str,
        intent_kind: str,
        expires_at_ms: int,
    ) -> TraceEvent:
        """Build the INTENT_EMITTED event for one staged intent."""
        return self._event(
            TraceEvent.INTENT_EMITTED,
            step_index=step_index,
            attributes={
                INTENT_ID: intent_id,
                TOOL_NAME: tool_name,
                INTENT_KIND: intent_kind,
                EXPIRES_AT_MS: str(expires_at_ms),
            },
        )

    def tool_call(self, *, step_index: int, tool_index: int, tool_name: str) -> TraceEvent:
        """A read-only tool executed inline.

        ``tool_index`` is a counter of its own, never the intent step cursor:
        advancing that cursor would change the ``intent_id``s the activation
        goes on to mint and invalidate in-flight continuations (D8). It is used
        only to keep tool spans distinct; ``step_index`` records where the call
        sat relative to the activation's other events.
        """
        event = self._event(
            TraceEvent.TOOL_CALL, step_index=step_index, attributes={TOOL_NAME: tool_name}
        )
        event.span_id = span_id_for(
            self._entity_key, self._seq, role_for_event_type(TraceEvent.TOOL_CALL), tool_index
        )
        return event

    def error(
        self,
        *,
        reason: str,
        error_type: str = "",
        role: str | None = None,
        failure: FailureContext | None = None,
    ) -> TraceEvent:
        """A failure record, synthesized from what the caller already holds (D5).

        Never derived from a failed activation's staged *effects*: those stay
        discarded, so correctness invariant 1 is untouched. ``failure``, when
        the route has one, adds the ``beam_agents.failure.*`` position scalars —
        counts and kinds about the rolled-back context, never its contents.
        Routes with no reachable context omit them entirely (absent, not
        defaulted).
        """
        attributes = {REASON: reason}
        if error_type:
            attributes[ERROR_TYPE] = error_type
        if failure is not None:
            attributes.update(failure.trace_attributes())
        return self._event(TraceEvent.ERROR, step_index=0, attributes=attributes, role=role)

    def _event(
        self,
        event_type: TraceEvent.EventType,
        *,
        step_index: int,
        attributes: dict[str, str],
        role: str | None = None,
    ) -> TraceEvent:
        event = TraceEvent(
            entity_key=self._entity_key,
            seq=self._seq,
            step_index=step_index,
            event_type=event_type,
            attributes=attributes,
            # Both ends are the injected activation clock: spans are zero-width
            # by design (D7), because measuring elapsed time would need a
            # wall-clock read inside the hot path.
            start_ms=self._now_ms,
            end_ms=self._now_ms,
        )
        return self.stamp(event, role=role)
