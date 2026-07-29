"""Observability for the agent runtime: correlated traces and runner metrics.

Two complementary surfaces live here:

**Traces** (:mod:`.traces`, :mod:`.exporters`): every activation is one trace,
scoped to ``(entity_key, seq)`` so a suspend → effector → resume cycle stays a
single trace with nothing but ``ToolIntent``'s ``trace_id`` on the wire.
Identity is derived, never generated: trace and span IDs are ``uuid5`` of
activation scope, the same rule ``intent_id_for`` follows, so a replayed bundle
emits byte-identical events and downstream dedup on
``(trace_id, span_id, event_type)`` collapses the at-least-once duplicates a
bundle retry produces. Correlation is stamped by the activation contexts, not
by the producers: the model facade, the tool path, and the loop driver stage
plain events and the context fills in the identity (design D3).

The event kinds and what each carries:

===================  =====================================================
``ACTIVATION_START`` ``activation.kind`` (``start``/``resume``)
``ACTIVATION_END``   ``activation.status`` (``completed``/``suspended``)
``LLM_CALL``         ``gen_ai.operation.name``, ``gen_ai.request.model``,
                     ``gen_ai.usage.*`` (when known), ``cache_hit``,
                     ``billed``, ``attempts``, ``circuit_state``,
                     ``error.type`` (on failure)
``TOOL_CALL``        ``tool_name``
``INTENT_EMITTED``   ``intent_id``, ``tool_name``, ``intent_kind``,
                     ``expires_at_ms``
``SUSPENDED``        ``deadline_ms``, ``adapter``, ``pending_intent_ids``
``ERROR``            ``reason``, ``error.type``
===================  =====================================================

Two rules about the numbers. Token counts are **true or absent**: a usage
attribute is only present when a response was actually decoded, so a consumer
summing ``gen_ai.usage.input_tokens`` never picks up a placeholder zero. And
``beam_agents.billed`` separates a cache hit's real-but-already-paid-for tokens
from new provider spend, so both "what did this activation consume" and "what
did we pay for" are answerable from one stream.

Timestamps come from the injected activation clock, so spans are zero-width by
design (D7): *duration* belongs to the metrics surface below, never to trace
bytes, which stay byte-identical under replay.

**Metrics** (:mod:`.metrics`, the ``runtime-metrics`` capability): the
runner-visible counters and distributions an operator reads off a Dataflow job
page or a Flink metrics reporter — including ``activation_ms`` and the
release-gating ``overhead_ms`` — recorded at commit from the per-activation
:class:`~beam_agents.observability.metrics.ActivationTally`, because a metric
update off the Beam worker thread is silently discarded.

This package is internal: nothing here is re-exported from ``beam_agents``.

Importing this package has no side effects.
"""

from beam_agents.observability.exporters import serialize_trace_event, trace_event_to_row
from beam_agents.observability.metrics import (
    ActivationTally,
    MetricsSink,
    NullMetrics,
    RuntimeMetrics,
)
from beam_agents.observability.traces import (
    ACTIVATION_KIND,
    ACTIVATION_STATUS,
    ADAPTER,
    ATTEMPTS,
    BILLED,
    CACHE_HIT,
    CIRCUIT_STATE,
    DEADLINE_MS,
    ERROR_TYPE,
    EXPIRES_AT_MS,
    INTENT_ID,
    INTENT_KIND,
    OPERATION_CHAT,
    OPERATION_NAME,
    PENDING_INTENT_IDS,
    REASON,
    REQUEST_MODEL,
    ROLE_ACTIVATION,
    ROLE_TIMER,
    TOOL_NAME,
    USAGE_INPUT_TOKENS,
    USAGE_OUTPUT_TOKENS,
    ActivationTrace,
    role_for_event_type,
    span_id_for,
    trace_id_for,
    usage_attributes,
)

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
    "ActivationTally",
    "ActivationTrace",
    "MetricsSink",
    "NullMetrics",
    "RuntimeMetrics",
    "role_for_event_type",
    "serialize_trace_event",
    "span_id_for",
    "trace_event_to_row",
    "trace_id_for",
    "usage_attributes",
]
