"""The YAML-facing transform: schema'd rows in, four named row streams out.

``run_agent(**config)`` is the whole integration contract with Beam YAML. A
Python-typed provider is pointed at the fully-qualified name
``beam_agents.yaml.run_agent`` and hands it the document's ``config:`` mapping as
keyword arguments; the constructor resolves the ``module:object`` references,
maps the rest onto :class:`~beam_agents.core.transform.AgentConfig`, and returns
a ``beam.PTransform`` wrapping the public ``RunAgent`` unchanged.

Beam YAML surface facts this module depends on (verified against the installed
Apache Beam 2.72.0 source, resolving the design's Open Questions; the drift
guard is ``tests/yaml/test_yaml_e2e.py``):

* **Provider declaration.** ``type: python`` is a registered provider type
  (``apache_beam/yaml/yaml_provider.py::python``). With no ``packages:`` it
  builds an ``InlineProvider`` that resolves each fully-qualified constructor
  *in-process* via ``PythonCallableWithSource.load_from_source``; with
  ``packages:`` it builds an ``ExternalPythonProvider`` that installs them into
  a managed venv and expands there. Either way our deliverable is a constructor,
  never a service.
* **Config passing.** ``InlineProvider.create_transform`` calls
  ``factory(**config)``. The constructor therefore takes explicit keyword-only
  parameters (so Beam's ``config_schema`` introspection sees real fields) plus a
  ``**unknown`` catch-all that raises ``ValueError`` listing the accepted keys —
  Python's own ``TypeError`` would neither be a ``ValueError`` nor name them.
* **Multi-output declaration.** ``expand_leaf_transform`` names outputs from a
  ``dict[str, PCollection]`` return; a tuple/list becomes ``out0``/``out1``/...
  and anything else is rejected outright. ``expand`` therefore returns a dict
  keyed by :data:`OUTPUT_NAMES`, and a downstream step addresses one as
  ``TransformName.output_name`` (``Scope.get_pcollection``). Because a
  multi-output transform cannot be addressed bare, a YAML consumer always names
  the stream it wants — including ``.output``.

This module imports nothing from ``apache_beam.yaml``: the dependency direction
is Beam YAML → us, which keeps the package importable and testable with plain
``apache-beam[gcp]``.

Importing this module has no side effects.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import apache_beam as beam
from apache_beam.pvalue import Row
from apache_beam.utils.timestamp import MIN_TIMESTAMP

from beam_agents._protos import AgentEnvelope, ToolIntent
from beam_agents.core.dofn import ActivationError
from beam_agents.core.error_records import activation_error_to_row
from beam_agents.core.transform import AgentConfig, RunAgent
from beam_agents.observability import trace_event_to_row
from beam_agents.yaml._config import build_agent_config, reject_unknown_keys
from beam_agents.yaml._refs import resolve_agent

if TYPE_CHECKING:
    from collections.abc import Iterator

    from beam_agents._protos import TraceEvent
    from beam_agents.core.agent import Agent

#: The named outputs, matching ``RunAgentOutputs``' attribute names so docs,
#: traces, and both surfaces agree on vocabulary. ``output`` is the main stream.
OUTPUT_NAMES: tuple[str, ...] = ("output", "intents", "traces", "errors")

#: Tag for input rows that could not be enveloped. A missing field is a *data*
#: error, not a config error: it dead-letters onto ``errors`` rather than
#: failing the bundle, per the project's route-element-failures-to-errors rule.
MALFORMED_TAG = "malformed"

#: The ``ActivationError.reason`` a malformed input row carries.
REASON_MALFORMED_ROW = "malformed_input_row"

_DEFAULT_KEY_FIELD = "key"
_DEFAULT_PAYLOAD_FIELD = "payload"


def _event_time_ms(timestamp: Any) -> int:
    """The element timestamp in epoch milliseconds, with the unstamped sentinel
    mapped to 0.

    Beam stamps an element that never carried a time (a bounded ``Create``, an
    unstamped source) with ``MIN_TIMESTAMP``, which is ~292 million years before
    the epoch: carried into an ``AgentEnvelope`` it would poison every derived
    timestamp downstream (``expires_at_ms``, the trace rows' RFC 3339
    ``event_time``, the TTL mark). Mapping it to the epoch keeps the fallback
    deterministic — no clock is read — and a pipeline that cares configures
    ``event_time_field`` instead.
    """
    if timestamp <= MIN_TIMESTAMP:
        return 0
    return int(timestamp.micros // 1000)


def _as_bytes(value: object) -> bytes | None:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    return None


class _RowToEnvelope(beam.DoFn):
    """Read the configured fields off a row and build a keyed ``AgentEnvelope``.

    ``event_time_ms`` comes from ``event_time_field`` when configured, else the
    element's own timestamp — so an upstream YAML source that already carries
    event time needs no extra mapping step.
    """

    def __init__(self, *, key_field: str, payload_field: str, event_time_field: str | None) -> None:
        super().__init__()
        self._key_field = key_field
        self._payload_field = payload_field
        self._event_time_field = event_time_field

    def process(
        self,
        element: Any,
        timestamp: Any = beam.DoFn.TimestampParam,
    ) -> Iterator[Any]:
        fallback_ms = _event_time_ms(timestamp)
        entity_key = _as_bytes(self._field(element, self._key_field))
        if entity_key is None:
            yield self._malformed(b"", self._key_field, "key_field", fallback_ms)
            return
        external_event = _as_bytes(self._field(element, self._payload_field))
        if external_event is None:
            yield self._malformed(entity_key, self._payload_field, "payload_field", fallback_ms)
            return
        if self._event_time_field is None:
            event_time_ms = fallback_ms
        else:
            configured = self._field(element, self._event_time_field)
            if not isinstance(configured, int):
                yield self._malformed(
                    entity_key,
                    self._event_time_field,
                    "event_time_field",
                    fallback_ms,
                    expected="an integer number of epoch milliseconds",
                )
                return
            event_time_ms = configured
        yield AgentEnvelope(
            entity_key=entity_key,
            event_time_ms=event_time_ms,
            external_event=external_event,
        )

    @staticmethod
    def _malformed(
        entity_key: bytes,
        field: str,
        setting: str,
        event_time_ms: int,
        *,
        expected: str = "str or bytes",
    ) -> beam.pvalue.TaggedOutput:
        return beam.pvalue.TaggedOutput(
            MALFORMED_TAG,
            ActivationError(
                entity_key=entity_key,
                reason=REASON_MALFORMED_ROW,
                detail=(f"{setting} {field!r} is missing from the input row, or is not {expected}"),
                event_time_ms=event_time_ms,
            ),
        )

    @staticmethod
    def _field(element: Any, name: str) -> object:
        try:
            return getattr(element, name)
        except AttributeError:
            if isinstance(element, Mapping):
                return element.get(name)
            return None


def _entity_key(envelope: AgentEnvelope) -> bytes:
    return envelope.entity_key


class _RowsToEnvelopes(beam.PTransform):
    """Rows → ``KV[bytes, AgentEnvelope]`` on ``keyed``, dead letters on ``malformed``.

    The keying step is the very idiom ``RunAgent`` documents for its callers —
    ``beam.WithKeys(entity_key).with_output_types(tuple[bytes, AgentEnvelope])``
    — so the wrapped transform's KV validation and coder inference see exactly
    the shape they require.
    """

    def __init__(self, *, key_field: str, payload_field: str, event_time_field: str | None) -> None:
        super().__init__()
        self._key_field = key_field
        self._payload_field = payload_field
        self._event_time_field = event_time_field

    def expand(self, pcoll: beam.pvalue.PCollection) -> dict[str, beam.pvalue.PCollection]:
        dofn = _RowToEnvelope(
            key_field=self._key_field,
            payload_field=self._payload_field,
            event_time_field=self._event_time_field,
        )
        tagged = pcoll | "RowToEnvelope" >> beam.ParDo(dofn).with_outputs(
            MALFORMED_TAG, main="envelopes"
        )
        keyed = tagged.envelopes | "KeyByEntity" >> beam.WithKeys(_entity_key).with_output_types(
            tuple[bytes, AgentEnvelope]
        )
        return {"keyed": keyed, MALFORMED_TAG: tagged[MALFORMED_TAG]}


def _output_to_row(output: bytes) -> Row:
    """The main stream as a row.

    ``RunAgent``'s main output is an unkeyed ``PCollection[bytes]`` — the DoFn
    yields ``result.outputs`` verbatim — so the row carries the opaque agent
    output and nothing else; there is no entity key on this stream to carry
    (tasks.md Revision 2). The runtime imposes no schema on those bytes either,
    so none is invented: a downstream ``MapToFields`` decodes them.
    """
    return Row(output=output)


def _intent_to_row(intent: ToolIntent) -> Row:
    """``ToolIntent``'s scalar fields, the counterpart of the shipped trace/error
    row mappings. ``kind`` is its enum *name* so a consumer need not carry the
    numbering, matching ``trace_event_to_row``'s treatment of ``event_type``.
    """
    return Row(
        intent_id=intent.intent_id,
        entity_key=intent.entity_key.hex(),
        seq=intent.seq,
        step_index=intent.step_index,
        tool_name=intent.tool_name,
        args_json=intent.args_json,
        created_at_ms=intent.created_at_ms,
        expires_at_ms=intent.expires_at_ms,
        attempt=intent.attempt,
        kind=ToolIntent.Kind.Name(intent.kind),
        trace_id=intent.trace_id.hex(),
    )


def _trace_to_row(event: TraceEvent) -> Row:
    """The shipped ``trace_event_to_row`` shape, with attributes as nested rows."""
    fields = trace_event_to_row(event)
    attributes = [Row(**attribute) for attribute in fields.pop("attributes")]
    return Row(**fields, attributes=attributes)


def _error_to_row(error: ActivationError) -> Row:
    """The shipped ``activation_error_to_row`` shape, as a row."""
    return Row(**activation_error_to_row(error))


class RunAgentFromYaml(beam.PTransform):
    """Wraps :class:`~beam_agents.core.transform.RunAgent` for Beam YAML.

    Constructed by :func:`run_agent`; ``agent`` and ``config`` expose the
    resolved objects the wrapped transform was built with.
    """

    def __init__(
        self,
        agent: Agent,
        *,
        config: AgentConfig,
        key_field: str,
        payload_field: str,
        event_time_field: str | None,
    ) -> None:
        super().__init__()
        self._agent = agent
        self._config = config
        self._key_field = key_field
        self._payload_field = payload_field
        self._event_time_field = event_time_field

    @property
    def agent(self) -> Agent:
        """The agent object the ``agent`` reference resolved to."""
        return self._agent

    @property
    def config(self) -> AgentConfig:
        """The ``AgentConfig`` the YAML config mapped onto."""
        return self._config

    def expand(self, pcoll: beam.pvalue.PCollection) -> dict[str, beam.pvalue.PCollection]:
        rows = pcoll | "RowsToEnvelopes" >> _RowsToEnvelopes(
            key_field=self._key_field,
            payload_field=self._payload_field,
            event_time_field=self._event_time_field,
        )
        outputs = rows["keyed"] | "RunAgent" >> RunAgent(self._agent, config=self._config)
        # Malformed input rows join the activation dead letters, so `errors` is
        # the single place a YAML pipeline reads to see everything that failed.
        errors = (outputs.errors, rows[MALFORMED_TAG]) | "FlattenErrors" >> beam.Flatten()
        return {
            "output": outputs.output | "OutputToRow" >> beam.Map(_output_to_row),
            "intents": outputs.intents | "IntentToRow" >> beam.Map(_intent_to_row),
            "traces": outputs.traces | "TraceToRow" >> beam.Map(_trace_to_row),
            "errors": errors | "ErrorToRow" >> beam.Map(_error_to_row),
        }


def run_agent(
    *,
    agent: str,
    provider: str,
    provider_config: Mapping[str, Any] | None = None,
    decode: str | None = None,
    tool_registry: str | None = None,
    activation_timeout_s: float | None = None,
    ttl_ms: int | None = None,
    cancel_grace_s: float | None = None,
    intents_to: str | None = None,
    traces_to: str | None = None,
    errors_to: str | None = None,
    hitl: Mapping[str, Any] | None = None,
    key_field: str = _DEFAULT_KEY_FIELD,
    payload_field: str = _DEFAULT_PAYLOAD_FIELD,
    event_time_field: str | None = None,
    **unknown: Any,
) -> RunAgentFromYaml:
    """Run an agent as a Beam YAML transform over a stream of schema'd rows.

    Args:
      agent: ``module:object`` reference to the agent, e.g.
        ``"my_pkg.agents:fraud_agent"``. Resolved by import at expansion time.
      provider: ``module:object`` reference to a callable returning an
        ``LLMClient`` — a provider class or a factory function.
      provider_config: Keyword arguments bound onto ``provider``, producing the
        zero-argument provider factory the runtime holds.
      decode: Optional ``module:object`` reference to the provider's paired
        response decoder; unset means LLM traces omit their usage attributes.
      tool_registry: Optional ``module:object`` reference to a prebuilt
        ``ToolRegistry`` of the read-only tools the agent may run inline.
      activation_timeout_s: Wall-clock budget for one activation.
      ttl_ms: Working-memory time-to-live for a key, in milliseconds.
      cancel_grace_s: Grace period for cancelling a timed-out activation.
      intents_to: Sink URI for the ``intents`` stream (the outbox topic).
      traces_to: Sink URI for the ``traces`` stream.
      errors_to: Sink URI for the ``errors`` stream.
      hitl: Human-in-the-loop policy mapping: ``timeout_ms``, ``intent_ttl_ms``,
        ``approval_channel``, ``max_escalations``, and ``on_timeout`` (a
        ``module:object`` reference to a pure, module-level route function).
      key_field: Input-row field carrying the entity key (str or bytes).
      payload_field: Input-row field carrying the opaque event payload.
      event_time_field: Input-row field carrying event time in epoch
        milliseconds; unset uses the element's own timestamp.
    """
    reject_unknown_keys(unknown)
    resolved_agent = resolve_agent(agent)
    config = build_agent_config(
        provider=provider,
        provider_config=provider_config,
        decode=decode,
        tool_registry=tool_registry,
        activation_timeout_s=activation_timeout_s,
        ttl_ms=ttl_ms,
        cancel_grace_s=cancel_grace_s,
        intents_to=intents_to,
        traces_to=traces_to,
        errors_to=errors_to,
        hitl=hitl,
    )
    return RunAgentFromYaml(
        resolved_agent,
        config=config,
        key_field=key_field,
        payload_field=payload_field,
        event_time_field=event_time_field,
    )
