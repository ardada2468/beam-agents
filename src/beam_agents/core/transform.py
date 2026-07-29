"""``RunAgent`` — the public PTransform that turns an agent into a keyed,
stateful, fault-tolerant Beam step.

Usage::

    outputs = keyed_envelopes | RunAgent(agent, config=AgentConfig(provider_factory=make_client))
    outputs.output   # terminal agent outputs (bytes)
    outputs.intents  # ToolIntent side-effect requests -> outbox topic
    outputs.traces   # TraceEvent observability records
    outputs.errors   # ActivationError dead-letter records

Input is a pre-keyed ``PCollection[KV[bytes, AgentEnvelope]]`` — the caller
``Flatten``s its event/tool-result/approval streams and keys them with
``beam.WithKeys(entity_key).with_output_types(tuple[bytes, AgentEnvelope])``
upstream, matching the documented Dataflow shape. ``RunAgent`` does not key
elements itself; it validates the input is KV-shaped at pipeline-construction
(``expand``) time and raises ``ValueError`` otherwise.

``AgentConfig`` bundles the provider factory, runtime knobs, and optional sink
URIs (``intents_to``/``traces_to``/``errors_to``); it validates itself at
construction time so misconfiguration fails at the site of the typo rather than
deep inside a runner. Configured sink URIs are resolved to Beam write
transforms via a pluggable ``SinkResolver`` and attached as terminal branches
to their tagged output; unset sinks leave that output exposed for the caller.

Importing this module has no side effects.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, runtime_checkable
from urllib.parse import urlparse

import apache_beam as beam

from beam_agents._protos import ToolIntent
from beam_agents.actions.write_intents import (
    WriteIntents,
    WriteIntentsResult,
    encode_intent_dead_letter,
    is_kv_shaped,
)
from beam_agents.core.coders import register_coders
from beam_agents.core.dofn import _AgentDoFn
from beam_agents.hitl import HitlPolicy
from beam_agents.observability import serialize_trace_event, trace_event_to_row

if TYPE_CHECKING:
    from collections.abc import Callable

    from beam_agents.core.agent import Agent
    from beam_agents.model.client import LLMClient
    from beam_agents.model.facade import Decode

# Output tags. ``output`` is the main (untagged) output.
INTENTS_TAG = "intents"
TRACES_TAG = "traces"
ERRORS_TAG = "errors"

_DEFAULT_ACTIVATION_TIMEOUT_S = 30.0
_DEFAULT_TTL_MS = 3_600_000
_DEFAULT_CANCEL_GRACE_S = 5.0

_SINK_FIELDS = ("intents_to", "traces_to", "errors_to")
_SINK_LABELS = {
    "intents_to": "WriteIntents",
    "traces_to": "WriteTraces",
    "errors_to": "WriteErrors",
}


class UnknownSinkSchemeError(ValueError):
    """A sink URI's scheme is unrecognized, or the URI is malformed for its scheme."""


@runtime_checkable
class SinkResolver(Protocol):
    """Resolves a sink URI to a Beam write transform.

    ``validate`` runs at ``AgentConfig`` construction: it must be cheap and
    import-free, raising :class:`UnknownSinkSchemeError` for an unrecognized
    scheme or a malformed URI. ``resolve`` runs only at ``RunAgent.expand`` and
    may construct real IO clients. Both take ``field_name`` (one of
    ``intents_to``/``traces_to``/``errors_to``) so a resolver can special-case
    a field independently of URI scheme.
    """

    def validate(self, field_name: str, uri: str) -> None: ...

    def resolve(self, field_name: str, uri: str) -> beam.PTransform: ...


class _KeyedWriteIntents(beam.PTransform):
    """Keys ``ToolIntent``s by ``entity_key`` and writes them via ``WriteIntents``.

    Adapts ``RunAgent``'s unkeyed ``.intents`` output (``PCollection[ToolIntent]``)
    to the pre-keyed ``PCollection[KV[bytes, ToolIntent]]`` that ``WriteIntents``
    requires, preserving per-key emission order end to end.
    """

    def __init__(self, uri: str) -> None:
        super().__init__()
        self._uri = uri

    def expand(self, pcoll: beam.pvalue.PCollection) -> WriteIntentsResult:
        keyed = pcoll | "KeyIntentsByEntity" >> beam.WithKeys(
            lambda intent: intent.entity_key
        ).with_output_types(tuple[bytes, ToolIntent])
        return keyed | WriteIntents(self._uri)


class _WriteTraces(beam.PTransform):
    """Encodes ``TraceEvent``s for ``sink``'s scheme, then writes them.

    ``.traces`` is a ``PCollection[TraceEvent]`` and none of the write
    transforms accept a proto message, so without this step a configured
    ``traces_to`` fails at runtime (design D9). Kafka/Pub/Sub take keyed
    deterministic bytes; BigQuery takes a row mapping.
    """

    def __init__(self, sink: beam.PTransform, *, to_row: bool) -> None:
        super().__init__()
        self._sink = sink
        self._to_row = to_row

    def expand(self, pcoll: beam.pvalue.PCollection) -> beam.pvalue.PCollection:
        if self._to_row:
            encoded = pcoll | "TraceEventToRow" >> beam.Map(trace_event_to_row)
        else:
            encoded = pcoll | "SerializeTraceEvent" >> beam.Map(
                serialize_trace_event
            ).with_output_types(tuple[bytes, bytes])
        return encoded | "WriteEncodedTraces" >> self._sink


class DefaultSinkResolver:
    """Resolves ``kafka://``, ``pubsub://``, and ``bigquery://`` sink URIs.

    URI grammar:

    - ``kafka://<bootstrap-servers>/<topic>`` — comma-separated ``host:port`` list.
    - ``pubsub://<project>/<topic>``
    - ``bigquery://<project>/<dataset>/<table>``

    For ``intents_to`` with a ``kafka://`` or ``pubsub://`` scheme, resolution
    returns the keyed :class:`WriteIntents` outbox writer (see
    ``actions/write_intents.py``) instead of a bare write transform, so each
    key's intents are routed to a single partition/ordering key and
    serialization failures are dead-lettered rather than dropped.

    IO client modules are imported lazily inside :meth:`resolve` so
    :meth:`validate` (called at ``AgentConfig`` construction) never imports them.
    """

    _SCHEMES = frozenset({"kafka", "pubsub", "bigquery"})
    _BIGQUERY_URI_SEGMENTS = 2  # <dataset>/<table>

    def validate(self, field_name: str, uri: str) -> None:
        self._parse(field_name, uri)

    def resolve(self, field_name: str, uri: str) -> beam.PTransform:
        scheme, parts = self._parse("<sink>", uri)
        if field_name == "intents_to" and scheme in ("kafka", "pubsub"):
            return _KeyedWriteIntents(uri)
        if field_name == "traces_to":
            return _WriteTraces(self._write_transform(scheme, parts), to_row=scheme == "bigquery")
        return self._write_transform(scheme, parts)

    def _write_transform(self, scheme: str, parts: tuple[str, ...]) -> beam.PTransform:
        if scheme == "kafka":
            from apache_beam.io.kafka import WriteToKafka

            servers, topic = parts
            return WriteToKafka(producer_config={"bootstrap.servers": servers}, topic=topic)
        if scheme == "pubsub":
            from apache_beam.io.gcp.pubsub import WriteToPubSub

            project, topic = parts
            return WriteToPubSub(topic=f"projects/{project}/topics/{topic}")
        from apache_beam.io.gcp.bigquery import WriteToBigQuery

        project, dataset, table = parts
        return WriteToBigQuery(table=f"{project}:{dataset}.{table}")

    def _parse(self, field_name: str, uri: str) -> tuple[str, tuple[str, ...]]:
        parsed = urlparse(uri)
        scheme = parsed.scheme
        if scheme not in self._SCHEMES:
            raise UnknownSinkSchemeError(
                f"{field_name}: unknown sink URI scheme {(scheme or uri)!r}; "
                f"expected one of {sorted(self._SCHEMES)}"
            )
        segments = [s for s in parsed.path.split("/") if s]
        if scheme == "kafka":
            if not parsed.netloc or len(segments) != 1:
                raise UnknownSinkSchemeError(
                    f"{field_name}: malformed kafka URI {uri!r}; "
                    "expected kafka://<bootstrap-servers>/<topic>"
                )
            return scheme, (parsed.netloc, segments[0])
        if scheme == "pubsub":
            if not parsed.netloc or len(segments) != 1:
                raise UnknownSinkSchemeError(
                    f"{field_name}: malformed pubsub URI {uri!r}; "
                    "expected pubsub://<project>/<topic>"
                )
            return scheme, (parsed.netloc, segments[0])
        if not parsed.netloc or len(segments) != self._BIGQUERY_URI_SEGMENTS:
            raise UnknownSinkSchemeError(
                f"{field_name}: malformed bigquery URI {uri!r}; "
                "expected bigquery://<project>/<dataset>/<table>"
            )
        return scheme, (parsed.netloc, *segments)


def _require_positive(name: str, value: float) -> None:
    if value <= 0:
        raise ValueError(f"AgentConfig.{name} must be positive, got {value!r}")


@dataclass(frozen=True, slots=True)
class AgentConfig:
    """Immutable, self-validating ``RunAgent`` configuration.

    Bundles the provider factory, the runtime knobs, and the optional sink
    URIs. Validation runs in ``__post_init__``, so a misconfigured value raises
    ``ValueError`` at the construction site — before any pipeline exists.
    """

    provider_factory: Callable[[], LLMClient]
    # The provider's paired response decoder (e.g. `model.anthropic_decode`).
    # Optional, and unset means "token counts unknown": the LLM_CALL traces
    # then omit their usage attributes rather than report zeros, which anything
    # summing them would read as real (design D4).
    decode: Decode | None = field(default=None, kw_only=True)
    activation_timeout_s: float = field(default=_DEFAULT_ACTIVATION_TIMEOUT_S, kw_only=True)
    ttl_ms: int = field(default=_DEFAULT_TTL_MS, kw_only=True)
    cancel_grace_s: float = field(default=_DEFAULT_CANCEL_GRACE_S, kw_only=True)
    hitl_policy: HitlPolicy = field(default_factory=HitlPolicy, kw_only=True)
    intents_to: str | None = field(default=None, kw_only=True)
    traces_to: str | None = field(default=None, kw_only=True)
    errors_to: str | None = field(default=None, kw_only=True)
    sink_resolver: SinkResolver = field(default_factory=DefaultSinkResolver, kw_only=True)

    def __post_init__(self) -> None:
        _require_positive("activation_timeout_s", self.activation_timeout_s)
        _require_positive("ttl_ms", self.ttl_ms)
        _require_positive("cancel_grace_s", self.cancel_grace_s)
        # `HitlPolicy` validates itself on construction; re-checking here covers
        # an instance that reached us another way (a frozen dataclass is still
        # mutable through `object.__setattr__`) and keeps every misconfiguration
        # surfacing at the same place: the config's construction site.
        self.hitl_policy.validate()
        for field_name in _SINK_FIELDS:
            uri = getattr(self, field_name)
            if uri is not None:
                self.sink_resolver.validate(field_name, uri)


@dataclass(frozen=True, slots=True)
class RunAgentOutputs:
    """``RunAgent``'s named outputs: main, the three tagged streams, and the
    intents dead-letter branch.

    ``dead_letter`` is ``None`` unless ``intents_to`` resolved to a
    ``WriteIntents`` outbox writer (a ``kafka://``/``pubsub://`` scheme). It is
    always exposed here, whether or not ``errors_to`` is also set, so a caller
    can consume it directly instead of it being silently dropped by Beam as an
    unconsumed ``PCollection`` when ``errors_to`` is unset.
    """

    output: beam.pvalue.PCollection
    intents: beam.pvalue.PCollection
    traces: beam.pvalue.PCollection
    errors: beam.pvalue.PCollection
    dead_letter: beam.pvalue.PCollection | None = None


def _validate_kv_input(pcoll: beam.pvalue.PCollection) -> None:
    """Raise ``ValueError`` if ``pcoll`` is positively not KV-shaped.

    An absent/erased element type is allowed to pass (the DoFn's downstream KV
    requirement is the backstop); only a definite non-pair type is rejected.
    """
    if not is_kv_shaped(pcoll.element_type):
        raise ValueError(
            "RunAgent requires a PCollection[KV[bytes, AgentEnvelope]] input "
            f"(pre-keyed by entity_key); got element type {pcoll.element_type!r}. Key "
            "upstream with beam.WithKeys(entity_key)"
            ".with_output_types(tuple[bytes, AgentEnvelope]) before RunAgent."
        )


class RunAgent(beam.PTransform):
    """Run ``agent`` as a keyed stateful transform over a pre-keyed envelope stream."""

    def __init__(self, agent: Agent, *, config: AgentConfig) -> None:
        super().__init__()
        self._agent = agent
        self._config = config

    def expand(self, pcoll: beam.pvalue.PCollection) -> RunAgentOutputs:
        _validate_kv_input(pcoll)
        register_coders()
        dofn = _AgentDoFn(
            self._agent,
            provider_factory=self._config.provider_factory,
            activation_timeout_s=self._config.activation_timeout_s,
            ttl_ms=self._config.ttl_ms,
            cancel_grace_s=self._config.cancel_grace_s,
            hitl_policy=self._config.hitl_policy,
            decode=self._config.decode,
        )
        tagged = pcoll | "Activate" >> beam.ParDo(dofn).with_outputs(
            INTENTS_TAG, TRACES_TAG, ERRORS_TAG, main="output"
        )
        tagged_by_field = {
            "intents": tagged.intents,
            "traces": tagged.traces,
            "errors": tagged.errors,
        }
        dead_letter: beam.pvalue.PCollection | None = None
        for field_name in _SINK_FIELDS:
            uri = getattr(self._config, field_name)
            if uri is None:
                continue
            sink = self._config.sink_resolver.resolve(field_name, uri)
            result = (
                tagged_by_field[field_name.removesuffix("_to")] | _SINK_LABELS[field_name] >> sink
            )
            if field_name == "intents_to" and isinstance(result, WriteIntentsResult):
                dead_letter = result.dead_letter
                if self._config.errors_to is not None:
                    errors_sink = self._config.sink_resolver.resolve(
                        "errors_to", self._config.errors_to
                    )
                    (
                        dead_letter
                        | "EncodeIntentDeadLetter"
                        >> beam.Map(encode_intent_dead_letter).with_output_types(
                            tuple[bytes, bytes]
                        )
                        | "IntentDeadLetterToErrors" >> errors_sink
                    )
        return RunAgentOutputs(
            output=tagged.output,
            intents=tagged.intents,
            traces=tagged.traces,
            errors=tagged.errors,
            dead_letter=dead_letter,
        )
