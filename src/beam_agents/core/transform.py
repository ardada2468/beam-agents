"""``RunAgent`` — the public PTransform that turns an agent into a keyed,
stateful, fault-tolerant Beam step.

Usage::

    outputs = envelopes | RunAgent(agent, provider_factory=make_client)
    outputs.output   # terminal agent outputs (bytes)
    outputs.intents  # ToolIntent side-effect requests -> outbox topic
    outputs.traces   # TraceEvent observability records
    outputs.errors   # ActivationError dead-letter records

Input is a ``PCollection[AgentEnvelope]``; this transform keys each envelope by
``entity_key`` so the stateful ``_AgentDoFn`` gets per-key serialization, and
registers the deterministic proto coders so no element or state value ever falls
back to pickle.

Importing this module has no side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import apache_beam as beam

from beam_agents._protos import AgentEnvelope
from beam_agents.core.coders import register_coders
from beam_agents.core.dofn import _AgentDoFn

if TYPE_CHECKING:
    from collections.abc import Callable

    from beam_agents.core.agent import Agent
    from beam_agents.model.client import LLMClient

# Output tags. ``output`` is the main (untagged) output.
INTENTS_TAG = "intents"
TRACES_TAG = "traces"
ERRORS_TAG = "errors"


def _key_by_entity(envelope: AgentEnvelope) -> tuple[bytes, AgentEnvelope]:
    return (envelope.entity_key, envelope)


class RunAgent(beam.PTransform):
    """Run ``agent`` as a keyed stateful transform over a stream of envelopes."""

    def __init__(
        self,
        agent: Agent,
        *,
        provider_factory: Callable[[], LLMClient],
        activation_timeout_s: float = 30.0,
        ttl_ms: int = 3_600_000,
        cancel_grace_s: float = 5.0,
    ) -> None:
        super().__init__()
        self._agent = agent
        self._provider_factory = provider_factory
        self._activation_timeout_s = activation_timeout_s
        self._ttl_ms = ttl_ms
        self._cancel_grace_s = cancel_grace_s

    def expand(self, pcoll: beam.pvalue.PCollection) -> beam.pvalue.DoOutputsTuple:
        register_coders()
        dofn = _AgentDoFn(
            self._agent,
            provider_factory=self._provider_factory,
            activation_timeout_s=self._activation_timeout_s,
            ttl_ms=self._ttl_ms,
            cancel_grace_s=self._cancel_grace_s,
        )
        return (
            pcoll
            | "WithEntityKey"
            >> beam.Map(_key_by_entity).with_output_types(tuple[bytes, AgentEnvelope])
            | "Activate"
            >> beam.ParDo(dofn).with_outputs(INTENTS_TAG, TRACES_TAG, ERRORS_TAG, main="output")
        )
