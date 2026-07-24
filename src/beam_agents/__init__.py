"""beam-agents: run AI agents as keyed, stateful, fault-tolerant Beam transforms.

The public entry point is :class:`RunAgent`, which turns an ``Agent`` (a plain
async activation function) into a stateful Beam step with durable keyed memory,
effectively-once side effects via ``ToolIntent``s, and a replay cache.
:class:`AgentConfig` bundles the provider factory, runtime knobs, and optional
sink URIs; :class:`RunAgentOutputs` exposes the transform's four named outputs.

Importing this package has no side effects.
"""

from __future__ import annotations

from beam_agents.core.transform import AgentConfig, RunAgent, RunAgentOutputs

__all__ = ["AgentConfig", "RunAgent", "RunAgentOutputs"]
