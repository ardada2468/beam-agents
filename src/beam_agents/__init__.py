"""beam-agents: run AI agents as keyed, stateful, fault-tolerant Beam transforms.

The public entry point is :class:`RunAgent`, which turns an ``Agent`` (a plain
async activation function) into a stateful Beam step with durable keyed memory,
effectively-once side effects via ``ToolIntent``s, and a replay cache.
:class:`AgentConfig` bundles the provider factory, runtime knobs, and optional
sink URIs; :class:`RunAgentOutputs` exposes the transform's four named outputs.

Human-in-the-loop configuration is a :class:`HitlPolicy` on the config: it sets
the suspension timeout and intent TTL, names the approval channel, and decides
what a timed-out suspension does via a pure routing function returning
:class:`Deny`, :class:`Drop`, or :class:`Escalate` for the
:class:`FallbackContext` it is handed.

Importing this package has no side effects.
"""

from __future__ import annotations

from beam_agents.core.agent import FallbackContext
from beam_agents.core.transform import AgentConfig, RunAgent, RunAgentOutputs
from beam_agents.hitl import Deny, Drop, Escalate, HitlPolicy

__all__ = [
    "AdkAgent",
    "AgentConfig",
    "Deny",
    "Drop",
    "Escalate",
    "FallbackContext",
    "HitlPolicy",
    "LangGraphAgent",
    "RunAgent",
    "RunAgentOutputs",
]

# Adapter classes are public API, but their framework dependencies are optional
# extras: resolve them lazily so `import beam_agents` never imports a framework,
# and absence surfaces as an ImportError naming the extra to install.
_LANGGRAPH_DISTRIBUTIONS = ("langgraph", "langchain", "langchain_core")
# ADK imports under the `google` namespace package, which core installs already
# provide (google-cloud dependencies), so the top-level-name check the LangGraph
# branch uses would never match: match the full dotted prefixes instead.
_ADK_MODULE_PREFIXES = ("google.adk", "google.genai")


def __getattr__(name: str) -> object:
    if name == "LangGraphAgent":
        try:
            from beam_agents.adapters.langgraph import LangGraphAgent
        except ModuleNotFoundError as exc:
            if exc.name and exc.name.partition(".")[0] in _LANGGRAPH_DISTRIBUTIONS:
                raise ImportError(
                    "beam_agents.LangGraphAgent requires the LangGraph adapter extra; "
                    "install it with `pip install 'beam-agents[langgraph]'`"
                ) from exc
            raise
        return LangGraphAgent
    if name == "AdkAgent":
        try:
            from beam_agents.adapters.adk import AdkAgent
        except ModuleNotFoundError as exc:
            if exc.name and any(
                exc.name == prefix or exc.name.startswith(f"{prefix}.")
                for prefix in _ADK_MODULE_PREFIXES
            ):
                raise ImportError(
                    "beam_agents.AdkAgent requires the Google ADK adapter extra; "
                    "install it with `pip install 'beam-agents[adk]'`"
                ) from exc
            raise
        return AdkAgent
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
