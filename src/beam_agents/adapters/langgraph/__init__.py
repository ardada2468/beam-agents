"""The LangGraph adapter: run a compiled LangGraph graph as a beam-agents agent.

Requires the ``langgraph`` extra (``pip install 'beam-agents[langgraph]'``);
this package is the only place LangGraph is imported at module scope. See the
change design (``openspec/changes/add-langgraph-adapter/design.md``) for the
load-bearing decisions: the adapter targets the runtime ``Agent`` protocol
directly (D1), checkpoints live latest-only under the reserved ``__langgraph__/``
memory namespace (D2), one suspension covers all pending graph work via a JSON
resume-map snapshot (D4), side-effect tools interrupt instead of executing (D5),
and recognized httpx-backed chat models are routed through the replay-cached
model path with a warning fallback for the rest (D6).
"""

from __future__ import annotations

from beam_agents.adapters.langgraph.agent import LangGraphAgent
from beam_agents.adapters.langgraph.checkpoint import BeamCheckpointSaver
from beam_agents.adapters.langgraph.toolnode import BeamToolNode

__all__ = [
    "BeamCheckpointSaver",
    "BeamToolNode",
    "LangGraphAgent",
]
