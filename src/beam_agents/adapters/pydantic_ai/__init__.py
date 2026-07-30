"""The Pydantic AI adapter: run a `pydantic_ai.Agent` as a beam-agents agent.

Requires the ``pydantic-ai`` extra (``pip install 'beam-agents[pydantic-ai]'``);
this package is the only place Pydantic AI is imported at module scope. See the
change design (``openspec/changes/add-pydantic-ai-adapter/design.md``) for the
load-bearing decisions: the adapter targets the runtime ``Agent`` protocol
directly with one ``Agent.run`` segment per activation (D1), message history
lives latest-only under the reserved ``__pydantic_ai__/`` memory namespace
(D2), deferred tool calls map to intents with one suspension covering all of
them (D3), runtime tools ride ``BeamToolset`` — read-only inline through
``run_tool``, side effects external, approvals gated (D4) — and recognized
httpx-backed models are routed through the replay-cached model path with a
warning fallback for the rest (D5).
"""

from __future__ import annotations

from beam_agents.adapters.pydantic_ai.agent import PydanticAIAgent
from beam_agents.adapters.pydantic_ai.toolset import BeamToolset

__all__ = [
    "BeamToolset",
    "PydanticAIAgent",
]
