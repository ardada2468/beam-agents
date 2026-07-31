"""The Google ADK adapter: run an ADK agent as a beam-agents activation.

Requires the ``adk`` extra (``pip install 'beam-agents[adk]'``); this package is
the only place ADK is imported at module scope. See the change design
(``openspec/changes/add-adk-adapter/design.md``) for the load-bearing decisions:
the adapter targets the runtime ``Agent`` protocol directly and runs the ADK
``Runner`` inside the activation (D1), the session lives one-per-key under the
reserved ``__adk__/`` memory namespace (D2), side-effect tools become long-running
function calls and one suspension covers all pending work (D4/D5), the event
stream is teed onto the existing trace vocabulary with strict determinism rules
(D7), and recognized google-genai clients are routed through the replay-cached
model path with a warning fallback for the rest (D6).
"""

from __future__ import annotations

from beam_agents.adapters.adk.agent import AdkAgent
from beam_agents.adapters.adk.session import BeamSessionService
from beam_agents.adapters.adk.tools import (
    BeamApprovalTool,
    BeamFunctionTool,
    BeamLongRunningTool,
    beam_tools,
)

__all__ = [
    "AdkAgent",
    "BeamApprovalTool",
    "BeamFunctionTool",
    "BeamLongRunningTool",
    "BeamSessionService",
    "beam_tools",
]
