"""The agent-authoring runtime surface: `StreamAgent`, `AgentContext`,
`AgentResult`, and `FunctionAgent`.

This is the capability's public surface (mirroring how `model`, `memory`, and
`tools` each export their own capability's names from their package root);
root `beam_agents/__init__.py` stays empty until `RunAgent`/`AgentConfig`
exist to anchor it (see `tests/test_import.py::test_public_surface_is_empty`).
Everything else under `core/` (e.g. `coders.py`) remains internal.

Importing this package has no side effects.
"""

from beam_agents.core.agent import FunctionAgent, StreamAgent
from beam_agents.core.context import AgentContext, AgentResult

__all__ = ["AgentContext", "AgentResult", "FunctionAgent", "StreamAgent"]
