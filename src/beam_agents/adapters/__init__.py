"""Framework adapters: run agents authored elsewhere on the beam-agents runtime.

An adapter wraps a foreign framework's agent (a LangGraph compiled graph, an ADK
agent, ...) as the runtime :class:`~beam_agents.core.agent.Agent` protocol — an
async callable over an :class:`~beam_agents.core.context.ActivationContext`
returning ``Complete | Suspend`` — so the framework's own persistence, HITL, and
tool-execution seams are expressed through the runtime's staged, bundle-atomic
equivalents. There is no adapter base class: the runtime protocol *is* the seam.

Each adapter subpackage owns its framework dependency: this package and the core
runtime import none of them. Importing this module has no side effects.
"""
