"""beam-agents: run AI agents as keyed, stateful, fault-tolerant Beam transforms.

The public entry point is ``beam_agents.core.transform.RunAgent``, which turns an
``Agent`` (a plain async activation function) into a stateful Beam step with
durable keyed memory, effectively-once side effects via ``ToolIntent``s, and a
replay cache. This package root intentionally re-exports nothing; import from the
submodule that owns each symbol.

Importing this package has no side effects.
"""
