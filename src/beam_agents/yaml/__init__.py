"""The Beam YAML surface for ``RunAgent``.

Beam YAML expresses the shape a system-triggered agent pipeline mostly is —
read a topic, key by entity, run the agent, drain the tagged outputs to sinks —
as a declarative document. This package is the thin construction-time adapter
that makes ``RunAgent`` reachable from one: a Python-typed provider is pointed
at the fully-qualified name ``beam_agents.yaml.run_agent`` and hands it the
document's ``config:`` mapping as keyword arguments.

::

    providers:
      - type: python
        config:
          packages: ["beam-agents==1.0.0a1"]
        transforms:
          RunAgent: "beam_agents.yaml.run_agent"

Three things bridge the gap between what YAML can carry and what the runtime
needs (see ``docs/yaml.md`` for the whole surface):

* **References.** An agent is a Python callable, and so are the provider
  factory, the decoder, the HITL route, and the tool registry. YAML *names*
  them with the setuptools entry-point spelling — ``my_pkg.agents:fraud_agent``
  — resolved by import at expansion time, so a typo raises ``ValueError``
  before any pipeline runs. No code from the document is ever evaluated.
* **Config mapping.** Scalar knobs and sink URIs pass through to
  ``AgentConfig`` verbatim; every value check stays in ``AgentConfig``,
  ``HitlPolicy``, and the sink resolver. An unknown key is an error, not a
  default.
* **Row boundary.** YAML pipelines carry schema'd rows, so the transform keys
  and envelopes input rows itself and emits its four outputs — ``output``,
  ``intents``, ``traces``, ``errors`` — as rows, addressable downstream by
  qualified name (``RunAgent.errors``).

Nothing here is an agent-authoring surface: a YAML document names an agent, and
the agent stays Python. This package imports nothing from ``apache_beam.yaml``
— the dependency direction is Beam YAML → us.

Importing this package has no side effects.
"""

from __future__ import annotations

from pathlib import Path

from beam_agents.yaml.transform import run_agent

#: Path to the packaged provider listing: the same ``RunAgent`` →
#: ``beam_agents.yaml.run_agent`` mapping as a standalone file, so a pipeline
#: can pull it in with ``providers: [{include: <path>}]`` instead of copying the
#: block. Ships inside the wheel.
PROVIDER_LISTING: str = str(Path(__file__).with_name("providers.yaml"))

__all__ = ["PROVIDER_LISTING", "run_agent"]
