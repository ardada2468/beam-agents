"""The plain async-agent protocol the runtime drives, and its activation outcomes.

`beam-agents` is a runtime, not a framework: agent authoring belongs to LangGraph,
ADK, or a plain async function. This module defines the *minimum* seam the
stateful DoFn needs to drive one activation — richer adapters (LangGraph
checkpoints, ADK) wrap this same protocol later.

An :class:`Agent` is an async callable invoked once per activation with an
:class:`~beam_agents.core.context.ActivationContext`. It reads/writes working
memory, calls the model, and requests side effects through ``ctx.act(...)``
(never by calling effectful tools directly — correctness invariant 5). It then
returns an :class:`Outcome`:

- :class:`Complete` — the activation finished; ``output`` is emitted on ``.output``
  and any live continuation is cleared.
- :class:`Suspend` — the activation staged one or more intents and is waiting for
  their results/approvals; a :class:`~beam_agents._protos.Continuation` is
  persisted and the HITL timer armed. When the result re-enters on the same key,
  the agent is invoked again with ``ctx.resume`` set and ``ctx.snapshot``
  restored, and is expected to continue from where it left off.

Importing this module has no side effects.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from beam_agents.core.context import ActivationContext

# Fixed namespace for deterministic intent IDs (correctness invariant 2):
# intent_id = uuid5(_INTENT_NAMESPACE, f"{key.hex()}|{seq}|{step_index}"). A
# replayed bundle walking the same path produces byte-identical intent IDs, and
# the effector dedups on them. The namespace is a stable, arbitrary UUID; it must
# never change without a state_schema_version bump.
_INTENT_NAMESPACE = uuid.UUID("6f3e9d2a-1c47-5b8e-9a10-2d4f6b8c0e11")


def intent_id_for(entity_key: bytes, seq: int, step_index: int) -> str:
    """Return the deterministic ``intent_id`` for an activation step.

    Pure function of ``(entity_key, seq, step_index)`` so the same activation,
    replayed, produces the same ID. Never reads a clock or a counter.
    """
    return str(uuid.uuid5(_INTENT_NAMESPACE, f"{entity_key.hex()}|{seq}|{step_index}"))


@dataclass(frozen=True, slots=True)
class Complete:
    """Terminal activation outcome: emit ``output`` and clear any continuation."""

    output: bytes = b""


@dataclass(frozen=True, slots=True)
class Suspend:
    """Suspended activation outcome: persist a continuation and await results.

    ``snapshot``/``adapter`` are the framework-opaque resume state the agent gets
    back (via ``ctx.snapshot``) on the next activation. ``timeout_ms`` is the
    real-time HITL deadline offset from the activation clock; when it elapses the
    HITL timer fires the fallback path (fail-closed, correctness invariant 6).
    """

    snapshot: bytes = b""
    adapter: str = ""
    timeout_ms: int | None = None


Outcome = Complete | Suspend


@dataclass(frozen=True, slots=True)
class FallbackContext:
    """Passed to an agent's HITL fallback when a pending approval/result times out.

    Carries the suspended activation's ``seq`` and the ``snapshot`` the agent
    persisted, so the fallback can emit a deterministic degraded output.
    """

    entity_key: bytes = field(default=b"")
    seq: int = 0
    snapshot: bytes = b""


@runtime_checkable
class Agent(Protocol):
    """An async activation function. Deterministic given its inputs and the
    replay cache, so bundle retries reproduce the same effects.
    """

    async def __call__(self, ctx: ActivationContext) -> Outcome: ...
