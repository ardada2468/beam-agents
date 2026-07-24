"""Agent-authoring contracts and the runtime's activation outcomes.

This module carries two complementary surfaces:

- The **authoring** contract (:class:`StreamAgent` + :class:`FunctionAgent`): the
  structural, ``runtime_checkable`` protocol adapters (langgraph, adk,
  pydantic_ai) and hand-written agents satisfy without inheriting a base class
  (see ``openspec/changes/add-agent-context/design.md`` D5). A ``StreamAgent``
  does all its work through the richer :class:`~beam_agents.core.context.AgentContext`.

- The **runtime driver** contract (:class:`Agent` + :class:`Complete`/:class:`Suspend`):
  the minimum seam the stateful DoFn drives per activation over an
  :class:`~beam_agents.core.context.ActivationContext`. The agent reads/writes
  working memory, calls the model, requests side effects via ``ctx.act(...)``
  (correctness invariant 5), and returns an :class:`Outcome` — ``Complete``
  (emit output, clear any continuation) or ``Suspend`` (persist a
  ``Continuation`` and arm the HITL timer; the agent is re-invoked with
  ``ctx.resume``/``ctx.snapshot`` when the result re-enters on the same key).

Importing this module has no side effects.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    # Type-only imports: keep this module runtime-independent of context.py so
    # context.py can import `intent_id_for` from here without a cycle.
    from beam_agents.core.context import ActivationContext, AgentContext

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


# -- Authoring contract (add-agent-context capability) --------------------------


@runtime_checkable
class StreamAgent(Protocol):
    """The agent-authoring runtime contract: one async entry point.

    An activation performs all of its work through `ctx` — reading and
    writing memory, calling the model, running read-only tools, staging side
    effects via `ctx.act(...)`, and emitting outputs via `ctx.emit(...)`.
    This is a runtime contract only; it defines no prompt templating or
    orchestration DSL.
    """

    async def activate(self, ctx: AgentContext) -> None: ...


class FunctionAgent:
    """Adapts a plain `async def fn(ctx) -> None` into a `StreamAgent`."""

    def __init__(self, fn: Callable[[AgentContext], Awaitable[None]]) -> None:
        self._fn = fn

    async def activate(self, ctx: AgentContext) -> None:
        await self._fn(ctx)


# -- Runtime driver contract (stateful-dofn-runtime capability) -----------------


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
