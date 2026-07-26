"""Human-in-the-loop: approval channels, timeout routing, and the expiry guard.

Correctness invariant 6 requires timeouts to fail closed at **both** layers, and
this module carries the parts of that contract that are pure functions:

- :func:`intent_expired` / :func:`refuse_expired` — the **layer-2** guard an
  effector calls before executing anything. Given a ``ToolIntent`` and a
  current-time value it decides expiry with no I/O, no clock read, and no Beam
  import, so it runs unchanged inside a separate effector process.
- :class:`HitlPolicy` and the :data:`Route` types — the **layer-1** timeout
  routing the stateful DoFn applies when ``HITL_TIMER`` fires over a live
  ``Continuation``.

The routing function (``HitlPolicy.on_timeout``) is a *pure, synchronous*
function of its :class:`~beam_agents.core.agent.FallbackContext`. That is a
correctness requirement, not a style preference: a timer callback re-executes
when its bundle is retried, so a fallback that called the model, read a clock,
or generated un-seeded randomness would make the retry diverge from the
original. Every time value the function could need is carried on the
``FallbackContext``.

Importing this module has no side effects.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from beam_agents._protos import ToolIntent, ToolResult

if TYPE_CHECKING:
    from collections.abc import Callable

    from beam_agents.core.agent import FallbackContext

# Default HITL deadline when a Suspend omits an explicit timeout_ms (24h). Real
# approvals are wall-clock bound; the value only sets when the fail-closed HITL
# timer fires, never how the activation runs.
DEFAULT_HITL_TIMEOUT_MS = 86_400_000

# Default lifetime stamped onto a staged intent's `expires_at_ms` (1h). An
# intent with a non-positive expiry is treated as already expired by every
# consumer, so every staging path must set one.
DEFAULT_INTENT_TTL_MS = 3_600_000

# The `tool_name` an approval request carries: the channel the effector routes
# it to, not a registered tool.
DEFAULT_APPROVAL_CHANNEL = "approval"

# Emitted on the main output when an approval/result never arrives and the
# default (deny) route runs.
HITL_TIMEOUT_OUTPUT = b"__hitl_timeout__"

# Error reason for a timeout routed to `.errors` (the drop route, and the
# fail-closed landing spot for a policy function that raises).
REASON_HITL_TIMEOUT = "hitl_timeout"


# -- layer 2: the effector-side expiry guard -----------------------------------


def intent_expired(intent: ToolIntent, now_ms: int) -> bool:
    """True when ``intent`` may no longer be executed at ``now_ms``.

    The boundary is inclusive — an intent is live *strictly before* its
    ``expires_at_ms``. A non-positive ``expires_at_ms`` reads as **expired**,
    never as unbounded: under invariant 6 the safe reading of "no expiry
    recorded" is "do not execute".
    """
    return intent.expires_at_ms <= 0 or intent.expires_at_ms <= now_ms


def refuse_expired(intent: ToolIntent, now_ms: int) -> ToolResult | None:
    """The refusal ``ToolResult`` for an expired ``intent``, else ``None``.

    An effector calls this before doing anything else and publishes the
    returned result instead of executing. Re-injected on the same key, an
    ``EXPIRED`` result resumes a still-live continuation so the agent can take
    its own degraded path.
    """
    if not intent_expired(intent, now_ms):
        return None
    return ToolResult(
        intent_id=intent.intent_id,
        entity_key=intent.entity_key,
        seq=intent.seq,
        status=ToolResult.EXPIRED,
        error_message=f"intent expired at {intent.expires_at_ms}",
        completed_at_ms=now_ms,
    )


# -- layer 1: timeout routes ---------------------------------------------------


@dataclass(frozen=True, slots=True)
class Deny:
    """Emit deterministic bytes on the main output and end the suspension."""

    output: bytes = HITL_TIMEOUT_OUTPUT


@dataclass(frozen=True, slots=True)
class Drop:
    """Emit nothing on the main output; record the timeout on ``.errors``."""

    reason: str = REASON_HITL_TIMEOUT


@dataclass(frozen=True, slots=True)
class Escalate:
    """Ask again, louder: stage a fresh approval intent and extend the deadline.

    ``tool_name`` is the escalation channel (a pager, a second approver queue).
    ``timeout_ms`` is how much longer to wait, measured from the timer's fire
    time. Bounded by ``HitlPolicy.max_escalations`` — an unbounded escalate
    loop would be a fail-*open* hole, since the point of the timer is that the
    wait ends.
    """

    tool_name: str
    args_json: str = "{}"
    timeout_ms: int = DEFAULT_HITL_TIMEOUT_MS


Route = Deny | Drop | Escalate


def deny(fallback: FallbackContext) -> Route:
    """The default timeout route: deny with the runtime's timeout output."""
    return Deny(HITL_TIMEOUT_OUTPUT)


@dataclass(frozen=True, slots=True)
class HitlPolicy:
    """Configuration for suspensions, approvals, and what a timeout does.

    ``on_timeout`` must be **pure and synchronous** (see the module docstring)
    and picklable — the DoFn holds the policy and serializes for the runner, so
    it must be a module-level function or another picklable callable, never a
    lambda or a closure.
    """

    timeout_ms: int = DEFAULT_HITL_TIMEOUT_MS
    intent_ttl_ms: int = DEFAULT_INTENT_TTL_MS
    approval_channel: str = DEFAULT_APPROVAL_CHANNEL
    max_escalations: int = 0
    on_timeout: Callable[[FallbackContext], Route] = field(default=deny)

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Raise ``ValueError`` naming the offending field, before any pipeline."""
        for name in ("timeout_ms", "intent_ttl_ms"):
            value = getattr(self, name)
            if value <= 0:
                raise ValueError(f"HitlPolicy.{name} must be positive; got {value!r}")
        if self.max_escalations < 0:
            raise ValueError(
                f"HitlPolicy.max_escalations must be >= 0; got {self.max_escalations!r}"
            )
        if not self.approval_channel:
            raise ValueError("HitlPolicy.approval_channel must be a non-empty channel name")
