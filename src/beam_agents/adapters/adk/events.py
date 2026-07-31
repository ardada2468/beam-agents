"""The event-stream tee: ADK run events projected onto the activation trace.

Only *deterministic projections* onto the existing ``TraceEvent`` vocabulary
are staged (change design D7):

- **Inline tool executions** (the shim's read-only tools) stage ``TOOL_CALL``
  events built with ``ActivationTrace.tool_call`` — its dedicated
  ``tool_index`` counter exists precisely so tool spans never perturb the
  intent step cursor — enriched with the ``beam_agents.adapter`` attribute.
- **Model turns** need no tee here: they surface as ``LLM_CALL`` events on the
  one existing ``call_model`` path via the transport hook.
- **Suspensions and intents** likewise: ``INTENT_EMITTED``/``SUSPENDED`` are
  staged by ``ctx.act``/``ctx.request_approval`` and the loop driver.

Determinism rules are strict: staged events use the activation clock and
per-activation counters only. ADK event ids, timestamps, and invocation ids
are wall-clock/random and are **never** copied into trace bytes — a replayed
bundle must emit byte-identical traces. Non-tool, non-model ADK events (agent
transfers in multi-agent trees, escalations) have no honest home in the
current closed ``EventType`` enum and are deferred to the additive
``ADAPTER_EVENT`` follow-up (design D7 / Open Questions); they are persisted
faithfully in the session but not traced.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import TYPE_CHECKING

from beam_agents.observability.traces import ADAPTER

if TYPE_CHECKING:
    from beam_agents.core.context import ActivationContext

#: The `beam_agents.adapter` attribute value this adapter stamps.
_ADAPTER_NAME = "adk"


class _TraceTee:
    """One activation's tee: owns the adapter-side ``tool_index`` counter so
    every inline execution gets a distinct, deterministic tool span."""

    __slots__ = ("_ctx", "_tool_index")

    def __init__(self, ctx: ActivationContext) -> None:
        self._ctx = ctx
        self._tool_index = 0

    def tool_call(self, tool_name: str) -> None:
        event = self._ctx.trace.tool_call(
            step_index=self._ctx.step_index,
            tool_index=self._tool_index,
            tool_name=tool_name,
        )
        event.attributes[ADAPTER] = _ADAPTER_NAME
        self._ctx.stage_trace(event)
        self._tool_index += 1


#: The tee of the activation currently driving the ADK run; set by `AdkAgent`
#: around `run_async` (same contextvar pattern as the transport).
_current_tee: ContextVar[_TraceTee | None] = ContextVar("beam_agents_adk_tee", default=None)


def _stage_tool_call(tool_name: str) -> None:
    """Stage a ``TOOL_CALL`` event for an inline shim execution.

    Outside an activation (a shim tool exercised in a plain ADK runner) there
    is no trace surface; the execution is read-only by declaration, so it
    proceeds untraced rather than failing.
    """
    tee = _current_tee.get()
    if tee is not None:
        tee.tool_call(tool_name)
