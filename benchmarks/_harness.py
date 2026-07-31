"""Shared benchmark harness: agents, FakeLLM scripts, and in-memory handles.

Everything the five benchmark modules need to drive ``_AgentDoFn``'s element
path without a runner: the measurement surface that includes everything the
runtime owns (bridge submission, activation loop, replay cache, staging, coder
encode at state write, commit ordering) and excludes everything it does not
(runner scheduling, bundle formation, shuffle).

The in-memory state/timer handles are deliberately duplicated from
``tests/core/_dofn_fakes.py`` rather than imported (design D1): the test tree
is not a public surface and the benchmarks must not depend on its internals.
The duplication is small and kept honest by the unit-tier smoke tests driving
both.

Agents and provider factories are module-level so they pickle by reference
into the DirectRunner for the ``RunInference`` comparison pipelines.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from beam_agents._protos import (
    AgentEnvelope,
    LlmCacheBlob,
    MemoryBlob,
    ToolIntent,
    ToolResult,
)
from beam_agents.core.agent import Complete, Suspend
from beam_agents.core.dofn import _AgentDoFn
from beam_agents.model.client import LlmRequest
from beam_agents.model.fake import FakeLLM, match_any, respond_with

if TYPE_CHECKING:
    from collections.abc import Callable

    from beam_agents.core.context import ActivationContext

# One key drives every fake-handle benchmark: per-key serialization means a
# single key exercises the whole element path, and cross-key parallelism
# belongs to the runner, which is outside this measurement surface.
KEY = b"bench-key"

# FakeLLM latency tiers (ms) for the overhead benchmark. The 50 ms tier is the
# gated one (densely sampled); 500/2000 exist to prove latency invariance.
TIERS_MS = (50, 500, 2000)
GATED_TIER_MS = 50

# Committed working-memory payload sizes for the state-commit benchmark; the
# last is the documented 100 KiB blob cap.
BLOB_SIZES_KIB = (1, 16, 64, 100)

# Fixed activation clock/deadlines: event time is frozen per activation by
# design, so any positive value works; the suspend deadline just has to
# outlive the scripted resume's event time.
EVENT_TIME_MS = 1_000
RESUME_TIME_MS = 2_000
SUSPEND_TIMEOUT_MS = 60_000

RESPONSE = b"pong"


# -- in-memory state/timer handles (duplicated from tests/core/_dofn_fakes.py) --


class FakeValue:
    """Stand-in for a ``ReadModifyWriteStateSpec`` handle."""

    def __init__(self, value: Any = None) -> None:
        self.value = value

    def read(self) -> Any:
        return self.value

    def write(self, value: Any) -> None:
        self.value = value

    def clear(self) -> None:
        self.value = None


class FakeBag:
    """Stand-in for a ``BagStateSpec`` handle."""

    def __init__(self) -> None:
        self.items: list[ToolIntent] = []

    def read(self) -> list[ToolIntent]:
        return list(self.items)

    def add(self, item: ToolIntent) -> None:
        self.items.append(item)

    def clear(self) -> None:
        self.items = []


class FakeSum:
    """Stand-in for the ``SEQ`` combining-value handle (sum accumulator)."""

    def __init__(self) -> None:
        self.value = 0

    def read(self) -> int:
        return self.value

    def add(self, n: int) -> None:
        self.value += n

    def clear(self) -> None:
        self.value = 0


class FakeTimer:
    """Stand-in for a ``TimerParam`` handle."""

    def __init__(self) -> None:
        self.set_to: Any = None

    def set(self, ts: Any) -> None:
        self.set_to = ts

    def clear(self) -> None:
        self.set_to = None


@dataclass(slots=True)
class Handles:
    """One key's state and timer handles, in ``process()`` keyword order."""

    memory: FakeValue = field(default_factory=lambda: FakeValue(MemoryBlob()))
    continuation: FakeValue = field(default_factory=FakeValue)
    llm_cache: FakeValue = field(default_factory=lambda: FakeValue(LlmCacheBlob()))
    pending: FakeBag = field(default_factory=FakeBag)
    seq: FakeSum = field(default_factory=FakeSum)
    ttl_timer: FakeTimer = field(default_factory=FakeTimer)
    hitl_timer: FakeTimer = field(default_factory=FakeTimer)

    def kwargs(self) -> dict[str, Any]:
        return {
            "memory": self.memory,
            "continuation": self.continuation,
            "llm_cache": self.llm_cache,
            "pending": self.pending,
            "seq": self.seq,
            "ttl_timer": self.ttl_timer,
            "hitl_timer": self.hitl_timer,
        }


def fresh_handles() -> Handles:
    return Handles()


# -- FakeLLM scripts -------------------------------------------------------------


def bench_request(payload: bytes) -> LlmRequest:
    """The one request shape both ``RunAgent`` and the ``RunInference``
    handler build, so the comparison holds the model work constant.
    """
    return LlmRequest(
        model_id="bench",
        messages=[payload.decode("utf-8", errors="replace") or "ping"],
        tools_schema=None,
        sampling_params=None,
    )


def zero_latency_provider() -> FakeLLM:
    """Module-level zero-arg factory: picklable into ``AgentConfig``."""
    return FakeLLM([(match_any(), respond_with(RESPONSE))])


def tier_provider_factory(latency_ms: int) -> Callable[[], FakeLLM]:
    """A provider factory whose single behavior sleeps ``latency_ms`` through
    ``FakeLLM``'s default real-sleep delay — the bridge's genuine wait path.
    """

    def factory() -> FakeLLM:
        return FakeLLM([(match_any(), respond_with(RESPONSE, latency_ms=latency_ms))])

    return factory


# -- module-level agents ----------------------------------------------------------


async def noop_agent(ctx: ActivationContext) -> Complete:
    """No model call, no tool, no memory write: the runtime's ceiling."""
    return Complete(output=b"ok")


async def single_call_agent(ctx: ActivationContext) -> Complete:
    """One model call built from the event payload; the overhead-tier agent."""
    response = await ctx.call_model(bench_request(ctx.single_event))
    return Complete(output=response.response)


async def suspending_agent(ctx: ActivationContext) -> Complete | Suspend:
    """Stage a side-effect intent and suspend; complete on the admitted resume."""
    if not ctx.is_resume:
        ctx.act("bench.effect", '{"n":1}', ttl_ms=SUSPEND_TIMEOUT_MS)
        return Suspend(snapshot=b"waiting", adapter="bench", timeout_ms=SUSPEND_TIMEOUT_MS)
    return Complete(output=b"resumed")


async def memory_write_agent(ctx: ActivationContext) -> Complete:
    """Write the event payload into working memory, so the committed blob size
    is the envelope's payload size (plus small framing).
    """
    ctx.memory.set("payload", ctx.single_event)
    return Complete(output=b"ok")


# -- DoFn construction and driving ------------------------------------------------


def make_dofn(agent: Any, *, latency_ms: int = 0) -> _AgentDoFn:
    """An ``_AgentDoFn`` over the given agent with a scripted ``FakeLLM``,
    already ``setup()`` (bridge thread started). Callers own ``teardown()``;
    benchmark worker processes simply exit (the bridge thread is a daemon).
    """
    provider = zero_latency_provider if latency_ms == 0 else tier_provider_factory(latency_ms)
    dofn = _AgentDoFn(agent, provider_factory=provider)
    dofn.setup()
    return dofn


def event_envelope(payload: bytes = b"go") -> AgentEnvelope:
    return AgentEnvelope(entity_key=KEY, event_time_ms=EVENT_TIME_MS, external_event=payload)


def tool_result_envelope(intent_id: str) -> AgentEnvelope:
    envelope = AgentEnvelope(entity_key=KEY, event_time_ms=RESUME_TIME_MS)
    envelope.tool_result.CopyFrom(
        ToolResult(intent_id=intent_id, entity_key=KEY, payload=b"done", status=ToolResult.OK)
    )
    return envelope


def drain(dofn: _AgentDoFn, envelope: AgentEnvelope, handles: Handles) -> list[object]:
    """One full ``process()`` drain: bridge submission, activation, commit."""
    return list(dofn.process((KEY, envelope), **handles.kwargs()))


# Re-exported for the modules' type hints and the smoke tests.
__all__ = [
    "BLOB_SIZES_KIB",
    "EVENT_TIME_MS",
    "GATED_TIER_MS",
    "KEY",
    "RESPONSE",
    "SUSPEND_TIMEOUT_MS",
    "TIERS_MS",
    "FakeBag",
    "FakeSum",
    "FakeTimer",
    "FakeValue",
    "Handles",
    "bench_request",
    "drain",
    "event_envelope",
    "fresh_handles",
    "make_dofn",
    "memory_write_agent",
    "noop_agent",
    "single_call_agent",
    "suspending_agent",
    "tier_provider_factory",
    "tool_result_envelope",
    "zero_latency_provider",
]
