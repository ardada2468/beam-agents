"""Module-level reference targets for the `yaml-provider` suites.

Every object here is addressable as a ``module:object`` reference and picklable
(module-level, never a closure) — the same constraint a real YAML pipeline's
agents, provider factories, decoders, and HITL routes must satisfy.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import apache_beam as beam

from beam_agents.core.agent import Complete, FallbackContext
from beam_agents.core.context import ActivationContext
from beam_agents.hitl import Drop, Route
from beam_agents.model.client import LlmRequest
from beam_agents.model.fake import FakeLLM, match_any, match_model_id, respond_with
from beam_agents.tools import ToolRegistry, tool

# A non-callable, non-agent module attribute: the "resolved object is not usable
# in its position" arm of the reference grammar.
NOT_AN_AGENT = "definitely-not-an-agent"

MODEL_ID = "fake-model"
PROMPT = "classify"
INTENT_TTL_MS = 60_000


def request() -> LlmRequest:
    """The single request `model_agent` issues, for FakeLLM call assertions."""
    return LlmRequest(model_id=MODEL_ID, messages=[PROMPT], tools_schema=None, sampling_params=None)


async def echo_agent(ctx: ActivationContext) -> Complete:
    """Complete with the event payload upper-cased — no model call."""
    return Complete(output=ctx.event.upper())


async def model_agent(ctx: ActivationContext) -> Complete:
    """Call the model once and complete with ``<payload>:<response>``."""
    response = await ctx.call_model(request())
    return Complete(output=ctx.event + b":" + response.response)


async def triage_agent(ctx: ActivationContext) -> Complete:
    """Call the model once and complete with ``<entity_key>|<payload>:<response>``.

    Carrying the entity key inside the output bytes is how a YAML pipeline keeps
    it: the runtime's main output stream is unkeyed (tasks.md Revision 2), so
    the key that ``key_field`` supplied is only observable if the agent emits it.
    """
    response = await ctx.call_model(request())
    return Complete(output=ctx.entity_key + b"|" + ctx.event + b":" + response.response)


async def acting_agent(ctx: ActivationContext) -> Complete:
    """Stage one side-effecting intent, then complete — populates ``intents``."""
    ctx.act("notify", '{"channel":"ops"}', ttl_ms=INTENT_TTL_MS)
    return Complete(output=b"acted:" + ctx.event)


class ActivateOnlyAgent:
    """A `StreamAgent`-shaped object: not callable, but has ``activate``.

    Pins the structural agent check's second arm (design D2).
    """

    async def activate(self, ctx: object) -> None:  # pragma: no cover - never driven
        raise NotImplementedError


activate_only_agent = ActivateOnlyAgent()


def make_fake_llm() -> FakeLLM:
    """Zero-argument provider factory: answers every request with ``b"ok"``."""
    return FakeLLM([(match_any(), respond_with(b"ok"))])


def make_recording_llm() -> FakeLLM:
    """Fail-closed provider factory for the end-to-end YAML pipeline.

    The single rule matches only `MODEL_ID`, and `FakeLLM` raises
    `UnmatchedRequestError` for anything else — so an activation that produces
    ``escalate`` in its output is proof that this `FakeLLM` matched, recorded,
    and served exactly the model call `model_agent` made.
    """
    return FakeLLM([(match_model_id(MODEL_ID), respond_with(b"escalate"))])


def make_scripted_llm(*, payload: str = "ok", latency_ms: int = 0) -> FakeLLM:
    """Keyword-taking provider factory — the `provider_config` binding target."""
    return FakeLLM([(match_any(), respond_with(payload.encode(), latency_ms=latency_ms))])


def exploding_factory() -> FakeLLM:
    """Provider factory that raises if it is ever called.

    Construction must never invoke the factory (design D3), so a config naming
    this one has to build cleanly.
    """
    raise RuntimeError("the provider factory was invoked at construction time")


def _make_local_factory() -> Callable[[], FakeLLM]:
    def factory() -> FakeLLM:  # pragma: no cover - never called
        return FakeLLM()

    return factory


# Module-level *name*, but not a module-level *object*: its ``__qualname__``
# points inside a function, so `pickle.dumps` cannot round-trip it. The
# construction-time picklability probe must reject a reference to it.
LOCAL_FACTORY = _make_local_factory()


def decode_len(response: bytes) -> tuple[int, int]:
    """A `Decode` stand-in: token counts derived from the response length."""
    return len(response), len(response)


def drop_on_timeout(fallback: FallbackContext) -> Route:
    """A module-level HITL timeout route (picklable, pure), for `hitl.on_timeout`."""
    return Drop()


@tool
def lookup(customer_id: str) -> str:
    """Uppercase a customer id: the read-only tool in `TOOL_REGISTRY`."""
    return customer_id.upper()


def _build_registry() -> ToolRegistry:
    registry = ToolRegistry()
    registry.register(lookup)
    return registry


# A prebuilt, module-level registry: exactly the shape `tool_registry:` names
# (design D4). Built once at import and never mutated afterwards.
TOOL_REGISTRY = _build_registry()


def identity(element: Any) -> Any:
    """Picklable no-op: the body of the stub sink writer below."""
    return element


def stub_sink_resolve(self: Any, field_name: str, uri: str) -> beam.PTransform:
    """Offline stand-in for `DefaultSinkResolver.resolve`.

    Lets a test configure ``intents_to`` (whose URI still goes through the real
    ``validate`` grammar) without importing a Kafka/Pub/Sub client or touching a
    network. Written as a plain function so it can be patched onto the class.
    """
    return beam.Map(identity)
