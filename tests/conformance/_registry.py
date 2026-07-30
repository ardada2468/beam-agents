"""The conformance adapter registry, the collection-time guard, and the
bundle-equivalence check.

Registration is an explicit list (greppable wiring, design D2); the guard
converts "shipped an adapter subpackage, forgot its conformance factory" into
a loud collection error rather than a silently smaller matrix. The
equivalence check runs on every built bundle before any pipeline does,
pinning each factory to its ``ScenarioSpec`` (tool names and effect classes,
matcher count, deadline) so a factory cannot drift into testing a different
conversation than the one the spec declares.
"""

from __future__ import annotations

import importlib
import pkgutil
from collections.abc import Callable, Collection
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING

from beam_agents.model.fake import FakeLLM
from tests.conformance._adapters.langgraph import build_langgraph_agent, build_langgraph_provider
from tests.conformance._adapters.pydantic_ai import (
    build_pydantic_ai_agent,
    build_pydantic_ai_provider,
)
from tests.conformance._adapters.reference import build_reference_agent, build_reference_provider
from tests.conformance._spec import SCENARIOS_BY_NAME, ScenarioSpec, registry_for

if TYPE_CHECKING:
    from beam_agents.core.agent import Agent, Outcome
    from beam_agents.core.context import ActivationContext
    from beam_agents.tools import ToolRegistry


@dataclass(frozen=True)
class AgentBundle:
    """One adapter's realization of one scenario: a fresh agent instance, its
    fresh scripted provider, the tool registry, and the deadline it wired."""

    agent: Agent
    provider: FakeLLM
    tool_registry: ToolRegistry
    hitl_timeout_ms: int | None


@dataclass(frozen=True)
class ConformanceAdapter:
    """One registered adapter on the conformance matrix's adapter axis."""

    name: str
    #: Importable package gating clean skips (``None`` for the reference agent).
    requires: str | None
    #: The ``beam_agents.adapters`` subpackage this registration covers
    #: (``None`` for the reference agent, which is the runtime protocol itself).
    adapters_subpackage: str | None
    build_agent: Callable[[ScenarioSpec], Agent]
    build_provider: Callable[[ScenarioSpec], FakeLLM]


ADAPTERS: tuple[ConformanceAdapter, ...] = (
    ConformanceAdapter(
        name="reference",
        requires=None,
        adapters_subpackage=None,
        build_agent=build_reference_agent,
        build_provider=build_reference_provider,
    ),
    ConformanceAdapter(
        name="langgraph",
        requires="langgraph",
        adapters_subpackage="langgraph",
        build_agent=build_langgraph_agent,
        build_provider=build_langgraph_provider,
    ),
    ConformanceAdapter(
        name="pydantic_ai",
        requires="pydantic_ai",
        adapters_subpackage="pydantic_ai",
        build_agent=build_pydantic_ai_agent,
        build_provider=build_pydantic_ai_provider,
    ),
)

ADAPTERS_BY_NAME: dict[str, ConformanceAdapter] = {a.name: a for a in ADAPTERS}

#: Every FakeLLM the cell provider factory has built in this process. The
#: DirectRunner is in-process, so summing ``call_count`` over these observes
#: real provider invocations across bundle attempts — including attempts Beam
#: discarded, which committed traces can never show.
LIVE_PROVIDERS: list[FakeLLM] = []


def bundle_for(adapter_name: str, scenario_name: str) -> AgentBundle:
    """Build one adapter's fresh bundle for one scenario (no validation)."""
    adapter = ADAPTERS_BY_NAME[adapter_name]
    spec = SCENARIOS_BY_NAME[scenario_name]
    return AgentBundle(
        agent=adapter.build_agent(spec),
        provider=adapter.build_provider(spec),
        tool_registry=registry_for(spec),
        hitl_timeout_ms=spec.hitl_timeout_ms,
    )


def validate_bundle(bundle: AgentBundle, spec: ScenarioSpec) -> None:
    """The equivalence check: a built bundle must match its spec, or the cell
    is testing a different conversation than the one it claims to."""
    expected_tools = {t.name: t.side_effect for t in spec.tools}
    registered_names = [str(schema["name"]) for schema in bundle.tool_registry.tools_schema]
    actual_tools = {name: bundle.tool_registry.get(name).side_effect for name in registered_names}
    assert actual_tools == expected_tools, (
        f"scenario {spec.name!r}: bundle tool set diverged from the spec — "
        f"expected {expected_tools!r}, built {actual_tools!r}"
    )
    # FakeLLM keeps its script private; the rule count is still the honest
    # "same conversation" signal, so the check reads it directly.
    rule_count = len(bundle.provider._rules)
    assert rule_count == len(spec.turns), (
        f"scenario {spec.name!r}: bundle scripted {rule_count} provider rules, "
        f"spec declares {len(spec.turns)} turns"
    )
    assert bundle.hitl_timeout_ms == spec.hitl_timeout_ms, (
        f"scenario {spec.name!r}: bundle wired hitl_timeout_ms="
        f"{bundle.hitl_timeout_ms!r}, spec declares {spec.hitl_timeout_ms!r}"
    )


def validated_bundle(adapter_name: str, scenario_name: str) -> AgentBundle:
    """Build and equivalence-check a bundle — the only construction path the
    cells use, so no pipeline ever runs an unvalidated bundle."""
    bundle = bundle_for(adapter_name, scenario_name)
    validate_bundle(bundle, SCENARIOS_BY_NAME[scenario_name])
    return bundle


def provider_for(adapter_name: str, scenario_name: str) -> FakeLLM:
    """``AgentConfig.provider_factory`` target: picklable via
    ``functools.partial`` over these two names; records every instance."""
    provider = ADAPTERS_BY_NAME[adapter_name].build_provider(SCENARIOS_BY_NAME[scenario_name])
    LIVE_PROVIDERS.append(provider)
    return provider


def live_provider_calls() -> int:
    """Real provider invocations recorded in this process (cache hits never
    reach the provider, so they are structurally excluded)."""
    return sum(provider.call_count for provider in LIVE_PROVIDERS)


class LazyCellAgent:
    """A pipeline-safe handle on one cell's agent: picklable as the
    ``(adapter, scenario)`` name pair, built (and equivalence-checked)
    worker-side on first activation — the same worker-side-lazy shape as
    ``tests/adapters/test_e2e_pipeline.py``, generalized."""

    def __init__(self, adapter: str, scenario: str) -> None:
        self._adapter = adapter
        self._scenario = scenario
        self._agent: Agent | None = None

    def __reduce__(self) -> tuple[type[LazyCellAgent], tuple[str, str]]:
        return (LazyCellAgent, (self._adapter, self._scenario))

    async def __call__(self, ctx: ActivationContext) -> Outcome:
        if self._agent is None:
            self._agent = validated_bundle(self._adapter, self._scenario).agent
        return await self._agent(ctx)


# -- the collection-time registry guard -------------------------------------------


class UnregisteredAdapterError(Exception):
    """An importable ``beam_agents.adapters`` subpackage has no conformance
    registration: the matrix would silently shrink, so collection must fail."""


def unregistered_adapters(package: ModuleType, registered: Collection[str]) -> list[str]:
    """Importable subpackages of ``package`` missing from ``registered``.

    A subpackage whose import fails with ``ImportError`` is *not* reported:
    its optional framework is absent from this environment, so its cells
    could not run here anyway (they skip via ``requires`` where registered).

    Underscore-prefixed names are *not* adapters: the project's convention
    marks internal modules that way, and ``adapters/_transport.py`` (the
    framework-neutral replay-transport core the adapters share) is one. Only
    public adapter modules carry a conformance obligation.
    """
    missing: list[str] = []
    for module_info in pkgutil.iter_modules(package.__path__):
        name = module_info.name
        if name.startswith("_") or name in registered:
            continue
        try:
            importlib.import_module(f"{package.__name__}.{name}")
        except ImportError:
            continue
        missing.append(name)
    return sorted(missing)


def enforce_registry() -> None:
    """Raise :class:`UnregisteredAdapterError` naming every importable adapter
    subpackage that lacks a registration. Called at conftest import, so a
    violation is a collection error for the whole conformance suite."""
    import beam_agents.adapters

    registered = {a.adapters_subpackage for a in ADAPTERS if a.adapters_subpackage is not None}
    missing = unregistered_adapters(beam_agents.adapters, registered)
    if missing:
        raise UnregisteredAdapterError(
            f"adapter package(s) {missing!r} are importable but have no conformance "
            "registration in tests/conformance/_registry.py — every shipped adapter "
            "must join the conformance matrix (add a ConformanceAdapter entry with "
            "its scenario factories)"
        )
