"""Store lifecycle wiring in the stateful DoFn (memory-facade capability).

Covers the lifecycle clause of "Long-term store access is explicit via
memory.longterm": the store client is built once per DoFn instance in
``setup()`` on the bridge loop, closed in ``teardown()``, and never
constructed when ``AgentConfig.longterm_memory`` is unset.
"""

from __future__ import annotations

import pytest

from beam_agents.core.dofn import _AgentDoFn
from beam_agents.memory.stores import InMemoryMemoryStore, MemoryStore
from tests.core._dofn_helpers import make_pong_provider, seq_agent


def test_setup_builds_the_store_and_teardown_closes_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    closed: list[MemoryStore] = []

    class _ClosableStore(InMemoryMemoryStore):
        async def close(self) -> None:
            closed.append(self)

    built: list[MemoryStore] = []

    def _build(scheme: str, parts: tuple[str, ...]) -> MemoryStore:
        assert scheme == "memory"
        store = _ClosableStore()
        built.append(store)
        return store

    monkeypatch.setattr("beam_agents.core.dofn.build_memory_store", _build)
    dofn = _AgentDoFn(seq_agent, provider_factory=make_pong_provider, longterm_memory="memory://")

    dofn.setup()
    try:
        assert built and dofn._longterm_store is built[0]
    finally:
        dofn.teardown()

    assert closed == built
    assert dofn._longterm_store is None


def test_no_store_is_constructed_when_the_uri_is_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Scenario: Unconfigured pipelines behave exactly as today.
    def _never(scheme: str, parts: tuple[str, ...]) -> MemoryStore:
        raise AssertionError("build_memory_store called with no URI configured")

    monkeypatch.setattr("beam_agents.core.dofn.build_memory_store", _never)
    dofn = _AgentDoFn(seq_agent, provider_factory=make_pong_provider)

    dofn.setup()
    try:
        assert dofn._longterm_store is None
    finally:
        dofn.teardown()
