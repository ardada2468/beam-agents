"""Store lifecycle wiring in the stateful DoFn (memory-facade capability).

Covers the lifecycle clause of "Long-term store access is explicit via
memory.longterm": the store client is built once per DoFn instance in
``setup()`` on the bridge loop, closed in ``teardown()``, and never
constructed when ``AgentConfig.longterm_memory`` is unset.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from beam_agents.core.bridge import AsyncBridge
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


def test_the_parsed_uri_parts_reach_the_store_constructor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # `setup()` parses the URI into `(scheme, parts)` and hands both to the
    # factory; `parts` is where every backend's addressing lives -- the Redis
    # URL, the Bigtable project/instance/table triple, the SQLAlchemy DSN. The
    # `memory://` scheme every other test here uses has *empty* parts, so
    # nothing in this suite could tell a forwarded `parts` from a dropped one:
    # `InMemoryMemoryStore()` ignores the argument entirely and a store built
    # from nothing looks exactly like a store built from the right thing.
    seen: list[tuple[str, tuple[str, ...]]] = []

    def _build(scheme: str, parts: tuple[str, ...]) -> MemoryStore:
        seen.append((scheme, parts))
        return InMemoryMemoryStore()

    monkeypatch.setattr("beam_agents.core.dofn.build_memory_store", _build)
    dofn = _AgentDoFn(
        seq_agent,
        provider_factory=make_pong_provider,
        longterm_memory="redis://cache.internal:6379/2",
    )

    dofn.setup()
    try:
        assert seen == [("redis", ("redis://cache.internal:6379/2",))]
    finally:
        dofn.teardown()


class _RecordingBridge(AsyncBridge):
    """Real bridge that also records the timeout every submission was given."""

    # Class-level because `setup()` constructs the bridge itself: there is no
    # instance for a test to hold before the submission it wants to observe.
    submitted_timeouts: ClassVar[list[float]] = []

    def run(self, coro_factory: Any, timeout_s: float) -> Any:
        type(self).submitted_timeouts.append(timeout_s)
        return super().run(coro_factory, timeout_s)


def test_the_store_lifecycle_submissions_are_bounded_by_the_activation_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Both store-lifecycle submissions run on the bridge, and both must carry
    # the configured budget. An unbounded submission blocks the Beam thread
    # forever against a wedged backend -- at `setup()` the worker never starts
    # processing, at `teardown()` the bundle never finishes -- and neither
    # shows up as anything but a stalled worker. `None` is the shape that
    # matters: `future.result(timeout=None)` waits without limit, so the
    # difference is invisible against any store that answers.
    monkeypatch.setattr(_RecordingBridge, "submitted_timeouts", [])
    monkeypatch.setattr("beam_agents.core.dofn.AsyncBridge", _RecordingBridge)
    monkeypatch.setattr(
        "beam_agents.core.dofn.build_memory_store", lambda scheme, parts: InMemoryMemoryStore()
    )
    dofn = _AgentDoFn(
        seq_agent,
        provider_factory=make_pong_provider,
        longterm_memory="memory://",
        activation_timeout_s=1.5,
    )

    dofn.setup()
    dofn.teardown()

    # One for the store construction, one for its close.
    assert _RecordingBridge.submitted_timeouts == [1.5, 1.5]


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
