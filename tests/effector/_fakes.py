"""Fakes for driving the effector loop offline.

The crash injector is the important one: "complete before publish" and "commit
after publish" are only real properties if a process can be killed *between*
those phases and the next delivery still lands correctly. Wrapping the dedup
store and the sinks lets a test do exactly that, with no docker and no signals.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

from beam_agents._protos import ToolIntent, ToolResult
from beam_agents.effector.config import EffectorConfig
from beam_agents.effector.dedup import ClaimOutcome, DedupStore, InMemoryDedupStore
from beam_agents.effector.service import EffectorService
from beam_agents.effector.sinks import InMemoryMessageSink, InMemoryResultSink
from beam_agents.effector.sources import DeliveredIntent, InMemoryIntentSource
from beam_agents.tools import ToolRegistry

NOW_MS = 1_700_000_000_000


class InjectedCrash(BaseException):
    """Stands in for a process kill at a named phase boundary.

    Deliberately a `BaseException`, not an `Exception`: a kill is not something
    the service may catch, retry, or turn into a status. Deriving from
    `Exception` would let the publish-retry wrapper swallow it and republish,
    which would test the retry path rather than the crash path.
    """


@dataclass
class RecordingDedupStore:
    """Wraps a `DedupStore`, recording calls and optionally crashing after one.

    ``crash_after`` names the phase to die *after*: ``"claim"`` or
    ``"complete"``. The wrapped call still takes effect, exactly as a kill
    landing after the RPC committed would leave it.
    """

    inner: DedupStore
    crash_after: str | None = None
    calls: list[str] = field(default_factory=list)

    def _maybe_crash(self, phase: str) -> None:
        if self.crash_after == phase:
            raise InjectedCrash(f"killed after {phase}")

    async def claim(self, intent_id: str, lease_ms: int) -> ClaimOutcome:
        self.calls.append("claim")
        outcome = await self.inner.claim(intent_id, lease_ms)
        self._maybe_crash("claim")
        return outcome

    async def complete(
        self, intent_id: str, token: str, result: ToolResult | None, ttl_ms: int
    ) -> bool:
        self.calls.append("complete")
        stored = await self.inner.complete(intent_id, token, result, ttl_ms)
        self._maybe_crash("complete")
        return stored

    async def release(self, intent_id: str, token: str) -> bool:
        self.calls.append("release")
        return await self.inner.release(intent_id, token)

    async def close(self) -> None:
        self.calls.append("close")
        await self.inner.close()


@dataclass
class CrashingResultSink:
    """A result sink that records the publish and then dies.

    Models a kill landing after the result reached the broker but before the
    offset was committed — the case that must redeliver, not lose.
    """

    inner: InMemoryResultSink = field(default_factory=InMemoryResultSink)
    crash: bool = True

    async def publish(self, result: ToolResult) -> None:
        await self.inner.publish(result)
        if self.crash:
            raise InjectedCrash("killed after publish")

    async def close(self) -> None:
        await self.inner.close()

    @property
    def published(self) -> list[ToolResult]:
        return self.inner.published


def a_config(**overrides: object) -> EffectorConfig:
    defaults: dict[str, object] = {
        "intents_from": "kafka://localhost:9092/intents",
        "results_to": "kafka://localhost:9092/results",
        "approvals_to": "kafka://localhost:9092/approvals",
        "dedup": "memory://",
        "consumer_group": "effector",
        "lease_ms": 60_000,
        "result_ttl_ms": 600_000,
        "tool_timeout_ms": 1_000,
        "in_flight_backoff_ms": 1,
        "in_flight_backoff_max_ms": 2,
        "publish_backoff_ms": 1,
    }
    defaults.update(overrides)
    return EffectorConfig(**defaults)  # type: ignore[arg-type]


def an_intent(
    intent_id: str = "intent-1",
    *,
    tool_name: str = "charge",
    args_json: str = '{"amount_cents":100}',
    entity_key: bytes = b"customer-7",
    seq: int = 3,
    step_index: int = 0,
    kind: ToolIntent.Kind = ToolIntent.TOOL,
    expires_at_ms: int = NOW_MS + 60_000,
) -> ToolIntent:
    return ToolIntent(
        intent_id=intent_id,
        entity_key=entity_key,
        seq=seq,
        step_index=step_index,
        tool_name=tool_name,
        args_json=args_json,
        created_at_ms=NOW_MS - 1_000,
        expires_at_ms=expires_at_ms,
        kind=kind,
    )


@dataclass
class Harness:
    """A wired-up service plus the fakes it runs against."""

    service: EffectorService
    source: InMemoryIntentSource
    results: InMemoryResultSink
    approvals: InMemoryMessageSink
    dedup: DedupStore
    calls: list[tuple[str, object]]
    dead_letters: InMemoryMessageSink | None = None

    @property
    def statuses(self) -> list[ToolResult.Status]:
        return self.results.statuses

    @property
    def committed_intent_ids(self) -> list[str]:
        return self.source.committed_intent_ids


def build_harness(
    *,
    registry: ToolRegistry,
    deliveries: Iterable[DeliveredIntent] | None = None,
    intents: Iterable[ToolIntent] | None = None,
    dedup: DedupStore | None = None,
    result_sink: object | None = None,
    config: EffectorConfig | None = None,
    clock: Callable[[], int] = lambda: NOW_MS,
    **service_kwargs: object,
) -> Harness:
    """Wire a service against in-memory collaborators."""
    source = InMemoryIntentSource()
    if intents is not None:
        source = InMemoryIntentSource.of(intents)
    if deliveries is not None:
        source.deliveries.extend(deliveries)
    results = result_sink if result_sink is not None else InMemoryResultSink()
    approvals = InMemoryMessageSink()
    store = dedup if dedup is not None else InMemoryDedupStore(clock=clock)
    service = EffectorService(
        config=config or a_config(),
        registry=registry,
        source=source,
        result_sink=results,  # type: ignore[arg-type]
        approval_sink=approvals,
        dedup=store,
        clock=clock,
        **service_kwargs,  # type: ignore[arg-type]
    )
    return Harness(
        service=service,
        source=source,
        results=results,  # type: ignore[arg-type]
        approvals=approvals,
        dedup=store,
        calls=[],
        dead_letters=service_kwargs.get("dead_letter_sink"),  # type: ignore[arg-type]
    )
