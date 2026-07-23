"""Activation-scoped `AgentContext` and the immutable `AgentResult` it drains.

See :mod:`beam_agents.core` for the capability overview and the change design
(``openspec/changes/add-agent-context/design.md``) for the load-bearing
decisions: context-owns-facade-construction from an injected provider so the
context is structurally the model facade's `StagingSink` (D1), a single
internal accumulator drained exactly once into a frozen `AgentResult` (D2),
`step_index`-anchored deterministic `intent_id`s (D3), and delegating
read-only tool execution to the existing `ToolRunner` guard (D4).
"""

from __future__ import annotations

import json
import random
import uuid
from collections.abc import Mapping
from dataclasses import dataclass

from beam_agents._protos import LlmCacheBlob, MemoryBlob, ToolIntent, TraceEvent
from beam_agents.memory import Memory
from beam_agents.model.client import LLMClient
from beam_agents.model.facade import (
    CircuitBreaker,
    Decode,
    LlmFacade,
    RetryPolicy,
    Sleep,
    TokenUsage,
)
from beam_agents.model.replay_cache import ReplayCache
from beam_agents.tools.registry import ToolRegistry
from beam_agents.tools.runner import ToolRunner

# Fixed namespace for `uuid5(NAMESPACE, entity_key + seq + step_index)` intent
# IDs (correctness invariant 2): a stable constant so the effector and any
# offline recomputation derive the same IDs as the running pipeline.
INTENT_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, "beam-agents.dev/tool-intent")


@dataclass(frozen=True, slots=True)
class AgentResult:
    """Immutable bundle drained from one activation's `AgentContext`.

    Produced only by :meth:`AgentContext.drain`; a plain value with no
    behavior that mutates keyed state. The owning DoFn commits it atomically
    with the Beam bundle.
    """

    outputs: tuple[object, ...]
    intents: tuple[ToolIntent, ...]
    traces: tuple[TraceEvent, ...]
    usage: TokenUsage
    memory_blob: MemoryBlob | None
    cache_blob: LlmCacheBlob | None


class AgentContext:
    """The single activation-scoped surface an agent is handed for one element.

    Owns every effect an activation can produce — memory mutations (via the
    injected `Memory`), replay-cache inserts (via the injected `ReplayCache`),
    `ToolIntent`s (via `act`), `TraceEvent`s and token usage (as the model
    facade's `StagingSink`), and outputs (via `emit`) — and stages all of them
    in-process. Nothing is applied to keyed state or emitted until `drain()`
    is called, and only the activation's owner calls `drain()`, and only after
    the agent's `activate` coroutine returns normally (correctness invariant
    1: a failed or timed-out activation mutates nothing).

    Every non-determinism source (`now_ms`, `rng`, `sleep`) is injected so a
    replayed bundle behaves identically; the context never reads wall-clock
    time, generates un-seeded randomness, or performs Beam state I/O.
    """

    def __init__(
        self,
        *,
        entity_key: bytes,
        seq: int,
        now_ms: int,
        memory: Memory,
        replay_cache: ReplayCache,
        provider: LLMClient,
        rng: random.Random,
        sleep: Sleep,
        breaker: CircuitBreaker,
        retry_policy: RetryPolicy,
        decode: Decode,
        tool_registry: ToolRegistry,
        tool_runner: ToolRunner | None = None,
    ) -> None:
        self._entity_key = entity_key
        self._seq = seq
        self._now_ms = now_ms
        self.memory = memory
        self._replay_cache = replay_cache
        self._tool_registry = tool_registry
        self._tool_runner = tool_runner if tool_runner is not None else ToolRunner()
        # Built with `staging=self`: the context is structurally the facade's
        # StagingSink, so every trace/usage effect the facade stages lands in
        # this same accumulator (design D1).
        self.model = LlmFacade(
            provider,
            replay_cache,
            now_ms=now_ms,
            rng=rng,
            sleep=sleep,
            breaker=breaker,
            retry_policy=retry_policy,
            decode=decode,
            staging=self,
        )

        self._step_index = 0
        self._intents: list[ToolIntent] = []
        self._traces: list[TraceEvent] = []
        self._outputs: list[object] = []
        self._prompt_tokens = 0
        self._completion_tokens = 0
        self._total_tokens = 0
        self._drained = False

    # -- activation scope -------------------------------------------------

    @property
    def entity_key(self) -> bytes:
        return self._entity_key

    @property
    def seq(self) -> int:
        return self._seq

    @property
    def now_ms(self) -> int:
        return self._now_ms

    # -- side effects: the only path is act() ------------------------------

    def act(self, tool_name: str, arguments: Mapping[str, object]) -> None:
        """Stage a `ToolIntent`; the underlying tool is never executed here.

        `tool_name` must resolve to a `side_effect=True` tool (correctness
        invariant 5). `intent_id` is `uuid5(INTENT_NAMESPACE, entity_key +
        seq + step_index)`, so a replayed activation that issues the same
        sequence of `act` calls produces byte-identical intents.
        """
        tool = self._tool_registry.get(tool_name)
        if not tool.side_effect:
            raise ValueError(
                f"tool {tool_name!r} is side_effect=False; call it via run_tool(...) "
                "instead of act(...)"
            )
        step_index = self._step_index
        args_json = json.dumps(
            dict(arguments),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        intent_id = str(
            uuid.uuid5(INTENT_NAMESPACE, f"{self._entity_key.hex()}:{self._seq}:{step_index}")
        )
        self._intents.append(
            ToolIntent(
                intent_id=intent_id,
                entity_key=self._entity_key,
                seq=self._seq,
                step_index=step_index,
                tool_name=tool_name,
                args_json=args_json,
                created_at_ms=self._now_ms,
            )
        )
        self._step_index += 1

    # -- read-only tools ----------------------------------------------------

    async def run_tool(self, tool_name: str, arguments: Mapping[str, object]) -> object:
        """Run a `side_effect=False` tool inline via the injected `ToolRunner`.

        Reuses `ToolRunner.run`'s existing guard: a `side_effect=True` tool
        raises `SideEffectToolError` rather than executing.
        """
        tool = self._tool_registry.get(tool_name)
        return await self._tool_runner.run(tool, arguments)

    # -- outputs --------------------------------------------------------------

    def emit(self, output: object) -> None:
        """Stage an output in emission order; withheld until `drain()`."""
        self._outputs.append(output)

    # -- StagingSink (consumed by the model facade) --------------------------

    def stage_trace_event(self, event: TraceEvent) -> None:
        self._traces.append(event)

    def accumulate_usage(self, usage: TokenUsage) -> None:
        self._prompt_tokens += usage.prompt_tokens
        self._completion_tokens += usage.completion_tokens
        self._total_tokens += usage.total_tokens

    # -- drain ------------------------------------------------------------

    def drain(self) -> AgentResult:
        """Snapshot every staged effect into a frozen `AgentResult`.

        Callable at most once; a second call raises. Only the activation's
        owner calls this, and only after the agent's `activate` returns
        normally.
        """
        if self._drained:
            raise RuntimeError("AgentContext.drain() called more than once")
        self._drained = True
        memory_blob = self.memory.to_blob() if self.memory.dirty else None
        cache_blob = self._replay_cache.to_blob() if self._replay_cache.dirty else None
        usage = TokenUsage(
            prompt_tokens=self._prompt_tokens,
            completion_tokens=self._completion_tokens,
            total_tokens=self._total_tokens,
        )
        return AgentResult(
            outputs=tuple(self._outputs),
            intents=tuple(self._intents),
            traces=tuple(self._traces),
            usage=usage,
            memory_blob=memory_blob,
            cache_blob=cache_blob,
        )
