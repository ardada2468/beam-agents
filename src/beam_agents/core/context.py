"""Activation-scoped contexts: the surfaces an agent mutates for one element.

Two context classes coexist here, one per authoring/runtime contract:

- :class:`AgentContext` (add-agent-context capability): the rich surface a
  ``StreamAgent`` is handed. It owns memory, replay-cache, the model
  :class:`~beam_agents.model.facade.LlmFacade` (built with ``staging=self`` so
  the context is structurally the facade's ``StagingSink``), read-only tool
  execution via :class:`~beam_agents.tools.runner.ToolRunner`, and ``emit`` for
  outputs. Everything stages in-process and is snapshotted exactly once by
  :meth:`AgentContext.drain` into a frozen :class:`AgentResult`.

- :class:`ActivationContext` (stateful-dofn-runtime capability): the leaner
  surface the stateful DoFn's loop driver constructs per element. It stages
  memory writes, replay-cache inserts, ``ToolIntent``s (``act``), traces, and a
  cache-first ``call_model``; the DoFn reads the staged blobs/intents/traces back
  and commits them atomically only on activation success.

Both are effect-staging surfaces: nothing touches Beam state directly, and every
non-determinism source (``now_ms``, ``rng``, ``sleep``) is injected so a replayed
bundle behaves identically (correctness invariant 1).

Importing this module has no side effects.
"""

from __future__ import annotations

import json
import random
from collections.abc import Mapping
from dataclasses import dataclass

from beam_agents._protos import (
    Continuation,
    LlmCacheBlob,
    MemoryBlob,
    ToolIntent,
    ToolResult,
    TraceEvent,
)
from beam_agents.core.agent import intent_id_for
from beam_agents.memory.facade import Compactor, Memory
from beam_agents.model.client import LLMClient, LlmRequest, LlmResponse
from beam_agents.model.facade import (
    CircuitBreaker,
    Decode,
    LlmFacade,
    RetryPolicy,
    Sleep,
    TokenUsage,
)
from beam_agents.model.replay_cache import ReplayCache, compute_cache_key
from beam_agents.tools.registry import ToolRegistry
from beam_agents.tools.runner import ToolRunner


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
        invariant 5). `intent_id` is `intent_id_for(entity_key, seq,
        step_index)` -- the same function `ActivationContext.act` uses -- so a
        replayed activation that issues the same sequence of `act` calls
        produces byte-identical intents, and the two context surfaces mint the
        same ID for the same `(entity_key, seq, step_index)`.
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
        intent_id = intent_id_for(self._entity_key, self._seq, step_index)
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


class ActivationContext:
    """Per-activation staging surface handed to the runtime driver's agent.

    Constructed by the loop driver from the loaded ``MemoryBlob``/``LlmCacheBlob``
    and the activation's ``seq``, ``now_ms``, resume payload, and snapshot.
    Implements :class:`~beam_agents.model.facade.StagingSink` so the model layer
    can stage trace/usage effects through the same object.
    """

    def __init__(
        self,
        *,
        entity_key: bytes,
        seq: int,
        now_ms: int,
        provider: LLMClient,
        memory_blob: MemoryBlob | None,
        cache_blob: LlmCacheBlob | None,
        event: bytes = b"",
        resume_result: ToolResult | None = None,
        resume_approval: object | None = None,
        snapshot: bytes = b"",
        compactor: Compactor | None = None,
    ) -> None:
        self.entity_key = entity_key
        self.seq = seq
        self.now_ms = now_ms
        self.event = event
        self.snapshot = snapshot
        self.resume_result = resume_result
        self.resume_approval = resume_approval

        self._provider = provider
        self._memory = Memory(memory_blob, now_ms=now_ms, compactor=compactor)
        self._replay_cache = ReplayCache(cache_blob, now_ms=now_ms)

        self._step_index = 0
        self._intents: list[ToolIntent] = []
        self._traces: list[TraceEvent] = []

    # -- agent-facing API -----------------------------------------------------

    @property
    def memory(self) -> Memory:
        """Working-memory facade; writes are staged until commit."""
        return self._memory

    @property
    def is_resume(self) -> bool:
        """True when this activation resumes a suspended one (result/approval in)."""
        return self.resume_result is not None or self.resume_approval is not None

    async def call_model(self, request: LlmRequest) -> LlmResponse:
        """Cache-first model call: a live cache hit returns with zero provider
        calls (correctness invariant 3), so bundle retries never re-hit the
        provider. A miss calls the provider and stages the response in the
        replay cache. Each call advances ``step_index``.
        """
        step = self._advance_step()
        cache_key = compute_cache_key(
            request.model_id,
            request.messages,
            request.tools_schema,
            request.sampling_params,
            self.entity_key,
            self.seq,
        )
        cached = self._replay_cache.get(cache_key)
        if cached is not None and not cached.digest_only:
            self._stage_llm_trace(step, cache_hit=True, model_id=request.model_id)
            return LlmResponse(cached.response)

        response = await self._provider.complete(request)
        self._replay_cache.put(cache_key, response.response)
        self._stage_llm_trace(step, cache_hit=False, model_id=request.model_id)
        return response

    def act(self, tool_name: str, args_json: str, *, ttl_ms: int) -> str:
        """Stage a side-effect ``ToolIntent`` and return its deterministic ID.

        This is the ONLY effect path (correctness invariant 5): the intent is
        emitted on ``.intents`` at commit and its result re-enters on the same
        key. ``intent_id`` is a pure function of ``(key, seq, step_index)``.
        """
        step = self._advance_step()
        intent_id = intent_id_for(self.entity_key, self.seq, step)
        self._intents.append(
            ToolIntent(
                intent_id=intent_id,
                entity_key=self.entity_key,
                seq=self.seq,
                step_index=step,
                tool_name=tool_name,
                args_json=args_json,
                created_at_ms=self.now_ms,
                expires_at_ms=self.now_ms + ttl_ms,
                attempt=0,
            )
        )
        return intent_id

    def stage_trace(self, event: TraceEvent) -> None:
        """Stage a trace event for emission on ``.traces`` at commit."""
        self._traces.append(event)

    # -- StagingSink protocol (model facade) ----------------------------------

    def stage_trace_event(self, event: TraceEvent) -> None:
        self._traces.append(event)

    def accumulate_usage(self, usage: object) -> None:  # pragma: no cover - thin sink
        # Usage accounting is folded into trace attributes by the model layer;
        # the runtime keeps no separate usage tally in this change.
        return None

    # -- loop-driver read-back ------------------------------------------------

    @property
    def staged_intents(self) -> list[ToolIntent]:
        return self._intents

    @property
    def staged_traces(self) -> list[TraceEvent]:
        return self._traces

    @property
    def step_index(self) -> int:
        return self._step_index

    def memory_blob(self) -> MemoryBlob:
        return self._memory.to_blob()

    def cache_blob(self) -> LlmCacheBlob:
        return self._replay_cache.to_blob()

    def build_continuation(
        self, *, snapshot: bytes, adapter: str, deadline_ms: int
    ) -> Continuation:
        """Assemble the ``Continuation`` persisted when the activation suspends."""
        return Continuation(
            state_schema_version=1,
            seq=self.seq,
            step_index=self._step_index,
            pending_intent_ids=[intent.intent_id for intent in self._intents],
            adapter=adapter,
            snapshot=snapshot,
            suspended_at_ms=self.now_ms,
            deadline_ms=deadline_ms,
        )

    # -- internals ------------------------------------------------------------

    def _advance_step(self) -> int:
        step = self._step_index
        self._step_index += 1
        return step

    def _stage_llm_trace(self, step: int, *, cache_hit: bool, model_id: str) -> None:
        self._traces.append(
            TraceEvent(
                entity_key=self.entity_key,
                seq=self.seq,
                step_index=step,
                event_type=TraceEvent.LLM_CALL,
                attributes={
                    "gen_ai.request.model": model_id,
                    "beam_agents.cache_hit": "true" if cache_hit else "false",
                },
                start_ms=self.now_ms,
                end_ms=self.now_ms,
            )
        )
