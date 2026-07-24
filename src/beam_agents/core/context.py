"""The staged activation context — the only surface an agent mutates.

Every effect an activation produces (memory writes, replay-cache inserts, tool
intents, traces, model calls) is staged on this object and *never* touches Beam
state directly. The stateful DoFn loads the keyed state blobs, constructs one
``ActivationContext``, runs the agent against it, and — only if the activation
succeeds — commits the staged blobs/intents/traces to Beam state atomically
(correctness invariant 1). A failed or timed-out activation is discarded whole,
so it mutates nothing.

Non-determinism is injected, never read: ``now_ms`` comes from the element's
event time, so a replayed bundle stamps identical LRU/TTL times and recomputes
identical cache keys and intent IDs.

Importing this module has no side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

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
from beam_agents.model.client import LlmRequest, LlmResponse
from beam_agents.model.replay_cache import ReplayCache, compute_cache_key

if TYPE_CHECKING:
    from beam_agents.model.client import LLMClient

# Approval is a nested message on AgentEnvelope; alias its type for the resume
# payload union without importing the generated symbol at module scope.
Approval = "AgentEnvelope.Approval"


class ActivationContext:
    """Per-activation staging surface handed to the agent.

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
