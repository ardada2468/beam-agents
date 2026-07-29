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
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from beam_agents._protos import (
    AgentEnvelope,
    Continuation,
    LlmCacheBlob,
    MemoryBlob,
    ToolIntent,
    ToolResult,
    TraceEvent,
)
from beam_agents.core.agent import intent_id_for
from beam_agents.hitl import DEFAULT_APPROVAL_CHANNEL, DEFAULT_INTENT_TTL_MS
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
from beam_agents.observability import (
    CACHE_HIT,
    OPERATION_CHAT,
    OPERATION_NAME,
    REQUEST_MODEL,
    ActivationTrace,
    usage_attributes,
)
from beam_agents.observability.metrics import ActivationTally
from beam_agents.tools.registry import ToolRegistry
from beam_agents.tools.runner import ToolRunner

# Injected monotonic clock for duration measurement. Injected like every other
# non-determinism source (`now_ms`, `rng`, `sleep`) so tests script exact
# durations instead of sleeping -- and read ONLY for measurement: no staged
# effect, cache key, intent ID, deadline, or branch depends on the value, so
# replay determinism is untouched. Event time is unusable here: `now_ms` is
# frozen per activation by design, so every duration from it would be zero.
MonotonicNs = Callable[[], int]
_NS_PER_MS = 1_000_000


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
    #: Worker-local metric tally (never persisted). Defaulted so the historical
    #: construction sites keep working.
    tally: ActivationTally = field(default_factory=ActivationTally)


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
        intent_ttl_ms: int = DEFAULT_INTENT_TTL_MS,
        approval_channel: str = DEFAULT_APPROVAL_CHANNEL,
    ) -> None:
        self._entity_key = entity_key
        self._seq = seq
        self._now_ms = now_ms
        self._intent_ttl_ms = intent_ttl_ms
        self._approval_channel = approval_channel
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

        # The activation's trace. Every event staged through this context is
        # stamped with it (design D3), so the facade and the tool path stay
        # ignorant of tracing.
        self._trace = ActivationTrace(entity_key=entity_key, seq=seq, now_ms=now_ms)

        self._step_index = 0
        # Read-only tool calls get a counter of their own: advancing
        # `_step_index` would change the `intent_id`s this activation goes on to
        # mint (design D8).
        self._tool_index = 0
        self._intents: list[ToolIntent] = []
        self._traces: list[TraceEvent] = []
        self._outputs: list[object] = []
        self._prompt_tokens = 0
        self._completion_tokens = 0
        self._total_tokens = 0
        self._tally = ActivationTally()
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
        self._stage_intent(tool_name, arguments, ToolIntent.TOOL, self._intent_ttl_ms)

    def request_approval(
        self,
        arguments: Mapping[str, object],
        *,
        channel: str | None = None,
        ttl_ms: int | None = None,
    ) -> str:
        """Stage a `kind = APPROVAL` intent and return its deterministic ID.

        The approval channel is *not* a registered tool -- it names where the
        effector routes the request (a queue, a pager) -- so no registry lookup
        happens and nothing is executed. Everything else matches `act`: the
        same canonical-JSON encoding, the same `intent_id_for` derivation, and
        the same monotonic step sequence, so an approval and a tool intent
        within one activation can never share an ID.
        """
        return self._stage_intent(
            channel if channel is not None else self._approval_channel,
            arguments,
            ToolIntent.APPROVAL,
            ttl_ms if ttl_ms is not None else self._intent_ttl_ms,
        )

    def _stage_intent(
        self,
        tool_name: str,
        arguments: Mapping[str, object],
        kind: ToolIntent.Kind,
        ttl_ms: int,
    ) -> str:
        step_index = self._step_index
        args_json = json.dumps(
            dict(arguments),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        intent_id = intent_id_for(self._entity_key, self._seq, step_index)
        expires_at_ms = self._now_ms + ttl_ms
        self._intents.append(
            ToolIntent(
                intent_id=intent_id,
                entity_key=self._entity_key,
                seq=self._seq,
                step_index=step_index,
                tool_name=tool_name,
                args_json=args_json,
                created_at_ms=self._now_ms,
                expires_at_ms=expires_at_ms,
                kind=kind,
                # Carries the trace across the one hop the effector breaks:
                # it consumes intents off a topic, so without this the
                # execution can never be joined back to the activation.
                trace_id=self._trace.trace_id,
            )
        )
        self._traces.append(
            self._trace.intent_emitted(
                step_index=step_index,
                intent_id=intent_id,
                tool_name=tool_name,
                intent_kind=ToolIntent.Kind.Name(kind),
                expires_at_ms=expires_at_ms,
            )
        )
        self._step_index += 1
        return intent_id

    # -- read-only tools ----------------------------------------------------

    async def run_tool(self, tool_name: str, arguments: Mapping[str, object]) -> object:
        """Run a `side_effect=False` tool inline via the injected `ToolRunner`.

        Reuses `ToolRunner.run`'s existing guard: a `side_effect=True` tool
        raises `SideEffectToolError` rather than executing.

        Traced as a `TOOL_CALL` child event, but deliberately *not* counted
        against the intent step cursor (design D8).
        """
        tool = self._tool_registry.get(tool_name)
        value = await self._tool_runner.run(tool, arguments)
        # Counted and traced after the call returns, so a refused side-effect
        # tool (or a failing one) is neither counted as an execution nor traced
        # as one. This is the only authoring-surface site where a tool actually
        # runs inside the pipeline; side-effecting tools leave as intents and
        # are counted by `intents_emitted`.
        self._tally.tool_calls += 1
        self._traces.append(
            self._trace.tool_call(
                step_index=self._step_index,
                tool_index=self._tool_index,
                tool_name=tool_name,
            )
        )
        self._tool_index += 1
        return value

    # -- outputs --------------------------------------------------------------

    def emit(self, output: object) -> None:
        """Stage an output in emission order; withheld until `drain()`."""
        self._outputs.append(output)

    # -- StagingSink (consumed by the model facade) --------------------------

    def stage_trace_event(self, event: TraceEvent) -> None:
        """Correlate the event against this activation's trace, then stage it.

        Correlation happens here rather than in the producer (design D3): only
        empty identity fields are filled, so the facade needs no trace
        parameters and any future producer is correlated for free.
        """
        self._traces.append(self._trace.stamp(event))

    def accumulate_usage(self, usage: TokenUsage) -> None:
        self._prompt_tokens += usage.prompt_tokens
        self._completion_tokens += usage.completion_tokens
        self._total_tokens += usage.total_tokens
        # The facade calls this only on a provider-reached call, so the flag
        # means "a real response was decoded" -- which is what lets the `tokens`
        # distribution skip activations whose usage nobody decoded rather than
        # padding it with zeros.
        self._tally.total_tokens += usage.total_tokens
        self._tally.usage_observed = True

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
        # This surface's step cursor starts at zero, so its advance *is* the
        # activation's step count.
        self._tally.iterations = self._step_index
        return AgentResult(
            outputs=tuple(self._outputs),
            intents=tuple(self._intents),
            traces=tuple(self._traces),
            usage=usage,
            memory_blob=memory_blob,
            cache_blob=cache_blob,
            tally=self._tally,
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
        resume_approval: AgentEnvelope.Approval | None = None,
        snapshot: bytes = b"",
        compactor: Compactor | None = None,
        step_index: int = 0,
        intent_ttl_ms: int = DEFAULT_INTENT_TTL_MS,
        approval_channel: str = DEFAULT_APPROVAL_CHANNEL,
        decode: Decode | None = None,
        monotonic_ns: MonotonicNs = time.monotonic_ns,
        tool_registry: ToolRegistry | None = None,
        tool_runner: ToolRunner | None = None,
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
        self._intent_ttl_ms = intent_ttl_ms
        self._approval_channel = approval_channel
        # Optional: without it the token counts of a call are genuinely
        # unknown, and the trace omits them rather than reporting zeros (D4).
        self._decode = decode

        # A resume shares the suspended activation's `seq`, so it recomputes the
        # same trace ID with nothing carried on the wire (D2); its own span is
        # distinguished by the step index it entered at.
        self._trace = ActivationTrace(
            entity_key=entity_key,
            seq=seq,
            now_ms=now_ms,
            entry_step_index=step_index,
            is_resume=resume_result is not None or resume_approval is not None,
        )

        # Seeded from the resumed Continuation's step_index, not reset to 0: a
        # resumed activation shares its suspended activation's `seq`, so
        # restarting the counter would re-mint an intent_id the suspension
        # already used and the effector would dedup the new effect away.
        self._step_index = step_index
        # ...which is why `iterations` is measured against the seed rather than
        # read off the cursor: a resume must report its own steps, not the
        # suspended activation's as well.
        self._start_step_index = step_index
        # Read-only tool calls get a counter of their own: advancing
        # `_step_index` would change the `intent_id`s this activation goes on
        # to mint (design D8). Used only for span derivation.
        self._tool_index = 0
        self._intents: list[ToolIntent] = []
        self._traces: list[TraceEvent] = []
        self._monotonic_ns = monotonic_ns
        self._tally = ActivationTally()
        self._tool_registry = tool_registry if tool_registry is not None else ToolRegistry()
        self._tool_runner = tool_runner if tool_runner is not None else ToolRunner()

    # -- agent-facing API -----------------------------------------------------

    @property
    def memory(self) -> Memory:
        """Working-memory facade; writes are staged until commit."""
        return self._memory

    @property
    def trace(self) -> ActivationTrace:
        """This activation's trace — the one the loop driver builds its
        activation events from, so the bracket and the child events cannot
        disagree about identity.
        """
        return self._trace

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
            # A hit is not a call: it neither counts nor contributes a duration
            # sample (counting would inflate `llm_calls`, and its microsecond
            # duration would drag `llm_ms` toward the cache-hit ratio; the
            # clock is not even read here). Its *trace*, though, decodes the
            # stored response, which is what makes a cache hit's token counts
            # true rather than absent (D4) — still marked unbilled, because no
            # provider call happened this time.
            self._stage_llm_trace(
                step,
                cache_hit=True,
                model_id=request.model_id,
                response=cached.response,
            )
            return LlmResponse(cached.response)

        started_ns = self._monotonic_ns()
        response = await self._provider.complete(request)
        elapsed_ns = self._monotonic_ns() - started_ns
        self._replay_cache.put(cache_key, response.response)
        # One increment and exactly one sample per provider-reached call, at the
        # same site, so the sample count always equals `llm_calls`.
        self._tally.llm_calls += 1
        self._tally.llm_ms.append(elapsed_ns // _NS_PER_MS)
        self._stage_llm_trace(
            step, cache_hit=False, model_id=request.model_id, response=response.response
        )
        return response

    async def run_tool(self, tool_name: str, arguments: Mapping[str, object]) -> object:
        """Run a ``side_effect=False`` tool inline via the injected ``ToolRunner``.

        The fast-path behavior the architecture documents ("pure/read-only
        tools execute inline"), on the runtime surface the stateful DoFn
        actually drives. ``ToolRunner.run`` refuses a ``side_effect=True`` tool
        with ``SideEffectToolError`` before it executes (correctness invariant
        5); a refused or failing tool is not counted.

        Deliberately does NOT advance the step cursor: the cursor mints intent
        IDs and orders replay-cache entries, and an inline read-only call must
        not perturb either — the `TOOL_CALL` trace event rides its own
        `tool_index` counter instead (design D8). The wall time is measured
        with the injected clock so ``overhead_ms`` can exclude tool time
        alongside model time; the trace event's timestamps stay on the
        activation clock, like every trace byte.
        """
        tool = self._tool_registry.get(tool_name)
        started_ns = self._monotonic_ns()
        value = await self._tool_runner.run(tool, arguments)
        elapsed_ns = self._monotonic_ns() - started_ns
        self._tally.tool_calls += 1
        self._tally.tool_ms.append(elapsed_ns // _NS_PER_MS)
        self._traces.append(
            self._trace.tool_call(
                step_index=self._step_index,
                tool_index=self._tool_index,
                tool_name=tool_name,
            )
        )
        self._tool_index += 1
        return value

    def act(self, tool_name: str, args_json: str, *, ttl_ms: int | None = None) -> str:
        """Stage a side-effect ``ToolIntent`` and return its deterministic ID.

        This is the ONLY effect path (correctness invariant 5): the intent is
        emitted on ``.intents`` at commit and its result re-enters on the same
        key. ``intent_id`` is a pure function of ``(key, seq, step_index)``.
        ``ttl_ms`` defaults to the configured intent TTL; the resulting
        ``expires_at_ms`` is what both the effector guard and the resume
        admission check read.
        """
        return self._stage_intent(tool_name, args_json, ToolIntent.TOOL, ttl_ms)

    def request_approval(
        self,
        args_json: str,
        *,
        channel: str | None = None,
        ttl_ms: int | None = None,
    ) -> str:
        """Stage a ``kind = APPROVAL`` intent and return its deterministic ID.

        The channel names where the effector routes the request; it is not a
        registered tool and nothing is executed. Shares the step sequence and
        ID derivation with :meth:`act`, so IDs cannot collide.
        """
        return self._stage_intent(
            channel if channel is not None else self._approval_channel,
            args_json,
            ToolIntent.APPROVAL,
            ttl_ms,
        )

    def _stage_intent(
        self,
        tool_name: str,
        args_json: str,
        kind: ToolIntent.Kind,
        ttl_ms: int | None,
    ) -> str:
        step = self._advance_step()
        intent_id = intent_id_for(self.entity_key, self.seq, step)
        expires_at_ms = self.now_ms + (ttl_ms if ttl_ms is not None else self._intent_ttl_ms)
        self._intents.append(
            ToolIntent(
                intent_id=intent_id,
                entity_key=self.entity_key,
                seq=self.seq,
                step_index=step,
                tool_name=tool_name,
                args_json=args_json,
                created_at_ms=self.now_ms,
                expires_at_ms=expires_at_ms,
                attempt=0,
                kind=kind,
                # Carries the trace across the one hop the effector breaks: it
                # consumes intents off a topic, so without this the execution
                # can never be joined back to the activation.
                trace_id=self._trace.trace_id,
            )
        )
        self._traces.append(
            self._trace.intent_emitted(
                step_index=step,
                intent_id=intent_id,
                tool_name=tool_name,
                intent_kind=ToolIntent.Kind.Name(kind),
                expires_at_ms=expires_at_ms,
            )
        )
        return intent_id

    def stage_trace(self, event: TraceEvent) -> None:
        """Stage a trace event for emission on ``.traces`` at commit."""
        self._traces.append(self._trace.stamp(event))

    # -- StagingSink protocol (model facade) ----------------------------------

    def stage_trace_event(self, event: TraceEvent) -> None:
        """Correlate the event against this activation's trace, then stage it.

        Correlation happens here rather than in the producer (design D3).
        """
        self._traces.append(self._trace.stamp(event))

    def accumulate_usage(self, usage: TokenUsage) -> None:
        """Accumulate decoded provider usage into the activation's tally.

        The model facade calls this only on a provider-reached call (a cache
        hit reports no usage), so `usage_observed` means "a real response was
        decoded". `call_model` on this surface awaits the provider directly and
        never decodes the opaque response bytes, so an activation that only uses
        `call_model` reports no usage and contributes no `tokens` sample --
        which is honest, where a zero sample would not be.

        This does NOT count `llm_calls`: that is counted in `call_model`, at the
        one site that knows a provider was reached, so the `llm_ms` sample count
        can never drift from the call count.
        """
        self._tally.total_tokens += usage.total_tokens
        self._tally.usage_observed = True

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

    @property
    def iterations(self) -> int:
        """Steps this activation consumed: the step cursor's advance.

        There is no loop counter to read -- the driver invokes the agent once
        and its internal control flow is opaque -- so the observable measure of
        "how much work did this activation do" is the advance of the same
        monotonic index that mints `intent_id`s.
        """
        return self._step_index - self._start_step_index

    def tally(self) -> ActivationTally:
        """The activation's worker-local metric tally, with `iterations`
        resolved from the step cursor. Idempotent; never persisted.
        """
        self._tally.iterations = self.iterations
        return self._tally

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

    def _stage_llm_trace(
        self, step: int, *, cache_hit: bool, model_id: str, response: bytes
    ) -> None:
        attributes = {
            OPERATION_NAME: OPERATION_CHAT,
            REQUEST_MODEL: model_id,
            CACHE_HIT: "true" if cache_hit else "false",
            # A hit re-serves tokens already paid for; only a provider call is
            # billed. Both report their real counts (D4).
            **usage_attributes(self._decoded_usage(response), billed=not cache_hit),
        }
        self.stage_trace_event(
            TraceEvent(
                entity_key=self.entity_key,
                seq=self.seq,
                step_index=step,
                event_type=TraceEvent.LLM_CALL,
                attributes=attributes,
                start_ms=self.now_ms,
                end_ms=self.now_ms,
            )
        )

    def _decoded_usage(self, response: bytes) -> TokenUsage | None:
        """The response's token usage, or ``None`` when it cannot be known.

        No configured ``decode`` means unknown, not zero: the caller omits the
        usage attributes entirely rather than reporting a count that would be
        summed as real.
        """
        if self._decode is None:
            return None
        return self._decode(response).usage
