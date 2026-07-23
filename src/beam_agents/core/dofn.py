"""Stateful Beam runtime boundary for one keyed agent activation at a time."""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from collections.abc import Callable, Coroutine, Iterable, Iterator
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Protocol, cast, runtime_checkable

import apache_beam as beam
from apache_beam.coders.typecoders import registry as _reg
from apache_beam.pvalue import TaggedOutput
from apache_beam.transforms import userstate
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.utils.timestamp import Timestamp
from google.protobuf.message import Message

from beam_agents._protos import (
    AgentEnvelope,
    Continuation,
    LlmCacheBlob,
    MemoryBlob,
    ToolIntent,
    TraceEvent,
)
from beam_agents._protos import (
    RuntimeError as RuntimeErrorProto,
)
from beam_agents.memory import Memory
from beam_agents.memory.facade import HARD_CAP_BYTES
from beam_agents.model import BLOB_CAP_BYTES, ReplayCache


@runtime_checkable
class _ReadState(Protocol):
    def read(self) -> object: ...

    def write(self, value: object) -> None: ...

    def clear(self) -> None: ...


@runtime_checkable
class _BagState(Protocol):
    def read(self) -> Iterable[object]: ...

    def add(self, value: object) -> None: ...

    def clear(self) -> None: ...


@runtime_checkable
class _CombiningState(Protocol):
    def read(self) -> object: ...

    def add(self, value: int) -> None: ...


@runtime_checkable
class _RuntimeTimer(Protocol):
    def set(self, timestamp: Timestamp) -> None: ...

    def clear(self) -> None: ...


@dataclass(frozen=True, slots=True)
class _ActivationInput:
    kind: str
    envelope: AgentEnvelope
    resumed_intent: ToolIntent | None = None


@dataclass(slots=True)
class _UsageTotals:
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0

    def add(self, *, prompt_tokens: int, completion_tokens: int, total_tokens: int) -> None:
        self.prompt_tokens += prompt_tokens
        self.completion_tokens += completion_tokens
        self.total_tokens += total_tokens


@dataclass(slots=True)
class _ActivationContext:
    entity_key: bytes
    activation_time_ms: int
    seq: int
    seq_delta: int
    memory: Memory
    replay_cache: ReplayCache
    continuation: Continuation | None
    pending_intents: dict[str, ToolIntent]
    emissions: list[object] = field(default_factory=list)
    usage: _UsageTotals = field(default_factory=_UsageTotals)
    ttl_deadline_ms: int | None = None
    hitl_deadline_ms: int | None = None
    _ttl_dirty: bool = False
    _hitl_dirty: bool = False

    def stage_output(self, value: object) -> None:
        self.emissions.append(value)

    def stage_tagged_output(self, tag: str, value: object) -> None:
        self.emissions.append(TaggedOutput(tag, value))

    def stage_trace(self, event: TraceEvent) -> None:
        self.emissions.append(TaggedOutput("traces", event))

    def accumulate_usage(
        self,
        *,
        prompt_tokens: int,
        completion_tokens: int,
        total_tokens: int,
    ) -> None:
        self.usage.add(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
        )

    def set_continuation(self, continuation: Continuation | None) -> None:
        self.continuation = continuation

    def replace_pending(self, intents: Iterable[ToolIntent]) -> None:
        self.pending_intents = {intent.intent_id: intent for intent in intents}

    def add_pending(self, intent: ToolIntent) -> None:
        self.pending_intents[intent.intent_id] = intent

    def remove_pending(self, intent_id: str) -> None:
        self.pending_intents.pop(intent_id, None)

    def set_ttl_deadline_ms(self, deadline_ms: int | None) -> None:
        self.ttl_deadline_ms = deadline_ms
        self._ttl_dirty = True

    def set_hitl_deadline_ms(self, deadline_ms: int | None) -> None:
        self.hitl_deadline_ms = deadline_ms
        self._hitl_dirty = True


@runtime_checkable
class _ActivationDriver(Protocol):
    async def run(
        self, activation_input: _ActivationInput, context: _ActivationContext
    ) -> None: ...


@runtime_checkable
class _LoopOwnedResource(Protocol):
    async def setup(self) -> None: ...

    async def close(self) -> None: ...


class _NoopLoopResource:
    async def setup(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _NoopActivationDriver:
    async def run(self, activation_input: _ActivationInput, context: _ActivationContext) -> None:
        context.stage_output((context.entity_key, context.seq, activation_input.kind))


class _ActivationBridge:
    """One asyncio loop thread per DoFn instance."""

    def __init__(self, resource: _LoopOwnedResource | None = None) -> None:
        self._resource = resource if resource is not None else _NoopLoopResource()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ready = threading.Event()
        self._lock = threading.Lock()
        self._futures: set[concurrent.futures.Future[object]] = set()

    @property
    def thread(self) -> threading.Thread | None:
        return self._thread

    @property
    def loop(self) -> asyncio.AbstractEventLoop | None:
        return self._loop

    def start(self) -> None:
        if self._thread is not None:
            return

        def runner() -> None:
            loop = asyncio.new_event_loop()
            self._loop = loop
            asyncio.set_event_loop(loop)
            self._ready.set()
            loop.run_forever()
            loop.close()

        self._thread = threading.Thread(
            target=runner, name="beam-agents-activation-loop", daemon=True
        )
        self._thread.start()
        self._ready.wait()
        self._submit_internal(self._resource.setup()).result()

    def submit(
        self, coro: Coroutine[object, object, _ActivationContext]
    ) -> concurrent.futures.Future[_ActivationContext]:
        return cast(
            concurrent.futures.Future[_ActivationContext],
            self._submit_internal(coro),
        )

    def _submit_internal(
        self, coro: Coroutine[object, object, object]
    ) -> concurrent.futures.Future[object]:
        loop = self._loop
        if loop is None:
            raise RuntimeError("activation bridge is not started")
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        with self._lock:
            self._futures.add(future)
        future.add_done_callback(self._discard_future)
        return future

    def _discard_future(self, future: concurrent.futures.Future[object]) -> None:
        with self._lock:
            self._futures.discard(future)

    def close(self, cancellation_grace_s: float) -> None:
        with self._lock:
            pending = tuple(self._futures)
        for future in pending:
            future.cancel()
        for future in pending:
            try:
                future.exception(timeout=cancellation_grace_s)
            except (concurrent.futures.TimeoutError, concurrent.futures.CancelledError):
                continue
        try:
            if self._loop is not None:
                self._submit_internal(self._resource.close()).result()
        finally:
            if self._loop is not None:
                self._loop.call_soon_threadsafe(self._loop.stop)
            if self._thread is not None:
                self._thread.join(timeout=5.0)
            self._thread = None
            self._loop = None


def _ms_to_timestamp(value_ms: int) -> Timestamp:
    return Timestamp(micros=int(value_ms * 1000))


def _derive_hitl_deadline_ms(
    continuation: Continuation | None, pending: Iterable[ToolIntent]
) -> int | None:
    deadlines: list[int] = []
    if continuation is not None and continuation.deadline_ms > 0:
        deadlines.append(int(continuation.deadline_ms))
    for intent in pending:
        if intent.expires_at_ms > 0:
            deadlines.append(int(intent.expires_at_ms))
    if not deadlines:
        return None
    return min(deadlines)


def _correlation_expired(
    *,
    continuation: Continuation,
    intent: ToolIntent,
    observed_at_ms: int,
) -> bool:
    return (intent.expires_at_ms > 0 and intent.expires_at_ms <= observed_at_ms) or (
        continuation.deadline_ms > 0 and continuation.deadline_ms <= observed_at_ms
    )


def _new_runtime_error(
    *,
    error_type: RuntimeErrorProto.ErrorType | str,
    entity_key: bytes,
    message: str,
    observed_at_ms: int,
    seq: int = 0,
    intent_id: str = "",
) -> RuntimeErrorProto:
    return RuntimeErrorProto(
        error_type=error_type,
        entity_key=entity_key,
        seq=seq,
        intent_id=intent_id,
        message=message,
        observed_at_ms=observed_at_ms,
    )


@dataclass(frozen=True, slots=True)
class _PreparedCommit:
    memory_blob: MemoryBlob
    replay_blob: LlmCacheBlob
    continuation: Continuation | None
    pending: tuple[ToolIntent, ...]
    seq_delta: int
    ttl_deadline_ms: int | None
    hitl_deadline_ms: int | None
    emissions: tuple[object, ...]


class _IntSumCombineFn(beam.CombineFn):
    def create_accumulator(self) -> int:
        return 0

    def add_input(self, accumulator: int, element: int) -> int:
        return accumulator + element

    def merge_accumulators(self, accumulators: Iterable[int]) -> int:
        return sum(accumulators)

    def extract_output(self, accumulator: int) -> int:
        return accumulator


class _AgentDoFn(beam.DoFn):
    """Internal runtime boundary: route, execute one activation, commit atomically."""

    # Lazy coder proxy to avoid import-time registration side effects. The
    # proxy delegates to whatever coder the Beam registry resolves at runtime
    # (e.g., after register_coders() is called in tests or pipeline setup).
    class _LazyCoder:
        def __init__(self, proto_type: type[Message]) -> None:
            self._proto_type = proto_type

        def encode(self, value: Message) -> bytes:
            coder = _reg.get_coder(self._proto_type)
            return coder.encode(value)

        def decode(self, encoded: bytes) -> Any:
            coder = _reg.get_coder(self._proto_type)
            c: Any = coder
            return cast(Message, c.decode(encoded))

        def is_deterministic(self) -> bool:  # best-effort delegate
            coder = _reg.get_coder(self._proto_type)
            try:
                return coder.is_deterministic()
            except Exception:
                return False

    MEMORY = userstate.ReadModifyWriteStateSpec("MEMORY", cast(Any, _LazyCoder(MemoryBlob)))
    CONTINUATION = userstate.ReadModifyWriteStateSpec(
        "CONTINUATION", cast(Any, _LazyCoder(Continuation))
    )
    LLM_CACHE = userstate.ReadModifyWriteStateSpec("LLM_CACHE", cast(Any, _LazyCoder(LlmCacheBlob)))
    PENDING = userstate.BagStateSpec("PENDING", cast(Any, _LazyCoder(ToolIntent)))
    SEQ = userstate.CombiningValueStateSpec(
        "SEQ",
        beam.coders.VarIntCoder(),
        _IntSumCombineFn(),  # type: ignore[no-untyped-call]
    )
    TTL_TIMER = userstate.TimerSpec("TTL_TIMER", TimeDomain.WATERMARK)
    HITL_TIMER = userstate.TimerSpec("HITL_TIMER", TimeDomain.REAL_TIME)
    _MEMORY_PARAM: _ReadState = cast(_ReadState, beam.DoFn.StateParam(MEMORY))
    _CONTINUATION_PARAM: _ReadState = cast(_ReadState, beam.DoFn.StateParam(CONTINUATION))
    _LLM_CACHE_PARAM: _ReadState = cast(_ReadState, beam.DoFn.StateParam(LLM_CACHE))
    _PENDING_PARAM: _BagState = cast(_BagState, beam.DoFn.StateParam(PENDING))
    _SEQ_PARAM: _CombiningState = cast(_CombiningState, beam.DoFn.StateParam(SEQ))
    _TTL_TIMER_PARAM: _RuntimeTimer = cast(_RuntimeTimer, beam.DoFn.TimerParam(TTL_TIMER))
    _HITL_TIMER_PARAM: _RuntimeTimer = cast(_RuntimeTimer, beam.DoFn.TimerParam(HITL_TIMER))
    _KEY_PARAM: bytes = cast(bytes, beam.DoFn.KeyParam)

    def __init__(
        self,
        *,
        driver: _ActivationDriver | None = None,
        bridge_factory: Callable[[], _ActivationBridge] | None = None,
        activation_timeout_s: float = 5.0,
        cancellation_grace_s: float = 0.1,
        memory_ttl_ms: int = 60_000,
        now_ms_fn: Callable[[], int] | None = None,
        commit_audit_hook: Callable[[str], None] | None = None,
    ) -> None:
        self._driver = driver if driver is not None else _NoopActivationDriver()
        self._bridge_factory = bridge_factory if bridge_factory is not None else _ActivationBridge
        self._activation_timeout_s = activation_timeout_s
        self._cancellation_grace_s = cancellation_grace_s
        self._memory_ttl_ms = memory_ttl_ms
        self._now_ms_fn = now_ms_fn if now_ms_fn is not None else self._wall_clock_ms
        self._bridge: _ActivationBridge | None = None
        self._commit_audit_hook = commit_audit_hook

    def setup(self) -> None:
        bridge = self._bridge_factory()
        bridge.start()
        self._bridge = bridge

    def teardown(self) -> None:
        if self._bridge is not None:
            self._bridge.close(self._cancellation_grace_s)
            self._bridge = None

    def process(
        self,
        element: tuple[bytes, AgentEnvelope],
        memory_state: _ReadState = _MEMORY_PARAM,
        continuation_state: _ReadState = _CONTINUATION_PARAM,
        cache_state: _ReadState = _LLM_CACHE_PARAM,
        pending_state: _BagState = _PENDING_PARAM,
        seq_state: _CombiningState = _SEQ_PARAM,
        ttl_timer: _RuntimeTimer = _TTL_TIMER_PARAM,
        hitl_timer: _RuntimeTimer = _HITL_TIMER_PARAM,
    ) -> Iterator[object]:
        yield from self._run_envelope(
            element=element,
            memory_state=memory_state,
            continuation_state=continuation_state,
            cache_state=cache_state,
            pending_state=pending_state,
            seq_state=seq_state,
            ttl_timer=ttl_timer,
            hitl_timer=hitl_timer,
        )

    @userstate.on_timer(TTL_TIMER)
    def on_ttl_timer(
        self,
        memory_state: _ReadState = _MEMORY_PARAM,
        continuation_state: _ReadState = _CONTINUATION_PARAM,
        cache_state: _ReadState = _LLM_CACHE_PARAM,
        pending_state: _BagState = _PENDING_PARAM,
        seq_state: _CombiningState = _SEQ_PARAM,
        ttl_timer: _RuntimeTimer = _TTL_TIMER_PARAM,
        hitl_timer: _RuntimeTimer = _HITL_TIMER_PARAM,
    ) -> None:
        _ = continuation_state, cache_state, pending_state, seq_state, hitl_timer
        loaded = memory_state.read()
        blob = loaded if isinstance(loaded, MemoryBlob) else None
        if blob is None or not blob.entries:
            ttl_timer.clear()
            return
        memory_state.clear()
        ttl_timer.clear()

    @userstate.on_timer(HITL_TIMER)
    def on_hitl_timer(
        self,
        key: bytes = _KEY_PARAM,
        memory_state: _ReadState = _MEMORY_PARAM,
        continuation_state: _ReadState = _CONTINUATION_PARAM,
        cache_state: _ReadState = _LLM_CACHE_PARAM,
        pending_state: _BagState = _PENDING_PARAM,
        seq_state: _CombiningState = _SEQ_PARAM,
        ttl_timer: _RuntimeTimer = _TTL_TIMER_PARAM,
        hitl_timer: _RuntimeTimer = _HITL_TIMER_PARAM,
    ) -> Iterator[object]:
        continuation_obj = continuation_state.read()
        continuation = continuation_obj if isinstance(continuation_obj, Continuation) else None
        if continuation is None:
            hitl_timer.clear()
            return
        envelope = AgentEnvelope(entity_key=key, event_time_ms=self._now_ms_fn())
        routed = _ActivationInput(kind="hitl_timeout", envelope=envelope)
        yield from self._run_routed(
            routed=routed,
            loaded_seq=self._read_seq(seq_state),
            memory_state=memory_state,
            continuation_state=continuation_state,
            cache_state=cache_state,
            pending_state=pending_state,
            seq_state=seq_state,
            ttl_timer=ttl_timer,
            hitl_timer=hitl_timer,
            failure_error_type=RuntimeErrorProto.TIMEOUT_HANDLING_FAILED,
            timeout_error_type=RuntimeErrorProto.TIMEOUT_HANDLING_FAILED,
            remove_resolved_intent=None,
        )

    def _run_envelope(
        self,
        *,
        element: tuple[bytes, AgentEnvelope],
        memory_state: _ReadState,
        continuation_state: _ReadState,
        cache_state: _ReadState,
        pending_state: _BagState,
        seq_state: _CombiningState,
        ttl_timer: _RuntimeTimer,
        hitl_timer: _RuntimeTimer,
    ) -> Iterator[object]:
        key, envelope = element
        observed_at_ms = self._now_ms_fn()
        continuation, pending_by_id = self._load_continuation_and_pending(
            continuation_state=continuation_state,
            pending_state=pending_state,
        )
        routed, route_error = self._route(
            key=key,
            envelope=envelope,
            continuation=continuation,
            pending_by_id=pending_by_id,
            observed_at_ms=observed_at_ms,
        )
        if route_error is not None:
            yield TaggedOutput("errors", route_error)
            return
        assert routed is not None
        resolved = routed.resumed_intent.intent_id if routed.resumed_intent is not None else None
        yield from self._run_routed(
            routed=routed,
            loaded_seq=self._read_seq(seq_state),
            memory_state=memory_state,
            continuation_state=continuation_state,
            cache_state=cache_state,
            pending_state=pending_state,
            seq_state=seq_state,
            ttl_timer=ttl_timer,
            hitl_timer=hitl_timer,
            failure_error_type=RuntimeErrorProto.ACTIVATION_FAILED,
            timeout_error_type=RuntimeErrorProto.ACTIVATION_TIMEOUT,
            remove_resolved_intent=resolved,
        )

    def _run_routed(
        self,
        *,
        routed: _ActivationInput,
        loaded_seq: int,
        memory_state: _ReadState,
        continuation_state: _ReadState,
        cache_state: _ReadState,
        pending_state: _BagState,
        seq_state: _CombiningState,
        ttl_timer: _RuntimeTimer,
        hitl_timer: _RuntimeTimer,
        failure_error_type: RuntimeErrorProto.ErrorType | str,
        timeout_error_type: RuntimeErrorProto.ErrorType | str,
        remove_resolved_intent: str | None,
    ) -> Iterator[object]:
        context = self._load_context(
            routed=routed,
            loaded_seq=loaded_seq,
            memory_state=memory_state,
            continuation_state=continuation_state,
            cache_state=cache_state,
            pending_state=pending_state,
            remove_resolved_intent=remove_resolved_intent,
        )
        try:
            completed_context = self._run_driver_with_timeout(
                routed=routed,
                context=context,
                timeout_error_type=timeout_error_type,
            )
        except _ActivationTimedOut as timeout:
            yield TaggedOutput("errors", timeout.error)
            return
        except Exception as exc:  # pragma: no cover - covered via failure tests
            yield TaggedOutput(
                "errors",
                _new_runtime_error(
                    error_type=failure_error_type,
                    entity_key=routed.envelope.entity_key,
                    seq=context.seq,
                    message=f"activation failed: {type(exc).__name__}: {exc}",
                    observed_at_ms=self._now_ms_fn(),
                ),
            )
            return

        try:
            prepared = self._prepare_commit(completed_context)
        except Exception as exc:
            yield TaggedOutput(
                "errors",
                _new_runtime_error(
                    error_type=failure_error_type,
                    entity_key=routed.envelope.entity_key,
                    seq=context.seq,
                    message=f"commit preparation failed: {type(exc).__name__}: {exc}",
                    observed_at_ms=self._now_ms_fn(),
                ),
            )
            return

        self._apply_commit(
            prepared=prepared,
            memory_state=memory_state,
            continuation_state=continuation_state,
            cache_state=cache_state,
            pending_state=pending_state,
            seq_state=seq_state,
            ttl_timer=ttl_timer,
            hitl_timer=hitl_timer,
        )
        yield from prepared.emissions

    def _route(  # noqa: PLR0911
        self,
        *,
        key: bytes,
        envelope: AgentEnvelope,
        continuation: Continuation | None,
        pending_by_id: dict[str, ToolIntent],
        observed_at_ms: int,
    ) -> tuple[_ActivationInput | None, RuntimeErrorProto | None]:
        if key != envelope.entity_key:
            return None, _new_runtime_error(
                error_type=RuntimeErrorProto.INVALID_ENVELOPE,
                entity_key=envelope.entity_key,
                message="KV key does not match envelope entity_key",
                observed_at_ms=observed_at_ms,
            )
        payload = envelope.WhichOneof("payload")
        if payload is None:
            return None, _new_runtime_error(
                error_type=RuntimeErrorProto.INVALID_ENVELOPE,
                entity_key=envelope.entity_key,
                message="envelope payload is required",
                observed_at_ms=observed_at_ms,
            )
        if payload == "external_event":
            if continuation is not None:
                return None, _new_runtime_error(
                    error_type=RuntimeErrorProto.BUSY_KEY,
                    entity_key=envelope.entity_key,
                    seq=continuation.seq,
                    message="external event arrived while continuation is pending",
                    observed_at_ms=observed_at_ms,
                )
            return _ActivationInput(kind="external_event", envelope=envelope), None
        if payload == "tool_result":
            intent_id = envelope.tool_result.intent_id
            intent = pending_by_id.get(intent_id)
            if (
                continuation is None
                or intent is None
                or intent_id not in continuation.pending_intent_ids
                or _correlation_expired(
                    continuation=continuation,
                    intent=intent,
                    observed_at_ms=observed_at_ms,
                )
            ):
                return None, _new_runtime_error(
                    error_type=RuntimeErrorProto.ORPHANED_RESULT,
                    entity_key=envelope.entity_key,
                    intent_id=intent_id,
                    message="tool result could not be correlated to pending continuation",
                    observed_at_ms=observed_at_ms,
                )
            return (
                _ActivationInput(kind="tool_result", envelope=envelope, resumed_intent=intent),
                None,
            )
        if payload == "approval":
            intent_id = envelope.approval.intent_id
            intent = pending_by_id.get(intent_id)
            if (
                continuation is None
                or intent is None
                or intent_id not in continuation.pending_intent_ids
                or _correlation_expired(
                    continuation=continuation,
                    intent=intent,
                    observed_at_ms=observed_at_ms,
                )
            ):
                return None, _new_runtime_error(
                    error_type=RuntimeErrorProto.ORPHANED_RESULT,
                    entity_key=envelope.entity_key,
                    intent_id=intent_id,
                    message="approval could not be correlated to pending continuation",
                    observed_at_ms=observed_at_ms,
                )
            return (
                _ActivationInput(kind="approval", envelope=envelope, resumed_intent=intent),
                None,
            )
        return None, _new_runtime_error(
            error_type=RuntimeErrorProto.INVALID_ENVELOPE,
            entity_key=envelope.entity_key,
            message=f"unsupported envelope payload discriminator: {payload}",
            observed_at_ms=observed_at_ms,
        )

    def _load_context(
        self,
        *,
        routed: _ActivationInput,
        loaded_seq: int,
        memory_state: _ReadState,
        continuation_state: _ReadState,
        cache_state: _ReadState,
        pending_state: _BagState,
        remove_resolved_intent: str | None,
    ) -> _ActivationContext:
        memory_blob_obj = memory_state.read()
        memory_blob = memory_blob_obj if isinstance(memory_blob_obj, MemoryBlob) else None
        continuation_obj = continuation_state.read()
        continuation = continuation_obj if isinstance(continuation_obj, Continuation) else None
        cache_blob_obj = cache_state.read()
        cache_blob = cache_blob_obj if isinstance(cache_blob_obj, LlmCacheBlob) else None
        pending = sorted(
            (item for item in pending_state.read() if isinstance(item, ToolIntent)),
            key=lambda item: item.intent_id,
        )
        pending_by_id = {intent.intent_id: intent for intent in pending}
        if remove_resolved_intent is not None:
            pending_by_id.pop(remove_resolved_intent, None)
        if routed.kind == "external_event":
            seq = loaded_seq + 1
            seq_delta = 1
        elif continuation is not None:
            seq = int(continuation.seq)
            seq_delta = 0
        else:
            seq = loaded_seq
            seq_delta = 0
        return _ActivationContext(
            entity_key=routed.envelope.entity_key,
            activation_time_ms=int(routed.envelope.event_time_ms),
            seq=seq,
            seq_delta=seq_delta,
            memory=Memory(memory_blob, now_ms=int(routed.envelope.event_time_ms)),
            replay_cache=ReplayCache(cache_blob, now_ms=int(routed.envelope.event_time_ms)),
            continuation=continuation,
            pending_intents=pending_by_id,
        )

    def _run_driver_with_timeout(
        self,
        *,
        routed: _ActivationInput,
        context: _ActivationContext,
        timeout_error_type: RuntimeErrorProto.ErrorType | str,
    ) -> _ActivationContext:
        bridge = self._bridge
        if bridge is None:
            raise RuntimeError("DoFn setup() has not been called")
        future = bridge.submit(self._execute_activation(routed=routed, context=context))
        try:
            return future.result(timeout=self._activation_timeout_s)
        except concurrent.futures.TimeoutError:
            future.cancel()
            with suppress(
                concurrent.futures.TimeoutError,
                concurrent.futures.CancelledError,
            ):
                future.exception(timeout=self._cancellation_grace_s)
            raise _ActivationTimedOut(
                _new_runtime_error(
                    error_type=timeout_error_type,
                    entity_key=routed.envelope.entity_key,
                    seq=context.seq,
                    message="activation timed out and was cancelled",
                    observed_at_ms=self._now_ms_fn(),
                )
            ) from None

    async def _execute_activation(
        self, *, routed: _ActivationInput, context: _ActivationContext
    ) -> _ActivationContext:
        await self._driver.run(routed, context)
        return context

    def _prepare_commit(self, context: _ActivationContext) -> _PreparedCommit:
        memory_blob = context.memory.to_blob()
        replay_blob = context.replay_cache.to_blob()
        if memory_blob.total_value_bytes > HARD_CAP_BYTES:
            raise ValueError("memory blob exceeds hard cap")
        if replay_blob.ByteSize() > BLOB_CAP_BYTES:
            raise ValueError("replay cache blob exceeds blob cap")
        pending = tuple(sorted(context.pending_intents.values(), key=lambda item: item.intent_id))
        for intent in pending:
            if not intent.intent_id:
                raise ValueError("pending intent_id cannot be empty")
        if context._ttl_dirty:
            ttl_deadline_ms = context.ttl_deadline_ms
        elif memory_blob.entries:
            ttl_deadline_ms = int(context.activation_time_ms + self._memory_ttl_ms)
        else:
            ttl_deadline_ms = None
        hitl_deadline_ms = (
            context.hitl_deadline_ms
            if context._hitl_dirty
            else _derive_hitl_deadline_ms(context.continuation, pending)
        )
        return _PreparedCommit(
            memory_blob=memory_blob,
            replay_blob=replay_blob,
            continuation=context.continuation,
            pending=pending,
            seq_delta=context.seq_delta,
            ttl_deadline_ms=ttl_deadline_ms,
            hitl_deadline_ms=hitl_deadline_ms,
            emissions=tuple(context.emissions),
        )

    def _apply_commit(
        self,
        *,
        prepared: _PreparedCommit,
        memory_state: _ReadState,
        continuation_state: _ReadState,
        cache_state: _ReadState,
        pending_state: _BagState,
        seq_state: _CombiningState,
        ttl_timer: _RuntimeTimer,
        hitl_timer: _RuntimeTimer,
    ) -> None:
        self._record_commit_step("MEMORY")
        if prepared.memory_blob.entries:
            memory_state.write(prepared.memory_blob)
        else:
            memory_state.clear()

        self._record_commit_step("LLM_CACHE")
        if prepared.replay_blob.entries:
            cache_state.write(prepared.replay_blob)
        else:
            cache_state.clear()

        self._record_commit_step("CONTINUATION")
        if prepared.continuation is None:
            continuation_state.clear()
        else:
            continuation_state.write(prepared.continuation)

        self._record_commit_step("PENDING")
        pending_state.clear()
        for intent in prepared.pending:
            pending_state.add(intent)

        self._record_commit_step("SEQ")
        seq_state.add(prepared.seq_delta)

        self._record_commit_step("TTL_TIMER")
        if prepared.ttl_deadline_ms is None:
            ttl_timer.clear()
        else:
            ttl_timer.set(_ms_to_timestamp(prepared.ttl_deadline_ms))

        self._record_commit_step("HITL_TIMER")
        if prepared.hitl_deadline_ms is None:
            hitl_timer.clear()
        else:
            hitl_timer.set(_ms_to_timestamp(prepared.hitl_deadline_ms))

    def _load_continuation_and_pending(
        self, *, continuation_state: _ReadState, pending_state: _BagState
    ) -> tuple[Continuation | None, dict[str, ToolIntent]]:
        continuation_obj = continuation_state.read()
        continuation = continuation_obj if isinstance(continuation_obj, Continuation) else None
        pending = {
            item.intent_id: item
            for item in pending_state.read()
            if isinstance(item, ToolIntent) and item.intent_id
        }
        return continuation, pending

    def _record_commit_step(self, step: str) -> None:
        if self._commit_audit_hook is not None:
            self._commit_audit_hook(step)

    @staticmethod
    def _read_seq(seq_state: _CombiningState) -> int:
        value = seq_state.read()
        if isinstance(value, int):
            return value
        return 0

    @staticmethod
    def _wall_clock_ms() -> int:
        return int(Timestamp.now().micros / 1000)


class _ActivationTimedOut(Exception):
    def __init__(self, error: RuntimeErrorProto) -> None:
        super().__init__(error.message)
        self.error = error
