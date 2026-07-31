"""Loading a replay bundle and re-running the activation it describes.

Three sources, each the system of record for what it holds (design D4):

- the **snapshot** carries the state blobs — and, for a resume, the
  ``Continuation``'s ``step_index`` and adapter snapshot;
- the **trace stream** carries the target scope ``(entity_key, seq)`` and the
  activation clock, recovered from the traced ``ACTIVATION_START.start_ms``
  (an attempt whose only record is the synthesized ``ERROR`` event — a failed
  activation commits nothing, so its staged events are discarded — yields the
  same clock from that event's ``start_ms``);
- the **envelope**, fetched by the operator off the durable events bus, carries
  the triggering payload. Traces deliberately carry no payloads and the DoFn
  retains no consumed envelope, so this is the only place it can come from.

Version handling is fail-closed (design D5): every blob is migrated on load
through the *same* per-blob migrations the DoFn applies lazily
(:func:`~beam_agents.core.migration.migrate_to_current`), so replay can never
disagree with the pipeline about what an old blob means, and anything newer than
the installed package refuses to load. Migration is applied to in-memory copies
only; nothing is ever written back to a snapshot.

Errors are typed by what an operator should do about them, which is what the
CLI's exit codes report: :class:`ReplayUsageError` is "fix the invocation or the
binary" (exit 2, including a version refusal — the snapshot cannot be
interpreted at all), :class:`ReplayIrreproducibleError` is "the inputs this
replay needs are not in the bundle" (exit 3: a cache miss, a digest-only entry,
a migration chain with a gap).

Importing this module has no side effects.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, TypeVar

from google.protobuf.message import DecodeError

from beam_agents._protos import (
    AgentEnvelope,
    Continuation,
    LlmCacheBlob,
    MemoryBlob,
    StateSnapshot,
    ToolIntent,
    TraceEvent,
)
from beam_agents.core.dofn import REASON_ERROR, _error_trace
from beam_agents.core.loop import ActivationFailed, run_activation
from beam_agents.core.migration import (
    CURRENT_STATE_SCHEMA_VERSION,
    StateMigrationError,
    StateSchemaFromFutureError,
    migrate_to_current,
)
from beam_agents.observability.traces import ACTIVATION_KIND
from beam_agents.replay.provider import (
    CacheOnlyLLMClient,
    ReplayCacheMissError,
    digest_only_digests,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from beam_agents.core.agent import Agent
    from beam_agents.core.loop import FailureContext
    from beam_agents.model.facade import Decode
    from beam_agents.tools.registry import ToolRegistry

__all__ = [
    "ReplayBundle",
    "ReplayError",
    "ReplayIrreproducibleError",
    "ReplayOutcome",
    "ReplayUsageError",
    "build_bundle",
    "frame_trace_events",
    "load_envelope",
    "load_snapshot",
    "parse_trace_stream",
    "run_replay",
]

# The pre-versioned baseline, matching `core/migration`: a blob (or snapshot)
# stamped 0 predates the version field and can only mean version 1.
_BASELINE_VERSION = 1

# The three versioned blobs `migrate_to_current` accepts. Constrained (not
# bound), exactly as `core/migration` types it, so a call site keeps its
# concrete blob type through the hook.
_Blob = TypeVar("_Blob", MemoryBlob, Continuation, LlmCacheBlob)

_VARINT_CONTINUATION_BIT = 0x80


class ReplayError(Exception):
    """Base for every replay failure the CLI turns into a non-zero exit."""


class ReplayUsageError(ReplayError):
    """The invocation or the binary is wrong: exit 2.

    Covers a malformed or unreadable input, an envelope that does not belong to
    the snapshot, a target scope with no traced events, and a schema version
    this package cannot interpret. Guessing forward is how silent corruption
    happens, so a newer snapshot lands here rather than being approximated.
    """


class ReplayIrreproducibleError(ReplayError):
    """The bundle lacks an input the re-run needs: exit 3.

    Distinct from divergence on purpose (design D2): the CLI can fail to
    reproduce an activation, but it must never fabricate a plausible-but-wrong
    reproduction, so a missing cached response aborts loudly rather than
    silently taking a different path.
    """


# --- framing ------------------------------------------------------------------


def frame_trace_events(events: Iterable[TraceEvent]) -> bytes:
    """Encode events as the canonical varint-length-delimited stream.

    The payloads are exactly what
    :func:`~beam_agents.observability.exporters.serialize_trace_event` produces,
    each preceded by its length, which is the interchange the CLI reads.
    """
    out = bytearray()
    for event in events:
        payload = event.SerializeToString(deterministic=True)
        out += _varint(len(payload)) + payload
    return bytes(out)


def parse_trace_stream(data: bytes) -> list[TraceEvent]:
    """Decode a varint-length-delimited ``TraceEvent`` stream."""
    events: list[TraceEvent] = []
    offset = 0
    size = len(data)
    while offset < size:
        length, offset = _read_varint(data, offset)
        end = offset + length
        if end > size:
            raise ReplayUsageError(
                f"truncated trace stream: a frame declares {length} bytes at offset "
                f"{offset} but only {size - offset} remain"
            )
        event = TraceEvent()
        try:
            event.ParseFromString(data[offset:end])
        except DecodeError as exc:
            raise ReplayUsageError(
                f"malformed TraceEvent at offset {offset} of the trace stream: {exc}"
            ) from exc
        events.append(event)
        offset = end
    return events


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | _VARINT_CONTINUATION_BIT)
        else:
            out.append(byte)
            return bytes(out)


def _read_varint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while True:
        if offset >= len(data):
            raise ReplayUsageError("truncated trace stream: a frame length is incomplete")
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not byte & _VARINT_CONTINUATION_BIT:
            return value, offset
        shift += 7


# --- loading ------------------------------------------------------------------


def load_snapshot(data: bytes) -> StateSnapshot:
    """Parse a ``StateSnapshot``, version-check it, and migrate its blobs.

    Returns a **new** message: the caller's bytes are never rewritten, and the
    migrated blobs are what the replay runs against.
    """
    snapshot = StateSnapshot()
    try:
        snapshot.ParseFromString(data)
    except DecodeError as exc:
        raise ReplayUsageError(f"could not parse the snapshot file: {exc}") from exc

    version = snapshot.state_schema_version or _BASELINE_VERSION
    if version > CURRENT_STATE_SCHEMA_VERSION:
        raise ReplayUsageError(
            f"snapshot state_schema_version={version} is newer than this package "
            f"supports (state_schema_version={CURRENT_STATE_SCHEMA_VERSION}); "
            "upgrade beam-agents to replay this snapshot"
        )

    migrated = StateSnapshot()
    migrated.CopyFrom(snapshot)
    migrated.state_schema_version = CURRENT_STATE_SCHEMA_VERSION
    if snapshot.HasField("memory"):
        migrated.memory.CopyFrom(_migrate(snapshot.memory))
    if snapshot.HasField("llm_cache"):
        migrated.llm_cache.CopyFrom(_migrate(snapshot.llm_cache))
    if snapshot.HasField("continuation"):
        migrated.continuation.CopyFrom(_migrate(snapshot.continuation))
    return migrated


def _migrate(blob: _Blob) -> _Blob:
    """Run one blob through the pipeline's own migration chain.

    One implementation, two call sites (the DoFn's lazy read and this loader),
    so replay can never disagree with the pipeline about what an old blob
    means. A future-version blob is a usage error — the same fail-closed stance
    the DoFn takes when it wedges the key rather than misreading it.
    """
    try:
        return migrate_to_current(blob)
    except StateSchemaFromFutureError as exc:
        raise ReplayUsageError(
            f"{exc.message_type.__name__} in the snapshot carries "
            f"state_schema_version={exc.found_version}, newer than this package's "
            f"{exc.current_version}; upgrade beam-agents to replay this snapshot"
        ) from exc
    except StateMigrationError as exc:
        raise ReplayIrreproducibleError(
            f"irreproducible: the snapshot's blobs cannot be migrated to the current schema — {exc}"
        ) from exc


def load_envelope(data: bytes) -> AgentEnvelope:
    """Parse the triggering ``AgentEnvelope`` an operator fetched off the bus."""
    envelope = AgentEnvelope()
    try:
        envelope.ParseFromString(data)
    except DecodeError as exc:
        raise ReplayUsageError(f"could not parse the event envelope file: {exc}") from exc
    return envelope


# --- reconstruction -----------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ReplayBundle:
    """Everything one local re-run needs, assembled from the three sources."""

    snapshot: StateSnapshot
    envelope: AgentEnvelope
    entity_key: bytes
    seq: int
    #: The activation's injected clock, recovered from the trace — never read
    #: from a wall clock, which is what makes the replayed timestamps identical.
    now_ms: int
    traced: tuple[TraceEvent, ...]
    is_resume: bool
    step_index: int = 0
    adapter_snapshot: bytes = b""


def build_bundle(
    *,
    snapshot: StateSnapshot,
    traces: Sequence[TraceEvent],
    envelope: AgentEnvelope,
    seq: int | None = None,
) -> ReplayBundle:
    """Select the target attempt and reconstruct ``run_activation``'s inputs."""
    entity_key = snapshot.entity_key
    if envelope.entity_key != entity_key:
        raise ReplayUsageError(
            f"the event envelope belongs to entity_key {envelope.entity_key.hex()} but the "
            f"snapshot is of entity_key {entity_key.hex()}; replay one key's activation "
            "with that key's own envelope"
        )

    scoped = [event for event in traces if event.entity_key == entity_key]
    if not scoped:
        raise ReplayUsageError(
            f"no traced events for entity_key {entity_key.hex()} in the trace stream"
        )
    target_seq = max(event.seq for event in scoped) if seq is None else seq
    attempt_events = [event for event in scoped if event.seq == target_seq]
    if not attempt_events:
        raise ReplayUsageError(
            f"no traced events at seq={target_seq} for entity_key {entity_key.hex()}"
        )

    is_resume = envelope.WhichOneof("payload") in ("tool_result", "approval")
    attempt = _select_attempt(attempt_events, is_resume=is_resume)

    step_index = 0
    adapter_snapshot = b""
    if is_resume:
        if not snapshot.HasField("continuation"):
            raise ReplayUsageError(
                "the envelope is a resume payload but the snapshot holds no continuation; "
                "export while the key is suspended, or replay the activation that started it"
            )
        continuation = snapshot.continuation
        intent_id = (
            envelope.tool_result.intent_id
            if envelope.WhichOneof("payload") == "tool_result"
            else envelope.approval.intent_id
        )
        if intent_id not in set(continuation.pending_intent_ids):
            raise ReplayUsageError(
                f"the envelope resumes intent {intent_id!r}, which the snapshot's "
                f"continuation does not pend (pending: "
                f"{list(continuation.pending_intent_ids)}); the pipeline would refuse this "
                "resume as an orphaned result"
            )
        step_index = continuation.step_index
        adapter_snapshot = continuation.snapshot

    return ReplayBundle(
        snapshot=snapshot,
        envelope=envelope,
        entity_key=entity_key,
        seq=target_seq,
        now_ms=_recover_now_ms(attempt),
        traced=tuple(attempt),
        is_resume=is_resume,
        step_index=step_index,
        adapter_snapshot=adapter_snapshot,
    )


def _select_attempt(events: Sequence[TraceEvent], *, is_resume: bool) -> list[TraceEvent]:
    """Split a seq's events into attempts and pick the one being replayed.

    One trace spans a suspend/resume cycle (they share a ``seq``), so a seq can
    hold several attempts. Each starts at an ``ACTIVATION_START``, whose
    ``beam_agents.activation.kind`` says whether it was a start or a resume —
    which is exactly what the supplied envelope selects between. A failed
    attempt has no ``ACTIVATION_START`` at all (nothing committed, so its staged
    events were discarded); its lone ``ERROR`` event forms its own group and is
    picked as the last one.
    """
    attempts: list[list[TraceEvent]] = []
    for event in events:
        if event.event_type == TraceEvent.ACTIVATION_START or not attempts:
            attempts.append([])
        attempts[-1].append(event)
    wanted = "resume" if is_resume else "start"
    matching = [
        attempt
        for attempt in attempts
        if attempt[0].event_type == TraceEvent.ACTIVATION_START
        and attempt[0].attributes.get(ACTIVATION_KIND) == wanted
    ]
    return matching[-1] if matching else attempts[-1]


def _recover_now_ms(attempt: Sequence[TraceEvent]) -> int:
    """The attempt's injected clock: every one of its events carries it."""
    for event in attempt:
        if event.event_type == TraceEvent.ACTIVATION_START:
            return event.start_ms
    return attempt[0].start_ms


# --- the re-run ---------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ReplayOutcome:
    """What the local re-run produced, on the surface the diff compares."""

    status: str
    traces: tuple[TraceEvent, ...]
    intents: tuple[ToolIntent, ...]
    outputs: tuple[bytes, ...]
    memory_blob: MemoryBlob
    #: Times the injected provider was reached. Zero on a reproduced replay:
    #: reaching it once already aborts (there is nothing it can serve).
    provider_calls: int = 0
    failure: FailureContext | None = None
    error_type: str = ""
    upserts: tuple[object, ...] = field(default=())


def run_replay(
    bundle: ReplayBundle,
    agent: Agent,
    *,
    tool_registry: ToolRegistry | None = None,
    decode: Decode | None = None,
) -> ReplayOutcome:
    """Re-run the bundle's activation locally, outside any Beam pipeline.

    Read-only tools re-execute for real (design D6); side effects cannot,
    because ``ctx.act`` only stages intents and a ``side_effect=True`` tool is
    refused before it runs — so a replay can be run any number of times without
    performing an effect, structurally.
    """
    snapshot = bundle.snapshot
    client = CacheOnlyLLMClient(
        entity_key=bundle.entity_key,
        seq=bundle.seq,
        digest_only=digest_only_digests(snapshot.llm_cache),
    )
    try:
        result = asyncio.run(
            run_activation(
                agent,
                entity_key=bundle.entity_key,
                seq=bundle.seq,
                now_ms=bundle.now_ms,
                provider=client,
                memory_blob=snapshot.memory if snapshot.HasField("memory") else None,
                cache_blob=snapshot.llm_cache if snapshot.HasField("llm_cache") else None,
                event=bundle.envelope.external_event,
                resume_result=(
                    bundle.envelope.tool_result
                    if bundle.envelope.WhichOneof("payload") == "tool_result"
                    else None
                ),
                resume_approval=(
                    bundle.envelope.approval
                    if bundle.envelope.WhichOneof("payload") == "approval"
                    else None
                ),
                snapshot=bundle.adapter_snapshot,
                step_index=bundle.step_index,
                decode=decode,
                tool_registry=tool_registry,
            )
        )
    except ActivationFailed as failed:
        miss = _cache_miss_in(failed)
        if miss is not None:
            raise ReplayIrreproducibleError(
                f"irreproducible: pre-activation state not captured or entry not committed — {miss}"
            ) from failed
        cause = failed.__cause__ if failed.__cause__ is not None else failed
        error_type = type(cause).__name__
        # A failed activation commits nothing, so what the pipeline *emitted*
        # for it is the DoFn's synthesized ERROR event, not the staged traces it
        # threw away. Rebuilding it through the DoFn's own builder is what makes
        # the failure comparable to the traced record byte for byte.
        error_event = _error_trace(
            bundle.entity_key,
            bundle.seq,
            bundle.now_ms,
            REASON_ERROR,
            error_type=error_type,
            failure=failed.context,
        ).value
        return ReplayOutcome(
            status="failed",
            traces=(error_event,),
            intents=(),
            outputs=(),
            memory_blob=MemoryBlob(),
            provider_calls=client.calls,
            failure=failed.context,
            error_type=error_type,
        )

    return ReplayOutcome(
        status=result.status,
        traces=tuple(result.traces),
        intents=tuple(result.intents),
        outputs=tuple(result.outputs),
        memory_blob=result.memory_blob,
        provider_calls=client.calls,
    )


def _cache_miss_in(error: BaseException) -> ReplayCacheMissError | None:
    """The cache miss inside a failure chain, if the failure is one.

    The tripwire raises inside ``call_model``, so it reaches the caller wrapped
    in ``ActivationFailed``; walking the chain keeps "the agent raised" and "the
    replay had nothing to serve" from being reported as the same thing.
    """
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        if isinstance(current, ReplayCacheMissError):
            return current
        seen.add(id(current))
        current = current.__cause__ if current.__cause__ is not None else current.__context__
    return None
