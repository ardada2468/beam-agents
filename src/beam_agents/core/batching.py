"""Adaptive batching: the policy enum, its settings, and the flush decisions.

`BatchPolicy.NONE` (the default) is today's runtime, unchanged: one activation
per event, ``ctx.event`` as ``bytes``, ``BATCH`` never read or written and
``FLUSH_TIMER`` never armed. `BatchPolicy.ADAPTIVE` buffers an event burst per
key and turns it into one activation over a list of events, flushed when the
buffer reaches ``max_batch_size`` or when ``max_wait_ms`` of *processing* time
has elapsed since the buffer's first element.

Everything here is pure: an enum, a frozen settings triple, one validating
resolver, and three predicates over integers. No Beam import, no clock, no
state — the DoFn owns all of those, and these decisions are unit-testable
without a runner. Importing this module has no side effects.

See ``openspec/changes/add-adaptive-batching/design.md`` for the load-bearing
decisions: the enum on `AgentConfig` with `NONE` as a zero-diff default (D1),
only ``external_event`` elements buffering (D2), arming the timer once on the
empty-to-non-empty transition from an injected wall clock (D3), one activation
per flush over the whole buffer (D4), whole-batch suspension (D5), deferral
while a continuation is live (D6), and the fail-closed failure/overflow paths
(D7).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Final

__all__ = [
    "BUFFER_HEADROOM",
    "DEFAULT_MAX_BATCH_SIZE",
    "DEFAULT_MAX_WAIT_MS",
    "TRACE_BATCH_SIZE",
    "TRACE_BATCH_TRIGGER",
    "TRIGGER_SIZE",
    "TRIGGER_TIMER",
    "BatchPolicy",
    "BatchSettings",
    "buffer_is_full",
    "resolve_batch_settings",
    "should_flush_on_size",
    "should_flush_on_timer",
]


class BatchPolicy(Enum):
    """How ``RunAgent`` turns incoming events into activations.

    ``NONE`` is the default and is specified to preserve per-event semantics
    byte-for-byte; ``ADAPTIVE`` is opt-in and changes the agent-visible
    contract (``ctx.event`` becomes a ``list[bytes]``), so it is never
    inferred from the presence of a knob.
    """

    NONE = "none"
    ADAPTIVE = "adaptive"


# Defaults for an `ADAPTIVE` pipeline that names no knobs. Ten events is a
# useful burst without risking the 100 KiB blob cap on ordinary payloads, and
# half a second is inside the freshness budget of the target workloads (IoT
# reaction, fraud triage) while still collapsing a burst into one decision.
DEFAULT_MAX_BATCH_SIZE: Final[int] = 10
DEFAULT_MAX_WAIT_MS: Final[int] = 500
# `max_buffered_events` defaults to this multiple of `max_batch_size`: the cap
# only binds while a suspension defers flushing (design D6/D7), so it needs
# headroom above the trigger threshold, not equality with it.
BUFFER_HEADROOM: Final[int] = 4

# Which trigger produced a flush. Carried on the flush's trace and on the
# dead-letter detail of a failed one, so a poison batch assembled by the size
# threshold is distinguishable from one the max_wait deadline assembled.
TRIGGER_SIZE: Final[str] = "size"
TRIGGER_TIMER: Final[str] = "timer"

# Trace attributes stamped on a flush activation's ACTIVATION_START event.
# `beam_agents.*` rather than `gen_ai.*` for the same reason the other runtime
# attributes are: the GenAI semantic conventions name nothing like this.
TRACE_BATCH_SIZE: Final[str] = "beam_agents.batch.size"
TRACE_BATCH_TRIGGER: Final[str] = "beam_agents.batch.trigger"


@dataclass(frozen=True, slots=True)
class BatchSettings:
    """The resolved, validated batching bounds for an `ADAPTIVE` pipeline.

    Constructed only by :func:`resolve_batch_settings` (from `AgentConfig`),
    which is where the validation lives; the DoFn receives this triple, or
    ``None`` under `BatchPolicy.NONE`, and "is batching on?" is exactly
    "is this not ``None``?".
    """

    #: Buffer length at which a flush triggers inline, in `process()`.
    max_batch_size: int
    #: Processing-time bound, measured from the buffer's first element.
    max_wait_ms: int
    #: Hard cap on buffered envelopes; beyond it, events dead-letter.
    max_buffered_events: int


def _require_positive(field_name: str, value: int) -> int:
    if value <= 0:
        raise ValueError(f"AgentConfig.{field_name} must be positive, got {value!r}")
    return value


def resolve_batch_settings(
    policy: BatchPolicy,
    *,
    max_batch_size: int | None,
    max_wait_ms: int | None,
    max_buffered_events: int | None,
) -> BatchSettings | None:
    """Validate the batch knobs against ``policy`` and resolve their defaults.

    Returns ``None`` under `BatchPolicy.NONE` — there is nothing to configure,
    and the DoFn takes that as "never touch ``BATCH`` or ``FLUSH_TIMER``".

    Raises ``ValueError`` naming the offending field: a non-positive bound, a
    ``max_buffered_events`` too small to ever hold a batch, or a knob set
    without opting in. That last one is deliberate rather than forgiving — a
    knob that silently does nothing is a misconfiguration trap, and the
    construction site is where a typo should surface (correctness: fail at the
    site of the mistake, before any pipeline exists).
    """
    if policy is BatchPolicy.NONE:
        for field_name, value in (
            ("max_batch_size", max_batch_size),
            ("max_wait_ms", max_wait_ms),
            ("max_buffered_events", max_buffered_events),
        ):
            if value is not None:
                raise ValueError(
                    f"AgentConfig.{field_name}={value!r} has no effect under "
                    f"batch_policy=BatchPolicy.NONE; set "
                    f"batch_policy=BatchPolicy.ADAPTIVE to enable batching, or "
                    f"leave {field_name} unset"
                )
        return None

    size = _require_positive(
        "max_batch_size",
        max_batch_size if max_batch_size is not None else DEFAULT_MAX_BATCH_SIZE,
    )
    wait_ms = _require_positive(
        "max_wait_ms", max_wait_ms if max_wait_ms is not None else DEFAULT_MAX_WAIT_MS
    )
    cap = _require_positive(
        "max_buffered_events",
        max_buffered_events if max_buffered_events is not None else BUFFER_HEADROOM * size,
    )
    if cap < size:
        raise ValueError(
            f"AgentConfig.max_buffered_events must be >= max_batch_size "
            f"({size}), got {cap!r}: a cap below the flush threshold would "
            f"dead-letter events the buffer is meant to hold"
        )
    return BatchSettings(max_batch_size=size, max_wait_ms=wait_ms, max_buffered_events=cap)


def should_flush_on_size(buffered: int, max_batch_size: int, *, continuation_live: bool) -> bool:
    """Whether appending brought the buffer to its inline flush threshold.

    ``>=`` rather than ``==``: deferral (design D6) lets the buffer grow past
    the threshold while a suspension is live, and the whole buffer flushes as
    one batch the moment it can — ``max_batch_size`` is a trigger threshold,
    not a hard batch cap.

    A live continuation suppresses the trigger entirely: a flush that itself
    suspended would overwrite the live `Continuation` (a single-value spec) and
    orphan its pending intents. That is the whole hazard class, closed here.
    """
    return buffered >= max_batch_size and not continuation_live


def should_flush_on_timer(buffered: int, *, continuation_live: bool) -> bool:
    """Whether a ``FLUSH_TIMER`` delivery should run a flush.

    Two guards, both no-ops rather than errors. An empty buffer means a stale
    mark — a size flush cleared the timer but the delivery arrived anyway —
    and mutating or emitting anything for it would re-process a consumed
    batch; this mirrors the stale-handle guard in ``on_hitl``. A live
    continuation means the same deferral as the size trigger, except the
    callback leaves the buffer intact for the resolving path to re-arm.
    """
    return buffered > 0 and not continuation_live


def buffer_is_full(buffered: int, max_buffered_events: int) -> bool:
    """Whether the buffer has reached its hard cap and must shed the next event.

    Deferral is time-bounded (a suspension always ends by its ``deadline_ms``),
    but a hot key can buffer a lot inside that window, so the cap is what keeps
    keyed state bounded. Past it the runtime dead-letters explicitly —
    counted and triageable on ``.errors`` — rather than growing state silently
    toward the 1 MiB working-state cap.
    """
    return buffered >= max_buffered_events
