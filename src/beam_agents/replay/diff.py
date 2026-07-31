"""Comparing a replayed activation against its traced record (design D7).

Everything in a replayed trace event is a pure function of the reconstructed
inputs — identifiers are ``uuid5`` over ``(entity_key, seq, role, index)``,
attributes are derived, and every timestamp is the injected ``now_ms`` — so the
primary comparison is exact: deterministic bytes, event by event, in order.

Exactly two attributes are normalized before that comparison, and the list is
closed: a call the original made against the provider reports
``beam_agents.cache_hit = false`` / ``beam_agents.billed = true``, while the
replay serving the same call from the snapshot's blob reports the opposite. That
difference is the point of replay, not a divergence. Nothing else is normalized
— a model-name or token-count drift is a real difference and is reported.

Outputs and the post-activation memory blob have **no traced counterpart**
(traces carry positions and identities, never payloads), so they are reported as
sha256 digests and sizes rather than diffed against a baseline the CLI does not
have.

Importing this module has no side effects.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING

from beam_agents._protos import ToolIntent, TraceEvent
from beam_agents.observability.traces import (
    ACTIVATION_STATUS,
    BILLED,
    CACHE_HIT,
    EXPIRES_AT_MS,
    INTENT_ID,
    INTENT_KIND,
    TOOL_NAME,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from beam_agents.replay.bundle import ReplayBundle, ReplayOutcome

__all__ = [
    "NORMALIZED_ATTRIBUTES",
    "DiffReport",
    "Difference",
    "compare",
    "normalize_event",
]

#: The closed normalization list (design D7). A replayed call is always served
#: from the blob, so these two attributes legitimately differ from a traced call
#: that reached the provider; every other attribute is compared as-is.
NORMALIZED_ATTRIBUTES: tuple[str, ...] = (CACHE_HIT, BILLED)

_NORMALIZED_VALUES = {CACHE_HIT: "true", BILLED: "false"}


def normalize_event(event: TraceEvent) -> TraceEvent:
    """Return a copy with the cache-serving attributes pinned to their replay values.

    Applies only to ``LLM_CALL`` events, which are the only ones that carry
    them; every other event is copied unchanged.
    """
    normalized = TraceEvent()
    normalized.CopyFrom(event)
    if event.event_type == TraceEvent.LLM_CALL:
        for name, value in _NORMALIZED_VALUES.items():
            normalized.attributes[name] = value
    return normalized


@dataclass(frozen=True, slots=True)
class Difference:
    """One reported divergence: what kind, and what differs."""

    kind: str
    detail: str


@dataclass(frozen=True, slots=True)
class DiffReport:
    """The verdict, the differences, and the digests of what has no baseline."""

    differences: tuple[Difference, ...]
    notes: tuple[str, ...]

    @property
    def reproduced(self) -> bool:
        """Whether the replay matched the recorded trace exactly."""
        return not self.differences

    def render(self) -> str:
        """The report as human-readable lines, one per note and difference."""
        lines = ["reproduced" if self.reproduced else "diverged"]
        lines.extend(f"  {note}" for note in self.notes)
        lines.extend(f"  {d.kind}: {d.detail}" for d in self.differences)
        return "\n".join(lines)


def compare(bundle: ReplayBundle, outcome: ReplayOutcome) -> DiffReport:
    """Compare a re-run against the traced record on the comparable surface."""
    differences: list[Difference] = []
    differences.extend(_status_differences(bundle.traced, outcome.status))
    differences.extend(_event_differences(bundle.traced, outcome.traces))
    differences.extend(_intent_differences(bundle.traced, outcome.intents))
    return DiffReport(differences=tuple(differences), notes=_notes(outcome))


def _traced_status(traced: Sequence[TraceEvent]) -> str:
    """The status the traced attempt ended with, or ``""`` if it has no terminal."""
    for event in traced:
        if event.event_type == TraceEvent.ERROR:
            return "failed"
    for event in traced:
        if event.event_type == TraceEvent.ACTIVATION_END:
            return event.attributes.get(ACTIVATION_STATUS, "")
    return ""


def _status_differences(traced: Sequence[TraceEvent], status: str) -> list[Difference]:
    expected = _traced_status(traced)
    if not expected or expected == status:
        return []
    return [Difference("status", f"traced {expected!r}, replayed {status!r}")]


def _event_differences(
    traced: Sequence[TraceEvent], replayed: Sequence[TraceEvent]
) -> list[Difference]:
    differences: list[Difference] = []
    if len(traced) != len(replayed):
        differences.append(
            Difference(
                "trace_event",
                f"event count differs: traced {len(traced)}, replayed {len(replayed)}",
            )
        )
    for index, (left, right) in enumerate(zip(traced, replayed, strict=False)):
        normalized_left = normalize_event(left)
        normalized_right = normalize_event(right)
        if normalized_left.SerializeToString(
            deterministic=True
        ) == normalized_right.SerializeToString(deterministic=True):
            continue
        # First divergence only: everything after it is downstream of the same
        # cause, and listing it would bury the one position that matters.
        differences.append(
            Difference(
                "trace_event",
                f"first divergence at position {index} "
                f"({TraceEvent.EventType.Name(left.event_type)}): "
                + "; ".join(_field_differences(normalized_left, normalized_right)),
            )
        )
        break
    return differences


def _field_differences(traced: TraceEvent, replayed: TraceEvent) -> list[str]:
    """Field-level detail for one diverging event, traced value first."""
    parts: list[str] = []
    for name in ("event_type", "step_index", "start_ms", "end_ms"):
        left = getattr(traced, name)
        right = getattr(replayed, name)
        if left != right:
            parts.append(f"{name}: traced {left!r}, replayed {right!r}")
    for name in ("trace_id", "span_id", "parent_span_id"):
        left_id = getattr(traced, name)
        right_id = getattr(replayed, name)
        if left_id != right_id:
            parts.append(f"{name}: traced {left_id.hex()}, replayed {right_id.hex()}")
    for key in sorted(set(traced.attributes) | set(replayed.attributes)):
        left_value = traced.attributes.get(key)
        right_value = replayed.attributes.get(key)
        if left_value != right_value:
            parts.append(f"{key}: traced {left_value!r}, replayed {right_value!r}")
    return parts or ["events differ in an unrepresented field"]


def _intent_differences(
    traced: Sequence[TraceEvent], intents: Sequence[ToolIntent]
) -> list[Difference]:
    """Compare staged intent identity against the traced INTENT_EMITTED attributes."""
    traced_intents = [
        (
            event.attributes.get(INTENT_ID, ""),
            event.attributes.get(TOOL_NAME, ""),
            event.attributes.get(INTENT_KIND, ""),
            event.attributes.get(EXPIRES_AT_MS, ""),
        )
        for event in traced
        if event.event_type == TraceEvent.INTENT_EMITTED
    ]
    replayed_intents = [
        (
            intent.intent_id,
            intent.tool_name,
            ToolIntent.Kind.Name(intent.kind),
            str(intent.expires_at_ms),
        )
        for intent in intents
    ]
    if len(traced_intents) != len(replayed_intents):
        return [
            Difference(
                "intent",
                f"intent count differs: traced {len(traced_intents)}, "
                f"replayed {len(replayed_intents)}",
            )
        ]
    differences: list[Difference] = []
    names = ("intent_id", "tool_name", "kind", "expires_at_ms")
    for index, (left, right) in enumerate(zip(traced_intents, replayed_intents, strict=True)):
        mismatched = [
            f"{name}: traced {a!r}, replayed {b!r}"
            for name, a, b in zip(names, left, right, strict=True)
            if a != b
        ]
        if mismatched:
            differences.append(Difference("intent", f"intent {index}: " + "; ".join(mismatched)))
    return differences


def _notes(outcome: ReplayOutcome) -> tuple[str, ...]:
    """Digests for what the trace has no counterpart for — reported, not diffed."""
    notes = [f"provider calls: {outcome.provider_calls}"]
    if outcome.outputs:
        for index, payload in enumerate(outcome.outputs):
            notes.append(
                f"outputs[{index}]: {len(payload)} bytes, "
                f"sha256={hashlib.sha256(payload).hexdigest()}"
            )
    else:
        notes.append("outputs: none")
    blob = outcome.memory_blob.SerializeToString(deterministic=True)
    notes.append(
        f"memory blob: {len(blob)} bytes, sha256={hashlib.sha256(blob).hexdigest()} "
        "(no traced counterpart — reported, not diffed)"
    )
    return tuple(notes)
