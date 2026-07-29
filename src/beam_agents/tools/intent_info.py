"""Intent identity handed to opt-in side-effecting tools.

A tool that declares a keyword-only ``intent: IntentInfo`` parameter receives,
at effector execution time, the deterministic identity of the `ToolIntent`
being executed. `intent_id` is uuid5-derived from ``entity_key + seq +
step_index`` (correctness invariant 2), so every invocation of the same
logical effect — across pipeline replays, sink duplicates, and lease-expiry
re-executions — carries the identical value, making it the ideal downstream
idempotency key (a Stripe ``Idempotency-Key``, a Redis ``SETNX`` key, a keyed
upsert).

Deliberately a plain frozen dataclass, not the wire proto: tool authors must
not need generated ``_pb2`` types, and the injection surface is exactly
identity — never the intent's payload, expiry, or kind.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class IntentInfo:
    """The identity of the `ToolIntent` a tool is executing under.

    ``attempt`` mirrors the intent's wire field verbatim; it is not an
    effector-side claim counter.
    """

    intent_id: str
    entity_key: bytes
    seq: int
    step_index: int
    attempt: int
