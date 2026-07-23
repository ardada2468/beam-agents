"""Deterministic Beam coders for the seven beam_agents.v1 wire/state messages.

Every element and keyed-state value in the runtime is one of seven protobuf
messages. Beam's stock ``ProtoCoder`` reports itself non-deterministic (so Beam
would reject these types as GroupByKey keys) and uses plain
``SerializeToString()``, which does not order map fields. This module defines a
coder that serializes with ``deterministic=True`` and advertises
``is_deterministic() == True`` so the types can be grouping keys and state
values without a pickle fallback ever touching a wire or state path.

Determinism scope (design D3 / risks): ``deterministic=True`` produces
byte-identical output only within a single, pinned ``protobuf`` library
version. That is exactly the promise the replay cache and deterministic
intent-ID rules depend on — byte-identity *within* a pipeline run and across
bundle retries on the same workers. Cross-version serialization drift is a
documented protobuf limitation, handled separately by the golden-blob compat
tests (which assert semantic, not byte, equality) and by pinning ``protobuf``
in ``uv.lock``.

Registration is explicit: importing this module has no side effects. Call
:func:`register_coders` (idempotent) from pipeline construction and test
fixtures.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import apache_beam as beam
from apache_beam.coders.typecoders import registry as _coder_registry
from google.protobuf.message import Message

from beam_agents._protos import (
    AgentEnvelope,
    Continuation,
    LlmCacheBlob,
    MemoryBlob,
    ToolIntent,
    ToolResult,
    TraceEvent,
)

if TYPE_CHECKING:
    from apache_beam.coders.typecoders import CoderRegistry

# The seven message types this module encodes. Exposed for tests and for
# `register_coders` to iterate.
MESSAGE_TYPES: tuple[type[Message], ...] = (
    MemoryBlob,
    ToolIntent,
    ToolResult,
    TraceEvent,
    AgentEnvelope,
    Continuation,
    LlmCacheBlob,
)


class DeterministicProtoCoder(beam.coders.Coder):
    """Encodes a single protobuf message type with deterministic serialization.

    Parameterized by the concrete message class: the class is required at decode
    time because the wire bytes do not carry it. ``encode`` uses
    ``SerializeToString(deterministic=True)`` so repeated encodings of an equal
    value — including map fields such as ``TraceEvent.attributes`` — are
    byte-identical within a pinned protobuf version.
    """

    def __init__(self, proto_message_type: type[Message]) -> None:
        self.proto_message_type = proto_message_type

    def encode(self, value: Message) -> bytes:
        return value.SerializeToString(deterministic=True)

    def decode(self, encoded: bytes) -> Message:
        message = self.proto_message_type()
        message.ParseFromString(encoded)
        return message

    def is_deterministic(self) -> bool:
        return True

    def to_type_hint(self) -> type[Message]:
        return self.proto_message_type

    @classmethod
    def from_type_hint(
        cls, typehint: type[Message], unused_registry: CoderRegistry
    ) -> DeterministicProtoCoder:
        # Beam types Coder.from_type_hint as `-> CoderT` (return same type as
        # cls); returning a concrete coder is the pattern Beam's own subclasses
        # use (e.g. MapCoder). The narrowed return trips mypy's LSP `override`
        # check, suppressed for this module in pyproject.toml.
        if not (isinstance(typehint, type) and issubclass(typehint, Message)):
            raise ValueError(
                f"Expected a subclass of google.protobuf.message.Message, got {typehint!r}"
            )
        return cls(typehint)

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, DeterministicProtoCoder)
            and self.proto_message_type is other.proto_message_type
        )

    def __hash__(self) -> int:
        return hash(self.proto_message_type)


def register_coders() -> None:
    """Register :class:`DeterministicProtoCoder` for all seven message types.

    Idempotent: safe to call from every entry point (pipeline construction,
    test fixtures). Does nothing but populate Beam's global coder registry, so
    that ``registry.get_coder(MsgType)`` resolves the deterministic coder rather
    than a pickle-based fallback.
    """
    for message_type in MESSAGE_TYPES:
        _coder_registry.register_coder(message_type, DeterministicProtoCoder)
