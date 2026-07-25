"""Side-effect intents and outbox sinks."""

from __future__ import annotations

from beam_agents.actions.write_intents import (
    DEAD_LETTER_TAG,
    UnknownIntentsSchemeError,
    WriteIntents,
    WriteIntentsResult,
)

__all__ = [
    "DEAD_LETTER_TAG",
    "UnknownIntentsSchemeError",
    "WriteIntents",
    "WriteIntentsResult",
]
