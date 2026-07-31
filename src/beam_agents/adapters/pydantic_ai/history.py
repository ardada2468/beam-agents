"""Message-history persistence: the conversation as one working-memory scalar.

The adapter serializes the run's full model-message list with Pydantic AI's
documented ``ModelMessagesTypeAdapter`` JSON round-trip and stores it
latest-only as the ``__pydantic_ai__/messages`` scalar through the
activation's :class:`~beam_agents.memory.facade.Memory` facade. Because the
facade stages in memory and the stateful DoFn commits the resulting
``MemoryBlob`` atomically with the Beam bundle, history durability *is*
bundle atomicity (correctness invariant 1): a failed or timed-out activation
leaves no history mutation, and a worker failover reloads the committed
history. Cross-activation conversation continuity and TTL GC fall out of
ordinary working-memory behavior.

Size caps: history counts against the 100 KiB blob guidance and the 1 MiB
working-memory hard cap — an oversized history raises
:class:`~beam_agents.memory.facade.MemoryOverflow`, failing the activation
closed to ``.errors`` with no partial state. Keep conversations small: trim or
summarize on the Pydantic AI side (history processors) well before the cap.

No version tag on the scalar (change task 3.2): the framework's message
schema is designed for durable JSON storage and validated on read, so a
future schema break surfaces as a loud validation error that fails the
activation closed — never silent corruption. A framework-side migration need
would be a new change, not a hidden branch here.

Compactors must not evict ``__pydantic_ai__/`` keys: they are load-bearing
conversation state, not cache (same trust model as ``__langgraph__/``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter

if TYPE_CHECKING:
    from collections.abc import Sequence

    from beam_agents.memory.facade import Memory

# The reserved working-memory namespace for Pydantic AI state. The adapter
# owns every key under this prefix; nothing else may write here.
_RESERVED_NAMESPACE = "__pydantic_ai__/"
_MESSAGES_KEY = _RESERVED_NAMESPACE + "messages"


def _load_history(memory: Memory) -> list[ModelMessage]:
    """The committed conversation for this key, or ``[]`` for a fresh one."""
    raw = memory.get(_MESSAGES_KEY)
    if raw is None:
        return []
    return list(ModelMessagesTypeAdapter.validate_json(raw))


def _save_history(memory: Memory, messages: Sequence[ModelMessage]) -> None:
    """Stage the run's full message list, latest-only, for bundle-atomic commit."""
    memory.set(_MESSAGES_KEY, ModelMessagesTypeAdapter.dump_json(list(messages)))
