"""Spec: pydantic-ai-adapter / Requirement: Message history persists through the
Memory facade and commits atomically.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("pydantic_ai")

from pydantic_ai import Agent

from beam_agents._protos import MemoryBlob
from beam_agents.adapters.pydantic_ai import PydanticAIAgent
from beam_agents.adapters.pydantic_ai.history import (
    MESSAGES_KEY,
    load_history,
    save_history,
)
from beam_agents.core.agent import Complete
from beam_agents.core.context import ActivationContext
from beam_agents.memory.facade import Memory, MemoryOverflow
from tests.adapters.pydantic_ai._helpers import (
    NOW_MS,
    RecognizedModel,
    make_ctx,
    scripted,
    tripwire,
)


async def _run_once(
    model_id: str, answer: str, **ctx_kwargs: Any
) -> tuple[Complete, ActivationContext]:
    model = RecognizedModel(model_id, tripwire())
    provider = scripted(model_id, [b'{"answer": "%s"}' % answer.encode()])
    agent = PydanticAIAgent(Agent(model))
    ctx = make_ctx(provider=provider, **ctx_kwargs)
    outcome = await agent(ctx)
    assert isinstance(outcome, Complete)
    return outcome, ctx


async def test_history_round_trips_through_the_reserved_scalar() -> None:
    # The serialization seam itself: a run's messages persist as one scalar
    # under `__pydantic_ai__/messages` and validate back to equal messages.
    model = RecognizedModel("m-round", tripwire())
    provider = scripted("m-round", [b'{"answer": "round"}'])
    agent = PydanticAIAgent(Agent(model))
    ctx = make_ctx(provider=provider)
    await agent(ctx)

    committed = ctx.memory_blob()
    keys = [entry.key for entry in committed.entries]
    assert keys == [MESSAGES_KEY]

    reloaded = load_history(Memory(committed, now_ms=NOW_MS))
    assert reloaded, "the committed scalar must decode to the run's messages"

    fresh = Memory(MemoryBlob(), now_ms=NOW_MS)
    save_history(fresh, reloaded)
    assert load_history(fresh) == reloaded


async def test_failed_activation_leaves_no_history_mutation() -> None:
    # Scenario: Failed activation leaves no history mutation — the committed
    # MemoryBlob is byte-identical to its pre-activation state.
    _, first_ctx = await _run_once("m-fail", "first")
    committed = first_ctx.memory_blob()
    before = committed.SerializeToString(deterministic=True)

    # A second activation whose run raises: the context is discarded without
    # drain, so nothing it staged can ever reach keyed state.
    model = RecognizedModel("m-fail", tripwire())
    provider = scripted("m-fail", [])  # every request is unmatched -> raises
    agent = PydanticAIAgent(Agent(model))
    failing_ctx = make_ctx(
        provider=provider,
        seq=2,
        memory_blob=MemoryBlob.FromString(before),
    )
    with pytest.raises(Exception):  # noqa: B017 - any activation failure fails closed
        await agent(failing_ctx)

    # The pre-activation blob the DoFn would keep is untouched.
    assert MemoryBlob.FromString(before).SerializeToString(deterministic=True) == before


async def test_conversation_continues_across_activations_on_the_same_key() -> None:
    # Scenario: Conversation continues across activations on the same key — the
    # second run receives the committed history and the committed history after
    # it reflects both conversations.
    model = RecognizedModel("m-continue", tripwire())
    provider = scripted("m-continue", [b'{"answer": "first"}', b'{"answer": "second"}'])
    agent = PydanticAIAgent(Agent(model))

    first_ctx = make_ctx(provider=provider, event=b"one", seq=1)
    first = await agent(first_ctx)
    assert isinstance(first, Complete)
    first_history = load_history(Memory(first_ctx.memory_blob(), now_ms=NOW_MS))

    second_ctx = make_ctx(
        provider=provider, event=b"two", seq=2, memory_blob=first_ctx.memory_blob()
    )
    second = await agent(second_ctx)
    assert isinstance(second, Complete)

    second_history = load_history(Memory(second_ctx.memory_blob(), now_ms=NOW_MS))
    assert len(second_history) > len(first_history), (
        "the second activation must extend the committed conversation"
    )
    assert second_history[: len(first_history)] == first_history


async def test_ttl_cleared_memory_starts_a_fresh_conversation() -> None:
    # Scenario: History expires with the memory TTL — an activation whose
    # working memory was GC'd sees no prior history.
    _, ctx = await _run_once("m-ttl", "first")
    assert load_history(Memory(ctx.memory_blob(), now_ms=NOW_MS))

    # TTL GC wipes the blob; the next activation loads an empty one.
    wiped = make_ctx(memory_blob=MemoryBlob(), seq=3)
    assert load_history(wiped.memory) == []


async def test_oversized_history_fails_closed_with_memory_overflow() -> None:
    # An oversized history raises MemoryOverflow, failing the activation closed
    # rather than committing a partial conversation.
    memory = Memory(MemoryBlob(), now_ms=NOW_MS)
    model = RecognizedModel("m-big", tripwire())
    provider = scripted("m-big", [b'{"answer": "%s"}' % (b"x" * 2_000_000)])
    agent = PydanticAIAgent(Agent(model))
    ctx = make_ctx(provider=provider)

    with pytest.raises(MemoryOverflow):
        await agent(ctx)
    assert memory.get(MESSAGES_KEY) is None
