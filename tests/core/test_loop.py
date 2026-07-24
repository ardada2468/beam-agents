"""Unit tests for the async activation driver (``run_activation``).

Beam-free coverage of the loop step: staged blobs, outcome handling, the
replay-cache zero-extra-provider-call invariant on retry, continuation assembly,
and error propagation (which leaves nothing to commit).
"""

from __future__ import annotations

import pytest

from beam_agents._protos import ToolResult
from beam_agents.core.loop import run_activation
from tests.core._dofn_helpers import (
    append_agent,
    make_pong_provider,
    model_agent,
    raising_agent,
    seq_agent,
    suspend_then_complete_agent,
)


async def test_completed_activation_stages_output_and_seq() -> None:
    result = await run_activation(
        seq_agent,
        entity_key=b"k",
        seq=5,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
    )
    assert result.status == "completed"
    assert result.seq == 5
    assert result.outputs == [b"5"]
    assert result.continuation is None


async def test_memory_write_is_staged_into_blob() -> None:
    result = await run_activation(
        append_agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
        event=b"a",
    )
    keys = {entry.key for entry in result.memory_blob.entries}
    assert keys == {"log"}
    assert result.outputs == [b"a#0"]


async def test_retry_incurs_zero_extra_provider_calls() -> None:
    # Scenario: replay of a retried bundle incurs zero extra provider calls.
    provider = make_pong_provider()
    first = await run_activation(
        model_agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=provider,
        memory_blob=None,
        cache_blob=None,
    )
    assert provider.call_count == 1
    assert first.outputs == [b"pong"]
    assert len(first.cache_blob.entries) == 1

    # A retried bundle re-runs the SAME activation (same seq) against the cache
    # committed by the first run: the provider is not called again.
    replay = await run_activation(
        model_agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=provider,
        memory_blob=None,
        cache_blob=first.cache_blob,
    )
    assert provider.call_count == 1
    assert replay.outputs == [b"pong"]


async def test_suspend_builds_continuation_and_intents() -> None:
    result = await run_activation(
        suspend_then_complete_agent,
        entity_key=b"k",
        seq=3,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
    )
    assert result.status == "suspended"
    assert result.outputs == []
    assert len(result.intents) == 1
    intent = result.intents[0]
    assert intent.seq == 3
    assert result.continuation is not None
    assert list(result.continuation.pending_intent_ids) == [intent.intent_id]
    assert result.continuation.seq == 3
    assert result.hitl_deadline_ms == 1000 + 1000


async def test_resume_uses_continuation_seq_and_completes() -> None:
    resume = ToolResult(intent_id="i", entity_key=b"k", seq=3, payload=b"done")
    result = await run_activation(
        suspend_then_complete_agent,
        entity_key=b"k",
        seq=3,
        now_ms=2000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
        resume_result=resume,
    )
    assert result.status == "completed"
    assert result.outputs == [b"resumed:done"]


async def test_agent_error_propagates_and_stages_nothing() -> None:
    # The exception surfaces; the caller commits nothing, so no ActivationResult
    # (and therefore no staged blob) escapes.
    with pytest.raises(RuntimeError, match="agent blew up"):
        await run_activation(
            raising_agent,
            entity_key=b"k",
            seq=0,
            now_ms=1000,
            provider=make_pong_provider(),
            memory_blob=None,
            cache_blob=None,
        )
