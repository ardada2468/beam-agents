"""Shared fixtures for the LangGraph adapter tests: ActivationContext builders."""

from __future__ import annotations

from typing import TYPE_CHECKING

from beam_agents.core.context import ActivationContext
from beam_agents.model.fake import FakeLLM

if TYPE_CHECKING:
    from beam_agents._protos import AgentEnvelope, LlmCacheBlob, MemoryBlob, ToolResult
    from beam_agents.model.client import LLMClient

NOW_MS = 1_700_000_000_000
ENTITY_KEY = b"entity-1"


def make_ctx(
    *,
    event: bytes = b"{}",
    seq: int = 1,
    provider: LLMClient | None = None,
    memory_blob: MemoryBlob | None = None,
    cache_blob: LlmCacheBlob | None = None,
    resume_result: ToolResult | None = None,
    resume_approval: AgentEnvelope.Approval | None = None,
    snapshot: bytes = b"",
    step_index: int = 0,
    now_ms: int = NOW_MS,
) -> ActivationContext:
    return ActivationContext(
        entity_key=ENTITY_KEY,
        seq=seq,
        now_ms=now_ms,
        provider=provider if provider is not None else FakeLLM(),
        memory_blob=memory_blob,
        cache_blob=cache_blob,
        event=event,
        resume_result=resume_result,
        resume_approval=resume_approval,
        snapshot=snapshot,
        step_index=step_index,
    )
