"""The loop driver's summarization pass (memory-compaction capability).

Covers where tier-2 compaction runs: `run_activation` invokes the configured
summarizer after the agent returns an outcome and before that outcome is folded
into a `Continuation`/`ActivationResult`, inside the existing failure wrap, and
only when the staged `memory.size_bytes` has reached the summarizer's trigger.
The placement is the whole point (design D2) — the call goes through
`ctx.call_model`, so it is replay-cached, staged, traced, and tallied like any
other model call, and a summarizer raise fails the activation atomically.
"""

from __future__ import annotations

import pytest

from beam_agents._protos import MemoryBlob, TraceEvent
from beam_agents.core.agent import Complete, Suspend
from beam_agents.core.context import ActivationContext
from beam_agents.core.loop import ActivationFailed, run_activation
from beam_agents.memory import Memory, SummarizeCompactor
from beam_agents.model.client import LlmRequest
from beam_agents.model.fake import FakeLLM, match_any, respond_with

_KEY = b"k"
_NOW_MS = 5_000
_TTL_MS = 60_000
_ITEMS = 20
_KEEP_RECENT = 8


def _provider() -> FakeLLM:
    return FakeLLM([(match_any(), respond_with(b"digest"))])


def build_request(items: tuple[bytes, ...], prior_summary: bytes | None) -> LlmRequest:
    """Pure function of its inputs — the summarizer's determinism obligation."""
    return LlmRequest(
        model_id="summarizer",
        messages=[[item.decode() for item in items], (prior_summary or b"").decode()],
        tools_schema=None,
        sampling_params=None,
    )


def extract_summary(response: bytes) -> bytes:
    return b"summary:" + response


def raising_extract(response: bytes) -> bytes:
    raise RuntimeError("summary extraction blew up")


def _summarizer(*, trigger_bytes: int, extract: object = extract_summary) -> SummarizeCompactor:
    return SummarizeCompactor(
        build_request=build_request,
        extract_summary=extract,  # type: ignore[arg-type]
        source_keys=("log",),
        keep_recent=_KEEP_RECENT,
        trigger_bytes=trigger_bytes,
    )


async def appending_agent(ctx: ActivationContext) -> Complete:
    for index in range(_ITEMS):
        ctx.memory.append("log", f"item-{index:02d}".encode(), max_items=64)
    return Complete(output=b"done")


async def appending_suspend_agent(ctx: ActivationContext) -> Suspend:
    for index in range(_ITEMS):
        ctx.memory.append("log", f"item-{index:02d}".encode(), max_items=64)
    ctx.act("http.post", '{"url":"x"}', ttl_ms=_TTL_MS)
    return Suspend(snapshot=b"waiting", adapter="test", timeout_ms=_TTL_MS)


def _committed(blob: MemoryBlob) -> Memory:
    return Memory(blob, now_ms=_NOW_MS)


async def test_crossing_the_trigger_folds_old_items_into_a_summary() -> None:
    # Scenario: Crossing the trigger folds old items into a summary.
    provider = _provider()
    result = await run_activation(
        appending_agent,
        entity_key=_KEY,
        seq=0,
        now_ms=_NOW_MS,
        provider=provider,
        memory_blob=None,
        cache_blob=None,
        summarizer=_summarizer(trigger_bytes=1),
    )

    assert provider.call_count == 1
    memory = _committed(result.memory_blob)
    assert memory.ring("log") == tuple(
        f"item-{i:02d}".encode() for i in range(_ITEMS - _KEEP_RECENT, _ITEMS)
    )
    assert memory.get("summary") == b"summary:digest"

    # Scenario support (task 4.4): the call lands in the existing observability
    # surfaces with no new plumbing.
    assert result.tally.llm_calls == 1
    assert len(result.tally.llm_ms) == 1
    llm_events = [e for e in result.traces if e.event_type == TraceEvent.LLM_CALL]
    assert len(llm_events) == 1
    assert llm_events[0].attributes["beam_agents.cache_hit"] == "false"


async def test_below_the_trigger_no_model_call_happens() -> None:
    # Scenario: Below the trigger no model call happens.
    provider = _provider()
    result = await run_activation(
        appending_agent,
        entity_key=_KEY,
        seq=0,
        now_ms=_NOW_MS,
        provider=provider,
        memory_blob=None,
        cache_blob=None,
        summarizer=_summarizer(trigger_bytes=1_000_000),
    )

    assert provider.call_count == 0
    memory = _committed(result.memory_blob)
    assert memory.ring("log") == tuple(f"item-{i:02d}".encode() for i in range(_ITEMS))
    assert memory.get("summary") is None
    assert result.tally.llm_calls == 0


def _staged_size_bytes() -> int:
    """The exact `memory.size_bytes` `appending_agent` leaves staged.

    Recomputed here through the same facade the driver reads rather than
    hard-coded: the encoding is `Memory`'s business, and a literal would go
    stale the first time it changed.
    """
    memory = Memory(MemoryBlob(), now_ms=_NOW_MS)
    for index in range(_ITEMS):
        memory.append("log", f"item-{index:02d}".encode(), max_items=64)
    return memory.size_bytes


@pytest.mark.parametrize(
    ("offset", "expect_compaction"),
    [(0, True), (1, False)],
)
async def test_the_trigger_fires_at_the_threshold_and_not_one_byte_above(
    offset: int, expect_compaction: bool
) -> None:
    # "the driver runs `compact` if and only if the staged `memory.size_bytes`
    # has *reached*" the trigger -- so the threshold itself is inside the range.
    # Pinned at exactly `size_bytes` and at `size_bytes + 1`: those are the two
    # adjacent values that tell `>=` from `>`, and every other trigger in the
    # suite sits far enough away that both comparisons agree.
    provider = _provider()

    result = await run_activation(
        appending_agent,
        entity_key=_KEY,
        seq=0,
        now_ms=_NOW_MS,
        provider=provider,
        memory_blob=None,
        cache_blob=None,
        summarizer=_summarizer(trigger_bytes=_staged_size_bytes() + offset),
    )

    assert provider.call_count == (1 if expect_compaction else 0)
    assert (_committed(result.memory_blob).get("summary") is not None) is expect_compaction


async def test_an_unconfigured_summarizer_leaves_the_activation_untouched() -> None:
    # Opt-in by construction: `AgentConfig.summarizer` defaults to None and the
    # driver then behaves exactly as before this change.
    provider = _provider()
    result = await run_activation(
        appending_agent,
        entity_key=_KEY,
        seq=0,
        now_ms=_NOW_MS,
        provider=provider,
        memory_blob=None,
        cache_blob=None,
    )

    assert provider.call_count == 0
    assert _committed(result.memory_blob).ring("log") == tuple(
        f"item-{i:02d}".encode() for i in range(_ITEMS)
    )


async def test_a_failing_summarizer_commits_nothing() -> None:
    # Scenario: A failing summarizer commits nothing. The summarization pass runs
    # inside the driver's failure wrap, so a raise there is an activation failure
    # like any other: `ActivationFailed` propagates and no `ActivationResult` is
    # produced, which is what leaves the caller committing nothing.
    provider = _provider()
    with pytest.raises(ActivationFailed) as excinfo:
        await run_activation(
            appending_agent,
            entity_key=_KEY,
            seq=0,
            now_ms=_NOW_MS,
            provider=provider,
            memory_blob=None,
            cache_blob=None,
            summarizer=_summarizer(trigger_bytes=1, extract=raising_extract),
        )

    cause = excinfo.value.__cause__
    assert isinstance(cause, RuntimeError)
    assert "summary extraction" in str(cause)


async def test_a_suspending_continuation_includes_the_summarizers_cursor_advance() -> None:
    # Scenario: A suspending activation's continuation includes the summarizer's
    # cursor advance. The summarizer runs before `build_continuation`, so the
    # persisted `step_index` already counts its `call_model`; a resume seeding
    # from that cursor can never re-mint an intent ID the suspension consumed.
    provider = _provider()
    result = await run_activation(
        appending_suspend_agent,
        entity_key=_KEY,
        seq=0,
        now_ms=_NOW_MS,
        provider=provider,
        memory_blob=None,
        cache_blob=None,
        summarizer=_summarizer(trigger_bytes=1),
    )

    assert result.status == "suspended"
    assert result.continuation is not None
    # act() consumed step 0; the summarizer's call_model consumed step 1.
    assert result.continuation.step_index == 2
    assert provider.call_count == 1
    # ...and the summarization is in the committed blob, not deferred to resume.
    assert _committed(result.memory_blob).get("summary") == b"summary:digest"


async def test_the_summarizers_response_is_staged_in_the_replay_cache() -> None:
    # The mechanism behind the replay guarantee the semantics gate proves end to
    # end: the call goes through `ctx.call_model`, so its response is staged in
    # the activation's `LlmCacheBlob` and committed with the bundle.
    provider = _provider()
    result = await run_activation(
        appending_agent,
        entity_key=_KEY,
        seq=0,
        now_ms=_NOW_MS,
        provider=provider,
        memory_blob=None,
        cache_blob=None,
        summarizer=_summarizer(trigger_bytes=1),
    )

    assert [entry.response for entry in result.cache_blob.entries] == [b"digest"]
