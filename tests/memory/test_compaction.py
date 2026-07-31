"""The two compaction strategies (memory-compaction capability).

`DropOldestCompactor` is tier 1: synchronous, LLM-free, invoked by the facade
itself at the soft-cap crossing and before hard-cap rejection. `SummarizeCompactor`
is tier 2: asynchronous, invoked by the loop driver *inside* the activation, so
its model calls ride `call_model`'s replay-cached path (design D1/D2).

Covers the requirements "DropOldestCompactor evicts least-recently-used entries
to a target and never touches protected prefixes" and the unit half of
"SummarizeCompactor runs inside the activation and calls the model only through
the activation's cache-first path"; the driver-level half lives in
``tests/core/test_loop_summarize.py`` and the replay half in
``tests/semantics/test_retry_determinism.py``.
"""

from __future__ import annotations

from unittest import mock

import pytest

from beam_agents._protos import MemoryBlob
from beam_agents.memory import (
    DropOldestCompactor,
    Memory,
    MemoryOverflow,
    SummarizeCompactor,
)
from beam_agents.memory.facade import HARD_CAP_BYTES
from beam_agents.model.client import LlmRequest, LlmResponse

NOW = 31_000
_LANGGRAPH_PREFIX = "__langgraph__/"


def _scalar_blob(*pairs: tuple[str, bytes]) -> MemoryBlob:
    blob = MemoryBlob(state_schema_version=1)
    total = 0
    for index, (key, value) in enumerate(pairs):
        encoded = b"\x00" + value
        blob.entries.add(key=key, value=encoded, last_access_ms=index)
        total += len(encoded)
    blob.total_value_bytes = total
    return blob


def _four_entries() -> MemoryBlob:
    """Four 101-byte entries in LRU order ``a, b, c, d`` (404 bytes total)."""
    return _scalar_blob(
        ("a", b"a" * 100),
        ("b", b"b" * 100),
        ("c", b"c" * 100),
        ("d", b"d" * 100),
    )


# --- DropOldestCompactor -------------------------------------------------------


def test_eviction_is_lru_first_and_stops_at_the_target() -> None:
    # Scenario: Eviction is LRU-first and stops at the target.
    mem = Memory(_four_entries(), now_ms=NOW)
    target = 210  # deleting `a` and `b` reaches it; deleting `c` would overshoot

    DropOldestCompactor(target_bytes=target).compact(mem)

    assert mem.keys() == ("c", "d")
    assert mem.size_bytes <= target
    assert mem.get("a") is None
    assert mem.get("b") is None


def test_eviction_stops_as_soon_as_the_target_is_reached() -> None:
    # The target is a stopping condition, not a budget to spend: an already
    # -compliant facade must lose nothing at all.
    mem = Memory(_four_entries(), now_ms=NOW)
    DropOldestCompactor(target_bytes=HARD_CAP_BYTES).compact(mem)
    assert mem.keys() == ("a", "b", "c", "d")


def test_protected_prefixes_survive_even_when_oldest() -> None:
    # Scenario: Protected prefixes survive even when oldest.
    blob = _scalar_blob(
        (f"{_LANGGRAPH_PREFIX}ckpt", b"resume-state"),
        ("a", b"a" * 100),
        ("b", b"b" * 100),
    )
    mem = Memory(blob, now_ms=NOW)

    DropOldestCompactor(target_bytes=120).compact(mem)

    persisted = {entry.key for entry in mem.to_blob().entries}
    assert f"{_LANGGRAPH_PREFIX}ckpt" in persisted
    assert "a" not in persisted  # the next-oldest unprotected entry went instead
    assert "b" in persisted


def test_only_protected_entries_left_still_over_target_is_not_an_error() -> None:
    # Scenario: Only protected entries left still over target is not an error.
    blob = _scalar_blob(
        (f"{_LANGGRAPH_PREFIX}one", b"x" * (HARD_CAP_BYTES // 2)),
        (f"{_LANGGRAPH_PREFIX}two", b"y" * (HARD_CAP_BYTES // 2)),
    )
    compactor = DropOldestCompactor(target_bytes=1_000)
    mem = Memory(blob, now_ms=NOW, compactor=compactor)

    compactor.compact(mem)  # returns without raising, evicts nothing

    assert mem.keys() == (f"{_LANGGRAPH_PREFIX}one", f"{_LANGGRAPH_PREFIX}two")

    # ...and the facade's own cap contract takes over: silently evicting
    # load-bearing resume state to admit a write would corrupt a suspended agent.
    with mock.patch("beam_agents.memory.facade.Metrics"), pytest.raises(MemoryOverflow):
        mem.set("payload", b"z" * 1_000)


def test_eviction_is_deterministic_across_replays() -> None:
    # Scenario: Eviction is deterministic across replays. `compact` reads only
    # the staged entries and its own frozen configuration -- no clock, no RNG,
    # no I/O -- so two facades built from one blob converge byte-for-byte.
    compactor = DropOldestCompactor(target_bytes=210)
    first = Memory(_four_entries(), now_ms=NOW)
    second = Memory(_four_entries(), now_ms=NOW)

    compactor.compact(first)
    compactor.compact(second)

    assert first.to_blob().SerializeToString(
        deterministic=True
    ) == second.to_blob().SerializeToString(deterministic=True)


def test_a_non_positive_target_is_refused_at_construction() -> None:
    # A misconfiguration, so it raises where the typo is -- not deep inside a
    # runner, mid-activation, with a facade half-evicted.
    with pytest.raises(ValueError, match="target_bytes"):
        DropOldestCompactor(target_bytes=0)
    with pytest.raises(ValueError, match="target_bytes"):
        DropOldestCompactor(target_bytes=-1)


def test_the_default_target_sits_below_the_soft_cap() -> None:
    # Hysteresis (design D3): one pass has to buy real headroom, or the facade
    # oscillates at the soft-cap boundary and tier 2 never gets room to work.
    assert DropOldestCompactor().target_bytes == HARD_CAP_BYTES // 2
    assert DropOldestCompactor().protected_prefixes == (_LANGGRAPH_PREFIX,)


def test_the_facade_invokes_the_default_compactor_at_the_hard_cap() -> None:
    # The tier-1 contract end to end: a hard-cap-crossing write that would have
    # raised now succeeds after LRU eviction, through the facade's existing
    # check->compact->reject order.
    blob = _scalar_blob(*[(f"old-{i}", b"x" * 150_000) for i in range(6)])
    mem = Memory(blob, now_ms=NOW, compactor=DropOldestCompactor())

    mem.set("new", b"n" * 150_000)

    stored = mem.keys()
    assert "new" in stored
    assert "old-0" not in stored
    assert mem.size_bytes <= HARD_CAP_BYTES


# --- SummarizeCompactor --------------------------------------------------------


class _FakeView:
    """The narrow surface the summarizer is handed: memory plus `call_model`.

    Deliberately has no `act`/`emit`/`stage_trace`: the summarizer must be
    structurally unable to stage an intent or an output.
    """

    def __init__(self, memory: Memory, response: bytes = b"SUMMARY") -> None:
        self.memory = memory
        self.response = response
        self.requests: list[LlmRequest] = []

    async def call_model(self, request: LlmRequest) -> LlmResponse:
        self.requests.append(request)
        return LlmResponse(self.response)


_BUILD_CALLS: list[tuple[tuple[bytes, ...], bytes | None]] = []


def _build_request(items: tuple[bytes, ...], prior_summary: bytes | None) -> LlmRequest:
    _BUILD_CALLS.append((items, prior_summary))
    return LlmRequest(
        model_id="m",
        messages=[[item.decode() for item in items], (prior_summary or b"").decode()],
        tools_schema=None,
        sampling_params=None,
    )


def _extract(response: bytes) -> bytes:
    return response


def _grow(response: bytes) -> bytes:
    """An `extract_summary` that inflates instead of shrinking."""
    return b"x" * 10_000


def _ring_memory(count: int, *, key: str = "log") -> Memory:
    mem = Memory(now_ms=NOW)
    for index in range(count):
        mem.append(key, f"item-{index:02d}".encode(), max_items=128)
    return mem


@pytest.fixture(autouse=True)
def _reset_build_calls() -> None:
    _BUILD_CALLS.clear()


async def test_the_oldest_items_fold_and_keep_recent_survives_verbatim() -> None:
    # Requirement: replace each source ring's items older than the newest
    # `keep_recent` with nothing, write the extracted summary under
    # `summary_key`, leave newer items verbatim.
    mem = _ring_memory(20)
    before = mem.size_bytes
    view = _FakeView(mem)
    compactor = SummarizeCompactor(
        build_request=_build_request,
        extract_summary=_extract,
        source_keys=("log",),
        keep_recent=8,
        trigger_bytes=1,
    )

    await compactor.compact(view)

    assert len(view.requests) == 1
    folded, prior = _BUILD_CALLS[0]
    assert folded == tuple(f"item-{i:02d}".encode() for i in range(12))
    assert prior is None
    assert mem.ring("log") == tuple(f"item-{i:02d}".encode() for i in range(12, 20))
    assert mem.get("summary") == b"SUMMARY"
    assert mem.size_bytes < before


async def test_a_prior_summary_is_handed_to_the_request_builder() -> None:
    # The fold is cumulative: an existing summary is an input to the next one,
    # or every pass would throw away everything the last one preserved.
    mem = _ring_memory(12)
    mem.set("summary", b"earlier")
    view = _FakeView(mem)
    compactor = SummarizeCompactor(
        build_request=_build_request,
        extract_summary=_extract,
        source_keys=("log",),
        keep_recent=4,
        trigger_bytes=1,
    )

    await compactor.compact(view)

    _, prior = _BUILD_CALLS[0]
    assert prior == b"earlier"
    assert mem.get("summary") == b"SUMMARY"


async def test_a_non_shrinking_summary_is_refused() -> None:
    # Requirement: an `extract_summary` result no smaller than the items it
    # replaces raises -- a "summary" that inflates memory is a defect, and the
    # raise fails the activation closed rather than committing the growth.
    mem = _ring_memory(20)
    view = _FakeView(mem)
    compactor = SummarizeCompactor(
        build_request=_build_request,
        extract_summary=_grow,
        source_keys=("log",),
        keep_recent=8,
        trigger_bytes=1,
    )

    with pytest.raises(ValueError, match="summary"):
        await compactor.compact(view)


async def test_nothing_to_fold_makes_no_model_call() -> None:
    # A ring at or below `keep_recent` has no old items; summarizing it would
    # be a provider call that removes nothing.
    mem = _ring_memory(5)
    view = _FakeView(mem)
    compactor = SummarizeCompactor(
        build_request=_build_request,
        extract_summary=_extract,
        source_keys=("log",),
        keep_recent=8,
        trigger_bytes=1,
    )

    await compactor.compact(view)

    assert view.requests == []
    assert mem.ring("log") == tuple(f"item-{i:02d}".encode() for i in range(5))
    assert mem.get("summary") is None


async def test_several_source_rings_fold_into_one_summary() -> None:
    mem = Memory(now_ms=NOW)
    for index in range(6):
        mem.append("log", f"log-{index}".encode(), max_items=64)
        mem.append("obs", f"obs-{index}".encode(), max_items=64)
    view = _FakeView(mem)
    compactor = SummarizeCompactor(
        build_request=_build_request,
        extract_summary=_extract,
        source_keys=("log", "obs"),
        keep_recent=2,
        trigger_bytes=1,
    )

    await compactor.compact(view)

    assert len(view.requests) == 1
    folded, _ = _BUILD_CALLS[0]
    assert folded == (
        b"log-0",
        b"log-1",
        b"log-2",
        b"log-3",
        b"obs-0",
        b"obs-1",
        b"obs-2",
        b"obs-3",
    )
    assert mem.ring("log") == (b"log-4", b"log-5")
    assert mem.ring("obs") == (b"obs-4", b"obs-5")


async def test_folding_every_item_removes_the_ring_entry() -> None:
    # `keep_recent=0` folds the whole ring; leaving an empty entry behind would
    # persist framing bytes for nothing.
    mem = _ring_memory(4)
    view = _FakeView(mem)
    compactor = SummarizeCompactor(
        build_request=_build_request,
        extract_summary=_extract,
        source_keys=("log",),
        keep_recent=0,
        trigger_bytes=1,
    )

    await compactor.compact(view)

    assert mem.keys() == ("summary",)


async def test_the_summarizer_surface_exposes_no_effect_path() -> None:
    # Requirement: it is handed a surface exposing only memory access and
    # `call_model`, so it cannot stage intents or outputs. The fake view has
    # neither method; a summarizer that reached for one would fail here.
    mem = _ring_memory(12)
    view = _FakeView(mem)
    assert not hasattr(view, "act")
    assert not hasattr(view, "emit")

    compactor = SummarizeCompactor(
        build_request=_build_request,
        extract_summary=_extract,
        source_keys=("log",),
        keep_recent=4,
        trigger_bytes=1,
    )
    await compactor.compact(view)

    assert mem.get("summary") == b"SUMMARY"


def test_summarizer_configuration_is_validated_at_construction() -> None:
    with pytest.raises(ValueError, match="source_keys"):
        SummarizeCompactor(build_request=_build_request, extract_summary=_extract, source_keys=())
    with pytest.raises(ValueError, match="keep_recent"):
        SummarizeCompactor(
            build_request=_build_request,
            extract_summary=_extract,
            source_keys=("log",),
            keep_recent=-1,
        )
    with pytest.raises(ValueError, match="trigger_bytes"):
        SummarizeCompactor(
            build_request=_build_request,
            extract_summary=_extract,
            source_keys=("log",),
            trigger_bytes=0,
        )
    with pytest.raises(ValueError, match="summary_key"):
        SummarizeCompactor(
            build_request=_build_request,
            extract_summary=_extract,
            source_keys=("log", "summary"),
            summary_key="summary",
        )
