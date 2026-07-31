"""Compaction strategies for working memory, split by *where they may run*.

See the change design (``openspec/changes/add-compaction-strategies/design.md``)
for the load-bearing decisions. The split is the whole point (D1):

- **Tier 1, in-write:** :class:`DropOldestCompactor` sits behind the facade's
  existing synchronous :class:`~beam_agents.memory.facade.Compactor` hook, which
  fires in the middle of a ``ctx.memory.set/append`` call on the bridge event
  loop. Nothing there can ``await``, so tier 1 is LLM-free by construction:
  deterministic LRU eviction to a target, and nothing else. It is the wired
  default (``AgentConfig.compactor``), so an unconfigured pipeline survives
  hard-cap pressure by eviction instead of dead-lettering forever (D3).
- **Tier 2, in-activation:** :class:`SummarizeCompactor` is invoked by the loop
  driver *inside* the activation, after the agent returns and before the outcome
  is folded into a ``Continuation``/``ActivationResult``. Its model calls go
  exclusively through ``ActivationContext.call_model``, so each one is keyed by
  ``(content, key, seq)``, staged in the replay cache, committed atomically with
  the bundle, and served from keyed state with ZERO provider calls on a bundle
  retry (correctness invariants 1 and 3 — D2).

:class:`FlushToLongterm` is the third piece: the shipped ``on_expire`` hook that
demotes a key's final ``MemoryBlob`` to the long-term tier at TTL fire, under
the single external-write carve-out correctness invariant 5 documents (D4).

The runtime owns *when* to summarize, *what* to feed, *where* the call runs, and
*how* the result lands; the caller owns the prompt and the parsing (D5) —
shipping a default summarization prompt would be prompt templating, which the
project constitution rejects outright.

This module deliberately does not import ``beam_agents.core``: the view a
summarizer is handed is a structural protocol declared here, which
``ActivationContext`` satisfies without either side importing the other.

Importing this module has no side effects.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from beam_agents._protos import MemoryBlob
from beam_agents.memory.facade import _SOFT_CAP_BYTES, HARD_CAP_BYTES, Memory
from beam_agents.memory.stores import MemoryRecord, MemoryStore
from beam_agents.model.client import LlmRequest, LlmResponse

__all__ = [
    "DEFAULT_EXPIRY_KEY",
    "DEFAULT_KEEP_RECENT",
    "DEFAULT_PROTECTED_PREFIXES",
    "DEFAULT_SUMMARY_KEY",
    "DEFAULT_TARGET_BYTES",
    "DEFAULT_TRIGGER_BYTES",
    "DropOldestCompactor",
    "ExpireHook",
    "ExpiringMemory",
    "FlushToLongterm",
    "SummarizationView",
    "SummarizeCompactor",
    "Summarizer",
]

#: Eviction target for the default compactor: half the hard cap, well below the
#: 75% soft cap, so one pass buys real headroom instead of leaving the facade
#: oscillating at the soft-cap boundary (design D3, hysteresis).
DEFAULT_TARGET_BYTES = HARD_CAP_BYTES // 2

#: The LangGraph checkpoint namespace. Those keys are load-bearing resume state
#: and ``adapters/langgraph/checkpoint.py`` already documents that compactors
#: must not evict them; the default compactor enforces it structurally.
DEFAULT_PROTECTED_PREFIXES = ("__langgraph__/",)

DEFAULT_SUMMARY_KEY = "summary"
DEFAULT_KEEP_RECENT = 8
#: Summarization triggers at the soft cap, where the facade already warns.
DEFAULT_TRIGGER_BYTES = _SOFT_CAP_BYTES
#: Long-term key the shipped expiry hook upserts the expiring blob under.
DEFAULT_EXPIRY_KEY = "working_memory"


class DropOldestCompactor:
    """Evict least-recently-used entries until memory is at or below a target.

    Satisfies the facade's :class:`~beam_agents.memory.facade.Compactor`
    protocol and is the wired default for ``AgentConfig.compactor``. It reaches
    memory only through the guarded API — ``keys()``/``entry_size()``/
    ``delete()`` — and is a pure function of the staged entries and its own
    frozen configuration: no clock, no randomness, no I/O, so a replayed
    activation evicts an identical set.

    Keys matching ``protected_prefixes`` are never deleted. The default protects
    the LangGraph checkpoint namespace ``__langgraph__/`` (see
    :mod:`beam_agents.adapters.langgraph.checkpoint`), whose entries are a
    suspended agent's resume state. When only protected entries remain above the
    target, compaction stops and the facade's existing contract takes over — the
    over-cap write raises ``MemoryOverflow``, which is the correct outcome:
    silently evicting resume state to admit a write would corrupt a suspension.
    """

    __slots__ = ("_protected_prefixes", "_target_bytes")

    def __init__(
        self,
        *,
        target_bytes: int = DEFAULT_TARGET_BYTES,
        protected_prefixes: Sequence[str] = DEFAULT_PROTECTED_PREFIXES,
    ) -> None:
        if target_bytes <= 0:
            raise ValueError(
                f"DropOldestCompactor target_bytes must be positive, got {target_bytes!r}"
            )
        self._target_bytes = target_bytes
        self._protected_prefixes = tuple(protected_prefixes)

    @property
    def target_bytes(self) -> int:
        return self._target_bytes

    @property
    def protected_prefixes(self) -> tuple[str, ...]:
        return self._protected_prefixes

    def compact(self, memory: Memory) -> None:
        """Delete unprotected entries, least-recently-used first, until
        ``memory.size_bytes`` is at or below ``target_bytes``.

        ``keys()`` returns a snapshot in LRU order and neither it nor
        ``entry_size`` re-stamps access order, so the iteration order is the
        eviction order — inspecting a candidate through ``get()`` would move it
        to most-recently-used and evict the wrong entries.
        """
        # Bound to a local first: `Memory.keys()` is not a mapping view (ruff's
        # SIM118 would read `for key in memory.keys()` as one), and the snapshot
        # is what makes deleting while iterating safe.
        candidates = memory.keys()
        for key in candidates:
            if memory.size_bytes <= self._target_bytes:
                return
            if self._is_protected(key):
                continue
            memory.delete(key)

    def _is_protected(self, key: str) -> bool:
        return any(key.startswith(prefix) for prefix in self._protected_prefixes)


#: Maps the folded items and any prior summary onto the provider request. Owned
#: by the caller (design D5) and REQUIRED to be a pure function of its inputs:
#: an impure builder hashes to a different cache key on replay, misses the
#: cache, and fails the retry-determinism gate's zero-extra-calls assertion.
BuildRequest = Callable[[tuple[bytes, ...], bytes | None], LlmRequest]
#: Maps opaque provider response bytes onto the summary bytes to store.
ExtractSummary = Callable[[bytes], bytes]


@runtime_checkable
class SummarizationView(Protocol):
    """The narrow surface a tier-2 strategy is handed: memory and ``call_model``.

    ``ActivationContext`` satisfies this structurally. Everything else the
    context offers — ``act``, ``request_approval``, ``stage_trace`` — is
    deliberately absent, so a summarizer *cannot* stage an intent or an output
    (design D2). Declared here rather than imported from ``core`` so the memory
    layer stays free of a core dependency.
    """

    @property
    def memory(self) -> Memory: ...

    async def call_model(self, request: LlmRequest) -> LlmResponse: ...


@runtime_checkable
class Summarizer(Protocol):
    """The tier-2 contract the loop driver invokes.

    ``trigger_bytes`` is read by the driver, which runs ``compact`` if and only
    if the staged ``memory.size_bytes`` has reached it. The predicate is a pure
    function of committed state plus the activation's deterministic walk — no
    clock, no sampling — so a replayed bundle makes the identical run/don't-run
    decision.
    """

    @property
    def trigger_bytes(self) -> int: ...

    async def compact(self, view: SummarizationView) -> None: ...


class SummarizeCompactor:
    """Fold each source ring's oldest items into one scalar summary entry.

    Runs inside the activation, driven by ``run_activation`` after the agent
    returns and before any ``Continuation``/``ActivationResult`` is assembled,
    and reaches the provider only through ``view.call_model`` — the same
    cache-first path the agent's own calls take. That single placement decision
    buys replay-caching, atomic staging, and trace/tally visibility for free
    (design D2).

    The newest ``keep_recent`` items of each source ring survive verbatim;
    everything older is handed, in ring order, to ``build_request`` together
    with any existing summary, and replaced by ``extract_summary``'s result
    under ``summary_key``. An extracted summary that is not smaller than the
    bytes it replaces raises ``ValueError`` — a "summary" that inflates memory
    is a defect, and failing here fails the activation closed instead of
    committing the growth.
    """

    __slots__ = (
        "_build_request",
        "_extract_summary",
        "_keep_recent",
        "_source_keys",
        "_summary_key",
        "_trigger_bytes",
    )

    def __init__(
        self,
        *,
        build_request: BuildRequest,
        extract_summary: ExtractSummary,
        source_keys: Sequence[str],
        summary_key: str = DEFAULT_SUMMARY_KEY,
        keep_recent: int = DEFAULT_KEEP_RECENT,
        trigger_bytes: int = DEFAULT_TRIGGER_BYTES,
    ) -> None:
        if not source_keys:
            raise ValueError("SummarizeCompactor source_keys must name at least one ring")
        if not summary_key:
            raise ValueError("SummarizeCompactor summary_key must be a non-empty string")
        if summary_key in tuple(source_keys):
            raise ValueError(
                f"SummarizeCompactor summary_key {summary_key!r} must not also be a source key"
            )
        if keep_recent < 0:
            raise ValueError(f"SummarizeCompactor keep_recent must be >= 0, got {keep_recent!r}")
        if trigger_bytes <= 0:
            raise ValueError(
                f"SummarizeCompactor trigger_bytes must be positive, got {trigger_bytes!r}"
            )
        self._build_request = build_request
        self._extract_summary = extract_summary
        self._source_keys = tuple(source_keys)
        self._summary_key = summary_key
        self._keep_recent = keep_recent
        self._trigger_bytes = trigger_bytes

    @property
    def trigger_bytes(self) -> int:
        return self._trigger_bytes

    @property
    def summary_key(self) -> str:
        return self._summary_key

    async def compact(self, view: SummarizationView) -> None:
        """Summarize the source rings' old items into ``summary_key``.

        A no-op (and, importantly, zero model calls) when no source ring holds
        anything older than ``keep_recent``: a provider call that removes
        nothing is pure cost and pure latency.
        """
        memory = view.memory
        present = set(memory.keys())
        folded: list[bytes] = []
        kept: list[tuple[str, tuple[bytes, ...]]] = []
        for key in self._source_keys:
            if key not in present:
                continue
            items = memory.ring(key)
            old = items if self._keep_recent == 0 else items[: -self._keep_recent]
            if not old:
                continue
            folded.extend(old)
            kept.append((key, items[len(old) :]))

        if not folded:
            return

        prior_summary = memory.get(self._summary_key)
        response = await view.call_model(self._build_request(tuple(folded), prior_summary))
        summary = self._extract_summary(response.response)
        replaced_bytes = sum(len(item) for item in folded)
        if len(summary) >= replaced_bytes:
            raise ValueError(
                f"summary of {len(summary)} bytes does not shrink the "
                f"{replaced_bytes} bytes of items it replaces"
            )

        for key, keep_items in kept:
            # There is no ring-replace on the guarded API, and there should not
            # be: rewriting through delete + append keeps every mutation inside
            # the facade's accounting and cap enforcement.
            memory.delete(key)
            for item in keep_items:
                memory.append(key, item, max_items=len(keep_items))
        memory.set(self._summary_key, summary)


@dataclass(frozen=True, slots=True)
class ExpiringMemory:
    """What a TTL fire hands the ``on_expire`` hook.

    Every field is replay-stable: ``blob`` and ``seq`` come from committed keyed
    state, and ``expired_at_ms`` is the timer's scheduled firing time, never a
    wall-clock reading — so a retried timer bundle produces a byte-identical
    upsert that the store's ``(entity_key, seq)`` guard dedups to nothing.
    """

    entity_key: bytes
    seq: int
    blob: MemoryBlob
    expired_at_ms: int


@runtime_checkable
class ExpireHook(Protocol):
    """Called by ``on_ttl`` before the wipe, on the DoFn's async bridge.

    Runs under a bounded timeout; raising propagates out of the timer callback,
    failing the bundle so the runner retries it — and leaving state un-wiped,
    which is exactly what the retry needs.
    """

    async def __call__(self, store: MemoryStore, expiry: ExpiringMemory) -> None: ...


@dataclass(frozen=True, slots=True)
class FlushToLongterm:
    """The shipped ``on_expire`` hook: one idempotent upsert of the final blob.

    This is the side effect correctness invariant 5 carves out in so many words
    — "documented idempotent upserts to the long-term MemoryStore keyed by
    ``(key, seq)``" — and it is written for precisely this moment: TTL fire has
    no activation, so there is no ``ctx.memory.longterm`` to stage through and
    no intent path scoped to an activation (design D4).

    The blob is stored under ``key`` as its deterministic serialization, so two
    flushes of the same expiry are byte-identical and the store's equal-seq
    guard collapses them onto one row.
    """

    key: str = DEFAULT_EXPIRY_KEY

    async def __call__(self, store: MemoryStore, expiry: ExpiringMemory) -> None:
        await store.save(
            MemoryRecord(
                entity_key=expiry.entity_key,
                key=self.key,
                value=expiry.blob.SerializeToString(deterministic=True),
                seq=expiry.seq,
                updated_at_ms=expiry.expired_at_ms,
            )
        )
