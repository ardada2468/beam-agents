"""The cache-only provider: a tripwire, because the context is already cache-first.

:class:`CacheOnlyLLMClient` implements the :class:`~beam_agents.model.client.LLMClient`
protocol with a ``complete`` that unconditionally raises. No lookup logic lives
here on purpose (design D3): ``ActivationContext.call_model`` already consults
the :class:`~beam_agents.model.replay_cache.ReplayCache` built from the
snapshot's blob before touching a provider, so under replay every cached request
is served without this client being invoked at all. It is reached only when the
cache-first path falls through — a genuine miss, or a ``digest_only`` entry,
which ``call_model`` treats as a miss by design. Both fail the replay loudly.

Two lookups would be two implementations of the serving path, free to drift;
keeping the production path (context, ``ReplayCache``, ``compute_cache_key``) as
the *only* serving path is what makes a replay exercise the real code.

"Never hits the network" is structural, not configured: the class holds no
transport, imports no HTTP client, and takes no endpoint. The one thing it does
hold beyond its scope is a map of ``cache_key -> response_digest`` for the
blob's **digest-only** entries — digests, never responses, used solely to make
the error message actionable. It cannot serve from them.

Importing this module has no side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from beam_agents.model.replay_cache import compute_cache_key

if TYPE_CHECKING:
    from collections.abc import Mapping

    from beam_agents._protos import LlmCacheBlob
    from beam_agents.model.client import LlmRequest, LlmResponse

__all__ = ["CacheOnlyLLMClient", "ReplayCacheMissError", "digest_only_digests"]


class ReplayCacheMissError(Exception):
    """A replayed request reached the provider, which under replay is a miss.

    Carries the recomputed cache key (the complete request material
    ``compute_cache_key`` covers), the request's model id, and — for a
    digest-only entry — the digest the blob retained, so an operator can verify
    a response fetched out of band by hand. The CLI never re-fetches.
    """

    def __init__(self, *, cache_key: str, model_id: str, response_digest: bytes = b"") -> None:
        self.cache_key = cache_key
        self.model_id = model_id
        self.response_digest = response_digest
        self.digest_only = bool(response_digest)
        if response_digest:
            message = (
                f"cached entry for cache key {cache_key} (model {model_id!r}) is "
                f"digest-only: the response exceeded the 100 KiB blob cap and was "
                f"dropped at insert, leaving only response_digest="
                f"{response_digest.hex()}"
            )
        else:
            message = (
                f"no cached response for cache key {cache_key} (model {model_id!r}); "
                "the request material differs from what was committed at this "
                "(entity_key, seq), or the entry was evicted (LRU 64 / 6h TTL)"
            )
        super().__init__(message)


def digest_only_digests(blob: LlmCacheBlob | None) -> dict[str, bytes]:
    """Map ``cache_key -> response_digest`` for the blob's digest-only entries.

    Deliberately excludes stored responses: what this map holds can never be
    served, only reported.
    """
    if blob is None:
        return {}
    return {entry.cache_key: entry.response_digest for entry in blob.entries if entry.digest_only}


class CacheOnlyLLMClient:
    """An ``LLMClient`` that can only ever fail, naming the cache key it missed."""

    __slots__ = ("_digest_only", "_entity_key", "_seq", "calls")

    def __init__(
        self,
        *,
        entity_key: bytes,
        seq: int,
        digest_only: Mapping[str, bytes] | None = None,
    ) -> None:
        self._entity_key = entity_key
        self._seq = seq
        self._digest_only = dict(digest_only or {})
        #: Times ``complete`` was reached. A reproduced replay leaves this at 0
        #: — reaching the provider once is already a failed replay.
        self.calls = 0

    async def complete(self, request: LlmRequest) -> LlmResponse:
        """Always raise :class:`ReplayCacheMissError`; never return a response.

        The cache key is recomputed here from the same six components the
        cache-first path hashed, so the error names the exact key that was
        looked up rather than a paraphrase of the request.
        """
        self.calls += 1
        cache_key = compute_cache_key(
            request.model_id,
            request.messages,
            request.tools_schema,
            request.sampling_params,
            self._entity_key,
            self._seq,
        )
        raise ReplayCacheMissError(
            cache_key=cache_key,
            model_id=request.model_id,
            response_digest=self._digest_only.get(cache_key, b""),
        )
