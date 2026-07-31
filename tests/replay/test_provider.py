"""The cache-only provider: a tripwire that can never reach a network.

Covers the `replay-cli` scenarios "A cache miss aborts loudly instead of calling
a provider", "A digest-only entry is not silently refetched", "Cache entries do
not expire at replay time", and "Replay makes zero provider calls".
"""

from __future__ import annotations

import socket
from typing import Any

import pytest

from beam_agents._protos import LlmCacheBlob
from beam_agents.model.replay_cache import TTL_MS, ReplayCache, compute_cache_key
from beam_agents.replay.bundle import (
    ReplayIrreproducibleError,
    build_bundle,
    run_replay,
)
from beam_agents.replay.provider import (
    CacheOnlyLLMClient,
    ReplayCacheMissError,
    digest_only_digests,
)
from tests.replay._fixtures import (
    KEY,
    NOW_MS,
    SEQ,
    exact_replay_agent,
    request,
    run_original,
)


def _cache_key(*, seq: int = SEQ, text: str = "hello") -> str:
    req = request(text)
    return compute_cache_key(
        req.model_id, req.messages, req.tools_schema, req.sampling_params, KEY, seq
    )


def _bundle(original: Any, **kwargs: Any) -> Any:
    return build_bundle(
        snapshot=original.snapshot,
        traces=original.traces,
        envelope=original.envelope,
        **kwargs,
    )


# --- Requirement: the provider serves nothing and fails loudly ----------------


async def test_complete_raises_unconditionally_naming_the_cache_key() -> None:
    # Scenario: A cache miss aborts loudly instead of calling a provider.
    # The client has no serving path at all: even a request whose response is
    # in the blob would raise here — reaching it *is* the miss.
    client = CacheOnlyLLMClient(entity_key=KEY, seq=SEQ)

    with pytest.raises(ReplayCacheMissError) as excinfo:
        await client.complete(request())

    assert excinfo.value.cache_key == _cache_key()
    assert _cache_key() in str(excinfo.value)
    assert excinfo.value.digest_only is False
    assert client.calls == 1


def test_the_client_holds_no_transport_and_no_endpoint() -> None:
    # "Never hits the network" is structural: the class carries only the scope
    # it needs to name a cache key, plus digests for error enrichment.
    client = CacheOnlyLLMClient(entity_key=KEY, seq=SEQ)

    assert set(CacheOnlyLLMClient.__slots__) == {"_digest_only", "_entity_key", "_seq", "calls"}
    assert client.calls == 0


async def test_a_digest_only_entry_is_not_silently_refetched() -> None:
    # Scenario: A digest-only entry is not silently refetched.
    digest = bytes(range(32))
    client = CacheOnlyLLMClient(entity_key=KEY, seq=SEQ, digest_only={_cache_key(): digest})

    with pytest.raises(ReplayCacheMissError) as excinfo:
        await client.complete(request())

    assert excinfo.value.digest_only is True
    assert excinfo.value.response_digest == digest
    assert digest.hex() in str(excinfo.value)


def test_digest_only_digests_reads_only_the_digest_only_entries() -> None:
    blob = LlmCacheBlob(state_schema_version=1)
    blob.entries.add(cache_key="stored", response=b"hello", response_digest=b"\x01")
    blob.entries.add(cache_key="dropped", response=b"", response_digest=b"\x02", digest_only=True)

    assert digest_only_digests(blob) == {"dropped": b"\x02"}
    assert digest_only_digests(None) == {}


# --- Requirement: a replay makes zero provider calls --------------------------


def test_replay_makes_zero_provider_calls() -> None:
    # Scenario: Replay makes zero provider calls. Every request the agent
    # issues is in the snapshot's cache blob, so the context's cache-first path
    # serves it and the tripwire is never reached.
    original = run_original()

    outcome = run_replay(_bundle(original), exact_replay_agent)

    assert outcome.provider_calls == 0
    assert outcome.status == "completed"


def test_a_replay_connects_to_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    # The structural claim, enforced: no code path in the replay package may
    # connect a socket. (The event loop's own self-pipe socketpair is not a
    # connection to anything; what must be impossible is reaching a provider.)
    def refuse(*args: object, **kwargs: object) -> None:
        raise AssertionError("replay attempted to open a network connection")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket.socket, "connect_ex", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    original = run_original()

    outcome = run_replay(_bundle(original), exact_replay_agent)

    assert outcome.provider_calls == 0


def test_a_missing_cache_entry_aborts_the_replay_naming_the_key() -> None:
    # A snapshot whose cache blob lost the entry (evicted, or never committed
    # because the attempt failed) is irreproducible, not divergent.
    original = run_original()
    del original.snapshot.llm_cache.entries[:]

    with pytest.raises(ReplayIrreproducibleError) as excinfo:
        run_replay(_bundle(original), exact_replay_agent)

    assert _cache_key() in str(excinfo.value)
    assert "irreproducible" in str(excinfo.value)


def test_a_digest_only_entry_aborts_the_replay_reporting_the_digest() -> None:
    # Scenario: A digest-only entry is not silently refetched — end to end,
    # through the context's cache-first path, which treats it as a miss.
    original = run_original()
    entry = original.snapshot.llm_cache.entries[0]
    digest = entry.response_digest
    entry.response = b""
    entry.digest_only = True

    with pytest.raises(ReplayIrreproducibleError) as excinfo:
        run_replay(_bundle(original), exact_replay_agent)

    assert digest.hex() in str(excinfo.value)


# --- Requirement: cache entries do not expire at replay time -------------------


def test_cache_entries_do_not_expire_at_replay_time() -> None:
    # Scenario: Cache entries do not expire at replay time. Replay evaluates
    # the blob against the *traced* activation clock, so an entry stays live
    # exactly as it was at activation time however much wall time has passed.
    original = run_original()
    bundle = _bundle(original)
    assert bundle.now_ms == NOW_MS

    outcome = run_replay(bundle, exact_replay_agent)

    assert outcome.provider_calls == 0
    # The clock is load-bearing, not incidental: evaluated a TTL later, the
    # same blob's entry is gone — which is what replay avoids by taking the
    # traced clock rather than reading `time.time()`.
    stale = ReplayCache(original.snapshot.llm_cache, now_ms=NOW_MS + TTL_MS + 1)
    assert stale.get(_cache_key()) is None
