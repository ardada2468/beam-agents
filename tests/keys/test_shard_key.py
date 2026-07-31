"""Unit coverage for the `key#shard_n` derivation and its inverse.

Scenarios from `openspec/changes/add-hot-key-sharding-guidance/specs/
key-sharding/spec.md`: shard assignment must depend only on `(payload, n)` --
never on process identity or `PYTHONHASHSEED` -- because a bundle retry that
re-derived a different physical key would re-mint `intent_id`s and miss the
`(key, seq)` replay cache (correctness invariants 2 and 3).

The golden values below are computed from the *spec's* definition (SHA-256 of
the payload, first eight digest bytes big-endian, modulo `n`), not read back
out of the implementation.
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from beam_agents.keys import shard_key, unshard_key

# Pinned goldens: `int.from_bytes(sha256(payload).digest()[:8]) % n`.
GOLDEN_N4 = {
    b"evt-a": b"user-1#1",
    b"evt-b": b"user-1#0",
    b"hello": b"user-1#2",
    b"": b"user-1#0",
}

# One payload per shard index for `n = 8` -- the spread case.
SPREAD_N8 = {
    0: b"payload-0",
    1: b"payload-8",
    2: b"payload-2",
    3: b"payload-20",
    4: b"payload-49",
    5: b"payload-1",
    6: b"payload-3",
    7: b"payload-4",
}


# --- Requirement: shard-key derivation is deterministic -----------------------


def test_the_same_payload_always_lands_on_the_same_shard() -> None:
    # Scenario: The same payload always lands on the same shard. Repeated
    # in-process calls agree with each other and with the pinned golden.
    for payload, expected in GOLDEN_N4.items():
        first = shard_key(b"user-1", 4, payload=payload)
        second = shard_key(b"user-1", 4, payload=payload)
        assert first == second == expected


def test_the_same_payload_lands_on_the_same_shard_in_a_separate_process() -> None:
    # Scenario: The same payload always lands on the same shard -- "including
    # from separately started Python processes". Two subprocesses with
    # different PYTHONHASHSEEDs must reproduce the in-process value: this is
    # what forbids Python's salted `hash()` as the derivation.
    program = (
        "from beam_agents.keys import shard_key;"
        "print(shard_key(b'user-1', 4, payload=b'evt-a').decode())"
    )
    results = []
    for seed in ("0", "1", "random"):
        env = {**os.environ, "PYTHONHASHSEED": seed}
        completed = subprocess.run(
            [sys.executable, "-c", program],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        results.append(completed.stdout.strip().encode())

    assert set(results) == {GOLDEN_N4[b"evt-a"]}


def test_varied_payloads_reach_every_shard() -> None:
    # Scenario: Varied payloads reach every shard. Every index in [0, 8) is
    # produced by some payload, and every result parses as `key#<digits>` with
    # the suffix in range.
    seen: set[int] = set()
    for expected_index, payload in SPREAD_N8.items():
        physical = shard_key(b"acct", 8, payload=payload)
        logical, _, digits = physical.rpartition(b"#")
        assert logical == b"acct"
        assert digits.isdigit()
        index = int(digits)
        assert 0 <= index < 8
        assert index == expected_index
        seen.add(index)

    assert seen == set(range(8))


def test_a_single_shard_still_carries_the_suffix() -> None:
    # `n = 1` still produces `#0`: the shape of a sharded key never depends on
    # the shard count, so `unshard_key` works uniformly.
    assert shard_key(b"user-1", 1, payload=b"anything") == b"user-1#0"


@pytest.mark.parametrize("n", [0, -1, -8])
def test_a_non_positive_shard_count_is_rejected(n: int) -> None:
    # Scenario: A non-positive shard count is rejected -- at the call site,
    # before any pipeline runs.
    with pytest.raises(ValueError, match="shard count"):
        shard_key(b"user-1", n, payload=b"evt-a")


# --- Requirement: shard keys round-trip through `unshard_key` -----------------


@pytest.mark.parametrize("n", [1, 2, 4, 8, 997])
@pytest.mark.parametrize("payload", [b"", b"evt-a", b"\x00\xff", b"payload-49"])
def test_sharding_then_unsharding_is_the_identity_on_the_logical_key(
    n: int, payload: bytes
) -> None:
    # Scenario: Sharding then unsharding is the identity on the logical key.
    logical = b"user-1"
    assert unshard_key(shard_key(logical, n, payload=payload)) == logical


@pytest.mark.parametrize("key", [b"user-1", b"", b"user#", b"user#x", b"#", b"user#1x"])
def test_an_unsharded_key_is_refused(key: bytes) -> None:
    # Scenario: An unsharded key is refused -- loudly, naming the expected
    # shape, rather than passing the key through and silently merging a
    # downstream regroup under the wrong key.
    with pytest.raises(ValueError, match=r"key#<shard>"):
        unshard_key(key)


def test_a_logical_key_that_already_ends_in_a_shard_suffix_is_ambiguous() -> None:
    # Design D5's documented residual ambiguity, pinned as behavior: a logical
    # key ending in `#<digits>` is indistinguishable from a sharded key, so
    # `unshard_key` strips its suffix. Documented as a restriction on the
    # inputs, not fixable while the `key#shard_n` shape is the convention.
    assert unshard_key(b"user#7") == b"user"
    # Only one suffix is stripped, so a genuinely sharded such key loses its
    # shard suffix and nothing more.
    assert unshard_key(shard_key(b"user#7", 4, payload=b"evt-a")) == b"user#7"
