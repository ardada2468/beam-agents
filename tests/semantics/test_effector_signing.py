"""Signed effects under adversarial traffic (semantics gate, offline).

The effectively-once gate proves the effector executes each genuine intent
exactly once across kills. This gate adds the authenticity half: with
``verify_intents=require``, a stream that mixes genuine signed intents with
tampered and outright forged ones must still execute each genuine
``intent_id`` exactly once, execute *nothing* for the adversarial deliveries,
publish no ``ToolResult`` for them at all, and account for every one of them on
the dead-letter channel.

The kills matter here for the same reason they matter in the effectively-once
gate: verification sits before the phases a crash can interleave, so a replay
after a kill re-verifies from the delivered bytes and cannot be tricked into
skipping the check. Offline by construction — the transport and dedup store are
in-memory and a "kill" is a `BaseException` raised at a named phase boundary.
"""

from __future__ import annotations

import pytest

from beam_agents._protos import ToolIntent, ToolResult
from beam_agents.effector.config import EffectorConfig
from beam_agents.effector.dedup import InMemoryDedupStore
from beam_agents.effector.sinks import InMemoryMessageSink, InMemoryResultSink
from beam_agents.effector.sources import DeliveredIntent
from beam_agents.intent_signing import sign_intent
from beam_agents.tools import ToolRegistry, tool
from tests.effector._fakes import (
    NOW_MS,
    CrashingResultSink,
    InjectedCrash,
    RecordingDedupStore,
    a_config,
    an_intent,
    build_harness,
)
from tests.effector.test_service import registry_with

pytestmark = pytest.mark.semantics

KEY_ID = "k1"
KEY = b"\x01" * 32
KEYRING = {KEY_ID: KEY}
LEASE_MS = 5_000
INTENT_TTL_MS = 600_000

KILL_POINTS = ("claim", "complete", "publish", "none")


class _MovableClock:
    def __init__(self, now_ms: int = NOW_MS) -> None:
        self.now_ms = now_ms

    def __call__(self) -> int:
        return self.now_ms


def _counting_registry(calls: list[str]) -> ToolRegistry:
    @tool(side_effect=True)
    def charge(amount_cents: int) -> str:
        calls.append(f"charge:{amount_cents}")
        return f"receipt-{amount_cents}"

    return registry_with(charge)


def _config(**overrides: object) -> EffectorConfig:
    return a_config(
        verify_intents="require",
        signing_keys="env:UNUSED_IN_THIS_GATE",
        dead_letters_to="kafka://localhost:9092/dead-letters",
        lease_ms=LEASE_MS,
        tool_timeout_ms=1_000,
        **overrides,
    )


def _genuine(intent_id: str, amount: int) -> ToolIntent:
    return sign_intent(
        an_intent(
            intent_id=intent_id,
            args_json=f'{{"amount_cents":{amount}}}',
            expires_at_ms=NOW_MS + INTENT_TTL_MS,
        ),
        key_id=KEY_ID,
        key=KEY,
    )


def _tampered(intent_id: str) -> ToolIntent:
    """A genuine intent whose amount was rewritten after signing (substitution)."""
    intent = _genuine(intent_id, 100)
    intent.args_json = '{"amount_cents":999999}'
    return intent


def _forged(intent_id: str) -> ToolIntent:
    """An intent minted by someone with topic-write access but no key."""
    intent = an_intent(
        intent_id=intent_id,
        args_json='{"amount_cents":424242}',
        expires_at_ms=NOW_MS + INTENT_TTL_MS,
    )
    intent.signature_scheme = ToolIntent.HMAC_SHA256
    intent.signing_key_id = KEY_ID
    intent.signature = b"\x00" * 32
    return intent


def _unsigned(intent_id: str) -> ToolIntent:
    return an_intent(
        intent_id=intent_id,
        args_json='{"amount_cents":7}',
        expires_at_ms=NOW_MS + INTENT_TTL_MS,
    )


ADVERSARIAL = {"tampered-1": _tampered, "forged-1": _forged, "unsigned-1": _unsigned}


@pytest.mark.parametrize("kill_point", KILL_POINTS)
async def test_only_genuine_signed_intents_execute_under_kills(kill_point: str) -> None:
    """A mixed adversarial stream yields one execution per genuine intent only."""
    calls: list[str] = []
    registry = _counting_registry(calls)
    clock = _MovableClock()
    # One durable dedup store outlives the "process", exactly as Redis would.
    store = InMemoryDedupStore(clock=clock)
    dead_letters = InMemoryMessageSink()
    published: list[ToolResult] = []

    stream = [
        _genuine("genuine-1", 100),
        _tampered("tampered-1"),
        _forged("forged-1"),
        _unsigned("unsigned-1"),
        _genuine("genuine-2", 250),
    ]
    # One partition per intent. A killed worker leaves its own partition's
    # bounded queue behind, and the dispatcher would block feeding a dead
    # worker a second element — a single-partition stream plus a mid-stream
    # kill is a deadlock in the harness, not a property of the service. Real
    # scale-out spreads keys across partitions anyway, so this is also the
    # more faithful shape.
    deliveries = [
        DeliveredIntent(
            intent=intent,
            partition=f"p-{index}",
            handle=index,
            payload=intent.SerializeToString(deterministic=True),
            key=intent.entity_key,
        )
        for index, intent in enumerate(stream)
    ]

    # Two passes over the same stream: the first is killed at `kill_point`, the
    # second is the redelivery a restarting replica sees.
    for attempt in range(2):
        crash_after = kill_point if attempt == 0 and kill_point in ("claim", "complete") else None
        crashing_publish = attempt == 0 and kill_point == "publish"
        result_sink: InMemoryResultSink | CrashingResultSink = (
            CrashingResultSink() if crashing_publish else InMemoryResultSink()
        )
        harness = build_harness(
            registry=registry,
            deliveries=deliveries,
            dedup=RecordingDedupStore(store, crash_after=crash_after),
            result_sink=result_sink,
            config=_config(),
            clock=clock,
            keyring=KEYRING,
            dead_letter_sink=dead_letters,
        )
        try:
            await harness.service.run()
        except InjectedCrash:
            # The killed worker's claim is recovered by lease expiry, exactly
            # as a SIGKILLed replica's would be.
            clock.now_ms += LEASE_MS + 1
        published.extend(harness.results.published)

    # 1. Exactly the genuine intents executed, and nothing else ever ran. The
    #    adversarial amounts (999999, 424242, 7) appearing here at all would be
    #    the failure this gate exists to catch.
    assert set(calls) == {"charge:100", "charge:250"}
    if kill_point == "none":
        # The control run has no crash window, so execution is strictly once
        # each. Under a kill, a worker killed between invoking a tool and
        # writing its durable completion record legitimately re-executes on
        # redelivery — the documented at-least-once window (docs/effector.md,
        # "What is guaranteed, and what is not"), which signing neither widens
        # nor narrows. Asserting exactly-once here would be asserting a
        # guarantee the runtime does not make.
        assert sorted(calls) == ["charge:100", "charge:250"]

    # 2. Nothing adversarial executed, and nothing adversarial was published —
    #    not REJECTED, not EXPIRED. A published result would inherit the
    #    attacker's entity_key and enter the keyed re-injection path.
    assert all(result.intent_id.startswith("genuine-") for result in published)
    assert {result.status for result in published} == {ToolResult.OK}

    # 3. Every adversarial delivery is accounted for on the dead-letter channel
    #    (both passes deliver all three, so each appears at least once).
    dead_lettered = {
        ToolIntent.FromString(payload).intent_id for _, payload in dead_letters.published
    }
    assert dead_lettered == set(ADVERSARIAL)


async def test_a_forged_intent_cannot_consume_a_genuine_intents_dedup_record() -> None:
    """Forging a *known* pending intent_id must not claim, complete, or publish.

    This is the substitution case of design D6 stated as a gate: without
    signing, whichever copy the effector processes first wins the claim, so an
    attacker's copy can beat the genuine one to the tool. With signing, the
    forgery never reaches the store, and the genuine intent still executes.
    """
    calls: list[str] = []
    clock = _MovableClock()
    store = RecordingDedupStore(InMemoryDedupStore(clock=clock))
    dead_letters = InMemoryMessageSink()
    genuine = _genuine("genuine-1", 100)
    substitute = _tampered("genuine-1")  # same intent_id, attacker's arguments

    harness = build_harness(
        registry=_counting_registry(calls),
        intents=[substitute, genuine],
        dedup=store,
        config=_config(),
        clock=clock,
        keyring=KEYRING,
        dead_letter_sink=dead_letters,
    )

    await harness.service.run()

    assert calls == ["charge:100"], "the attacker's copy must not win the claim"
    assert store.calls[0] == "claim", "the forgery must not have touched the store first"
    assert [r.intent_id for r in harness.results.published] == ["genuine-1"]
    assert len(dead_letters.published) == 1
    # Both deliveries committed: the forgery cannot wedge the partition head.
    assert harness.committed_intent_ids == ["genuine-1", "genuine-1"]
