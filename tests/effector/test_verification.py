"""Signature verification as the effector's phase zero (effector-security).

Covers "The effector verifies intent signatures before any other phase",
"Verification failures are dead-lettered and never produce a ToolResult", and
"Verification mode governs unsigned intents across the rollout".

The property under test is negative and therefore has to be asserted by
absence: a delivery that fails verification must leave *no* trace anywhere
downstream — no tool invocation, no dedup record, no published result of any
status — while still committing its offset so a forgery flood cannot wedge a
partition.
"""

from __future__ import annotations

import base64
import logging

import pytest

from beam_agents._protos import ToolIntent, ToolResult
from beam_agents.effector.config import EffectorConfig
from beam_agents.effector.dedup import DedupStore, InMemoryDedupStore
from beam_agents.effector.service import CountingMetrics
from beam_agents.effector.sinks import InMemoryMessageSink
from beam_agents.effector.sources import DeliveredIntent
from beam_agents.intent_signing import sign_intent
from beam_agents.tools import ToolRegistry, tool

from ._fakes import NOW_MS, Harness, RecordingDedupStore, a_config, an_intent, build_harness

KEY_ID = "k1"
KEY = b"\x01" * 32
KEYS_ENV = "TEST_EFFECTOR_INTENT_KEYS"
KEYRING = {KEY_ID: KEY}
DEAD_LETTERS_URI = "kafka://localhost:9092/dead-letters"


@pytest.fixture(autouse=True)
def _provision_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(KEYS_ENV, f"{KEY_ID}={base64.b64encode(KEY).decode()}\n")


def charging_registry(calls: list[int]) -> ToolRegistry:
    @tool(side_effect=True)
    def charge(amount_cents: int) -> str:
        calls.append(amount_cents)
        return "receipt"

    registry = ToolRegistry()
    registry.register(charge)
    return registry


def signed(intent: ToolIntent | None = None, *, key_id: str = KEY_ID) -> ToolIntent:
    return sign_intent(intent if intent is not None else an_intent(), key_id=key_id, key=KEY)


def tampered(intent: ToolIntent | None = None) -> ToolIntent:
    """A signed intent whose arguments were rewritten after signing."""
    forged = signed(intent)
    forged.args_json = '{"amount_cents":100000000}'
    return forged


def verifying_config(mode: str, dead_letters_to: str | None) -> EffectorConfig:
    return a_config(
        verify_intents=mode,
        signing_keys=f"env:{KEYS_ENV}",
        dead_letters_to=dead_letters_to,
    )


def build(
    *,
    calls: list[int],
    intents: list[ToolIntent] | None = None,
    deliveries: list[DeliveredIntent] | None = None,
    mode: str = "require",
    dead_letters_to: str | None = DEAD_LETTERS_URI,
    dedup: DedupStore | None = None,
) -> tuple[Harness, InMemoryMessageSink]:
    sink = InMemoryMessageSink()
    harness = build_harness(
        registry=charging_registry(calls),
        intents=intents if intents is not None else [],
        deliveries=deliveries,
        dedup=dedup,
        config=verifying_config(mode, dead_letters_to),
        keyring=KEYRING,
        dead_letter_sink=sink if dead_letters_to is not None else None,
    )
    return harness, sink


def counters(harness: Harness) -> dict[str, int]:
    metrics = harness.service.metrics
    assert isinstance(metrics, CountingMetrics)
    return metrics.counters


# --- Requirement: The effector verifies intent signatures before any other
# phase ------------------------------------------------------------------------


async def test_a_validly_signed_intent_executes_normally() -> None:
    # Scenario: A validly signed intent executes normally.
    calls: list[int] = []
    harness, dead_letters = build(intents=[signed()], calls=calls)

    await harness.service.run()

    assert calls == [100]
    assert harness.statuses == [ToolResult.OK]
    assert harness.committed_intent_ids == ["intent-1"]
    assert dead_letters.published == []


async def test_a_tampered_intent_never_reaches_the_dedup_store_or_a_tool() -> None:
    # Scenario: A tampered intent never reaches the dedup store or a tool.
    calls: list[int] = []
    store = RecordingDedupStore(InMemoryDedupStore(clock=lambda: NOW_MS))
    harness, dead_letters = build(intents=[tampered()], calls=calls, dedup=store)

    await harness.service.run()

    assert calls == []
    assert harness.results.published == []
    assert store.calls == [], "a forged intent must never consume a claim or a store write"
    assert len(dead_letters.published) == 1


async def test_an_unknown_signing_key_id_fails_verification_distinctly() -> None:
    # Scenario: An unknown signing key id fails verification distinctly, so a
    # mis-provisioned keyring is distinguishable from tampering at a glance.
    calls: list[int] = []
    harness, dead_letters = build(intents=[signed(key_id="retired")], calls=calls)

    await harness.service.run()

    assert counters(harness).get("unknown_signing_key") == 1
    assert "bad_signature" not in counters(harness)
    assert calls == []
    assert len(dead_letters.published) == 1


async def test_verification_precedes_the_expiry_check() -> None:
    # Scenario: Verification precedes the expiry check. An unauthenticated
    # message must not drive *any* behavior, and an EXPIRED result would carry
    # attacker-chosen entity_key/intent_id onto the keyed re-injection path.
    calls: list[int] = []
    harness, dead_letters = build(
        intents=[tampered(an_intent(expires_at_ms=NOW_MS - 1))], calls=calls
    )

    await harness.service.run()

    assert harness.results.published == [], "no EXPIRED result may be minted for a forged intent"
    assert "intents_expired" not in counters(harness)
    assert counters(harness).get("bad_signature") == 1
    assert len(dead_letters.published) == 1


# --- Requirement: Verification failures are dead-lettered and never produce a
# ToolResult -------------------------------------------------------------------


async def test_a_dead_lettered_delivery_is_preserved_verbatim_and_the_partition_continues() -> None:
    # Scenario: A dead-lettered delivery is preserved verbatim and the
    # partition continues. Committing past the forgery is deliberate: a wedged
    # partition head is a denial of service an attacker can produce at will.
    calls: list[int] = []
    forged = tampered()
    raw = forged.SerializeToString(deterministic=True)
    good = signed(an_intent(intent_id="intent-2"))
    harness, dead_letters = build(
        calls=calls,
        deliveries=[
            DeliveredIntent(
                intent=forged, partition="p-0", handle=0, payload=raw, key=forged.entity_key
            ),
            DeliveredIntent(
                intent=good,
                partition="p-0",
                handle=1,
                payload=good.SerializeToString(deterministic=True),
                key=good.entity_key,
            ),
        ],
    )

    await harness.service.run()

    assert dead_letters.published == [(forged.entity_key, raw)]
    assert harness.committed_intent_ids == ["intent-1", "intent-2"]
    assert calls == [100], "the valid intent behind the forgery still executes"


async def test_no_result_of_any_status_is_published_for_a_verification_failure() -> None:
    # Scenario: No result of any status is published for a verification
    # failure — not REJECTED, not EXPIRED.
    calls: list[int] = []
    harness, dead_letters = build(intents=[tampered()], calls=calls)

    await harness.service.run()

    assert harness.results.published == []
    assert counters(harness).get("bad_signature") == 1
    assert len(dead_letters.published) == 1


async def test_without_a_dead_letter_channel_the_failure_is_logged_and_counted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Scenario: Without a dead-letter channel the failure is logged and
    # counted. The log carries identity only: `args_json` is attacker-chosen
    # and must never be sprayed into logs.
    calls: list[int] = []
    harness, _ = build(intents=[tampered()], calls=calls, dead_letters_to=None)

    with caplog.at_level(logging.WARNING, logger="beam_agents.effector.service"):
        await harness.service.run()

    messages = [r.getMessage() for r in caplog.records if "bad_signature" in r.getMessage()]
    assert messages, "the failure must be logged when no dead-letter channel exists"
    assert "intent-1" in messages[0] and "p-0" in messages[0]
    assert "amount_cents" not in messages[0], "args_json must never reach the log"
    assert counters(harness).get("bad_signature") == 1
    assert harness.committed_intent_ids == ["intent-1"]


# --- Requirement: Verification mode governs unsigned intents across the
# rollout ----------------------------------------------------------------------


async def test_off_mode_is_byte_for_byte_todays_behavior() -> None:
    # Scenario: `off` is today's behavior on signed and unsigned streams —
    # signature fields are ignored, including one that would not verify,
    # because `off` means the dial has not been turned yet.
    calls: list[int] = []
    harness = build_harness(
        registry=charging_registry(calls),
        intents=[an_intent(), tampered(an_intent(intent_id="intent-2"))],
        config=a_config(),
    )

    await harness.service.run()

    assert calls == [100, 100000000]
    assert harness.statuses == [ToolResult.OK, ToolResult.OK]


async def test_permissive_mode_accepts_signed_and_unsigned_intents_side_by_side() -> None:
    # Scenario: Permissive mode accepts signed and unsigned intents side by
    # side — the coexistence window the whole rollout depends on.
    calls: list[int] = []
    harness, dead_letters = build(
        intents=[an_intent(), signed(an_intent(intent_id="intent-2"))],
        calls=calls,
        mode="permissive",
    )

    await harness.service.run()

    assert calls == [100, 100]
    assert counters(harness).get("unsigned_intents_accepted") == 1
    assert dead_letters.published == []


async def test_permissive_mode_still_refuses_a_tampered_signature() -> None:
    # Scenario: Permissive mode still refuses a tampered signature. A bad
    # signature is never acceptable in any verifying mode.
    calls: list[int] = []
    harness, dead_letters = build(intents=[tampered()], calls=calls, mode="permissive")

    await harness.service.run()

    assert calls == []
    assert harness.results.published == []
    assert counters(harness).get("bad_signature") == 1
    assert len(dead_letters.published) == 1


async def test_require_mode_dead_letters_unsigned_intents() -> None:
    # Scenario: Require mode dead-letters unsigned intents.
    calls: list[int] = []
    harness, dead_letters = build(intents=[an_intent()], calls=calls, mode="require")

    await harness.service.run()

    assert calls == []
    assert counters(harness).get("unsigned_intent") == 1
    assert len(dead_letters.published) == 1
    assert harness.committed_intent_ids == ["intent-1"]


# --- startup validation -------------------------------------------------------


def test_a_verifying_mode_without_a_keyring_fails_at_startup() -> None:
    # Scenario: A verifying mode without a keyring fails at startup — before
    # any client is constructed, which is the same eager-validation rule every
    # other effector setting follows.
    with pytest.raises(ValueError, match="signing_keys") as excinfo:
        a_config(verify_intents="require")

    assert "require" in str(excinfo.value)


def test_an_unknown_verification_mode_is_rejected() -> None:
    with pytest.raises(ValueError, match="verify_intents"):
        a_config(verify_intents="strict", signing_keys=f"env:{KEYS_ENV}")


def test_require_without_a_dead_letter_channel_warns(caplog: pytest.LogCaptureFixture) -> None:
    # Scenario: `require` without `dead_letters_to` warns — the failures then
    # exist only as logs and counters, which is a choice, not a default.
    with caplog.at_level(logging.WARNING, logger="beam_agents.effector.config"):
        a_config(verify_intents="require", signing_keys=f"env:{KEYS_ENV}")

    assert any("dead_letters_to" in record.getMessage() for record in caplog.records)
