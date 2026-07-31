"""The outbox signer for the effector-security capability.

Covers the pipeline half of "ToolIntents are signed at the outbox writer": the
signer spec's eager validation, worker-side key resolution, determinism across
a retried bundle, the untouched unsigned path, and the two properties that make
signing safe to bolt onto a correctness-critical writer — key material never
reaches the pipeline graph, and keyed state stays byte-identical because the
signature is stamped on a copy at emission (design D3).
"""

from __future__ import annotations

import base64
import bz2
import contextlib

import pytest
from apache_beam.internal import pickler

from beam_agents._protos import ToolIntent
from beam_agents.actions.write_intents import WriteIntents, _SerializeIntent
from beam_agents.core.context import ActivationContext
from beam_agents.intent_signing import (
    SCHEME_HMAC_SHA256,
    IntentSigner,
    VerificationResult,
    verify_intent,
)
from beam_agents.model import FakeLLM

KEY_ID = "k1"
KEY = b"\x01" * 32
KEYS_ENV = "TEST_OUTBOX_INTENT_KEYS"
KEY_REFERENCE = f"env:{KEYS_ENV}"


@pytest.fixture(autouse=True)
def _provision_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(KEYS_ENV, f"{KEY_ID}={base64.b64encode(KEY).decode()}\n")


def a_signer() -> IntentSigner:
    return IntentSigner(key_id=KEY_ID, key_reference=KEY_REFERENCE)


def an_intent(seq: int = 5) -> ToolIntent:
    return ToolIntent(
        intent_id=f"id-{seq}",
        entity_key=b"k",
        seq=seq,
        step_index=0,
        tool_name="http.post",
        args_json='{"url":"https://example.test"}',
        created_at_ms=1_700_000_000_000,
        expires_at_ms=1_700_000_060_000,
        kind=ToolIntent.TOOL,
    )


def _decoded_pickle(pickled: bytes | str) -> bytes:
    """The raw pickle bytes behind Beam's base64 + bz2 envelope.

    Searching the encoded form would pass vacuously — nothing is findable in
    compressed base64 — so the envelope is peeled first.
    """
    blob = pickled.encode() if isinstance(pickled, str) else pickled
    with contextlib.suppress(Exception):
        blob = base64.b64decode(blob)
    with contextlib.suppress(OSError, ValueError):
        blob = bz2.decompress(blob)
    return blob


def _serialize(intent: ToolIntent, *, signer: IntentSigner | None) -> bytes:
    dofn = _SerializeIntent(signer)
    dofn.setup()
    key, payload = next(iter(dofn.process((intent.entity_key, intent))))
    assert key == intent.entity_key
    return payload  # type: ignore[no-any-return]


# --- Requirement: ToolIntents are signed at the outbox writer -----------------


def test_a_signed_intent_verifies_against_the_signing_key() -> None:
    # Scenario: A signed intent verifies against the signing key.
    payload = _serialize(an_intent(), signer=a_signer())

    written = ToolIntent()
    written.ParseFromString(payload)

    assert written.signature_scheme == SCHEME_HMAC_SHA256
    assert written.signing_key_id == KEY_ID
    assert verify_intent(written, {KEY_ID: KEY}) is VerificationResult.OK


def test_a_retried_bundle_re_signs_to_byte_identical_messages() -> None:
    # Scenario: A retried bundle re-signs to byte-identical messages. The
    # deterministic intent id (invariant 2) is worth nothing on the outbox if
    # the signature makes the wire bytes differ per attempt.
    first = _serialize(an_intent(), signer=a_signer())
    second = _serialize(an_intent(), signer=a_signer())

    assert first == second


def test_no_signer_configured_preserves_todays_output() -> None:
    # Scenario: No signer configured preserves today's output — byte-compared
    # against the unsigned serialization, which is what `off`-mode effectors
    # and every retained pre-signing message on the topic still expect.
    intent = an_intent()

    assert _serialize(intent, signer=None) == intent.SerializeToString(deterministic=True)

    written = ToolIntent()
    written.ParseFromString(_serialize(intent, signer=None))
    assert written.signature == b""
    assert written.signature_scheme == ToolIntent.SIGNATURE_SCHEME_UNSPECIFIED


def test_key_bytes_never_enter_the_pipeline_graph() -> None:
    # Scenario: Key bytes never enter the pipeline graph. The runner pickles
    # the transform at submission; only the *reference* may travel, because the
    # graph is stored by the runner and visible in job descriptions.
    transform = WriteIntents("kafka://broker:9092/intents", signer=a_signer())

    raw = _decoded_pickle(pickler.dumps(transform))

    assert KEY not in raw
    assert base64.b64encode(KEY) not in raw
    # The reference *is* expected to travel — that is the whole point of it.
    # Asserting its presence is what proves the previous two assertions are
    # searching real pickle bytes rather than an opaque blob.
    assert KEY_REFERENCE.encode() in raw


def test_a_malformed_key_reference_is_rejected_at_construction() -> None:
    # The signer spec is validated where every other `WriteIntents` setting is:
    # eagerly, import-free, before a pipeline exists.
    with pytest.raises(ValueError, match="is not a secret reference"):
        WriteIntents("kafka://broker:9092/intents", signer=IntentSigner(KEY_ID, "/etc/keys"))


def test_a_signing_key_id_absent_from_the_keyring_fails_at_setup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A key id the provisioned keyring does not carry is a deployment error
    # that must surface when the worker starts, not as an unverifiable intent
    # stream discovered downstream.
    dofn = _SerializeIntent(IntentSigner(key_id="absent", key_reference=KEY_REFERENCE))

    with pytest.raises(ValueError, match="absent"):
        dofn.setup()


# --- Requirement: signing is a transport property, so keyed state is untouched


def test_signing_leaves_the_staged_intent_and_its_keyed_state_byte_identical() -> None:
    # Design D3's load-bearing claim: the signature is stamped at emission, not
    # in `ctx.act`'s staging path, so intents held in `PENDING` and the
    # `Continuation` that lists them stay byte-for-byte what they were before
    # this change. Beam hands the writer a *reference* to the staged proto, so
    # an in-place stamp would silently rewrite committed state.
    ctx = ActivationContext(
        entity_key=b"customer-7",
        seq=3,
        now_ms=1_700_000_000_000,
        provider=FakeLLM(),
        memory_blob=None,
        cache_blob=None,
    )
    ctx.act("charge", '{"amount_cents":100}')
    staged = ctx.staged_intents
    before = [intent.SerializeToString(deterministic=True) for intent in staged]
    continuation_before = ctx.build_continuation(
        snapshot=b"snap", adapter="protocol", deadline_ms=1
    ).SerializeToString(deterministic=True)

    payloads = [_serialize(intent, signer=a_signer()) for intent in staged]

    after = [intent.SerializeToString(deterministic=True) for intent in staged]
    continuation_after = ctx.build_continuation(
        snapshot=b"snap", adapter="protocol", deadline_ms=1
    ).SerializeToString(deterministic=True)

    assert after == before, "signing must not mutate the intent staged in keyed state"
    assert continuation_after == continuation_before
    assert payloads != before, "the emitted outbox bytes must carry the signature"
