"""The Beam-free intent signer/verifier for the effector-security capability.

Covers the library half of "ToolIntents are signed at the outbox writer" (the
signing function itself and its determinism) and of "The effector verifies
intent signatures before any other phase" (the verifier's four outcomes and
its keyring), independently of the transform and the service that use them.

Everything here is offline and dependency-free by construction: HMAC-SHA256 is
stdlib, which is the deciding factor in design D2.
"""

from __future__ import annotations

import base64
import hmac
from pathlib import Path
from typing import cast

import pytest

from beam_agents._protos import ToolIntent
from beam_agents.intent_signing import (
    SCHEME_HMAC_SHA256,
    IntentSigner,
    VerificationResult,
    load_keyring,
    resolve_secret_reference,
    sign_intent,
    signing_input,
    validate_secret_reference,
    verify_intent,
)

KEY_ID = "k1"
KEY = b"\x01" * 32
OTHER_KEY = b"\x02" * 32


def an_intent(**overrides: object) -> ToolIntent:
    fields: dict[str, object] = {
        "intent_id": "11111111-2222-5333-8444-555555555555",
        "entity_key": b"customer-7",
        "seq": 3,
        "step_index": 0,
        "tool_name": "charge",
        "args_json": '{"amount_cents":100}',
        "created_at_ms": 1_700_000_000_000,
        "expires_at_ms": 1_700_000_060_000,
        "kind": ToolIntent.TOOL,
    }
    fields.update(overrides)
    return ToolIntent(**fields)  # type: ignore[arg-type]


# --- Requirement: ToolIntents are signed at the outbox writer -----------------


def test_signing_the_same_intent_twice_produces_the_same_bytes() -> None:
    # Scenario: A retried bundle re-signs to byte-identical messages — the
    # function-level half. Determinism is what composes signing with
    # correctness invariant 2: a replayed bundle re-mints the same intent, and
    # the same intent must re-sign to the same wire bytes.
    first = sign_intent(an_intent(), key_id=KEY_ID, key=KEY)
    second = sign_intent(an_intent(), key_id=KEY_ID, key=KEY)

    assert first.SerializeToString(deterministic=True) == second.SerializeToString(
        deterministic=True
    )
    assert first.signature == second.signature


def test_a_signed_intent_verifies_against_its_key() -> None:
    # Scenario: A signed intent verifies against the signing key.
    signed = sign_intent(an_intent(), key_id=KEY_ID, key=KEY)

    assert signed.signature_scheme == SCHEME_HMAC_SHA256
    assert signed.signing_key_id == KEY_ID
    assert len(signed.signature) == 32
    assert verify_intent(signed, {KEY_ID: KEY}) is VerificationResult.OK


def test_signing_does_not_mutate_the_intent_it_was_given() -> None:
    # The staged intent lives in keyed state; Beam hands the writer a reference
    # to it, so a signer that stamped in place would rewrite committed state
    # bytes. `sign_intent` returns a signed copy (design D3).
    original = an_intent()
    before = original.SerializeToString(deterministic=True)

    signed = sign_intent(original, key_id=KEY_ID, key=KEY)

    assert original.SerializeToString(deterministic=True) == before
    assert signed.SerializeToString(deterministic=True) != before


def test_the_signing_input_is_the_message_with_its_signature_fields_cleared() -> None:
    # The definition both ends must agree on, asserted directly rather than
    # only through a round-trip: signing the whole message means every future
    # field is covered by default (design D3).
    intent = an_intent()
    signed = sign_intent(intent, key_id=KEY_ID, key=KEY)

    assert signing_input(signed) == intent.SerializeToString(deterministic=True)
    assert signed.signature == hmac.digest(KEY, signing_input(signed), "sha256")


def test_verification_compares_in_constant_time(monkeypatch: pytest.MonkeyPatch) -> None:
    # Constant-time comparison is a requirement of the verify phase, not an
    # implementation detail: a byte-at-a-time comparison leaks the expected MAC
    # to an attacker who can time deliveries. Pinned by intercepting the
    # primitive the module must be using.
    signed = sign_intent(an_intent(), key_id=KEY_ID, key=KEY)
    calls: list[tuple[bytes, bytes]] = []
    real = hmac.compare_digest

    def recording(a: bytes, b: bytes) -> bool:
        calls.append((bytes(a), bytes(b)))
        return real(a, b)

    monkeypatch.setattr(hmac, "compare_digest", recording)

    assert verify_intent(signed, {KEY_ID: KEY}) is VerificationResult.OK
    assert calls, "verification must go through hmac.compare_digest"


# --- Requirement: The effector verifies intent signatures before any other
# phase (the verifier's outcomes) ----------------------------------------------


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda i: setattr(i, "args_json", '{"amount_cents":1000000}'), id="args_json"),
        pytest.param(lambda i: setattr(i, "tool_name", "refund"), id="tool_name"),
        pytest.param(lambda i: setattr(i, "expires_at_ms", 1 << 40), id="expires_at_ms"),
        pytest.param(lambda i: setattr(i, "entity_key", b"customer-9"), id="entity_key"),
        pytest.param(lambda i: setattr(i, "seq", 999), id="seq"),
        pytest.param(lambda i: setattr(i, "kind", ToolIntent.APPROVAL), id="kind"),
    ],
)
def test_altering_any_field_after_signing_invalidates_the_signature(
    mutate: object,
) -> None:
    # Scenario: A tampered intent never reaches the dedup store or a tool — the
    # function-level half. The signature covers every field, so substitution on
    # a genuine intent_id is refused too (design D6).
    signed = sign_intent(an_intent(), key_id=KEY_ID, key=KEY)
    mutate(signed)  # type: ignore[operator]

    assert verify_intent(signed, {KEY_ID: KEY}) is VerificationResult.BAD_SIGNATURE


def test_an_unsigned_intent_verifies_as_unsigned_not_as_a_bad_signature() -> None:
    # The two are different operational events: unsigned is a rollout state,
    # a bad signature is an attack or corruption. They must not be conflated.
    assert verify_intent(an_intent(), {KEY_ID: KEY}) is VerificationResult.UNSIGNED


def test_an_unknown_key_id_is_distinguishable_from_a_bad_signature() -> None:
    # Scenario: An unknown signing key id fails verification distinctly, so
    # operators can tell mis-provisioned keys from tampering at a glance.
    signed = sign_intent(an_intent(), key_id="retired-key", key=KEY)

    assert verify_intent(signed, {KEY_ID: KEY}) is VerificationResult.UNKNOWN_SIGNING_KEY


def test_a_signature_made_with_the_wrong_key_fails() -> None:
    signed = sign_intent(an_intent(), key_id=KEY_ID, key=OTHER_KEY)

    assert verify_intent(signed, {KEY_ID: KEY}) is VerificationResult.BAD_SIGNATURE


def test_an_unrecognized_scheme_is_refused_rather_than_ignored() -> None:
    # The scheme enum is the seam an asymmetric scheme would land on; until one
    # exists, a scheme this build cannot evaluate must fail closed.
    signed = sign_intent(an_intent(), key_id=KEY_ID, key=KEY)
    # A future scheme these bindings do not know. Assigned through the enum's
    # own wrapper, since the generated stubs type the field as the enum.
    signed.signature_scheme = cast("ToolIntent.SignatureScheme", 7)

    assert verify_intent(signed, {KEY_ID: KEY}) is VerificationResult.BAD_SIGNATURE


def test_a_scheme_without_a_signature_still_reads_as_unsigned() -> None:
    intent = an_intent()
    intent.signature_scheme = SCHEME_HMAC_SHA256

    assert verify_intent(intent, {KEY_ID: KEY}) is VerificationResult.UNSIGNED


# --- Keyring and credential references ---------------------------------------


def test_a_keyring_loads_from_an_env_reference(monkeypatch: pytest.MonkeyPatch) -> None:
    # Scenario: keyring loading from `env:` references. The config carries the
    # reference; the key material is only ever in the workload's environment.
    monkeypatch.setenv(
        "TEST_INTENT_KEYS",
        f"{KEY_ID}={base64.b64encode(KEY).decode()}\nk2={base64.b64encode(OTHER_KEY).decode()}\n",
    )

    keyring = load_keyring("env:TEST_INTENT_KEYS")

    assert keyring == {KEY_ID: KEY, "k2": OTHER_KEY}


def test_a_keyring_loads_from_a_file_reference(tmp_path: Path) -> None:
    # Scenario: keyring loading from `file:` references — the secret-manager
    # pattern's other materialization (a mounted secret volume).
    path = tmp_path / "intent-keys"
    path.write_text(
        f"# provisioned by the secret manager\n{KEY_ID}={base64.b64encode(KEY).decode()}\n\n"
    )

    assert load_keyring(f"file:{path}") == {KEY_ID: KEY}


def test_a_malformed_keyring_reference_is_rejected() -> None:
    # Scenario: malformed reference rejected — actionable, and before anything
    # tries to use a key that will never exist.
    with pytest.raises(ValueError, match="is not a secret reference") as excinfo:
        load_keyring("/var/run/secrets/intent-keys")

    assert "env:" in str(excinfo.value) and "file:" in str(excinfo.value)


def test_an_empty_keyring_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_INTENT_KEYS", "\n# nothing here\n")

    with pytest.raises(ValueError, match="no keys"):
        load_keyring("env:TEST_INTENT_KEYS")


def test_a_missing_env_reference_names_the_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TEST_INTENT_KEYS", raising=False)

    with pytest.raises(ValueError, match="TEST_INTENT_KEYS"):
        load_keyring("env:TEST_INTENT_KEYS")


def test_a_malformed_keyring_line_names_the_line_but_not_its_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A parse error must not echo the line: half of a malformed keyring line is
    # still key material.
    monkeypatch.setenv("TEST_INTENT_KEYS", "k1 <no separator> c2VjcmV0LW1hdGVyaWFs")

    with pytest.raises(ValueError) as excinfo:
        load_keyring("env:TEST_INTENT_KEYS")

    assert "c2VjcmV0LW1hdGVyaWFs" not in str(excinfo.value)
    assert "line 1" in str(excinfo.value)


def test_an_unreadable_file_reference_names_the_path_not_its_contents(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="cannot read"):
        load_keyring(f"file:{tmp_path / 'never-provisioned'}")


def test_a_non_base64_keyring_line_is_rejected_without_echoing_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TEST_INTENT_KEYS", "k1=not!valid!base64!")

    with pytest.raises(ValueError, match="not valid base64") as excinfo:
        load_keyring("env:TEST_INTENT_KEYS")

    assert "not!valid!base64!" not in str(excinfo.value)


def test_a_signer_spec_without_a_key_id_is_rejected() -> None:
    with pytest.raises(ValueError, match="key_id"):
        IntentSigner(key_id="", key_reference="env:TEST_INTENT_KEYS")


def test_a_signer_spec_naming_an_uncomputable_scheme_is_rejected() -> None:
    # The scheme enum is the seam for a future asymmetric scheme; until one
    # exists, naming it at *configuration* time must fail rather than produce
    # intents nothing can verify.
    with pytest.raises(ValueError, match="HMAC_SHA256"):
        IntentSigner(
            key_id="k1",
            key_reference="env:TEST_INTENT_KEYS",
            scheme=cast("ToolIntent.SignatureScheme", 7),
        )


def test_an_empty_reference_target_is_rejected() -> None:
    with pytest.raises(ValueError, match="names nothing"):
        validate_secret_reference("sasl_password", "env:")


def test_reference_syntax_validates_without_resolving() -> None:
    # Eager, import-free validation is what makes a misconfigured deployment
    # fail at construction rather than on its first message.
    validate_secret_reference("sasl_password", "env:KAFKA_PASSWORD")
    validate_secret_reference("sasl_password", "file:/var/run/secrets/kafka")

    with pytest.raises(ValueError, match="sasl_password"):
        validate_secret_reference("sasl_password", "hunter2")


def test_resolving_a_reference_returns_the_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_KAFKA_PASSWORD", "s3cret\n")

    assert resolve_secret_reference("sasl_password", "env:TEST_KAFKA_PASSWORD") == "s3cret"
