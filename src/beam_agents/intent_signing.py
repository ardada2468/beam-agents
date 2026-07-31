"""Intent signing and verification: the application-level provenance layer.

The effects loop is asynchronous — the pipeline and the effector never hold a
connection to each other — so "authentication between them" cannot mean a
channel handshake. It means: the pipeline signs each ``ToolIntent`` at the
outbox writer, and the effector verifies before anything else runs. Without
this, write access to the outbox topic *is* the authority to execute any
registered ``side_effect=True`` tool with arbitrary arguments, and the dedup
store is no defense at all (it collapses duplicates of an ``intent_id``; a
forged intent simply carries a novel one).

Three properties shape everything here:

- **Beam-free and stdlib-only.** The signer lives in the pipeline and the
  verifier lives in the effector, which imports neither Beam nor
  ``beam_agents.core``. HMAC-SHA256 is ``hmac``/``hashlib``, so both halves
  share this module with no new dependency on either side (design D2).
- **Deterministic.** A retried bundle re-mints byte-identical intents
  (correctness invariant 2) and must re-sign them to byte-identical wire
  messages, or the outbox write stops being replay-stable.
- **Key material by reference.** Configuration carries ``env:VAR`` or
  ``file:/path``; the key bytes are resolved worker-side and never pickled into
  a pipeline graph, stored back on a config object, or rendered by a ``repr``
  (design D7).

The signing input is the message with its three signature fields cleared,
serialized deterministically — see ``ToolIntent`` in ``protos/beam_agents.proto``
for why that definition survives schema skew.

Importing this module has no side effects.
"""

from __future__ import annotations

import base64
import binascii
import hmac
import os
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final, TypeAlias

from beam_agents._protos import ToolIntent

# The one scheme this build can compute and verify. Re-exported from the proto
# rather than redefined, so the wire value and the code path cannot drift.
SCHEME_HMAC_SHA256: Final[ToolIntent.SignatureScheme] = ToolIntent.HMAC_SHA256

# Cleared before the MAC is computed, on both ends.
_SIGNATURE_FIELDS: Final = ("signature_scheme", "signing_key_id", "signature")

_ENV_PREFIX: Final = "env:"
_FILE_PREFIX: Final = "file:"

# `key_id → key` map. Deliberately a plain mapping rather than a class: the
# keyring is loaded at startup from a reference and passed by value to the
# verifier, so there is no object holding key material whose `repr` could leak
# it into a log or a traceback.
Keyring: TypeAlias = Mapping[str, bytes]


class VerificationResult(StrEnum):
    """The four outcomes of verifying a delivered intent.

    The three failures are deliberately distinct: ``UNSIGNED`` is a rollout
    state, ``BAD_SIGNATURE`` is tampering or corruption, and
    ``UNKNOWN_SIGNING_KEY`` is almost always a mis-provisioned keyring. Folding
    them together would make a bad deploy indistinguishable from an attack.

    The values double as the dead-letter reason vocabulary, extending the
    reasons ``REASON_INTENT_DEAD_LETTER`` established pipeline-side.
    """

    OK = "ok"
    UNSIGNED = "unsigned_intent"
    BAD_SIGNATURE = "bad_signature"
    UNKNOWN_SIGNING_KEY = "unknown_signing_key"


@dataclass(frozen=True, slots=True)
class IntentSigner:
    """Which key to sign outbox intents with, named by reference.

    Carries no key material: this object is pickled into the pipeline graph,
    which the runner stores and renders in job descriptions. ``key_reference``
    is resolved worker-side in ``DoFn.setup()``.
    """

    key_id: str
    key_reference: str
    scheme: ToolIntent.SignatureScheme = SCHEME_HMAC_SHA256

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Reject a malformed spec eagerly, importing nothing."""
        if not self.key_id:
            raise ValueError("IntentSigner.key_id must be a non-empty string")
        validate_secret_reference("IntentSigner.key_reference", self.key_reference)
        if self.scheme != SCHEME_HMAC_SHA256:
            raise ValueError(
                f"IntentSigner.scheme {self.scheme!r} is not a scheme this build can compute; "
                f"expected HMAC_SHA256 ({SCHEME_HMAC_SHA256})"
            )

    def resolve_key(self) -> bytes:
        """Load the keyring and return this signer's key. Never cached here."""
        keyring = load_keyring(self.key_reference)
        try:
            return keyring[self.key_id]
        except KeyError:
            raise ValueError(
                f"IntentSigner.key_id {self.key_id!r} is absent from the keyring at "
                f"{self.key_reference!r}, which carries {sorted(keyring)!r}"
            ) from None


# -- signing input -------------------------------------------------------------


def signing_input(intent: ToolIntent) -> bytes:
    """The bytes a signature is computed over: the intent, signature fields cleared.

    Computed on a copy, so the caller's message — which for a staged intent is
    the object held in keyed state — is never touched.
    """
    unsigned = ToolIntent()
    unsigned.CopyFrom(intent)
    for field in _SIGNATURE_FIELDS:
        unsigned.ClearField(field)
    return unsigned.SerializeToString(deterministic=True)


def sign_intent(intent: ToolIntent, *, key_id: str, key: bytes) -> ToolIntent:
    """Return a signed **copy** of ``intent``.

    A copy rather than an in-place stamp on purpose (design D3): Beam hands the
    outbox writer a reference to the intent staged in ``PENDING`` state, so
    stamping in place would rewrite committed state bytes and break the
    keyed-state byte-identity this change promises.
    """
    signed = ToolIntent()
    signed.CopyFrom(intent)
    signed.signature_scheme = SCHEME_HMAC_SHA256
    signed.signing_key_id = key_id
    signed.signature = hmac.digest(key, signing_input(signed), "sha256")
    return signed


def verify_intent(intent: ToolIntent, keyring: Mapping[str, bytes]) -> VerificationResult:
    """Verify ``intent`` against ``keyring``; a pure function of the delivered bytes.

    Purity is what lets verification sit ahead of every other phase without
    weakening any crash argument: it acquires nothing a crash could leak.
    """
    if not intent.signature:
        return VerificationResult.UNSIGNED
    if intent.signature_scheme != SCHEME_HMAC_SHA256:
        # A scheme this build cannot evaluate fails closed. Treating it as
        # unsigned would let an attacker downgrade past `require` by naming a
        # scheme nobody implements.
        return VerificationResult.BAD_SIGNATURE
    key = keyring.get(intent.signing_key_id)
    if key is None:
        return VerificationResult.UNKNOWN_SIGNING_KEY
    expected = hmac.digest(key, signing_input(intent), "sha256")
    if not hmac.compare_digest(expected, intent.signature):
        return VerificationResult.BAD_SIGNATURE
    return VerificationResult.OK


# -- secret references ---------------------------------------------------------


def validate_secret_reference(field_name: str, reference: str) -> None:
    """Reject a reference that is neither ``env:VAR`` nor ``file:/path``.

    Eager and import-free, so a misconfigured deployment fails at construction
    rather than on its first message. Deliberately does NOT resolve: validation
    runs where the secret may not be materialized yet.
    """
    if not reference.startswith((_ENV_PREFIX, _FILE_PREFIX)):
        raise ValueError(
            f"{field_name}: {reference!r} is not a secret reference; expected "
            f"'env:VAR_NAME' or 'file:/path'. Secrets are supplied by reference so the "
            "value never reaches configuration, a repr, or argv."
        )
    if not reference.split(":", 1)[1]:
        raise ValueError(f"{field_name}: {reference!r} names nothing after its prefix")


def resolve_secret_reference(field_name: str, reference: str) -> str:
    """Resolve ``env:VAR``/``file:/path`` to its value, stripped of trailing newline.

    The returned value is handed straight to a client constructor and never
    stored back on a configuration object.
    """
    validate_secret_reference(field_name, reference)
    kind, _, target = reference.partition(":")
    if kind == "env":
        value = os.environ.get(target)
        if value is None:
            raise ValueError(
                f"{field_name}: environment variable {target!r} is not set; the deployment's "
                "secret manager must materialize it before the process starts"
            )
        return value.strip()
    path = Path(target)
    try:
        return path.read_text().strip()
    except OSError as exc:
        raise ValueError(f"{field_name}: cannot read {target!r}: {exc.strerror}") from None


def load_keyring(reference: str) -> dict[str, bytes]:
    """Load a ``key_id=base64(key)`` keyring from an ``env:``/``file:`` reference.

    Blank lines and ``#`` comments are ignored so a mounted secret file can
    carry provenance. Parse errors never echo the offending line: half of a
    malformed keyring line is still key material.
    """
    raw = resolve_secret_reference("intent signing keyring", reference)
    keyring: dict[str, bytes] = {}
    for number, line in enumerate(raw.splitlines(), start=1):
        entry = line.strip()
        if not entry or entry.startswith("#"):
            continue
        key_id, separator, encoded = entry.partition("=")
        if not separator or not key_id.strip():
            raise ValueError(
                f"intent signing keyring {reference!r}: malformed entry on line {number}; "
                "expected 'key_id=base64(key)'"
            )
        try:
            keyring[key_id.strip()] = base64.b64decode(encoded.strip(), validate=True)
        except (binascii.Error, ValueError):
            raise ValueError(
                f"intent signing keyring {reference!r}: key material on line {number} is not "
                "valid base64"
            ) from None
    if not keyring:
        raise ValueError(
            f"intent signing keyring {reference!r} carries no keys; a verifying mode needs "
            "at least one 'key_id=base64(key)' entry"
        )
    return keyring


__all__ = [
    "SCHEME_HMAC_SHA256",
    "IntentSigner",
    "Keyring",
    "VerificationResult",
    "load_keyring",
    "resolve_secret_reference",
    "sign_intent",
    "signing_input",
    "validate_secret_reference",
    "verify_intent",
]
