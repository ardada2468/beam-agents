"""The shipped security baseline for the effector-security capability.

Covers "The security baseline is documented, including the prohibition on
secrets in intent payloads". Doctrine that is not written down is not doctrine,
and the two clauses asserted here — secrets never in tool arguments, and a
per-principal least-privilege matrix — are the parts a deployment cannot infer
from the code.
"""

from __future__ import annotations

from pathlib import Path

import pytest

DOC = Path(__file__).resolve().parents[2] / "docs" / "security.md"


@pytest.fixture(scope="module")
def text() -> str:
    assert DOC.exists(), f"docs/security.md is missing: {DOC}"
    return DOC.read_text()


def _prose(text: str) -> str:
    """Lowercased, with hyphens flattened, so phrase checks are not spelling checks."""
    return text.lower().replace("-", " ").replace("\n", " ")


def test_the_security_document_states_the_secrets_in_args_contract(text: str) -> None:
    # Scenario: The security document states the secrets-in-args contract.
    prose = _prose(text)

    assert "args_json" in text
    assert "tool argument" in prose
    # The reason, not just the rule: args_json is copied into keyed state, onto
    # the broker, and into dead letters, so a secret there is unrecoverable.
    assert "keyed state" in prose
    assert "dead letter" in prose
    assert "outbox topic" in prose
    # And the replacement pattern, so the rule is actionable.
    assert "os.environ" in text


def test_the_security_document_gives_the_least_privilege_matrix(text: str) -> None:
    # Scenario: The security document gives the least-privilege matrix.
    prose = _prose(text)

    for principal in ("pipeline", "effector"):
        assert principal in prose
    for channel in ("intents", "results", "approvals", "dead letter"):
        assert channel in prose
    assert "admin" in prose, "the matrix must say neither principal gets broker admin"
    assert "roles/pubsub" in text, "the Pub/Sub IAM half of the matrix must be concrete"


def test_the_security_document_covers_the_rollout_dial_and_its_counters(text: str) -> None:
    # The rollout is the part an operator executes; the counters are how they
    # know it is safe to advance.
    for phase in ("off", "permissive", "require"):
        assert phase in text
    assert "unsigned_intents_accepted" in text
    for reason in ("unsigned_intent", "bad_signature", "unknown_signing_key"):
        assert reason in text
    assert "result_ttl_ms" in text, "the replay-window rule must be stated"


def test_the_effector_guide_points_at_the_security_document() -> None:
    effector_doc = (DOC.parent / "effector.md").read_text()

    assert "security.md" in effector_doc
    # The composed phase order gains its zeroth step.
    assert "verify" in effector_doc.lower()
