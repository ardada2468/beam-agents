"""Secret redaction for the effector-security capability.

Covers "Secrets embedded in URIs never appear in errors or reprs". These are
the three concrete leak paths the proposal identified in shipped code: an
`EffectorConfigError` interpolating a raw credentialed URI, the default
dataclass `repr` of `EffectorConfig`, and `__main__.py`'s stderr startup path.

Each test asserts on the *absence* of the password rather than the presence of
a mask, because a leak that changes shape (a different message, a chained
exception, a different renderer) must still fail the test.
"""

from __future__ import annotations

import pytest

from beam_agents.effector.__main__ import main
from beam_agents.effector.config import EffectorConfig, EffectorConfigError, redact_uri

PASSWORD = "sup3rs3cret"
CREDENTIALED = f"redis://admin:{PASSWORD}@redis.internal:6379"

_VALID = {
    "intents_from": "kafka://localhost:9092/intents",
    "results_to": "kafka://localhost:9092/results",
    "approvals_to": "kafka://localhost:9092/approvals",
    "dedup": CREDENTIALED,
    "consumer_group": "effector",
}


def _chain_text(exc: BaseException) -> str:
    """Every message in an exception's cause/context chain, concatenated."""
    parts: list[str] = []
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        parts.extend((str(current), repr(current)))
        current = current.__cause__ or current.__context__
    return "\n".join(parts)


# --- Requirement: Secrets embedded in URIs never appear in errors or reprs ----


def test_a_malformed_credentialed_uri_is_reported_redacted() -> None:
    # Scenario: A malformed credentialed URI is reported redacted.
    # `redis://user:password@` is all userinfo and no host — the exact shape a
    # credential-carrying typo takes, and the one whose message must not echo
    # the credential.
    with pytest.raises(EffectorConfigError) as excinfo:
        EffectorConfig(**{**_VALID, "dedup": f"redis://admin:{PASSWORD}@"})  # type: ignore[arg-type]

    assert PASSWORD not in _chain_text(excinfo.value)
    assert "redis" in str(excinfo.value)


def test_a_malformed_credentialed_bigtable_uri_is_reported_redacted() -> None:
    with pytest.raises(EffectorConfigError) as excinfo:
        EffectorConfig(**{**_VALID, "dedup": f"bigtable://admin:{PASSWORD}@project"})  # type: ignore[arg-type]

    assert PASSWORD not in _chain_text(excinfo.value)


@pytest.mark.parametrize(
    "field",
    ["intents_from", "results_to", "approvals_to"],
)
def test_a_malformed_credentialed_transport_uri_is_reported_redacted(field: str) -> None:
    # The dedup URI is the documented example, but every URI the config parses
    # can carry userinfo, and every one of them lands in an error message.
    with pytest.raises(EffectorConfigError) as excinfo:
        EffectorConfig(**{**_VALID, field: f"kafka://user:{PASSWORD}@broker:9092"})  # type: ignore[arg-type]

    assert PASSWORD not in _chain_text(excinfo.value)


def test_an_unknown_scheme_on_a_credentialed_uri_is_reported_redacted() -> None:
    with pytest.raises(EffectorConfigError) as excinfo:
        EffectorConfig(**{**_VALID, "dedup": f"mongodb://admin:{PASSWORD}@host:27017"})  # type: ignore[arg-type]

    assert PASSWORD not in _chain_text(excinfo.value)


def test_the_config_repr_masks_credentials() -> None:
    # Scenario: The config repr masks credentials. A `repr` reaches logs,
    # tracebacks, and test output without anyone deciding it should.
    config = EffectorConfig(**_VALID)  # type: ignore[arg-type]

    rendered = repr(config)

    assert PASSWORD not in rendered
    assert "redis.internal" in rendered, "the host must survive so the repr stays useful"
    assert "EffectorConfig(" in rendered


def test_the_startup_error_path_prints_redacted_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Scenario: `beam-agents-effector` startup error path prints redacted
    # output. Startup failure is the most likely moment for a credentialed URI
    # to reach a terminal, a CI log, or a crash report.
    exit_code = main(
        [
            "--registry",
            "tests.effector.test_main:TOOLS",
            "--intents-from",
            f"kafka://user:{PASSWORD}@broker:9092",
            "--results-to",
            "kafka://localhost:9092/results",
            "--approvals-to",
            "kafka://localhost:9092/approvals",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert PASSWORD not in captured.err + captured.out


def test_redact_uri_leaves_credential_free_uris_untouched() -> None:
    for uri in (
        "kafka://broker:9092/intents",
        "pubsub://my-project/intents",
        "memory://",
        "bigtable://p/i/t",
    ):
        assert redact_uri(uri) == uri


def test_redact_uri_masks_userinfo_in_free_text() -> None:
    # The helper is applied to whole messages, not only to bare URIs, so that
    # an interpolation site added later is covered by default.
    text = f"could not connect to redis://admin:{PASSWORD}@redis.internal:6379/0 after 3 attempts"

    redacted = redact_uri(text)

    assert PASSWORD not in redacted
    assert "admin" not in redacted
    assert "redis.internal:6379/0" in redacted
    assert "after 3 attempts" in redacted
