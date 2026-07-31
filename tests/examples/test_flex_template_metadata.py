"""The Flex Template's `metadata.json`, parsed and held to the launcher.

`metadata.json` is what the console form and `gcloud dataflow flex-template
run` validate against; `launch.py` is what actually parses the values. Those
are two descriptions of one parameter surface, and nothing in the build makes
them agree — so this module is the drift guard: it parses the committed
document and asserts, against the launcher's own declared surface, that the
names match, the required/optional split matches, and every regex accepts a
value the launcher accepts and rejects one it rejects.

Offline, no docker, no GCP. Implements the `dataflow-flex-template` scenario
"metadata.json declares the full parameter surface" and the regex/parser
agreement risk recorded in the change's design (Risks).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from examples.fraud_triage_dataflow import launch

METADATA_PATH = Path(launch.__file__).with_name("metadata.json")

#: Values the launcher must accept, one per parameter. Curated rather than
#: generated: a regex describes a shape, not a semantically valid value, so
#: agreement is asserted on worked examples of each.
ACCEPTED: dict[str, str] = {
    "input_topic": "pubsub://my-project/tx-events",
    "approvals_topic": "pubsub://my-project/fraud-approvals",
    "output_topic": "pubsub://my-project/fraud-decisions",
    "intents_topic": "pubsub://my-project/fraud-intents",
    "model": "examples.fraud_triage:make_provider",
    "hitl_timeout_ms": "60000",
    "errors_to": "bigquery://my-project/agents/errors",
    "traces_to": "otlp://collector:4318",
    "model_api_key_secret": "projects/my-project/secrets/anthropic-key/versions/3",
}

#: Values the launcher must reject — and the metadata regex must reject too, so
#: the console rejects them before a job is ever created.
REJECTED: dict[str, str] = {
    # Wrong scheme: the read/write topics are Pub/Sub, not Kafka.
    "input_topic": "kafka://broker:9092/tx-events",
    "approvals_topic": "fraud-approvals",
    "output_topic": "pubsub://my-project",
    "intents_topic": "pubsub:///fraud-intents",
    # No colon: a dotted path is not a `module:object` reference.
    "model": "examples.fraud_triage.make_provider",
    # Non-positive: `HitlPolicy` refuses it, so the regex must too.
    "hitl_timeout_ms": "0",
    # `otlp://` is a lossy best-effort tap; the errors stream needs a real sink.
    "errors_to": "otlp://collector:4318",
    "traces_to": "redis://cache:6379/traces",
    # A bare secret name is not a version resource name.
    "model_api_key_secret": "projects/my-project/secrets/anthropic-key",
}


@pytest.fixture(scope="module")
def metadata() -> dict[str, Any]:
    with METADATA_PATH.open(encoding="utf-8") as handle:
        parsed: dict[str, Any] = json.load(handle)
    return parsed


def _by_name(metadata: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {parameter["name"]: parameter for parameter in metadata["parameters"]}


# --- Requirement: the template declares a validated parameter surface covering
# --- topics, model, and HITL timeout -------------------------------------------


def test_metadata_declares_the_full_parameter_surface(metadata: dict[str, Any]) -> None:
    # Scenario: metadata.json declares the full parameter surface. The committed
    # document declares exactly the specified parameters, with the five topic +
    # model parameters required and the rest optional, each carrying help text.
    parameters = _by_name(metadata)

    assert set(parameters) == {
        "input_topic",
        "approvals_topic",
        "output_topic",
        "intents_topic",
        "model",
        "hitl_timeout_ms",
        "errors_to",
        "traces_to",
        "model_api_key_secret",
    }

    required = {name for name, spec in parameters.items() if not spec.get("isOptional", False)}
    assert required == {
        "input_topic",
        "approvals_topic",
        "output_topic",
        "intents_topic",
        "model",
    }

    for name, spec in parameters.items():
        assert spec.get("helpText"), f"{name} carries no help text"
        assert spec.get("label"), f"{name} carries no label"

    assert metadata["name"]
    assert metadata["description"]


def test_every_topic_parameter_carries_the_pubsub_grammar_regex(metadata: dict[str, Any]) -> None:
    # Scenario: metadata.json declares the full parameter surface — the topic
    # half. All four topic parameters pin the sink resolver's
    # `pubsub://<project>/<topic>` grammar in the document itself, so the
    # console rejects a malformed URI before a job exists.
    parameters = _by_name(metadata)
    for name in ("input_topic", "approvals_topic", "output_topic", "intents_topic"):
        regexes = parameters[name].get("regexes")
        assert regexes, f"{name} declares no regex"
        assert len(regexes) == 1
        assert regexes[0].startswith("^pubsub://"), (
            f"{name}'s regex does not pin the pubsub:// grammar: {regexes[0]!r}"
        )


def test_metadata_parameters_match_the_launchers_accepted_flags(metadata: dict[str, Any]) -> None:
    # Scenario: metadata.json declares the full parameter surface — the drift
    # guard. The document and the launcher are two descriptions of one surface;
    # this asserts they are the same one, name for name and flag for flag.
    documented = {
        parameter["name"]: not parameter.get("isOptional", False)
        for parameter in metadata["parameters"]
    }
    accepted = {parameter.name: parameter.required for parameter in launch.PARAMETERS}
    assert documented == accepted


# --- Requirement: provider API keys are supplied via Secret Manager resource
# --- names, never as parameter values ------------------------------------------


def test_no_parameter_accepts_a_credential_value(metadata: dict[str, Any]) -> None:
    # Scenario: Secret value never transits the launch surface — the static half.
    # A key passed as a parameter is a key logged, so no declared parameter may
    # be a credential slot. `model_api_key_secret` names a Secret Manager
    # resource, which is why its regex pins the resource-name grammar and
    # nothing else.
    for parameter in metadata["parameters"]:
        name = parameter["name"]
        assert not launch.is_credential_parameter(name), (
            f"{name} reads as a credential-valued parameter; secrets are supplied "
            "by Secret Manager resource name only"
        )

    secret = _by_name(metadata)["model_api_key_secret"]
    assert secret["regexes"] == [launch.SECRET_RESOURCE_PATTERN]
    assert secret.get("isOptional") is True


# --- Requirement: metadata regexes and the launcher's validation agree ---------


@pytest.mark.parametrize("name", sorted(ACCEPTED))
def test_a_value_each_regex_accepts_is_accepted_by_the_launcher(
    metadata: dict[str, Any], name: str
) -> None:
    # Design Risks: "Grammar drift between metadata.json regexes and Python
    # validation". The regexes are a UX prefilter and the launcher is
    # authoritative; drift between them has to fail offline in `ci`, not at a
    # nightly launch.
    value = ACCEPTED[name]
    for pattern in _by_name(metadata)[name].get("regexes", []):
        assert re.fullmatch(pattern, value), f"{name}: {value!r} does not match {pattern!r}"
    launch.validate_parameter(name, value)  # must not raise


@pytest.mark.parametrize("name", sorted(REJECTED))
def test_a_value_each_regex_rejects_is_rejected_by_the_launcher(
    metadata: dict[str, Any], name: str
) -> None:
    # The other direction: a value the console form turns away must also fail
    # the launcher, so the two layers never disagree about what is valid.
    value = REJECTED[name]
    for pattern in _by_name(metadata)[name].get("regexes", []):
        assert not re.fullmatch(pattern, value), (
            f"{name}: {value!r} unexpectedly matches {pattern!r}"
        )
    with pytest.raises(ValueError, match=re.escape(name)):
        launch.validate_parameter(name, value)
