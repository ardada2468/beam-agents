"""Broker transport security for the effector-security capability.

Covers "Broker transport security is configurable with credentials by
reference": the settings reach every Kafka client the library constructs, the
reference grammar is validated eagerly and import-free, and the *resolved*
secret exists only inside the client object — never on the configuration, its
`repr`, or an error it raises.
"""

from __future__ import annotations

import ssl
import types
from pathlib import Path
from typing import Any

import pytest

from beam_agents.actions.write_intents import _kafka_producer_config
from beam_agents.effector.config import EffectorConfig, TransportSecurity
from beam_agents.effector.sinks import KafkaMessageSink
from beam_agents.effector.sources import KafkaIntentSource

from .test_adapters import fake_aiokafka  # noqa: F401  (pytest fixture)

PASSWORD_ENV = "TEST_KAFKA_PASSWORD"
PASSWORD = "s3cret-broker-password"

_VALID = {
    "intents_from": "kafka://localhost:9092/intents",
    "results_to": "kafka://localhost:9092/results",
    "approvals_to": "kafka://localhost:9092/approvals",
    "dedup": "memory://",
    "consumer_group": "effector",
}


@pytest.fixture(autouse=True)
def _provision_password(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(PASSWORD_ENV, PASSWORD)


def sasl_security() -> TransportSecurity:
    return TransportSecurity(
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_username_reference="env:TEST_KAFKA_USER",
        sasl_password_reference=f"env:{PASSWORD_ENV}",
    )


@pytest.fixture(autouse=True)
def _provision_user(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_KAFKA_USER", "effector")


# --- Requirement: Broker transport security is configurable with credentials
# by reference -----------------------------------------------------------------


def test_sasl_settings_reach_the_kafka_source(fake_aiokafka: types.ModuleType) -> None:  # noqa: F811
    # Scenario: SASL settings reach the Kafka clients (consumer half).
    source = KafkaIntentSource("broker:9092", "intents", "effector", security=sasl_security())
    consumer: Any = source._consumer

    assert consumer.kwargs["security_protocol"] == "SASL_SSL"
    assert consumer.kwargs["sasl_mechanism"] == "SCRAM-SHA-512"
    assert consumer.kwargs["sasl_plain_username"] == "effector"
    assert consumer.kwargs["sasl_plain_password"] == PASSWORD


def test_sasl_settings_reach_the_kafka_sink(fake_aiokafka: types.ModuleType) -> None:  # noqa: F811
    # Scenario: SASL settings reach the Kafka clients (producer half).
    sink = KafkaMessageSink("broker:9092", "results", security=sasl_security())
    producer: Any = sink._producer

    assert producer.kwargs["security_protocol"] == "SASL_SSL"
    assert producer.kwargs["sasl_mechanism"] == "SCRAM-SHA-512"
    assert producer.kwargs["sasl_plain_password"] == PASSWORD
    # Idempotence is not negotiable away by adding security settings.
    assert producer.kwargs["enable_idempotence"] is True


def test_sasl_settings_reach_the_outbox_producer_config() -> None:
    # Scenario: SASL settings reach the Kafka clients (WriteIntents' producer).
    # The outbox writer is Beam's cross-language Kafka sink, so its client is
    # configured with Java client properties rather than aiokafka kwargs.
    config = _kafka_producer_config("broker:9092", sasl_security())

    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "SCRAM-SHA-512"
    assert PASSWORD in config["sasl.jaas.config"]
    assert config["bootstrap.servers"] == "broker:9092"


def test_tls_material_paths_reach_the_client(fake_aiokafka: types.ModuleType) -> None:  # noqa: F811
    # mTLS is the other supported baseline; the CA path must become a real SSL
    # context rather than being silently dropped. Uses the platform's own CA
    # bundle because `ssl` will not load a fabricated one, and the property
    # under test is that the path is honored at all.
    cafile = ssl.get_default_verify_paths().cafile
    if cafile is None or not Path(cafile).exists():
        pytest.skip("no platform CA bundle to point the context at")
    security = TransportSecurity(security_protocol="SSL", ssl_ca_location=cafile)

    source = KafkaIntentSource("broker:9092", "intents", "effector", security=security)
    consumer: Any = source._consumer

    assert consumer.kwargs["security_protocol"] == "SSL"
    context = consumer.kwargs["ssl_context"]
    assert isinstance(context, ssl.SSLContext)
    assert context.get_ca_certs(), "the configured CA material must be loaded into the context"


def test_a_plaintext_protocol_builds_no_ssl_context(fake_aiokafka: types.ModuleType) -> None:  # noqa: F811
    security = TransportSecurity(security_protocol="PLAINTEXT")

    source = KafkaIntentSource("broker:9092", "intents", "effector", security=security)
    consumer: Any = source._consumer

    assert "ssl_context" not in consumer.kwargs


def test_a_malformed_credential_reference_fails_eagerly_and_import_free() -> None:
    # Scenario: A malformed credential reference fails eagerly and import-free.
    with pytest.raises(ValueError, match="sasl_password_reference") as excinfo:
        EffectorConfig(
            **_VALID,  # type: ignore[arg-type]
            transport_security=TransportSecurity(
                security_protocol="SASL_SSL",
                sasl_mechanism="PLAIN",
                sasl_username_reference="env:TEST_KAFKA_USER",
                sasl_password_reference="hunter2",
            ),
        )

    assert "env:" in str(excinfo.value) and "file:" in str(excinfo.value)


def test_a_sasl_mechanism_without_a_protocol_that_uses_it_is_rejected() -> None:
    with pytest.raises(ValueError, match="sasl_mechanism"):
        TransportSecurity(security_protocol="PLAINTEXT", sasl_mechanism="PLAIN")


def test_an_unrecognized_security_protocol_is_rejected() -> None:
    with pytest.raises(ValueError, match="security_protocol"):
        TransportSecurity(security_protocol="SASL_TLS")


def test_a_sasl_mechanism_without_both_credential_references_is_rejected() -> None:
    # Half a credential is a runtime authentication failure at first connect,
    # which is exactly the class of error eager validation exists to prevent.
    with pytest.raises(ValueError, match="sasl_username_reference"):
        TransportSecurity(
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_password_reference=f"env:{PASSWORD_ENV}",
        )


def test_resolved_secrets_are_absent_from_the_configuration_object(
    fake_aiokafka: types.ModuleType,  # noqa: F811
) -> None:
    # Scenario: Resolved secrets are absent from the configuration object.
    config = EffectorConfig(**_VALID, transport_security=sasl_security())  # type: ignore[arg-type]

    KafkaIntentSource("broker:9092", "intents", "effector", security=config.transport_security)

    assert PASSWORD not in repr(config)
    assert PASSWORD not in repr(config.transport_security)
    assert PASSWORD not in str(vars(config))
    assert config.transport_security is not None
    assert config.transport_security.sasl_password_reference == f"env:{PASSWORD_ENV}"


def test_an_unresolvable_reference_names_the_variable_not_a_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(PASSWORD_ENV, raising=False)
    security = sasl_security()

    with pytest.raises(ValueError, match=PASSWORD_ENV):
        security.client_kwargs()


def test_the_java_producer_config_uses_the_right_login_module_per_mechanism() -> None:
    # PLAIN and SCRAM take different JAAS login modules; emitting the wrong one
    # is a broker-side authentication failure with a confusing message, and the
    # offline lane is the only place this is cheap to pin.
    scram = _kafka_producer_config("broker:9092", sasl_security())
    plain = _kafka_producer_config(
        "broker:9092",
        TransportSecurity(
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username_reference="env:TEST_KAFKA_USER",
            sasl_password_reference=f"env:{PASSWORD_ENV}",
        ),
    )

    assert "ScramLoginModule" in scram["sasl.jaas.config"]
    assert "PlainLoginModule" in plain["sasl.jaas.config"]


def test_tls_material_paths_reach_the_java_producer_config(tmp_path: Path) -> None:
    # The Java client reads keystores where the Python client reads PEM; both
    # sides must at least receive the paths they were given.
    security = TransportSecurity(
        security_protocol="SSL",
        ssl_ca_location=str(tmp_path / "truststore.jks"),
        ssl_certificate_location=str(tmp_path / "keystore.jks"),
    )

    config = _kafka_producer_config("broker:9092", security)

    assert config["ssl.truststore.location"] == str(tmp_path / "truststore.jks")
    assert config["ssl.keystore.location"] == str(tmp_path / "keystore.jks")
    assert "sasl.mechanism" not in config


def test_a_client_certificate_is_loaded_into_the_ssl_context(
    fake_aiokafka: types.ModuleType,  # noqa: F811
) -> None:
    # mTLS: the client certificate and key must reach the context, and an
    # unusable pair must fail loudly at client construction rather than as an
    # opaque handshake error later.
    security = TransportSecurity(
        security_protocol="SSL",
        ssl_certificate_location="/nonexistent/client.pem",
        ssl_key_location="/nonexistent/client.key",
    )

    with pytest.raises(OSError, match=r"client\.pem|No such file"):
        KafkaIntentSource("broker:9092", "intents", "effector", security=security)


def test_no_security_block_leaves_the_clients_exactly_as_they_were(
    fake_aiokafka: types.ModuleType,  # noqa: F811
) -> None:
    # The default must stay byte-for-byte today's construction: an unconfigured
    # deployment gains no new kwargs it never asked for.
    source = KafkaIntentSource("broker:9092", "intents", "effector")
    consumer: Any = source._consumer

    assert set(consumer.kwargs) == {
        "bootstrap_servers",
        "group_id",
        "enable_auto_commit",
        "auto_offset_reset",
    }
    assert _kafka_producer_config("broker:9092", None) == {
        "bootstrap.servers": "broker:9092",
        "enable.idempotence": "true",
    }
