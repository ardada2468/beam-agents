"""The console entry point for the effector-service capability.

Covers the startup half of "Configuration is validated eagerly, before any
client is constructed": misconfiguration must fail at startup with an
actionable message rather than on the first message.
"""

from __future__ import annotations

import asyncio
import base64
import sys
import types

import pytest

from beam_agents.effector.__main__ import (
    build_parser,
    build_service,
    config_from_args,
    load_registry,
    main,
    serve,
    transport_security_from_args,
)
from beam_agents.effector.dedup import Claimed, InMemoryDedupStore
from beam_agents.effector.runner import EffectorToolRunner
from beam_agents.effector.service import EffectorService
from beam_agents.effector.sinks import KafkaMessageSink
from beam_agents.effector.sources import KafkaIntentSource
from beam_agents.tools import ToolRegistry, tool

from ._fakes import NOW_MS, RecordingDedupStore, an_intent, build_harness
from .test_adapters import _FakeConsumer, _FakeProducer, _FakeTopicPartition

TOOLS = ToolRegistry()


@tool(side_effect=True)
def charge(amount_cents: int) -> str:
    return "receipt"


TOOLS.register(charge)


def _args(**overrides: str) -> object:
    argv = [
        "--registry",
        "tests.effector.test_main:TOOLS",
        "--intents-from",
        "kafka://localhost:9092/intents",
        "--results-to",
        "kafka://localhost:9092/results",
        "--approvals-to",
        "kafka://localhost:9092/approvals",
    ]
    for name, value in overrides.items():
        argv += [f"--{name.replace('_', '-')}", value]
    return build_parser().parse_args(argv)


def test_a_registry_is_loaded_from_its_import_path() -> None:
    assert load_registry("tests.effector.test_main:TOOLS") is TOOLS


@pytest.mark.parametrize("path", ["tests.effector.test_main", "TOOLS", ""])
def test_a_malformed_registry_path_is_rejected(path: str) -> None:
    with pytest.raises(ValueError, match="module:attribute"):
        load_registry(path)


def test_an_unknown_registry_attribute_is_rejected() -> None:
    with pytest.raises(ValueError, match="no attribute"):
        load_registry("tests.effector.test_main:NOPE")


def test_config_is_built_from_parsed_arguments() -> None:
    config = config_from_args(_args())  # type: ignore[arg-type]

    assert config.intents_from == "kafka://localhost:9092/intents"
    assert config.consumer_group == "beam-agents-effector"
    assert config.dedup == "memory://"


def test_missing_required_settings_are_named() -> None:
    args = build_parser().parse_args([])

    with pytest.raises(ValueError, match="--registry") as excinfo:
        config_from_args(args)

    assert "--intents-from" in str(excinfo.value)


def test_main_exits_with_a_message_on_misconfiguration(capsys: pytest.CaptureFixture[str]) -> None:
    assert main([]) == 2

    assert "error:" in capsys.readouterr().err


def test_main_exits_with_a_message_on_an_invalid_uri(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(
        [
            "--registry",
            "tests.effector.test_main:TOOLS",
            "--intents-from",
            "amqp://localhost/intents",
            "--results-to",
            "kafka://localhost:9092/results",
            "--approvals-to",
            "kafka://localhost:9092/approvals",
        ]
    )

    assert code == 2
    assert "amqp" in capsys.readouterr().err


async def test_serve_shuts_down_cleanly_when_cancelled() -> None:
    # A signalled shutdown drains rather than drops: the service closes its
    # collaborators instead of leaving claims and connections dangling.
    harness = build_harness(registry=TOOLS, intents=[an_intent()], clock=lambda: NOW_MS)

    await serve(harness.service)

    assert harness.source.closed
    assert harness.results.closed
    assert harness.approvals.closed


async def test_serve_releases_unexecuted_claims_on_shutdown() -> None:
    # An interrupted worker must hand its claim back, or a restarting replica
    # waits out a full lease for an intent nobody is executing.
    gate = asyncio.Event()

    class _StallingRunner(EffectorToolRunner):
        """Stalls after the claim, before the callable is invoked."""

        async def run(
            self,
            t: object,
            arguments: object,
            *,
            on_invoke: object = None,
            intent_info: object = None,
        ) -> object:
            await gate.wait()
            return await super().run(
                t,  # type: ignore[arg-type]
                arguments,  # type: ignore[arg-type]
                on_invoke=on_invoke,  # type: ignore[arg-type]
                intent_info=intent_info,  # type: ignore[arg-type]
            )

    store = InMemoryDedupStore(clock=lambda: NOW_MS)
    dedup = RecordingDedupStore(store)
    harness = build_harness(
        registry=TOOLS,
        intents=[an_intent()],
        dedup=dedup,
        runner=_StallingRunner(tool_timeout_ms=1_000),
    )

    serving = asyncio.create_task(serve(harness.service))
    for _ in range(50):
        await asyncio.sleep(0)
        if "claim" in dedup.calls:
            break
    assert "claim" in dedup.calls

    serving.cancel()
    await asyncio.gather(serving, return_exceptions=True)

    assert "release" in dedup.calls
    assert isinstance(await store.claim("intent-1", 60_000), Claimed)
    assert harness.committed_intent_ids == []


def test_the_service_builder_wires_every_adapter_from_the_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # `build_service` is the only place the URIs become live clients; a wrong
    # dispatch here would surface as a connection error in production, not a
    # test failure.
    module = types.ModuleType("aiokafka")
    module.AIOKafkaConsumer = _FakeConsumer  # type: ignore[attr-defined]
    module.AIOKafkaProducer = _FakeProducer  # type: ignore[attr-defined]
    module.TopicPartition = _FakeTopicPartition  # type: ignore[attr-defined]
    module.ConsumerRebalanceListener = object  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "aiokafka", module)

    config = config_from_args(_args())  # type: ignore[arg-type]
    service = build_service(config, TOOLS)

    assert isinstance(service, EffectorService)
    assert isinstance(service._source, KafkaIntentSource)
    assert isinstance(service._dedup, InMemoryDedupStore)


def _fake_aiokafka(monkeypatch: pytest.MonkeyPatch) -> None:
    module = types.ModuleType("aiokafka")
    module.AIOKafkaConsumer = _FakeConsumer  # type: ignore[attr-defined]
    module.AIOKafkaProducer = _FakeProducer  # type: ignore[attr-defined]
    module.TopicPartition = _FakeTopicPartition  # type: ignore[attr-defined]
    module.ConsumerRebalanceListener = object  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "aiokafka", module)


# --- effector-security: the CLI's half of the verification wiring ------------


def test_verification_settings_reach_the_config_from_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TEST_MAIN_KEYS", f"k1={base64.b64encode(b'k' * 32).decode()}")
    args = _args(
        verify_intents="require",
        signing_keys="env:TEST_MAIN_KEYS",
        dead_letters_to="kafka://localhost:9092/dead-letters",
    )

    config = config_from_args(args)  # type: ignore[arg-type]

    assert config.verify_intents == "require"
    assert config.signing_keys == "env:TEST_MAIN_KEYS"
    assert config.dead_letters_to == "kafka://localhost:9092/dead-letters"


def test_the_service_builder_loads_the_keyring_and_the_dead_letter_sink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_aiokafka(monkeypatch)
    monkeypatch.setenv("TEST_MAIN_KEYS", f"k1={base64.b64encode(b'k' * 32).decode()}")
    config = config_from_args(
        _args(
            verify_intents="require",
            signing_keys="env:TEST_MAIN_KEYS",
            dead_letters_to="kafka://localhost:9092/dead-letters",
        )  # type: ignore[arg-type]
    )

    service = build_service(config, TOOLS)

    assert service._keyring == {"k1": b"k" * 32}
    assert isinstance(service._dead_letter_sink, KafkaMessageSink)


def test_a_verifying_mode_with_an_unresolvable_keyring_fails_before_any_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Scenario: A verifying mode without a keyring fails at startup — the
    # *unresolvable* half. Reference syntax is fine; the secret was never
    # materialized, which is a mis-provisioned deployment, not a typo.
    monkeypatch.delenv("TEST_MAIN_MISSING_KEYS", raising=False)
    config = config_from_args(
        _args(verify_intents="permissive", signing_keys="env:TEST_MAIN_MISSING_KEYS")  # type: ignore[arg-type]
    )
    # aiokafka is deliberately NOT installed here: if the keyring were loaded
    # after client construction this would raise ImportError instead.
    with pytest.raises(ValueError, match="TEST_MAIN_MISSING_KEYS"):
        build_service(config, TOOLS)


def test_off_mode_builds_no_keyring_and_no_dead_letter_sink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_aiokafka(monkeypatch)

    service = build_service(config_from_args(_args()), TOOLS)  # type: ignore[arg-type]

    assert service._keyring == {}
    assert service._dead_letter_sink is None


def test_transport_security_is_absent_unless_a_flag_asks_for_it() -> None:
    # An unconfigured deployment must construct exactly the clients it did
    # before this change, so "no flags" has to mean `None`, not an empty block.
    assert transport_security_from_args(build_parser().parse_args([])) is None


def test_transport_security_flags_become_a_validated_block() -> None:
    args = build_parser().parse_args(
        [
            "--kafka-security-protocol",
            "SASL_SSL",
            "--kafka-sasl-mechanism",
            "SCRAM-SHA-512",
            "--kafka-sasl-username-reference",
            "env:KAFKA_USER",
            "--kafka-sasl-password-reference",
            "env:KAFKA_PASSWORD",
        ]
    )

    security = transport_security_from_args(args)

    assert security is not None
    assert security.security_protocol == "SASL_SSL"
    assert security.sasl_password_reference == "env:KAFKA_PASSWORD"


def test_main_runs_the_service_to_completion(monkeypatch: pytest.MonkeyPatch) -> None:
    # The happy path returns 0 after the service finishes, so a supervisor sees
    # a clean exit rather than a crash loop.
    served: list[object] = []

    async def _fake_serve(service: object) -> None:
        served.append(service)

    monkeypatch.setattr(
        "beam_agents.effector.__main__.build_service", lambda config, registry: "svc"
    )
    monkeypatch.setattr("beam_agents.effector.__main__.serve", _fake_serve)

    code = main(
        [
            "--registry",
            "tests.effector.test_main:TOOLS",
            "--intents-from",
            "kafka://localhost:9092/intents",
            "--results-to",
            "kafka://localhost:9092/results",
            "--approvals-to",
            "kafka://localhost:9092/approvals",
        ]
    )

    assert code == 0
    assert served == ["svc"]


def test_main_constructs_the_service_inside_the_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Regression (found by the effectively-once e2e gate): the Kafka adapters
    # construct aiokafka clients in __init__, and aiokafka requires a running
    # loop at construction — so `main` must call `build_service` from inside
    # the loop it starts. Built outside, the CLI crashes at startup for every
    # real (non-memory://) transport, which no memory://-based test can see.
    built_inside_loop: list[bool] = []

    def _probing_build(config: object, registry: object) -> str:
        asyncio.get_running_loop()  # raises RuntimeError when no loop runs
        built_inside_loop.append(True)
        return "svc"

    async def _fake_serve(service: object) -> None:
        assert service == "svc"

    monkeypatch.setattr("beam_agents.effector.__main__.build_service", _probing_build)
    monkeypatch.setattr("beam_agents.effector.__main__.serve", _fake_serve)

    code = main(
        [
            "--registry",
            "tests.effector.test_main:TOOLS",
            "--intents-from",
            "kafka://localhost:9092/intents",
            "--results-to",
            "kafka://localhost:9092/results",
            "--approvals-to",
            "kafka://localhost:9092/approvals",
        ]
    )

    assert code == 0
    assert built_inside_loop == [True]
