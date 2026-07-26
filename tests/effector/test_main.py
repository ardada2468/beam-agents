"""The console entry point for the effector-service capability.

Covers the startup half of "Configuration is validated eagerly, before any
client is constructed": misconfiguration must fail at startup with an
actionable message rather than on the first message.
"""

from __future__ import annotations

import asyncio

import pytest

from beam_agents.effector.__main__ import build_parser, config_from_args, load_registry, main, serve
from beam_agents.effector.dedup import Claimed, InMemoryDedupStore
from beam_agents.effector.runner import EffectorToolRunner
from beam_agents.tools import ToolRegistry, tool

from ._fakes import NOW_MS, RecordingDedupStore, an_intent, build_harness

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

        async def run(self, t: object, arguments: object, *, on_invoke: object = None) -> object:
            await gate.wait()
            return await super().run(t, arguments, on_invoke=on_invoke)  # type: ignore[arg-type]

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
