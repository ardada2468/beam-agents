"""The Slack approval surface from `docs/examples/slack-approval.md`, exercised.

Doc-contract tests for `examples/slack_approval`: the offline leg drives the
whole consume -> post -> decide -> publish loop with the effector's in-memory
source/sink fakes and the scripted `FakeSlackGateway` — no docker, no Slack
workspace, no `slack-sdk`. The compose-Kafka closed loop lives in
`test_slack_approval_kafka.py` (`-m integration`).

The intent under test is minted by the same deterministic derivation the
in-pipeline activation uses (`intent_id_for(entity_key, seq, step_index)`), and
`test_the_demo_activation_stages_exactly_the_intent_the_surface_consumes`
proves the hand-built copy is byte-identical to what the FakeLLM-driven demo
activation actually stages. Every other test then feeds that same intent to the
surface, so the envelope the surface publishes matches the pending intent the
suspended activation holds — which is what lets the resume tests below close
the loop in one scripted pipeline run.
"""

from __future__ import annotations

import ast
import asyncio
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.transforms.window import TimestampedValue

import beam_agents
from beam_agents._protos import AgentEnvelope, ToolIntent
from beam_agents.core.agent import intent_id_for
from beam_agents.core.transform import RunAgent
from beam_agents.effector.sinks import InMemoryMessageSink
from beam_agents.effector.sources import InMemoryIntentSource
from beam_agents.observability import trace_id_for
from examples.slack_approval import blocks
from examples.slack_approval.agent import (
    APPROVED_OUTPUT,
    DEMO_TTL_MS,
    DENIED_OUTPUT,
    demo_args_json,
    demo_config,
    refund_agent,
)
from examples.slack_approval.slack import FakeSlackGateway
from examples.slack_approval.surface import ApprovalSurface
from tests.core._dofn_helpers import keyed

_ENTITY_KEY = b"customer-7"
_SEQ = 0
_ORDER = "order-42"
_T0_MS = 1_000
# The demo agent's approval request is its first (only) staged step.
_INTENT_ID = intent_id_for(_ENTITY_KEY, _SEQ, 0)
_EXPIRES_AT_MS = _T0_MS + DEMO_TTL_MS
_CHANNEL = "#approvals"

_REPO_ROOT = Path(__file__).resolve().parents[2]
_EXAMPLE_DIR = _REPO_ROOT / "examples" / "slack_approval"
# The demo pipeline wiring is the one module allowed to import Beam; __main__
# only wires transports, but it is not part of the surface loop either.
_SERVICE_MODULES = ("__init__.py", "blocks.py", "config.py", "slack.py", "surface.py")


def staged_intent() -> ToolIntent:
    """The approval intent the demo activation stages for `_ORDER` at `_T0_MS`.

    Hand-built from the same deterministic derivation the runtime uses;
    `test_the_demo_activation_stages_exactly_the_intent_the_surface_consumes`
    holds it byte-identical to the pipeline-staged original.
    """
    return ToolIntent(
        intent_id=_INTENT_ID,
        entity_key=_ENTITY_KEY,
        seq=_SEQ,
        step_index=0,
        tool_name="approval",
        args_json=demo_args_json(_ORDER),
        created_at_ms=_T0_MS,
        expires_at_ms=_EXPIRES_AT_MS,
        attempt=0,
        kind=ToolIntent.APPROVAL,
        trace_id=trace_id_for(_ENTITY_KEY, _SEQ),
    )


class _Clock:
    """Injectable clock: the tests advance `now_ms`; nothing ever sleeps."""

    def __init__(self, now_ms: int) -> None:
        self.now_ms = now_ms

    def __call__(self) -> int:
        return self.now_ms


def _surface(
    source: InMemoryIntentSource,
    gateway: FakeSlackGateway,
    sink: InMemoryMessageSink,
    clock: _Clock,
) -> ApprovalSurface:
    return ApprovalSurface(
        source=source,
        sink=sink,
        gateway=gateway,
        channel=_CHANNEL,
        time_fn=clock,
    )


def _loop_parts() -> tuple[InMemoryIntentSource, FakeSlackGateway, InMemoryMessageSink, _Clock]:
    return (
        InMemoryIntentSource.of([staged_intent()]),
        FakeSlackGateway(),
        InMemoryMessageSink(),
        _Clock(_T0_MS + 1),
    )


def _expected_envelope_bytes(*, approved: bool, approver: str, decided_at_ms: int) -> bytes:
    envelope = AgentEnvelope(entity_key=_ENTITY_KEY, event_time_ms=decided_at_ms)
    envelope.approval.intent_id = _INTENT_ID
    envelope.approval.approved = approved
    envelope.approval.approver = approver
    envelope.approval.decided_at_ms = decided_at_ms
    return envelope.SerializeToString(deterministic=True)


def _has_actions(rendered: list[blocks.Block]) -> bool:
    return any(block.get("type") == "actions" for block in rendered)


# -- Requirement: each approval-kind intent becomes exactly one interactive message


def _check_staged_intent(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert len(items) == 1, f"expected exactly one staged intent, got {items!r}"
    staged = items[0].SerializeToString(deterministic=True)
    expected = staged_intent().SerializeToString(deterministic=True)
    assert staged == expected, "the hand-built intent drifted from the runtime's derivation"


def _streaming_pipeline() -> BeamTestPipeline:
    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    return BeamTestPipeline(options=options)


def _event(t_ms: int) -> TimestampedValue[AgentEnvelope]:
    env = AgentEnvelope(entity_key=_ENTITY_KEY, event_time_ms=t_ms, external_event=_ORDER.encode())
    return TimestampedValue(env, t_ms / 1000)


def test_the_demo_activation_stages_exactly_the_intent_the_surface_consumes() -> None:
    # Scenario: An approval intent becomes a Block Kit message and is then
    # committed — the "staged by a FakeLLM-driven activation's request_approval"
    # half: the intent every offline test feeds the surface is byte-identical to
    # the one the demo pipeline commits.
    with _streaming_pipeline() as p:
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([_event(_T0_MS)])
            .advance_watermark_to_infinity()
        )
        out = keyed(p | stream) | RunAgent(refund_agent, config=demo_config())
        assert_that(out.intents, _check_staged_intent, label="intents")


async def test_an_approval_intent_becomes_a_block_kit_message_and_is_then_committed() -> None:
    # Scenario: An approval intent becomes a Block Kit message and is then
    # committed.
    source, gateway, sink, clock = _loop_parts()
    surface = _surface(source, gateway, sink, clock)

    await surface.consume()

    assert len(gateway.posts) == 1
    posted = gateway.posts[0]
    assert posted.channel == _CHANNEL
    values = blocks.interactive_action_values(posted.blocks)
    assert len(values) == 2, "expected an approve and a deny action"
    for raw in values:
        value = blocks.decode_action_value(raw)
        assert value.intent_id == _INTENT_ID
        assert value.entity_key_hex == _ENTITY_KEY.hex()
        assert value.expires_at_ms == _EXPIRES_AT_MS
    assert source.committed_intent_ids == [_INTENT_ID]


async def test_a_crash_before_posting_loses_nothing() -> None:
    # Scenario: A crash before posting loses nothing.
    source, gateway, sink, clock = _loop_parts()
    gateway.fail_post = RuntimeError("slack is down")
    surface = _surface(source, gateway, sink, clock)

    with pytest.raises(RuntimeError, match="slack is down"):
        await surface.consume()

    assert source.committed == [], "a failed post must not commit the delivery"
    assert gateway.posts == []
    assert sink.published == []


async def test_non_approval_intents_are_skipped() -> None:
    # Scenario: Non-approval intents are skipped.
    tool_intent = staged_intent()
    tool_intent.intent_id = "tool-1"
    tool_intent.kind = ToolIntent.TOOL
    unspecified = staged_intent()
    unspecified.intent_id = "unspecified-1"
    unspecified.kind = ToolIntent.TOOL_KIND_UNSPECIFIED

    source = InMemoryIntentSource.of([tool_intent, unspecified])
    gateway, sink, clock = FakeSlackGateway(), InMemoryMessageSink(), _Clock(_T0_MS + 1)
    surface = _surface(source, gateway, sink, clock)

    await surface.consume()

    assert gateway.posts == []
    assert sink.published == []
    assert source.committed_intent_ids == ["tool-1", "unspecified-1"]


async def test_a_redelivered_intent_does_not_double_post_within_a_process() -> None:
    # Scenario: A redelivered intent does not double-post within a process.
    intent = staged_intent()
    source = InMemoryIntentSource.of([intent, intent])
    gateway, sink, clock = FakeSlackGateway(), InMemoryMessageSink(), _Clock(_T0_MS + 1)
    surface = _surface(source, gateway, sink, clock)

    await surface.consume()

    assert len(gateway.posts) == 1, "the duplicate delivery must not post again"
    assert source.committed_intent_ids == [_INTENT_ID, _INTENT_ID]


# -- Requirement: an approve decision publishes an Approval envelope ------------


async def test_approve_click_publishes_a_keyed_approval_envelope() -> None:
    # Scenario: Approve click publishes a keyed approval envelope.
    source, gateway, sink, clock = _loop_parts()
    surface = _surface(source, gateway, sink, clock)
    await surface.consume()

    decided_at_ms = _T0_MS + 5_000
    decision = gateway.click(approved=True, approver="U-ALICE", decided_at_ms=decided_at_ms)
    await surface.handle_decision(decision)

    expected = _expected_envelope_bytes(
        approved=True, approver="U-ALICE", decided_at_ms=decided_at_ms
    )
    assert sink.published == [(_ENTITY_KEY, expected)]
    assert len(gateway.edits) == 1
    edit = gateway.edits[0]
    assert edit.ref == gateway.posts[0].ref
    assert not _has_actions(edit.blocks), "the verdict edit must remove the buttons"


async def test_a_second_click_on_a_decided_intent_publishes_nothing() -> None:
    # Scenario: A second click on a decided intent publishes nothing.
    source, gateway, sink, clock = _loop_parts()
    surface = _surface(source, gateway, sink, clock)
    await surface.consume()

    first = gateway.click(approved=True, approver="U-ALICE", decided_at_ms=_T0_MS + 5_000)
    await surface.handle_decision(first)
    second = gateway.click(approved=False, approver="U-BOB", decided_at_ms=_T0_MS + 6_000)
    await surface.handle_decision(second)

    assert len(sink.published) == 1, "the second click must not publish a second envelope"
    assert len(gateway.answered) == 1
    answered_decision, answer_text = gateway.answered[0]
    assert answered_decision.approver == "U-BOB"
    assert "decided" in answer_text


# -- Requirement: a deny decision publishes a fail-closed denial ----------------


async def test_deny_click_publishes_approved_false() -> None:
    # Scenario: Deny click publishes approved=false and the agent takes the
    # denied path — the envelope half; the resume half is below.
    source, gateway, sink, clock = _loop_parts()
    surface = _surface(source, gateway, sink, clock)
    await surface.consume()

    decision = gateway.click(approved=False, approver="U-BOB", decided_at_ms=_T0_MS + 5_000)
    await surface.handle_decision(decision)

    expected = _expected_envelope_bytes(
        approved=False, approver="U-BOB", decided_at_ms=_T0_MS + 5_000
    )
    assert sink.published == [(_ENTITY_KEY, expected)]


# -- Scenario: the published envelope resumes the suspended activation ----------


def _check_output_approved(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert items == [APPROVED_OUTPUT], f"unexpected output: {items!r}"


def _check_output_denied(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert items == [DENIED_OUTPUT], f"unexpected output: {items!r}"


def _check_no_errors(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert items == [], f"unexpected errors: {items!r}"


async def _surface_published_envelope(*, approved: bool) -> tuple[bytes, bytes]:
    """Run the offline surface loop; return the (key, payload) it published."""
    source, gateway, sink, clock = _loop_parts()
    surface = _surface(source, gateway, sink, clock)
    await surface.consume()
    decision = gateway.click(approved=approved, approver="U-ALICE", decided_at_ms=_T0_MS + 5_000)
    await surface.handle_decision(decision)
    assert len(sink.published) == 1
    return sink.published[0]


def _resume_pipeline_with(key: bytes, payload: bytes, check: object) -> None:
    envelope = AgentEnvelope()
    envelope.ParseFromString(payload)
    assert envelope.entity_key == key
    with _streaming_pipeline() as p:
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([_event(_T0_MS)])
            .add_elements([TimestampedValue(envelope, envelope.event_time_ms / 1000)])
            .advance_watermark_to_infinity()
        )
        out = keyed(p | stream) | RunAgent(refund_agent, config=demo_config())
        assert_that(out.output, check, label="output")
        assert_that(out.errors, _check_no_errors, label="errors")


async def test_the_published_envelope_resumes_the_suspended_activation() -> None:
    # Scenario: The published envelope resumes the suspended activation.
    key, payload = await _surface_published_envelope(approved=True)
    _resume_pipeline_with(key, payload, _check_output_approved)


async def test_the_denied_envelope_resumes_the_agent_onto_its_denied_path() -> None:
    # Scenario: Deny click publishes approved=false and the agent takes the
    # denied path — the resume half.
    key, payload = await _surface_published_envelope(approved=False)
    _resume_pipeline_with(key, payload, _check_output_denied)


# -- Requirement: an expired intent is never actionable at the surface ----------


async def test_an_already_expired_intent_is_surfaced_without_buttons() -> None:
    # Scenario: An already-expired intent is surfaced without buttons.
    source = InMemoryIntentSource.of([staged_intent()])
    gateway, sink = FakeSlackGateway(), InMemoryMessageSink()
    clock = _Clock(_EXPIRES_AT_MS)  # the boundary is inclusive: at-expiry is expired
    surface = _surface(source, gateway, sink, clock)

    await surface.consume()

    assert len(gateway.posts) == 1
    assert not _has_actions(gateway.posts[0].blocks)
    assert source.committed_intent_ids == [_INTENT_ID]
    assert sink.published == []


async def test_a_non_positive_expiry_reads_as_expired_never_unbounded() -> None:
    # Scenario: An already-expired intent is surfaced without buttons —
    # including a non-positive `expires_at_ms`.
    unbounded = staged_intent()
    unbounded.expires_at_ms = 0
    source = InMemoryIntentSource.of([unbounded])
    gateway, sink, clock = FakeSlackGateway(), InMemoryMessageSink(), _Clock(_T0_MS)
    surface = _surface(source, gateway, sink, clock)

    await surface.consume()

    assert len(gateway.posts) == 1
    assert not _has_actions(gateway.posts[0].blocks)
    assert source.committed_intent_ids == [_INTENT_ID]


async def test_expiry_while_pending_removes_the_buttons() -> None:
    # Scenario: Expiry while pending removes the buttons.
    source, gateway, sink, clock = _loop_parts()
    surface = _surface(source, gateway, sink, clock)
    await surface.consume()
    assert _has_actions(gateway.posts[0].blocks)

    clock.now_ms = _EXPIRES_AT_MS
    await surface.sweep_once()

    assert len(gateway.edits) == 1
    edit = gateway.edits[0]
    assert edit.ref == gateway.posts[0].ref
    assert not _has_actions(edit.blocks)
    assert sink.published == []

    # The sweep is idempotent: a second pass has nothing left to edit.
    await surface.sweep_once()
    assert len(gateway.edits) == 1


async def test_a_click_racing_expiry_publishes_nothing() -> None:
    # Scenario: A click racing expiry publishes nothing.
    source, gateway, sink, clock = _loop_parts()
    surface = _surface(source, gateway, sink, clock)
    await surface.consume()

    clock.now_ms = _EXPIRES_AT_MS
    decision = gateway.click(approved=True, approver="U-ALICE", decided_at_ms=_EXPIRES_AT_MS)
    await surface.handle_decision(decision)

    assert sink.published == []
    assert len(gateway.edits) == 1
    assert not _has_actions(gateway.edits[0].blocks)


async def test_a_click_after_a_surface_restart_is_still_checked_for_expiry() -> None:
    # Scenario: A click racing expiry publishes nothing — including one arriving
    # after a surface restart emptied the pending map. The check rides on the
    # `expires_at_ms` carried in the action value, not on in-process state.
    source, gateway, sink, clock = _loop_parts()
    first = _surface(source, gateway, sink, clock)
    await first.consume()
    decision = gateway.click(approved=True, approver="U-ALICE", decided_at_ms=_EXPIRES_AT_MS)

    clock.now_ms = _EXPIRES_AT_MS
    restarted = _surface(InMemoryIntentSource.of([]), gateway, sink, clock)
    await restarted.handle_decision(decision)

    assert sink.published == []
    assert len(gateway.edits) == 1
    assert not _has_actions(gateway.edits[0].blocks)


# -- Requirement: gateway seam + fully-offline testability ----------------------


async def test_the_offline_loop_runs_with_fakes_only() -> None:
    # Scenario: The offline loop runs with fakes only — the full `run()` loop
    # (consume + decisions + sweep) end to end against the in-memory fakes.
    source, gateway, sink, clock = _loop_parts()
    surface = _surface(source, gateway, sink, clock)

    async def _eventually(condition: object) -> None:
        deadline = asyncio.get_running_loop().time() + 10.0
        while not condition():  # type: ignore[operator]
            assert asyncio.get_running_loop().time() < deadline, "timed out waiting"
            await asyncio.sleep(0.01)

    run_task = asyncio.create_task(surface.run())
    try:
        await _eventually(lambda: gateway.posts)
        gateway.push(gateway.click(approved=True, approver="U-ALICE", decided_at_ms=_T0_MS + 5_000))
        await _eventually(lambda: sink.published)
    finally:
        surface.stop()
        await run_task

    expected = _expected_envelope_bytes(
        approved=True, approver="U-ALICE", decided_at_ms=_T0_MS + 5_000
    )
    assert sink.published == [(_ENTITY_KEY, expected)]
    assert source.closed and sink.closed and gateway.closed


def test_no_service_module_imports_beam_or_the_slack_sdk_eagerly() -> None:
    # Scenario: The service modules import without Beam — pinned statically so
    # the closure cannot regress even where an import is lazy or guarded.
    offenders: list[str] = []
    for name in _SERVICE_MODULES:
        path = _EXAMPLE_DIR / name
        for node in ast.walk(ast.parse(path.read_text())):
            names: list[str] = []
            if not isinstance(node, ast.Import | ast.ImportFrom):
                continue
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif node.module is not None and node.level == 0:
                names = [node.module]
            for imported in names:
                root = imported.split(".")[0]
                # Module-level (column 0) imports are eager; the lazy slack-sdk
                # import inside `SocketModeGateway.__init__` is the allowed form.
                if root in ("apache_beam", "slack_sdk") and node.col_offset == 0:
                    offenders.append(f"{name}: {imported}")
                if imported.startswith("beam_agents.core"):
                    offenders.append(f"{name}: {imported}")
    assert offenders == [], f"service modules must not import Beam or slack-sdk: {offenders}"


def test_the_service_modules_import_without_beam_or_slack_sdk() -> None:
    # Scenario: The service modules import with `apache_beam` (and `slack_sdk`)
    # blocked. Like the effector's own boundary test, the Beam-importing parent
    # `beam_agents/__init__.py` is stubbed: the surface imports only
    # `beam_agents.effector`/`beam_agents.hitl`/`beam_agents._protos`, which is
    # the standalone-deployment view of the package.
    src_dir = _REPO_ROOT / "src" / "beam_agents"
    preamble = textwrap.dedent(
        f"""
        import sys, types

        class _Blocker:
            def find_spec(self, fullname, path=None, target=None):
                for blocked in ("apache_beam", "slack_sdk"):
                    if fullname == blocked or fullname.startswith(blocked + "."):
                        raise ImportError(f"blocked by the boundary test: {{fullname}}")
                return None

        sys.meta_path.insert(0, _Blocker())
        sys.path.insert(0, {str(_REPO_ROOT)!r})

        _pkg = types.ModuleType("beam_agents")
        _pkg.__path__ = [{str(src_dir)!r}]
        sys.modules["beam_agents"] = _pkg
        """
    )
    body = textwrap.dedent(
        """
        import importlib

        for name in ("config", "blocks", "slack", "surface"):
            importlib.import_module(f"examples.slack_approval.{name}")

        from examples.slack_approval.config import SurfaceConfig
        from examples.slack_approval.slack import FakeSlackGateway

        SurfaceConfig(
            intents_from="kafka://localhost:9092/approval-requests",
            approvals_to="kafka://localhost:9092/approvals",
            slack_channel="#approvals",
        )
        FakeSlackGateway()

        assert "apache_beam" not in sys.modules, "a service module imported Beam"
        assert "slack_sdk" not in sys.modules, "a service module imported slack-sdk"
        assert "beam_agents.core" not in sys.modules, "a service module imported the runtime"
        print("OK")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", preamble + body],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_the_example_is_absent_from_the_public_api() -> None:
    # Scenario: the example is not importable from `beam_agents` — `__all__`
    # gains nothing from this change.
    assert not [name for name in beam_agents.__all__ if "slack" in name.lower()]
    assert not [name for name in beam_agents.__all__ if "approval" in name.lower()]


def test_an_oversized_action_value_raises_an_actionable_error() -> None:
    # Design risk "Button action value size": the composed value is bounded by
    # Slack's documented 2000-character button `value` limit, enforced at post
    # time with an error naming the limit.
    huge = staged_intent()
    huge.entity_key = b"k" * 2_000
    with pytest.raises(ValueError, match=str(blocks.MAX_ACTION_VALUE_CHARS)):
        blocks.action_value_for(huge)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
