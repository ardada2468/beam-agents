"""Spec: adk-adapter / Requirement: Session state commits atomically with the
bundle.

Scenarios: Failed activation leaves no partial session; Worker failover resumes
from the committed session; One session per key.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("google.adk")

from google.adk.events.event import Event
from google.adk.events.event_actions import EventActions
from google.adk.sessions.base_session_service import GetSessionConfig
from google.genai import types

from beam_agents._protos import MemoryBlob, ToolResult
from beam_agents.adapters.adk.session import RESERVED_NAMESPACE, BeamSessionService
from beam_agents.core.agent import Complete, Suspend
from beam_agents.memory.facade import HARD_CAP_BYTES, Memory, MemoryOverflow
from tests.adapters._helpers import ENTITY_KEY, NOW_MS, make_ctx
from tests.adapters.adk._helpers import call_turn, scripted_adk_agent, text_turn
from tests.conformance._spec import charge

_APP = "beam_agents"
_KEY_HEX = ENTITY_KEY.hex()


def _service(blob: MemoryBlob | None = None) -> tuple[BeamSessionService, Memory]:
    memory = Memory(blob, now_ms=NOW_MS)
    return BeamSessionService(memory, now_ms=NOW_MS), memory


def _texts(events: list[Event]) -> list[str]:
    """The first text part of each event (the shape these fixtures build)."""
    out: list[str] = []
    for event in events:
        assert event.content is not None and event.content.parts
        out.append(event.content.parts[0].text or "")
    return out


def _event(text: str, state_delta: dict[str, object] | None = None) -> Event:
    return Event(
        author="probe",
        invocation_id="inv-1",
        content=types.Content(role="model", parts=[types.Part(text=text)]),
        actions=EventActions(state_delta=dict(state_delta or {})),
    )


async def test_session_round_trips_through_the_reserved_namespace() -> None:
    service, memory = _service()
    session = await service.create_session(app_name=_APP, user_id=_KEY_HEX, session_id=_KEY_HEX)
    await service.append_event(session, _event("first", {"seen": 1}))
    await service.append_event(session, _event("second", {"seen": 2}))

    # Every stored key lives under the reserved prefix.
    stored = [entry.key for entry in memory.to_blob().entries]
    assert stored == [RESERVED_NAMESPACE + "session"]

    # A service rebuilt on a fresh facade over the committed blob sees it all.
    reloaded, _ = _service(memory.to_blob())
    loaded = await reloaded.get_session(app_name=_APP, user_id=_KEY_HEX, session_id=_KEY_HEX)
    assert loaded is not None
    assert _texts(loaded.events) == ["first", "second"]
    # state_delta application is the base contract; persistence carries it.
    assert loaded.state == {"seen": 2}


async def test_partial_events_are_not_persisted() -> None:
    service, memory = _service()
    session = await service.create_session(app_name=_APP, user_id=_KEY_HEX, session_id=_KEY_HEX)
    partial = _event("streaming")
    partial.partial = True
    await service.append_event(session, partial)

    reloaded, _ = _service(memory.to_blob())
    loaded = await reloaded.get_session(app_name=_APP, user_id=_KEY_HEX, session_id=_KEY_HEX)
    assert loaded is not None
    assert loaded.events == []


async def test_create_session_is_idempotent_for_the_per_key_identity() -> None:
    service, memory = _service()
    session = await service.create_session(app_name=_APP, user_id=_KEY_HEX, session_id=_KEY_HEX)
    await service.append_event(session, _event("kept"))

    # A second create for the same identity must not wipe the history: the
    # Runner calls get-then-create, and a lost race would erase the session.
    again, _ = _service(memory.to_blob())
    recreated = await again.create_session(app_name=_APP, user_id=_KEY_HEX, session_id=_KEY_HEX)
    assert _texts(recreated.events) == ["kept"]


async def test_one_session_per_key() -> None:
    # Scenario: One session per key — listing for the key's identity returns at
    # most one session, holding the accumulated committed state and events.
    service, memory = _service()
    session = await service.create_session(app_name=_APP, user_id=_KEY_HEX, session_id=_KEY_HEX)
    await service.append_event(session, _event("a", {"n": 1}))
    await service.append_event(session, _event("b", {"n": 2}))

    reloaded, _ = _service(memory.to_blob())
    listing = await reloaded.list_sessions(app_name=_APP, user_id=_KEY_HEX)
    assert len(listing.sessions) == 1
    only = listing.sessions[0]
    assert only.state == {"n": 2}
    assert len(only.events) == 2

    # A different user id has no session here.
    other = await reloaded.list_sessions(app_name=_APP, user_id="someone-else")
    assert other.sessions == []


async def test_get_session_config_filters_recent_events() -> None:
    service, memory = _service()
    session = await service.create_session(app_name=_APP, user_id=_KEY_HEX, session_id=_KEY_HEX)
    for text in ("a", "b", "c"):
        await service.append_event(session, _event(text))

    reloaded, _ = _service(memory.to_blob())
    trimmed = await reloaded.get_session(
        app_name=_APP,
        user_id=_KEY_HEX,
        session_id=_KEY_HEX,
        config=GetSessionConfig(num_recent_events=2),
    )
    assert trimmed is not None
    assert _texts(trimmed.events) == ["b", "c"]


async def test_max_events_trims_the_oldest_on_append() -> None:
    memory = Memory(None, now_ms=NOW_MS)
    service = BeamSessionService(memory, now_ms=NOW_MS, max_events=2)
    session = await service.create_session(app_name=_APP, user_id=_KEY_HEX, session_id=_KEY_HEX)
    for text in ("a", "b", "c"):
        await service.append_event(session, _event(text))

    reloaded, _ = _service(memory.to_blob())
    loaded = await reloaded.get_session(app_name=_APP, user_id=_KEY_HEX, session_id=_KEY_HEX)
    assert loaded is not None
    assert _texts(loaded.events) == ["b", "c"]


async def test_delete_session_clears_the_reserved_key() -> None:
    service, memory = _service()
    await service.create_session(app_name=_APP, user_id=_KEY_HEX, session_id=_KEY_HEX)
    await service.delete_session(app_name=_APP, user_id=_KEY_HEX, session_id=_KEY_HEX)

    assert memory.get(RESERVED_NAMESPACE + "session") is None
    assert await service.get_session(app_name=_APP, user_id=_KEY_HEX, session_id=_KEY_HEX) is None


async def test_oversized_session_fails_closed_via_the_memory_cap() -> None:
    # An oversized session raises MemoryOverflow, failing the activation closed
    # (routed to .errors by the DoFn) rather than committing partial state.
    service, _memory = _service()
    session = await service.create_session(app_name=_APP, user_id=_KEY_HEX, session_id=_KEY_HEX)
    with pytest.raises(MemoryOverflow):
        await service.append_event(session, _event("x" * (HARD_CAP_BYTES + 1)))


async def test_failed_activation_leaves_no_partial_session() -> None:
    # Scenario: Failed activation leaves no partial session — the activation
    # raises after the run appended events, and the committed MemoryBlob is
    # byte-identical to its pre-activation state.
    agent, _model = scripted_adk_agent(
        [call_turn(("charge", {"amount": "5"})), text_turn("unreached")], [charge]
    )
    first_ctx = make_ctx(event=b"go", seq=0)
    suspended = await agent(first_ctx)
    assert isinstance(suspended, Suspend)
    committed = first_ctx.memory_blob()
    committed_bytes = committed.SerializeToString(deterministic=True)

    # The resume fails closed on an unknown intent, after the service has
    # already been constructed over the staged facade.
    failing_ctx = make_ctx(
        seq=0,
        memory_blob=MemoryBlob.FromString(committed_bytes),
        snapshot=suspended.snapshot,
        resume_result=ToolResult(intent_id="unknown", entity_key=ENTITY_KEY),
        step_index=first_ctx.step_index,
    )
    with pytest.raises(ValueError):
        await agent(failing_ctx)

    # The DoFn discards a failed activation's staged effects: the durable blob
    # is the one committed before it, unchanged.
    assert committed.SerializeToString(deterministic=True) == committed_bytes


async def test_worker_failover_resumes_from_the_committed_session() -> None:
    # Scenario: Worker failover resumes from the committed session — the
    # resuming element is processed by a FRESH adapter instance over state
    # round-tripped through serialized protobuf bytes alone.
    script = [call_turn(("charge", {"amount": "5"})), text_turn("done-failover")]
    first_agent, _ = scripted_adk_agent(script, [charge])
    first_ctx = make_ctx(event=b"go", seq=0)
    suspended = await first_agent(first_ctx)
    assert isinstance(suspended, Suspend)
    intent_id = first_ctx.staged_intents[0].intent_id

    # Nothing object-identical survives: fresh agent, fresh model, blob bytes.
    failover_agent, _ = scripted_adk_agent(script, [charge])
    resume_ctx = make_ctx(
        seq=0,
        memory_blob=MemoryBlob.FromString(
            first_ctx.memory_blob().SerializeToString(deterministic=True)
        ),
        snapshot=suspended.snapshot,
        resume_result=ToolResult(
            intent_id=intent_id, entity_key=ENTITY_KEY, payload=b"ack", status=ToolResult.OK
        ),
        step_index=first_ctx.step_index,
    )
    resumed = await failover_agent(resume_ctx)

    assert isinstance(resumed, Complete)
    assert resumed.output == b"done-failover"
    # The full history came from the committed session, not from the run.
    session = json.loads(resume_ctx.memory.get(RESERVED_NAMESPACE + "session") or b"{}")
    assert len(session["events"]) >= 4
