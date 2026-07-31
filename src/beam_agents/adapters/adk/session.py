"""`BeamSessionService`: the ADK session in the activation's working memory.

The direct parallel of the LangGraph adapter's ``BeamCheckpointSaver`` (change
design D2): the service holds the activation's staged
:class:`~beam_agents.memory.facade.Memory` facade and persists the per-key
session — its ``state`` dict plus its full event history, with each appended
event's ``state_delta`` already applied — as one canonical-JSON scalar under
the reserved ``__adk__/`` key namespace. Because the facade stages in memory
and the stateful DoFn commits the resulting ``MemoryBlob`` atomically with the
Beam bundle, session durability *is* bundle atomicity (correctness invariant
1): a failed or timed-out activation leaves no partial session, and a worker
failover reloads the committed blob and resumes from the last committed
session.

Retention is one session per key by design: ``app_name`` is an adapter
constant and ``user_id``/``session_id`` derive from the entity key, so
``list_sessions`` returns at most that one session (carrying its committed
state and events — a deliberate superset of the ADK base contract, which
allows listing lightweight sessions). Session size is bounded by the working
memory hard cap — an oversized session raises
:class:`~beam_agents.memory.facade.MemoryOverflow`, failing the activation
closed with no partial state. Keep histories small: the ``max_events`` knob
(off by default) drops the oldest events on append, and long conversations
should trim ADK-side well before the 1 MiB cap.

ADK's ``BaseSessionService`` ABC is async-first, and every method body here
touches only staged in-memory facade state — no network, no blocking — so
nothing ever blocks the bridge event loop.

Compactors must not evict ``__adk__/`` keys: they are load-bearing resume
state, not cache.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from google.adk.sessions.base_session_service import (
    BaseSessionService,
    GetSessionConfig,
    ListSessionsResponse,
)
from google.adk.sessions.session import Session
from typing_extensions import override

if TYPE_CHECKING:
    from google.adk.events.event import Event

    from beam_agents.memory.facade import Memory

__all__ = [
    "BeamSessionService",
]

# The reserved working-memory namespace for ADK state. The adapter owns every
# key under this prefix; nothing else may write here.
_RESERVED_NAMESPACE = "__adk__/"
_SESSION_KEY = _RESERVED_NAMESPACE + "session"

# Milliseconds per second: `Session.last_update_time` is float seconds, the
# activation clock is integer milliseconds.
_MS_PER_S = 1000.0


class BeamSessionService(BaseSessionService):
    """One-session-per-key persistence over one activation's working memory."""

    def __init__(self, memory: Memory, *, now_ms: int, max_events: int | None = None) -> None:
        self._memory = memory
        self._now_ms = now_ms
        self._max_events = max_events

    @override
    async def create_session(
        self,
        *,
        app_name: str,
        user_id: str,
        state: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> Session:
        resolved_id = session_id if session_id else user_id
        existing = self._load()
        if existing is not None and self._matches(existing, app_name, user_id, resolved_id):
            # Idempotent for the fixed per-key identity: the committed session
            # is the session; re-creating it must not wipe its history.
            return existing
        session = Session(
            id=resolved_id,
            app_name=app_name,
            user_id=user_id,
            state=dict(state) if state else {},
            last_update_time=self._now_ms / _MS_PER_S,
        )
        self._persist(session)
        return session

    @override
    async def get_session(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str,
        config: GetSessionConfig | None = None,
    ) -> Session | None:
        session = self._load()
        if session is None or not self._matches(session, app_name, user_id, session_id):
            return None
        if config is not None:
            if config.after_timestamp is not None:
                session.events = [
                    event for event in session.events if event.timestamp >= config.after_timestamp
                ]
            if config.num_recent_events is not None:
                session.events = (
                    session.events[-config.num_recent_events :]
                    if config.num_recent_events > 0
                    else []
                )
        return session

    @override
    async def list_sessions(
        self, *, app_name: str, user_id: str | None = None
    ) -> ListSessionsResponse:
        session = self._load()
        if (
            session is None
            or session.app_name != app_name
            or (user_id is not None and session.user_id != user_id)
        ):
            return ListSessionsResponse(sessions=[])
        return ListSessionsResponse(sessions=[session])

    @override
    async def delete_session(self, *, app_name: str, user_id: str, session_id: str) -> None:
        session = self._load()
        if session is not None and self._matches(session, app_name, user_id, session_id):
            self._memory.delete(_SESSION_KEY)

    @override
    async def append_event(self, session: Session, event: Event) -> Event:
        # The base implementation applies the event's `state_delta` to the
        # session state and appends the event (skipping partials); persistence
        # is this override's job.
        event = await super().append_event(session, event)
        if event.partial:
            return event
        if self._max_events is not None and len(session.events) > self._max_events:
            del session.events[: len(session.events) - self._max_events]
        session.last_update_time = self._now_ms / _MS_PER_S
        self._persist(session)
        return event

    # -- internals -------------------------------------------------------------

    def _matches(self, session: Session, app_name: str, user_id: str, session_id: str) -> bool:
        return (
            session.app_name == app_name and session.user_id == user_id and session.id == session_id
        )

    def _load(self) -> Session | None:
        raw = self._memory.get(_SESSION_KEY)
        if raw is None:
            return None
        return Session.model_validate(json.loads(raw))

    def _persist(self, session: Session) -> None:
        # `exclude_none`: ADK's Event/Content models carry dozens of optional
        # fields, and serializing their nulls costs ~4x the blob for zero
        # information (measured: 7035 -> 1865 bytes on a two-turn session) —
        # real headroom against the 1 MiB working-memory cap. It is lossless
        # here: pydantic's exclude_none drops only *model fields* whose value is
        # None (each re-defaulting to None on load), never entries inside
        # `state`, so a session-state key explicitly set to None survives the
        # round-trip.
        payload = session.model_dump(mode="json", exclude_none=True)
        self._memory.set(
            _SESSION_KEY,
            json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8"),
        )
