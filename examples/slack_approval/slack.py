"""The Slack gateway seam: protocol, scripted fake, and the Socket Mode transport.

All Slack I/O goes through :class:`SlackGateway`, so every behavior of the
surface is testable offline with :class:`FakeSlackGateway` — no workspace, no
network, no slack-sdk installed. The real transport is **Socket Mode**
(:class:`SocketModeGateway`): interactions arrive over an app-token-authenticated
WebSocket, so the example carries no public HTTPS endpoint and no
request-signature verification code (see `docs/examples/slack-approval.md` for
what an HTTP-webhook deployment must add).

Socket Mode contract, verified against docs.slack.dev/apis/events-api/using-socket-mode
on 2026-07-30: every envelope must be acknowledged by echoing its `envelope_id`
"so that Slack knows whether to retry" — an unacknowledged interaction is
redelivered. The gateway therefore acks immediately on receipt, *before* any
`chat.update`; a redelivery surfaces as a duplicate `Decision`, which the
surface collapses through its decided set (first verdict wins, later clicks are
answered as already decided).

`slack-sdk` is imported lazily inside `SocketModeGateway.__init__` (the same
pattern as the effector's transport adapters), so importing this module — and
running the whole offline test suite — needs nothing installed.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from .blocks import (
    APPROVE_ACTION_ID,
    DENY_ACTION_ID,
    Block,
    decode_action_value,
    interactive_action_values,
)

_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class MessageRef:
    """One posted Slack message: what `chat.update` needs to find it again."""

    channel: str
    ts: str


@dataclass(frozen=True)
class Decision:
    """One operator interaction, parsed from a `block_actions` payload.

    `intent_id`, `entity_key_hex`, and `expires_at_ms` come from the button's
    action value — minted at post time, echoed back by Slack — so a decision is
    self-contained: it can be checked and answered even by a surface process
    that never posted the message. `decided_at_ms` is the interaction's own
    `action_ts`, so the envelope is a function of the click, not of when the
    surface got around to it.
    """

    intent_id: str
    entity_key_hex: str
    expires_at_ms: int
    approved: bool
    approver: str
    decided_at_ms: int
    message: MessageRef


@runtime_checkable
class SlackGateway(Protocol):
    """Everything the surface asks of Slack."""

    async def post(self, channel: str, *, text: str, blocks: list[Block]) -> MessageRef:
        """Post one message; returns the ref later edits address."""
        ...

    async def update(self, ref: MessageRef, *, text: str, blocks: list[Block]) -> None:
        """Replace a posted message's text and blocks."""
        ...

    def decisions(self) -> AsyncIterator[Decision]:
        """The stream of operator interactions; ends when the gateway closes."""
        ...

    async def answer(self, decision: Decision, text: str) -> None:
        """Tell the clicking user something without publishing (best-effort)."""
        ...

    async def close(self) -> None:
        """Stop delivering decisions and release the gateway's connections."""
        ...


@dataclass(frozen=True)
class PostedMessage:
    """One message this fake gateway posted."""

    ref: MessageRef
    channel: str
    text: str
    blocks: list[Block]


@dataclass(frozen=True)
class EditedMessage:
    """One in-place edit this fake gateway applied to a posted message."""

    ref: MessageRef
    text: str
    blocks: list[Block]


@dataclass
class FakeSlackGateway:
    """Scripted in-memory `SlackGateway`: records posts/edits, scripts clicks.

    `click(...)` builds the `Decision` a real interaction on a recorded post
    would produce — by parsing the posted blocks' action values exactly as
    `SocketModeGateway` parses a `block_actions` payload — so tests exercise
    the encode/decode round trip, not a hand-typed copy of it.
    """

    posts: list[PostedMessage] = field(default_factory=list)
    edits: list[EditedMessage] = field(default_factory=list)
    answered: list[tuple[Decision, str]] = field(default_factory=list)
    # Raised (once per post attempt) instead of posting, so commit-after-post is
    # an assertable property rather than a comment.
    fail_post: Exception | None = None
    closed: bool = field(default=False, init=False)
    _queue: asyncio.Queue[Decision | None] = field(default_factory=asyncio.Queue, init=False)

    async def post(self, channel: str, *, text: str, blocks: list[Block]) -> MessageRef:
        """Record the post and return a synthetic message ref; raise if scripted to."""
        if self.fail_post is not None:
            raise self.fail_post
        ref = MessageRef(channel=channel, ts=f"1700000000.{len(self.posts):06d}")
        self.posts.append(PostedMessage(ref=ref, channel=channel, text=text, blocks=blocks))
        return ref

    async def update(self, ref: MessageRef, *, text: str, blocks: list[Block]) -> None:
        """Record the edit."""
        self.edits.append(EditedMessage(ref=ref, text=text, blocks=blocks))

    async def decisions(self) -> AsyncIterator[Decision]:
        """Yield scripted decisions until :meth:`close` queues the sentinel."""
        while True:
            item = await self._queue.get()
            if item is None:
                return
            yield item

    async def answer(self, decision: Decision, text: str) -> None:
        """Record the ephemeral answer instead of sending one."""
        self.answered.append((decision, text))

    async def close(self) -> None:
        """End the decision stream and mark the gateway closed."""
        self.closed = True
        self._queue.put_nowait(None)

    # -- scripting -------------------------------------------------------------

    def click(
        self,
        *,
        approved: bool,
        approver: str,
        decided_at_ms: int,
        index: int = -1,
    ) -> Decision:
        """The `Decision` a click on the posted message at `index` produces."""
        posted = self.posts[index]
        values = interactive_action_values(posted.blocks)
        if not values:
            raise AssertionError(f"message {posted.ref!r} has no interactive actions to click")
        # Both buttons carry the same value; index 0 is approve, 1 is deny.
        value = decode_action_value(values[0 if approved else 1])
        return Decision(
            intent_id=value.intent_id,
            entity_key_hex=value.entity_key_hex,
            expires_at_ms=value.expires_at_ms,
            approved=approved,
            approver=approver,
            decided_at_ms=decided_at_ms,
            message=posted.ref,
        )

    def push(self, decision: Decision) -> None:
        """Deliver a decision to whoever is iterating `decisions()`."""
        self._queue.put_nowait(decision)


class SocketModeGateway:
    """`SlackGateway` over slack-sdk's Socket Mode client and async WebClient.

    Plain slack-sdk, not Bolt: an example should show the wiring. The two
    tokens are a bot token (`xoxb-`, `chat:write`) and a Socket Mode app-level
    token (`xapp-`, `connections:write`); see the app manifest in
    `docs/examples/slack-approval.md`.
    """

    def __init__(self, *, bot_token: str, app_token: str) -> None:
        if not bot_token or not app_token:
            raise ValueError(
                "SocketModeGateway needs both tokens: a bot token (SLACK_BOT_TOKEN, xoxb-...) "
                "and a Socket Mode app token (SLACK_APP_TOKEN, xapp-...)"
            )
        # Lazy: slack-sdk belongs to the `examples` dev surface and must not be
        # needed to import this module (mirrors the effector's adapters). The
        # aiohttp-backed Socket Mode client is the asyncio-native one.
        from slack_sdk.socket_mode.aiohttp import SocketModeClient
        from slack_sdk.web.async_client import AsyncWebClient

        self._web = AsyncWebClient(token=bot_token)
        self._socket = SocketModeClient(app_token=app_token, web_client=self._web)
        self._socket.socket_mode_request_listeners.append(self._on_request)
        self._queue: asyncio.Queue[Decision | None] = asyncio.Queue()
        self._connected = False

    async def post(self, channel: str, *, text: str, blocks: list[Block]) -> MessageRef:
        """Post the message via ``chat.postMessage`` and return its ref."""
        response = await self._web.chat_postMessage(channel=channel, text=text, blocks=blocks)
        return MessageRef(channel=str(response["channel"]), ts=str(response["ts"]))

    async def update(self, ref: MessageRef, *, text: str, blocks: list[Block]) -> None:
        """Edit the message in place via ``chat.update``."""
        await self._web.chat_update(channel=ref.channel, ts=ref.ts, text=text, blocks=blocks)

    async def decisions(self) -> AsyncIterator[Decision]:
        """Connect the socket if needed, then yield decisions as they arrive."""
        if not self._connected:
            await self._socket.connect()
            self._connected = True
        while True:
            item = await self._queue.get()
            if item is None:
                return
            yield item

    async def answer(self, decision: Decision, text: str) -> None:
        """Answer the clicking user with an ephemeral message.

        Best-effort by design: the message edit is the durable answer, so a
        failed ephemeral is logged, never raised.
        """
        # chat.postEphemeral needs a channel and a user; a `block_actions`
        # payload for a channel message carries both (`container.channel_id`,
        # `user.id` — docs.slack.dev/reference/interaction-payloads, 2026-07-30).
        # Best-effort by design: the message edit is the durable answer, so a
        # failed ephemeral is logged, never raised.
        try:
            await self._web.chat_postEphemeral(
                channel=decision.message.channel, user=decision.approver, text=text
            )
        except Exception:  # cosmetic path; the edit already happened
            _LOG.warning("could not send ephemeral reply for intent %s", decision.intent_id)

    async def _on_request(self, client: Any, request: Any) -> None:
        # Ack FIRST, by echoing the envelope_id: Slack redelivers unacked
        # envelopes, and everything after this line (parsing, the surface's
        # publish, the edit) can outlive Slack's patience. A redelivered
        # interaction becomes a duplicate Decision the surface collapses.
        from slack_sdk.socket_mode.response import SocketModeResponse

        await client.send_socket_mode_response(SocketModeResponse(envelope_id=request.envelope_id))
        if request.type != "interactive":
            return
        payload = request.payload
        if payload.get("type") != "block_actions":
            return
        container = payload.get("container", {})
        message = MessageRef(
            channel=str(container.get("channel_id", "")),
            ts=str(container.get("message_ts", "")),
        )
        approver = str(payload.get("user", {}).get("id", ""))
        for action in payload.get("actions", []):
            action_id = action.get("action_id")
            if action_id not in (APPROVE_ACTION_ID, DENY_ACTION_ID):
                continue
            try:
                value = decode_action_value(str(action.get("value", "")))
            except ValueError:
                _LOG.warning("ignoring interaction with malformed action value: %r", action)
                continue
            self._queue.put_nowait(
                Decision(
                    intent_id=value.intent_id,
                    entity_key_hex=value.entity_key_hex,
                    expires_at_ms=value.expires_at_ms,
                    approved=action_id == APPROVE_ACTION_ID,
                    approver=approver,
                    decided_at_ms=int(float(action.get("action_ts", "0")) * 1000),
                    message=message,
                )
            )

    async def close(self) -> None:
        """End the decision stream and close the socket connection."""
        self._queue.put_nowait(None)
        if self._connected:
            await self._socket.close()
            self._connected = False
