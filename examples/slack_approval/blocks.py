"""Block Kit rendering for the approval surface.

Every Slack-shaped payload lives here: the interactive approval request, the
non-interactive expired notice, and the decided/expired edits. The approve and
deny buttons carry a JSON action value with the `intent_id`, the hex-encoded
`entity_key`, and `expires_at_ms` — everything the decision path needs, so the
decision-time expiry check never depends on in-process state (a click on a
message posted by a previous process is still checked).

Slack limits, verified against docs.slack.dev on 2026-07-30:

- button `value`: max 2000 characters
  (reference/block-kit/block-elements/button-element)
- button `text`: max 75 characters; `action_id`: max 255 characters (same page)
- `blocks` per message: max 50 (reference/block-kit/blocks)

Importing this module has no side effects and needs neither Beam nor slack-sdk.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from beam_agents._protos import ToolIntent

# One rendered Block Kit block, as slack-sdk's WebClient accepts it.
Block = dict[str, object]

# Slack's documented bound on a button element's `value` (see module docstring).
MAX_ACTION_VALUE_CHARS = 2000

# Namespaced so a workspace app multiplexing listeners can route on the prefix;
# well under Slack's 255-character action_id bound.
APPROVE_ACTION_ID = "beam_agents.approve"
DENY_ACTION_ID = "beam_agents.deny"

_ACTION_IDS = (APPROVE_ACTION_ID, DENY_ACTION_ID)


@dataclass(frozen=True)
class ActionValue:
    """What a button's `value` carries: the decision path's whole context."""

    intent_id: str
    entity_key_hex: str
    expires_at_ms: int


def encode_action_value(value: ActionValue) -> str:
    """Serialize an `ActionValue` to the button `value` string.

    Raises `ValueError` when the composed value exceeds Slack's documented
    2000-character bound — at post time, with the offending intent named,
    rather than as an opaque Slack API error.
    """
    encoded = json.dumps(
        {
            "entity_key": value.entity_key_hex,
            "expires_at_ms": value.expires_at_ms,
            "intent_id": value.intent_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    if len(encoded) > MAX_ACTION_VALUE_CHARS:
        raise ValueError(
            f"action value for intent {value.intent_id!r} is {len(encoded)} characters, over "
            f"Slack's {MAX_ACTION_VALUE_CHARS}-character button `value` limit; the hex entity_key "
            f"({len(value.entity_key_hex)} characters) is the usual culprit — shorten the key or "
            "carry a reference to an external store instead"
        )
    return encoded


def decode_action_value(raw: str) -> ActionValue:
    """Parse a button `value` back into an `ActionValue`; `ValueError` if malformed."""
    try:
        decoded = json.loads(raw)
        return ActionValue(
            intent_id=str(decoded["intent_id"]),
            entity_key_hex=str(decoded["entity_key"]),
            expires_at_ms=int(decoded["expires_at_ms"]),
        )
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError(f"malformed action value {raw!r}: {exc}") from exc


def action_value_for(intent: ToolIntent) -> str:
    """The encoded action value both of an intent's buttons carry."""
    return encode_action_value(
        ActionValue(
            intent_id=intent.intent_id,
            entity_key_hex=intent.entity_key.hex(),
            expires_at_ms=intent.expires_at_ms,
        )
    )


def _summary_block(intent: ToolIntent, status_line: str) -> Block:
    """The request section: channel, arguments, expiry, and a status line."""
    text = (
        f"*Approval requested* on channel `{intent.tool_name}`\n"
        f"```{intent.args_json}```\n"
        f"intent `{intent.intent_id}` · expires at {intent.expires_at_ms} (ms epoch)\n"
        f"{status_line}"
    )
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _button(label: str, action_id: str, value: str, style: str | None) -> Block:
    button: Block = {
        "type": "button",
        "text": {"type": "plain_text", "text": label},  # <= 75 characters
        "action_id": action_id,
        "value": value,
    }
    if style is not None:
        button["style"] = style
    return button


def approval_message(intent: ToolIntent) -> tuple[str, list[Block]]:
    """The interactive request: `(fallback_text, blocks)` with approve/deny actions."""
    value = action_value_for(intent)
    blocks: list[Block] = [
        _summary_block(intent, "_Awaiting a decision._"),
        {
            "type": "actions",
            "elements": [
                _button("Approve", APPROVE_ACTION_ID, value, "primary"),
                _button("Deny", DENY_ACTION_ID, value, "danger"),
            ],
        },
    ]
    return f"Approval requested: intent {intent.intent_id}", blocks


def expired_notice(intent: ToolIntent) -> tuple[str, list[Block]]:
    """The non-interactive notice for an intent already expired at consume time.

    Posted rather than silently dropped, so the channel stays an honest log of
    what was requested.
    """
    blocks = [_summary_block(intent, ":hourglass: *Expired before it could be surfaced.*")]
    return f"Approval request expired: intent {intent.intent_id}", blocks


def decided_edit(intent_id: str, *, approved: bool, approver: str) -> tuple[str, list[Block]]:
    """The verdict edit: buttons removed, decision and approver shown."""
    verdict = ":white_check_mark: *Approved*" if approved else ":no_entry: *Denied*"
    blocks: list[Block] = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{verdict} by <@{approver}>\nintent `{intent_id}`",
            },
        }
    ]
    return f"Approval {'approved' if approved else 'denied'}: intent {intent_id}", blocks


def expired_edit(intent_id: str) -> tuple[str, list[Block]]:
    """The expiry edit: buttons removed, expired status shown."""
    blocks: list[Block] = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":hourglass: *Expired — no longer actionable.*\nintent `{intent_id}`",
            },
        }
    ]
    return f"Approval request expired: intent {intent_id}", blocks


def interactive_action_values(blocks: list[Block]) -> list[str]:
    """Every approve/deny button `value` in `blocks`, in order.

    Empty for a non-interactive message — which is how tests (and the fake
    gateway's scripted clicks) tell the two apart.
    """
    values: list[str] = []
    for block in blocks:
        if block.get("type") != "actions":
            continue
        elements = block.get("elements")
        if not isinstance(elements, list):
            continue
        for element in elements:
            if isinstance(element, dict) and element.get("action_id") in _ACTION_IDS:
                values.append(str(element["value"]))
    return values
