"""Surface configuration: URIs, tokens, and eager import-free validation.

Mirrors `EffectorConfig`'s stance and reuses its transport-URI grammar
(`kafka://<brokers>/<topic>` / `pubsub://<project>/<topic-or-subscription>`):
a misconfigured deployment fails at construction with an actionable message,
and validation imports no client library and no slack-sdk.

`intents_from` is the channel the surface consumes — the effector's
`approvals_to` topic in the primary wiring, or the outbox itself in the
minimal no-effector demo (sound when the demo agent stages only approval
intents; the surface's kind filter makes any other wiring inert).
`approvals_to` is the pipeline's approvals input topic, where verdicts land.
"""

from __future__ import annotations

from dataclasses import dataclass

from beam_agents.effector.config import parse_transport_uri

DEFAULT_CONSUMER_GROUP = "slack-approval-surface"
DEFAULT_SWEEP_INTERVAL_MS = 30_000


@dataclass(frozen=True)
class SurfaceConfig:
    """Everything the surface service needs, validated at construction.

    Tokens default empty so the offline/fake-gateway paths never require them;
    `SocketModeGateway` raises its own actionable error when they are missing.
    """

    intents_from: str
    approvals_to: str
    slack_channel: str
    consumer_group: str = DEFAULT_CONSUMER_GROUP
    sweep_interval_ms: int = DEFAULT_SWEEP_INTERVAL_MS
    bot_token: str = ""
    app_token: str = ""

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Reject malformed URIs and settings, importing no client library."""
        parse_transport_uri("intents_from", self.intents_from)
        parse_transport_uri("approvals_to", self.approvals_to)
        if not self.slack_channel:
            raise ValueError("SurfaceConfig.slack_channel must be a non-empty channel name")
        if not self.consumer_group:
            raise ValueError("SurfaceConfig.consumer_group must be a non-empty string")
        if self.sweep_interval_ms <= 0:
            raise ValueError(
                f"SurfaceConfig.sweep_interval_ms must be positive; got {self.sweep_interval_ms!r}"
            )
