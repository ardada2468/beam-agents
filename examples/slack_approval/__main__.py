"""Run the Slack approval surface: ``python -m examples.slack_approval``.

Wires the real transports (via the effector's builders, so ``kafka://`` and
``pubsub://`` both work) to the Socket Mode gateway and runs the surface until
SIGINT/SIGTERM. Tokens come from the environment — ``SLACK_BOT_TOKEN``
(``xoxb-``) and ``SLACK_APP_TOKEN`` (``xapp-``) — never from argv, so they
cannot leak into process listings.

See `docs/examples/slack-approval.md` for the compose walkthrough.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import signal

from beam_agents.effector.config import parse_transport_uri
from beam_agents.effector.sinks import build_message_sink
from beam_agents.effector.sources import build_intent_source

from .config import DEFAULT_CONSUMER_GROUP, DEFAULT_SWEEP_INTERVAL_MS, SurfaceConfig
from .slack import SocketModeGateway
from .surface import ApprovalSurface


def build_surface(config: SurfaceConfig) -> ApprovalSurface:
    """Construct the surface a validated config names (real transports)."""
    source_scheme, source_parts = parse_transport_uri("intents_from", config.intents_from)
    sink_scheme, sink_parts = parse_transport_uri("approvals_to", config.approvals_to)
    return ApprovalSurface(
        source=build_intent_source(
            source_scheme, source_parts, consumer_group=config.consumer_group
        ),
        sink=build_message_sink(sink_scheme, sink_parts),
        gateway=SocketModeGateway(bot_token=config.bot_token, app_token=config.app_token),
        channel=config.slack_channel,
        sweep_interval_ms=config.sweep_interval_ms,
    )


async def _run(config: SurfaceConfig) -> None:
    surface = build_surface(config)
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):  # non-POSIX event loops
            loop.add_signal_handler(signum, surface.stop)
    await surface.run()


def main(argv: list[str] | None = None) -> int:
    """CLI entry point: run the approval surface until interrupted.

    Returns the process exit status: non-zero for a configuration
    error, zero for a clean shutdown.
    """
    parser = argparse.ArgumentParser(
        prog="python -m examples.slack_approval",
        description="Slack approval surface for beam-agents HITL approvals.",
    )
    parser.add_argument(
        "--intents-from",
        required=True,
        help="channel topic to consume approval intents from, e.g. "
        "kafka://localhost:19092/approval-requests (the effector's --approvals-to)",
    )
    parser.add_argument(
        "--approvals-to",
        required=True,
        help="pipeline approvals input topic verdicts are published to, e.g. "
        "kafka://localhost:19092/approvals",
    )
    parser.add_argument(
        "--slack-channel", required=True, help="Slack channel to post approval requests in"
    )
    parser.add_argument("--consumer-group", default=DEFAULT_CONSUMER_GROUP)
    parser.add_argument("--sweep-interval-ms", type=int, default=DEFAULT_SWEEP_INTERVAL_MS)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)
    config = SurfaceConfig(
        intents_from=args.intents_from,
        approvals_to=args.approvals_to,
        slack_channel=args.slack_channel,
        consumer_group=args.consumer_group,
        sweep_interval_ms=args.sweep_interval_ms,
        bot_token=os.environ.get("SLACK_BOT_TOKEN", ""),
        app_token=os.environ.get("SLACK_APP_TOKEN", ""),
    )
    asyncio.run(_run(config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
