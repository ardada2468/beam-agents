"""Read trace events off a topic a pipeline is already writing.

The cheapest adoption path there is: a deployment exporting with
``traces_to="kafka://…"`` gets a console by starting one, with no pipeline
change, no redeploy, and no restart.

Two defaults are deliberate. It reads **from the end** by default, because a
viewer opened now is almost always asking about now, and replaying a retained
topic from the beginning on every start would make the first minute useless.
And it **commits no offsets**, so restarting never blocks on a consumer group's
rebalance and two consoles watching one topic never starve each other — this is
a read-only viewer, and durable group membership would be a liability rather
than a feature.

``aiokafka`` is imported inside the constructor, so importing this module works
with no extras installed.

Importing this module has no side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from beam_agents.console._store import ConsoleStore

__all__ = ["EXTRA_NAME", "KafkaTraceSource"]

# Named in the error a missing client raises, so the fix is in the message.
EXTRA_NAME = "console-ingest"


class KafkaTraceSource:
    """A background consumer that stores trace events from a Kafka topic.

    ``uri`` uses the same ``kafka://<brokers>/<topic>`` grammar the runtime's
    sink resolver already parses, so the value can be copied verbatim from the
    pipeline's ``traces_to``.
    """

    def __init__(
        self,
        uri: str,
        store: ConsoleStore,
        *,
        from_beginning: bool = False,
        on_batch: Any = None,
        **options: Any,
    ) -> None:
        """Configure the consumer; raise naming the extra if the client is absent."""
        raise NotImplementedError

    @property
    def decode_failures(self) -> int:
        """Messages that were not valid trace events, counted and skipped."""
        raise NotImplementedError

    @property
    def records_stored(self) -> int:
        """Trace events successfully handed to the store."""
        raise NotImplementedError

    async def run(self) -> None:
        """Consume until cancelled, storing what decodes and counting what does not."""
        raise NotImplementedError

    async def stop(self) -> None:
        """Stop the consumer and release its connections. Idempotent."""
        raise NotImplementedError
