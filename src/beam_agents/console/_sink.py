"""The ``console://`` sink: pipeline records pushed straight to a console.

This deliberately copies ``observability/otlp.py``'s shape rather than inventing
a second telemetry-delivery posture (design D3). Batch in ``process()``, hand
batches to one daemon sender through a bounded queue, and **drop-and-count**:
never raise, never retry indefinitely, never apply backpressure.

Any other posture is unsound. A console is exactly the kind of endpoint that
goes away mid-pipeline — a developer closes the laptop — and telemetry delivery
failing must never fail an activation or slow the agent's real work. Reusing the
contract also means a reader who understands the OTLP exporter already
understands this one, and the drop behaviour is auditable the same way.

One deliberate difference from the OTLP exporter: this transmits
``ACTIVATION_START``. OTLP drops it because it shares a span ID with
``ACTIVATION_END`` and the format cannot represent two events on one span; the
native record carries ``event_type`` as a first-class field, so the start event
is both representable and load-bearing — it is what distinguishes a fresh
attempt from a resume.

:class:`ConsoleSinkResolver` *wraps* ``DefaultSinkResolver`` rather than
extending it, so no module on the hot path is modified and every other scheme
keeps behaving exactly as it does today (design D2).

Importing this module has no side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import apache_beam as beam

if TYPE_CHECKING:
    from beam_agents.core.transform import SinkResolver

__all__ = [
    "COUNTERS",
    "COUNTER_BATCHES_SENT",
    "COUNTER_EXPORT_FAILURES",
    "COUNTER_RECORDS_DROPPED",
    "COUNTER_RECORDS_EXPORTED",
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_FLUSH_DEADLINE_S",
    "DEFAULT_QUEUE_BATCHES",
    "NAMESPACE",
    "SCHEME",
    "ConsoleSinkResolver",
    "WriteToConsole",
]

SCHEME = "console"

DEFAULT_BATCH_SIZE = 256
DEFAULT_FLUSH_DEADLINE_S = 2.0
DEFAULT_QUEUE_BATCHES = 8

# Distinct from `beam_agents.runtime` and from `beam_agents.otlp`: these count
# telemetry delivery to one particular sink, not agent work, and the
# drop-and-count contract is only auditable if the counters cannot be confused
# with either of the others.
NAMESPACE = "beam_agents.console"
COUNTER_RECORDS_EXPORTED = "records_exported"
COUNTER_RECORDS_DROPPED = "records_dropped"
COUNTER_EXPORT_FAILURES = "export_failures"
COUNTER_BATCHES_SENT = "batches_sent"
COUNTERS = (
    COUNTER_RECORDS_EXPORTED,
    COUNTER_RECORDS_DROPPED,
    COUNTER_EXPORT_FAILURES,
    COUNTER_BATCHES_SENT,
)


class WriteToConsole(beam.PTransform):
    """Best-effort delivery of pipeline records to a console endpoint.

    Accepts ``TraceEvent``, ``ActivationErrorRecord``, and ``StateSnapshot``
    elements — the element types of ``.traces``, ``.errors``, and
    ``.snapshots``. Returns an empty ``PCollection``: this is a terminal write,
    and nothing downstream should be able to depend on delivery having happened.
    """

    def __init__(
        self,
        endpoint: str,
        *,
        record_kind: str = "traces",
        batch_size: int = DEFAULT_BATCH_SIZE,
        flush_deadline_s: float = DEFAULT_FLUSH_DEADLINE_S,
        queue_batches: int = DEFAULT_QUEUE_BATCHES,
    ) -> None:
        """Configure delivery to ``endpoint`` for one record kind."""
        super().__init__()
        raise NotImplementedError

    def expand(self, pcoll: beam.pvalue.PCollection) -> beam.pvalue.PCollection:
        """Batch elements and hand them to the background sender."""
        raise NotImplementedError


class ConsoleSinkResolver:
    """A ``SinkResolver`` that adds ``console://`` and delegates everything else.

    Install it where the sinks are already chosen::

        AgentConfig(
            ...,
            traces_to="console://localhost:8787",
            errors_to="console://localhost:8787",
            sink_resolver=ConsoleSinkResolver(),
        )

    Unlike ``otlp://`` — which the default resolver refuses for anything but
    traces, because the OTLP encoding cannot represent an error record or a
    state snapshot — ``console://`` is accepted for ``traces_to``, ``errors_to``,
    and ``snapshots_to``. The native encoding is the protos themselves, so there
    is nothing to lose.
    """

    def __init__(self, delegate: SinkResolver | None = None, **options: Any) -> None:
        """Wrap ``delegate``, defaulting to the runtime's own resolver."""
        raise NotImplementedError

    def validate(self, field_name: str, uri: str) -> None:
        """Reject a URI that cannot serve ``field_name``.

        Import-free, as the protocol requires: this runs at ``AgentConfig``
        construction and must not pull an HTTP client or touch the network.
        """
        raise NotImplementedError

    def resolve(self, field_name: str, uri: str) -> beam.PTransform:
        """Build the writer transform ``uri`` names for ``field_name``."""
        raise NotImplementedError
