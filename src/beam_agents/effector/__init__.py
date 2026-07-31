"""The reference effector: consume intents, dedup, execute, publish results.

This package is the external service half of the runtime's effectively-once
design. The pipeline stages a ``ToolIntent`` with a deterministic ``intent_id``
(correctness invariant 2) and writes it to the outbox; the effector consumes
that outbox, collapses duplicates on ``intent_id``, executes the side-effecting
tool, and publishes a ``ToolResult`` that re-enters the pipeline on the
originating key.

It is deliberately **outside** the pipeline: nothing here imports Beam or
``beam_agents.core``, and none of these symbols are re-exported from
``beam_agents/__init__.py``. Transport and dedup clients are optional
dependencies (the ``effector`` extra) imported lazily inside their adapters, so
this package imports cleanly with none of them installed.

Importing this package has no side effects.
"""

from __future__ import annotations

from beam_agents.effector.config import (
    EffectorConfig,
    EffectorConfigError,
    TransportSecurity,
    redact_uri,
)
from beam_agents.effector.dedup import (
    Claimed,
    ClaimOutcome,
    DedupStore,
    Done,
    InFlight,
    InMemoryDedupStore,
)
from beam_agents.effector.runner import EffectorToolRunner, ReadOnlyToolError
from beam_agents.effector.service import EffectorService
from beam_agents.effector.sinks import ResultSink
from beam_agents.effector.sources import DeliveredIntent, IntentSource

__all__ = [
    "ClaimOutcome",
    "Claimed",
    "DedupStore",
    "DeliveredIntent",
    "Done",
    "EffectorConfig",
    "EffectorConfigError",
    "EffectorService",
    "EffectorToolRunner",
    "InFlight",
    "InMemoryDedupStore",
    "IntentSource",
    "ReadOnlyToolError",
    "ResultSink",
    "TransportSecurity",
    "redact_uri",
]
