"""Observability surfaces: metrics now, traces and exporters later.

The runtime already emits per-activation ``TraceEvent``s on ``RunAgent``'s
``.traces`` output; this package is the aggregate side of that story — the
runner-visible counters and distributions an operator reads off a Dataflow job
page or a Flink metrics reporter, without standing up a trace sink first.

:mod:`beam_agents.observability.metrics` owns the whole metric vocabulary: the
namespace, the names, the :class:`~beam_agents.observability.metrics.MetricsSink`
seam the runtime records through, and the per-activation
:class:`~beam_agents.observability.metrics.ActivationTally` that carries counts
and durations from the async bridge thread (which has no Beam metrics container)
back to the Beam thread that can record them.

This package is internal: nothing here is re-exported from ``beam_agents``.

Importing this package has no side effects.
"""

from beam_agents.observability.metrics import (
    ActivationTally,
    MetricsSink,
    NullMetrics,
    RuntimeMetrics,
)

__all__ = ["ActivationTally", "MetricsSink", "NullMetrics", "RuntimeMetrics"]
