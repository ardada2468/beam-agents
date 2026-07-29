"""Activation-latency Beam metrics: where *duration* lives, by design.

Trace events take both timestamps from the injected activation clock, so spans
are zero-width (design D7): measuring elapsed time inside the hot path would
either weaken `model-facade`'s no-wall-clock requirement or make replayed
bundles emit different trace bytes, and neither is worth a dashboard field.
Duration therefore lands here instead — Beam metrics, surfaced to runner
dashboards, outside the committed effects entirely. A metric update is not a
staged effect: a retried bundle re-counting an attempt is ordinary Beam metrics
behavior (runners distinguish committed from attempted), and no trace byte
changes.

The metric names are dashboard wire surface, the same way proto field numbers
are wire surface: renaming one silently breaks every chart built on it.

- ``beam_agents/activation_ms`` (distribution): wall time of one *successful*
  activation, from bridge submission to result. Failures are excluded — a
  timeout's "latency" is just the configured budget, and folding it in would
  corrupt the p99 this distribution exists to watch.
- ``beam_agents/activations_completed`` · ``activations_suspended`` ·
  ``activations_failed`` (counters): one increment per activation attempt, by
  outcome. A refused resume (`orphaned_result`) runs no activation and counts
  as nothing; the `.errors` dead-letter stream owns that signal.

Importing this module has no side effects: `Metrics.counter`/`distribution`
build delegating handles without touching any registry, and recording through
them outside a metrics container (as the direct-invocation unit tests do) is a
silent no-op by Beam's own contract.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from apache_beam.metrics.metric import Metrics
from apache_beam.metrics.metricbase import MetricName

if TYPE_CHECKING:
    from typing import Literal

METRICS_NAMESPACE = "beam_agents"

_ACTIVATION_MS_NAME = "activation_ms"
_COMPLETED_NAME = "activations_completed"
_SUSPENDED_NAME = "activations_suspended"
_FAILED_NAME = "activations_failed"

ACTIVATION_MS = MetricName(METRICS_NAMESPACE, _ACTIVATION_MS_NAME)
ACTIVATIONS_COMPLETED = MetricName(METRICS_NAMESPACE, _COMPLETED_NAME)
ACTIVATIONS_SUSPENDED = MetricName(METRICS_NAMESPACE, _SUSPENDED_NAME)
ACTIVATIONS_FAILED = MetricName(METRICS_NAMESPACE, _FAILED_NAME)

_ACTIVATION_MS = Metrics.distribution(METRICS_NAMESPACE, _ACTIVATION_MS_NAME)
_BY_STATUS = {
    "completed": Metrics.counter(METRICS_NAMESPACE, _COMPLETED_NAME),
    "suspended": Metrics.counter(METRICS_NAMESPACE, _SUSPENDED_NAME),
}
_FAILED = Metrics.counter(METRICS_NAMESPACE, _FAILED_NAME)


def record_activation(status: Literal["completed", "suspended"], elapsed_ms: int) -> None:
    """Record one successful activation: its outcome counter and its latency."""
    _ACTIVATION_MS.update(elapsed_ms)
    _BY_STATUS[status].inc()


def record_activation_failure() -> None:
    """Count a failed activation (timeout or raise). Deliberately no latency
    sample — see the module docstring."""
    _FAILED.inc()
