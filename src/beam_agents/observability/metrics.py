"""The runner-visible metric surface: names, sink, and per-activation tally.

See the change design (``openspec/changes/add-runtime-metrics/design.md``) for
the load-bearing decisions: stage the tally and record it at commit rather than
updating a metric mid-activation (D1), counters defined to partition the
transform's outputs (D2), provider-reached-only LLM metrics (D3), usage through
the existing staging sink (D4), ``iterations`` as the step-cursor delta (D5),
failure-inclusive ``activation_ms`` measured on the Beam thread (D6), injected
monotonic clocks (D7), and this module's sink/recorder/null trio (D8).

Two facts shape everything here.

**A metric update off the Beam worker thread is silently discarded.** Beam
resolves a metric cell through ``statesampler.get_current_tracker()``, backed by
a ``threading.local()``: with no tracker the update is dropped with no exception
and no log. Every LLM call, tool call, and memory write in this runtime happens
on the async bridge thread, so the counts they produce are accumulated into an
:class:`ActivationTally` (a plain object, no Beam involved) and recorded by the
DoFn on the Beam thread at commit.

**These are attempted values, not an effect ledger.** Most runners report
attempted metrics, so a retried bundle re-applies its increments while its state
and outputs roll back. Nothing in the runtime may read a metric back; the
authoritative records remain ``.traces``, ``.intents``, and ``.errors``.

Importing this module has no side effects: building a handle allocates a
``MetricName`` and a delegating object, and touches no global state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from apache_beam.metrics.metric import Metrics

# Distinct from `beam_agents.memory`, which owns `soft_cap_warnings`.
NAMESPACE = "beam_agents.runtime"

# -- counters -----------------------------------------------------------------
# One activation that reached the commit path (a start or a resume). This is the
# same event as the SEQ increment, by definition.
COUNTER_ACTIVATIONS = "activations"
# One model call that reached the provider. A replay-cache hit is not a call:
# counting it would inflate the volume signal used for cost and rate-limit
# reasoning, and it is what makes "a replay adds zero provider calls" visible.
COUNTER_LLM_CALLS = "llm_calls"
# One read-only tool executed inline. Side-effecting tools never execute in the
# pipeline, so they are counted by `intents_emitted` instead.
COUNTER_TOOL_CALLS = "tool_calls"
# One `ToolIntent` put on `.intents`, including one minted by a HITL escalation.
COUNTER_INTENTS_EMITTED = "intents_emitted"
# One dead letter on `.errors` that is not an orphaned result.
COUNTER_AGENT_ERRORS = "agent_errors"
# One committed activation whose outcome was `Suspend`.
COUNTER_SUSPENSIONS = "suspensions"
# One dead letter on `.errors` with reason `orphaned_result`.
COUNTER_ORPHANED_RESULTS = "orphaned_results"
# One long-term memory row flushed through the `MemoryStore` in an activation's
# commit tail. Counted per row, on committed activations only: a failed
# activation flushes nothing and a failed flush fails the activation.
COUNTER_LONGTERM_UPSERTS = "longterm_upserts"

# -- distributions (Beam distributions are integer-only) ----------------------
# Wall time of running the agent for one element, on every exit including
# failure and timeout: the timeout tail is the interesting part.
DISTRIBUTION_ACTIVATION_MS = "activation_ms"
# The activation's wall time minus its model-call and inline-tool time, clamped
# at zero: the runtime's own cost per committed activation, and the direct
# instrument for the release-gating overhead budget (which excludes LLM/tool
# time). One sample per committed activation; a failed activation's tally does
# not escape, so it contributes `activation_ms` but no overhead sample.
DISTRIBUTION_OVERHEAD_MS = "overhead_ms"
# Wall time of one provider-reached model call. One sample per `llm_calls`.
DISTRIBUTION_LLM_MS = "llm_ms"
# Total tokens for an activation, sampled only when usage was actually decoded.
DISTRIBUTION_TOKENS = "tokens"
# Committed working-memory size for an activation.
DISTRIBUTION_MEMORY_BYTES = "memory_bytes"
# Agent steps consumed by an activation (the step cursor's advance).
DISTRIBUTION_ITERATIONS = "iterations"

# Declaration order is the surface's documented order; a name is part of the
# observable contract, so adding one is a change and renaming one is breaking.
COUNTERS = (
    COUNTER_ACTIVATIONS,
    COUNTER_LLM_CALLS,
    COUNTER_TOOL_CALLS,
    COUNTER_INTENTS_EMITTED,
    COUNTER_AGENT_ERRORS,
    COUNTER_SUSPENSIONS,
    COUNTER_ORPHANED_RESULTS,
    COUNTER_LONGTERM_UPSERTS,
)
DISTRIBUTIONS = (
    DISTRIBUTION_ACTIVATION_MS,
    DISTRIBUTION_OVERHEAD_MS,
    DISTRIBUTION_LLM_MS,
    DISTRIBUTION_TOKENS,
    DISTRIBUTION_MEMORY_BYTES,
    DISTRIBUTION_ITERATIONS,
)

__all__ = [
    "COUNTERS",
    "DISTRIBUTIONS",
    "NAMESPACE",
    "ActivationTally",
    "MetricsSink",
    "NullMetrics",
    "RuntimeMetrics",
]


@dataclass(slots=True)
class ActivationTally:
    """Worker-local counts and durations accumulated during one activation.

    Deliberately a plain mutable object with no Beam dependency: it is filled in
    on the async bridge thread, where a Beam metric update would be discarded,
    and read on the Beam thread at commit.

    Never persisted. It is not written to ``MemoryBlob``, ``Continuation``, or
    any wire message — putting a wall-clock reading into a blob would break the
    retry-determinism gate, which compares committed bytes exactly.
    """

    #: Model calls that reached the provider (cache hits excluded).
    llm_calls: int = 0
    #: Read-only tools executed inline.
    tool_calls: int = 0
    #: Steps this activation consumed, resolved from the step cursor at read.
    iterations: int = 0
    #: Total tokens summed across decoded provider responses.
    total_tokens: int = 0
    #: Whether any usage was decoded at all. Distinguishes "nobody decoded
    #: usage" from "the model genuinely reported zero", so the `tokens`
    #: distribution is not padded with zeros from a path that never decodes.
    usage_observed: bool = False
    #: One entry per provider-reached call, in call order.
    llm_ms: list[int] = field(default_factory=list)
    #: One entry per inline tool execution, in call order. Subtracted (with
    #: `llm_ms`) from the activation's wall time to produce `overhead_ms`.
    tool_ms: list[int] = field(default_factory=list)


@runtime_checkable
class MetricsSink(Protocol):
    """Where the runtime records. Write-only by design: metrics are output, and
    nothing in the runtime may branch on a recorded value, so there is no reader
    on this protocol.
    """

    def incr(self, name: str, n: int = 1) -> None: ...

    def observe(self, name: str, value: int) -> None: ...


class RuntimeMetrics:
    """Beam-backed sink over the declared counters and distributions.

    Handles are built once per recorder rather than per call: ``Metrics.counter``
    allocates a ``MetricName`` and a delegating object every time, and this sits
    on the per-element path under a 15 ms p50 overhead budget.

    An undeclared name raises ``KeyError`` rather than creating a phantom metric
    nobody queries — a typo has to fail in a test, not on a dashboard.
    """

    def __init__(self) -> None:
        self._counters = {name: Metrics.counter(NAMESPACE, name) for name in COUNTERS}
        self._distributions = {
            name: Metrics.distribution(NAMESPACE, name) for name in DISTRIBUTIONS
        }

    def incr(self, name: str, n: int = 1) -> None:
        # Off a Beam worker thread (or outside a pipeline entirely) Beam finds
        # no state sampler and drops this; that is the documented behavior the
        # staging design exists to work around, not an error the caller handles.
        self._counters[name].inc(n)

    def observe(self, name: str, value: int) -> None:
        self._distributions[name].update(value)


class NullMetrics:
    """No-op sink for components constructed outside a pipeline.

    Accepts any name: validating them here would make this a second place to
    keep the declared surface in sync.
    """

    def incr(self, name: str, n: int = 1) -> None:
        return None

    def observe(self, name: str, value: int) -> None:
        return None
