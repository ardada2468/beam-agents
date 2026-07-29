"""Chaos helper for the retry-determinism semantics gate.

Beam's classic (streaming-capable) DirectRunner already retries a bundle that
raises during ``process()`` (``TransformExecutor._MAX_RETRY_PER_BUNDLE = 4``,
``apache_beam.runners.direct.executor``), replaying it from a clean, rolled
-back read of pre-bundle keyed state — verified empirically against
``_AgentDoFn`` before writing this module. There is no need to simulate a
retry or model a rollback: this helper only needs to make one targeted commit
fail once, and let Beam's own retry mechanics do the rest.

``fail_first_matching_commit`` replaces ``_AgentDoFn._commit`` for the
duration of a ``with`` block, restoring the original on exit. No production
code is touched; the fault is injected and removed from this test module
only.

Importing this module has no side effects.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable, Iterator
from typing import Any

import apache_beam as beam

from beam_agents.core import dofn as _dofn_module
from beam_agents.core.dofn import _AgentDoFn
from beam_agents.core.loop import ActivationResult

Matcher = Callable[[ActivationResult], bool]

# ``_commit``'s state/timer parameters are Beam's dynamic StateParam/TimerParam
# handles (see the identical ``_State``/``_Timer`` aliases in ``core/dofn.py``);
# they are not statically typed by Beam itself.
_State = Any
_Timer = Any


class ChaosBundleFailure(Exception):
    """Raised by the chaos-wrapped commit to simulate an infra-level bundle
    failure (e.g. a worker restart mid-checkpoint). Beam's own bundle retry
    then re-invokes ``process()`` from a clean pre-bundle state read.
    """


def match_any() -> Matcher:
    """Convenience matcher: matches the first commit of any activation."""
    return lambda _result: True


@contextlib.contextmanager
def fail_first_matching_commit(matcher: Matcher | None = None) -> Iterator[None]:
    """Fail the first ``_AgentDoFn`` commit whose result satisfies ``matcher``.

    Every other commit — including Beam's own retry of the same failed bundle
    — proceeds through the original, unmodified commit logic. Restores the
    original ``_commit`` on exit, so the fault does not leak into later
    pipelines or tests. ``matcher`` defaults to matching any activation.
    """
    active_matcher = matcher if matcher is not None else match_any()
    original_commit = _AgentDoFn._commit
    already_failed = False

    def chaos_commit(
        self: _AgentDoFn,
        result: ActivationResult,
        now_ms: int,
        activation_ms: int,
        memory: _State,
        continuation: _State,
        llm_cache: _State,
        pending: _State,
        seq: _State,
        ttl_timer: _Timer,
        hitl_timer: _Timer,
    ) -> Iterator[object]:
        nonlocal already_failed
        if not already_failed and active_matcher(result):
            already_failed = True
            raise ChaosBundleFailure(
                f"chaos: forced commit failure for seq={result.seq} status={result.status}"
            )
        yield from original_commit(
            self,
            result,
            now_ms,
            activation_ms,
            memory,
            continuation,
            llm_cache,
            pending,
            seq,
            ttl_timer,
            hitl_timer,
        )

    _dofn_module._AgentDoFn._commit = chaos_commit  # type: ignore[method-assign]
    try:
        yield
    finally:
        _dofn_module._AgentDoFn._commit = original_commit  # type: ignore[method-assign]


@contextlib.contextmanager
def fail_first_hitl_fire() -> Iterator[None]:
    """Fail the first ``HITL_TIMER`` firing *after* it has done its work.

    The wrapper runs the real ``on_hitl`` to completion — staging its outputs
    and its state writes — and only then raises, so the discarded attempt is
    the realistic one: a bundle that did everything and lost it at commit.
    Beam's retry then re-invokes the callback from rolled-back state, which is
    where the escalation's determinism is actually on the line.

    The replacement mirrors ``on_hitl``'s signature exactly, defaults included:
    Beam's ``MethodWrapper`` reads those defaults to decide which key, timer,
    and state handles to inject, so a ``*args``-style stand-in would silently
    receive nothing. It also has to be swapped into the ``TimerSpec``'s
    ``_attached_callback`` under the same ``__name__``: ``validate_stateful_dofn``
    rejects a DoFn whose timer callback is not identical to the method the spec
    was decorated with ("perhaps it was overwritten?" — it was).
    """
    original_on_hitl = _AgentDoFn.on_hitl
    already_failed = False

    def chaos_on_hitl(
        self: _AgentDoFn,
        key: bytes = beam.DoFn.KeyParam,  # type: ignore[assignment]
        timestamp: Any = beam.DoFn.TimestampParam,
        continuation: _State = beam.DoFn.StateParam(_AgentDoFn.CONTINUATION),  # noqa: B008
        pending: _State = beam.DoFn.StateParam(_AgentDoFn.PENDING),  # noqa: B008
        hitl_timer: _Timer = beam.DoFn.TimerParam(_AgentDoFn.HITL_TIMER),  # noqa: B008
        ttl_timer: _Timer = beam.DoFn.TimerParam(_AgentDoFn.TTL_TIMER),  # noqa: B008
    ) -> Iterator[object]:
        nonlocal already_failed
        emitted = list(
            original_on_hitl(
                self,
                key=key,
                timestamp=timestamp,
                continuation=continuation,
                pending=pending,
                hitl_timer=hitl_timer,
                ttl_timer=ttl_timer,
            )
        )
        if not already_failed:
            already_failed = True
            raise ChaosBundleFailure(f"chaos: forced HITL timer failure at {timestamp}")
        yield from emitted

    chaos_on_hitl.__name__ = original_on_hitl.__name__
    _dofn_module._AgentDoFn.on_hitl = chaos_on_hitl  # type: ignore[method-assign]
    _AgentDoFn.HITL_TIMER._attached_callback = chaos_on_hitl
    try:
        yield
    finally:
        _dofn_module._AgentDoFn.on_hitl = original_on_hitl  # type: ignore[method-assign]
        _AgentDoFn.HITL_TIMER._attached_callback = original_on_hitl
