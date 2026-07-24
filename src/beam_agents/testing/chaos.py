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
