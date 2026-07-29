"""The Redis execution ledger: counting at the side effect itself, two ways.

Everything downstream of the effector is at-least-once by construction
(publish-then-commit), so counting messages proves nothing — duplicates are
the *expected* case. The gate's central measurement therefore lives in the
test tool's body, keyed by the injected ``intent.intent_id``, as two
countings per intent:

- **attempts** — an unconditional ``HINCRBY``: the raw number of times the
  callable ran, which retains the crash-window bound (a SIGKILL between the
  effect and the durable completion record re-executes after lease expiry).
- **effective** — a first-writer-wins ``HSETNX``: the modeled idempotent
  downstream. A crash-window re-invocation arrives with the byte-identical
  ``intent_id`` and loses the race, so the strong-form assertion is exactly
  one effective execution per minted intent, kills or not.

The tool runs inside effector worker processes (which the harness SIGKILLs),
so both writes must be durable and external — Redis, the same store the
effector's dedup uses, though under distinct key namespaces per run.

Sync ``redis`` client only: the tool executes on the effector's loop via the
registry, and the reader runs in the test process; neither needs pipelining.
"""

from __future__ import annotations

from typing import cast

DEFAULT_URL = "redis://localhost:16379"


class ExecutionLedger:
    """Run-scoped ``{intent_id: count}`` hashes in Redis: attempts + effective."""

    def __init__(self, run_id: str, url: str = DEFAULT_URL) -> None:
        # Deferred import, matching every other _e2e client: redis is absent
        # from the ci unit lane on purpose (import-boundary proof), and pytest
        # imports this module at collection time even where the gate itself is
        # deselected by marker.
        import redis

        self._attempts_key = f"e2e:{run_id}:attempts"
        self._effective_key = f"e2e:{run_id}:effective"
        self._client = redis.Redis.from_url(url)

    def record_attempt(self, intent_id: str) -> int:
        """Count one invocation of ``intent_id``; returns the new count."""
        return int(self._client.hincrby(self._attempts_key, intent_id, 1))

    def record_effective(self, intent_id: str, *, attempt: int) -> bool:
        """First-writer-wins effect keyed on ``intent_id``.

        Returns whether THIS invocation performed the effect; the stored value
        is the winning attempt number, for post-run attribution.
        """
        return bool(self._client.hsetnx(self._effective_key, intent_id, attempt))

    def attempts(self) -> dict[str, int]:
        return self._read(self._attempts_key)

    def effective(self) -> dict[str, int]:
        return self._read(self._effective_key)

    def _read(self, key: str) -> dict[str, int]:
        # `cast`, not an ignore: redis is absent from the ci unit lane on
        # purpose (import-boundary proof), where mypy sees `hgetall` as Any
        # and would flag an ignore as unused; with redis installed the return
        # is the Awaitable-or-value union. The cast is valid in both worlds.
        raw = cast("dict[bytes, bytes]", self._client.hgetall(key))
        return {k.decode(): int(v) for k, v in raw.items()}

    def clear(self) -> None:
        self._client.delete(self._attempts_key, self._effective_key)

    def close(self) -> None:
        self._client.close()
