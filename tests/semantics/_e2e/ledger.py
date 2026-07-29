"""The Redis execution ledger: counting executions at the side effect itself.

Everything downstream of the effector is at-least-once by construction
(publish-then-commit), so counting messages proves nothing — duplicates are
the *expected* case. The gate's central assertion therefore counts at the only
place "execution" is real: the test tool's body, which INCRs a run-scoped
Redis hash under its own ``intent_id``. ``max(counts) == 1`` is "no duplicate
effect"; every minted intent present with ``count == 1`` is "no lost effect".

The tool runs inside effector worker processes (which the harness SIGKILLs),
so the increment must be durable and external — Redis, the same store the
effector's dedup uses, though under a distinct key namespace per run.

Sync ``redis`` client only: the tool executes on the effector's loop via the
registry, and the reader runs in the test process; neither needs pipelining.
"""

from __future__ import annotations

from typing import cast

DEFAULT_URL = "redis://localhost:16379"


class ExecutionLedger:
    """Run-scoped ``{intent_id: execution_count}`` hash in Redis."""

    def __init__(self, run_id: str, url: str = DEFAULT_URL) -> None:
        # Deferred import, matching every other _e2e client: redis is absent
        # from the ci unit lane on purpose (import-boundary proof), and pytest
        # imports this module at collection time even where the gate itself is
        # deselected by marker.
        import redis

        self._key = f"e2e:{run_id}:ledger"
        self._client = redis.Redis.from_url(url)

    @property
    def key(self) -> str:
        return self._key

    def record(self, intent_id: str) -> int:
        """Count one execution of ``intent_id``; returns the new count."""
        return int(self._client.hincrby(self._key, intent_id, 1))

    def counts(self) -> dict[str, int]:
        # `cast`, not an ignore: redis is absent from the ci unit lane on
        # purpose (import-boundary proof), where mypy sees `hgetall` as Any
        # and would flag an ignore as unused; with redis installed the return
        # is the Awaitable-or-value union. The cast is valid in both worlds.
        raw = cast("dict[bytes, bytes]", self._client.hgetall(self._key))
        return {k.decode(): int(v) for k, v in raw.items()}

    def clear(self) -> None:
        self._client.delete(self._key)

    def close(self) -> None:
        self._client.close()
