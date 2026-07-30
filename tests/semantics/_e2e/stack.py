"""Per-run provisioning and teardown for the e2e gate.

Isolation is by run id, and the run id is *load-bearing*: it is embedded in
every entity key (so ``intent_id = uuid5(key, seq, step)`` can never collide
with a previous run's ids in a shared dedup store), in every topic name, in
the consumer group, in the ledger namespace, and in the spool directory.

The reusable stack machinery — compose control, ``freshen_flink``, health
checks, ``InfraFailure``, run-id topic naming — lives in
``tests/semantics/_flink_stack.py`` (shared with the adapter conformance
matrix's Flink leg) and is re-exported here unchanged; this module keeps the
e2e gate's own layout: the run's topic/population naming, the spool
provisioning, and total teardown.
"""

from __future__ import annotations

import shutil
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from tests.semantics._flink_stack import (
    ARTIFACT_ENDPOINT,
    COMPOSE,
    CONTAINER_SPOOL_ROOT,
    FLINK_REST,
    HOST_BROKERS,
    HOST_SPOOL_ROOT,
    INTERNAL_BROKERS,
    JOB_ENDPOINT,
    REDIS_URL,
    REPO_ROOT,
    FlinkStackControl,
    InfraFailure,
    run_topic,
)

__all__ = [
    "ARTIFACT_ENDPOINT",
    "COMPOSE",
    "CONTAINER_SPOOL_ROOT",
    "FLINK_REST",
    "HOST_BROKERS",
    "HOST_SPOOL_ROOT",
    "INTERNAL_BROKERS",
    "JOB_ENDPOINT",
    "REDIS_URL",
    "REPO_ROOT",
    "InfraFailure",
    "RunConfig",
    "Stack",
    "new_run",
]


@dataclass(frozen=True)
class RunConfig:
    """Everything a single gate run needs to name its world."""

    run_id: str
    seed: int
    events: int

    @property
    def events_topic(self) -> str:
        return run_topic("e2e", self.run_id, "events")

    @property
    def intents_topic(self) -> str:
        return run_topic("e2e", self.run_id, "intents")

    @property
    def results_topic(self) -> str:
        return run_topic("e2e", self.run_id, "results")

    @property
    def approval_requests_topic(self) -> str:
        return run_topic("e2e", self.run_id, "approval-req")

    @property
    def decisions_topic(self) -> str:
        return run_topic("e2e", self.run_id, "decisions")

    @property
    def output_topic(self) -> str:
        return run_topic("e2e", self.run_id, "output")

    @property
    def errors_topic(self) -> str:
        return run_topic("e2e", self.run_id, "errors")

    @property
    def effector_group(self) -> str:
        return f"e2e-{self.run_id}-effector"

    @property
    def drainer_group(self) -> str:
        return f"e2e-{self.run_id}-drainer"

    @property
    def host_spool(self) -> Path:
        return HOST_SPOOL_ROOT / self.run_id

    @property
    def container_spool(self) -> str:
        return f"{CONTAINER_SPOOL_ROOT}/{self.run_id}"

    def entity_key(self, prefix: bytes, index: int) -> bytes:
        return prefix + self.run_id.encode() + b"-" + f"{index:05d}".encode()

    @property
    def all_topics(self) -> tuple[str, ...]:
        return (
            self.events_topic,
            self.intents_topic,
            self.results_topic,
            self.approval_requests_topic,
            self.decisions_topic,
            self.output_topic,
            self.errors_topic,
        )


def new_run(seed: int, events: int) -> RunConfig:
    return RunConfig(run_id=uuid.uuid4().hex[:12], seed=seed, events=events)


@dataclass
class Stack(FlinkStackControl):
    """Owns the run's external resources; teardown is idempotent and total."""

    config: RunConfig
    _submitted_jobs: list[str] = field(default_factory=list)

    def note_job(self, name_fragment: str) -> None:
        self._submitted_jobs.append(name_fragment)

    # -- topics ------------------------------------------------------------------

    async def create_topics(self) -> None:  # type: ignore[override]
        await super().create_topics(self.config.all_topics)

    async def delete_topics(self) -> None:  # type: ignore[override]
        await super().delete_topics(self.config.all_topics)

    # -- lifecycle ----------------------------------------------------------------

    def provision_spool(self) -> None:
        shutil.rmtree(self.config.host_spool, ignore_errors=True)
        self.config.host_spool.mkdir(parents=True, exist_ok=True)

    async def teardown(self) -> None:
        """Total cleanup: jobs, topics, spool. Ledger/dedup keys expire with Redis."""
        self._cancel_all_jobs()
        await self.delete_topics()
        shutil.rmtree(self.config.host_spool, ignore_errors=True)
