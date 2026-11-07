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

import base64
import secrets
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
    "KEYRING_NAME",
    "REDIS_URL",
    "REPO_ROOT",
    "SIGNING_KEY_ID",
    "InfraFailure",
    "RunConfig",
    "Stack",
    "new_run",
]

# The reference deployment's intent-signing key, named by id as the pipeline's
# `IntentSigner` and the effector's keyring both name it. The key *material*
# is minted fresh per run by `Stack.provision_keyring` (32 CSPRNG bytes, the
# size docs/security.md prescribes) and written to the spool directory, which
# is the one filesystem both halves of the loop already share: the signer
# DoFn resolves it inside the beam-sdk-harness container, the host-side
# effector pool resolves it from the same bind mount — each through a `file:`
# reference, exactly the production provisioning shape. Nothing about the key
# is committed, and the whole directory is removed at teardown.
SIGNING_KEY_ID = "e2e-dev"
KEYRING_NAME = "intent-keys"  # not `*.seg`, so the spool source never reads it


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
    def dead_letters_topic(self) -> str:
        """Where the effector preserves deliveries that fail verification."""
        return run_topic("e2e", self.run_id, "dead-letters")

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

    @property
    def host_keyring(self) -> Path:
        """The per-run signing keyring as the host-side effector pool reads it."""
        return self.host_spool / KEYRING_NAME

    @property
    def container_keyring(self) -> str:
        """The same file as the signer DoFn inside beam-sdk-harness reads it."""
        return f"{self.container_spool}/{KEYRING_NAME}"

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
            self.dead_letters_topic,
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

    def provision_keyring(self) -> bytes:
        """Mint this run's signing key and write the keyring both halves read.

        Returns the raw key so the gate can prove, independently of either
        half, what a correctly signed intent looks like. Requires
        ``provision_spool`` to have run: the keyring lives in the spool
        directory because that is the filesystem the pipeline container and
        the host already share, and because teardown's ``rmtree`` then removes
        the key material along with everything else the run minted.
        """
        key = secrets.token_bytes(32)
        encoded = base64.b64encode(key).decode("ascii")
        self.config.host_keyring.write_text(
            "# beam-agents e2e gate: per-run intent-signing keyring (dev key, minted by\n"
            "# tests/semantics/_e2e/stack.py; never committed; removed at teardown).\n"
            f"{SIGNING_KEY_ID}={encoded}\n"
        )
        # Readable by the (root) harness container through the bind mount; no
        # wider than the run's own user on the host.
        self.config.host_keyring.chmod(0o600)
        return key

    async def teardown(self) -> None:
        """Total cleanup: jobs, topics, spool (keyring included). Ledger/dedup
        keys expire with Redis."""
        self._cancel_all_jobs()
        await self.delete_topics()
        shutil.rmtree(self.config.host_spool, ignore_errors=True)
