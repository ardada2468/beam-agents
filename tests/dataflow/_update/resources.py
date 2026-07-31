"""Everything the run creates, and the guarantee that it all goes away.

Design D6. A streaming Dataflow job that nobody cancels bills until somebody
notices, so this module makes cleanup structural rather than well-intentioned:

- **Naming.** One run id (date + random suffix) is embedded in the job name,
  every topic, every subscription, and the GCS temp prefix, so two runs can
  never collide and every artifact is attributable at a glance.
- **Labels.** Both jobs carry `beam-agents-test=update-compat`, which is what
  makes the sweeper possible at all.
- **A ledger.** Resources are appended to `RunLedger` *as they are created*, so
  teardown cancels and deletes exactly what exists — a crash halfway through
  provisioning is torn down correctly.
- **Total teardown.** `guaranteed_teardown` runs on success, failure, and
  timeout alike, keeps going after an individual cleanup error (one dead
  resource must not strand the rest), and never replaces the exception that
  made the gate red.
- **A sweeper.** Labelled, still-active jobs older than the age threshold are
  force-cancelled before this run provisions anything, bounding a crashed
  runner's blast radius to one night.

The GCP clients are imported lazily inside the constructors that need them, so
this module imports cleanly in the offline unit lane where the gate module
beside it is deselected.
"""

from __future__ import annotations

import contextlib
import logging
import secrets
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

from tests.dataflow._update.poll import JobStatus

LOGGER = logging.getLogger("beam_agents.update_compat")

#: The label every job, and by convention every resource name, carries.
LABEL_KEY = "beam-agents-test"
LABEL_VALUE = "update-compat"

#: A run older than this that is still active was leaked by a crashed runner:
#: the whole gate is budgeted at 35 minutes.
SWEEP_MAX_AGE_S = 2 * 60 * 60

_NAME_PREFIX = "ba-update-compat"


def new_run_id(*, now: float | None = None) -> str:
    """`YYYYmmdd-<random>`: dated for a human, unique for a machine."""
    moment = datetime.fromtimestamp(now if now is not None else time.time(), tz=UTC)
    return f"{moment:%Y%m%d}-{secrets.token_hex(3)}"


@dataclass(frozen=True)
class RunResources:
    """The names this run owns. Every one embeds the run id."""

    run_id: str
    project: str
    region: str
    temp_bucket: str

    @property
    def job_name(self) -> str:
        return f"{_NAME_PREFIX}-{self.run_id}"

    def _topic(self, suffix: str) -> str:
        return f"projects/{self.project}/topics/{_NAME_PREFIX}-{self.run_id}-{suffix}"

    def _subscription(self, suffix: str) -> str:
        return f"projects/{self.project}/subscriptions/{_NAME_PREFIX}-{self.run_id}-{suffix}"

    @property
    def events_topic(self) -> str:
        return self._topic("events")

    @property
    def events_subscription(self) -> str:
        return self._subscription("events")

    @property
    def outputs_topic(self) -> str:
        return self._topic("outputs")

    @property
    def outputs_subscription(self) -> str:
        return self._subscription("outputs")

    @property
    def intents_topic(self) -> str:
        return self._topic("intents")

    @property
    def intents_subscription(self) -> str:
        return self._subscription("intents")

    @property
    def temp_location(self) -> str:
        return f"gs://{self.temp_bucket}/{_NAME_PREFIX}/{self.run_id}"

    @property
    def labels(self) -> dict[str, str]:
        return {LABEL_KEY: LABEL_VALUE}


# -- the sweeper ----------------------------------------------------------------


def parse_rfc3339(text: str) -> float:
    """Epoch seconds for a Dataflow `createTime`, which is RFC 3339 with a `Z`."""
    normalized = text.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).timestamp()


def sweep_targets(
    jobs: Sequence[JobStatus],
    *,
    now_s: float,
    max_age_s: float = SWEEP_MAX_AGE_S,
    label_key: str = LABEL_KEY,
    label_value: str = LABEL_VALUE,
) -> list[JobStatus]:
    """Labelled, still-active jobs older than `max_age_s`.

    All three conditions matter: the label keeps the sweeper off jobs it does
    not own, "active" keeps it from re-cancelling finished ones, and the age
    threshold keeps a concurrent run of this very gate safe.
    """
    targets = []
    for job in jobs:
        if job.labels.get(label_key) != label_value or not job.is_active:
            continue
        if not job.create_time:
            continue
        if now_s - parse_rfc3339(job.create_time) < max_age_s:
            continue
        targets.append(job)
    return targets


# -- teardown -------------------------------------------------------------------


class JobsClient(Protocol):
    def cancel(self, job_id: str) -> None: ...


class PubSubClient(Protocol):
    def delete_topic(self, name: str) -> None: ...
    def delete_subscription(self, name: str) -> None: ...


@dataclass
class RunLedger:
    """What this run has actually created, appended to as it creates it."""

    job_ids: list[str] = field(default_factory=list)
    topics: list[str] = field(default_factory=list)
    subscriptions: list[str] = field(default_factory=list)
    teardown_errors: list[str] = field(default_factory=list)


def teardown(ledger: RunLedger, *, jobs: JobsClient, pubsub: PubSubClient) -> list[str]:
    """Cancel every job and delete every resource. Never raises.

    Subscriptions go before their topics (deleting a topic orphans its
    subscriptions rather than removing them), and every step is attempted even
    after an earlier one failed — a single dead resource must not strand the
    rest, which is exactly how a streaming job survives the night.
    """
    for job_id in ledger.job_ids:
        _attempt(ledger, f"cancel job {job_id}", lambda job_id=job_id: jobs.cancel(job_id))
    for subscription in ledger.subscriptions:
        _attempt(
            ledger,
            f"delete subscription {subscription}",
            lambda name=subscription: pubsub.delete_subscription(name),
        )
    for topic in ledger.topics:
        _attempt(ledger, f"delete topic {topic}", lambda name=topic: pubsub.delete_topic(name))
    if ledger.teardown_errors:
        LOGGER.warning(
            "teardown finished with %d error(s):\n  %s",
            len(ledger.teardown_errors),
            "\n  ".join(ledger.teardown_errors),
        )
    return ledger.teardown_errors


def _attempt(ledger: RunLedger, what: str, action: Any) -> None:
    try:
        action()
    except Exception as exc:  # cleanup is best-effort by construction
        ledger.teardown_errors.append(f"{what}: {type(exc).__name__}: {exc}")


@contextlib.contextmanager
def guaranteed_teardown(
    ledger: RunLedger, *, jobs: JobsClient, pubsub: PubSubClient
) -> Iterator[RunLedger]:
    """Run a body with teardown guaranteed on pass, fail, and timeout.

    `finally`, not `except`: pytest-timeout interrupts with a `BaseException`,
    and the whole point of this gate's cost bound is that a timed-out run still
    cancels its jobs.
    """
    try:
        yield ledger
    finally:
        teardown(ledger, jobs=jobs, pubsub=pubsub)


# -- Pub/Sub --------------------------------------------------------------------


class PubSub:
    """The Pub/Sub calls the gate makes: provision, publish, pull, delete.

    `google-cloud-pubsub` arrives with `apache-beam[gcp]`, but it is imported
    lazily all the same so this module stays importable wherever the gate is
    merely collected.
    """

    def __init__(self, *, timeout_s: float = 60.0) -> None:
        import google.cloud.pubsub_v1

        self._publisher = google.cloud.pubsub_v1.PublisherClient()
        self._subscriber = google.cloud.pubsub_v1.SubscriberClient()
        self._timeout_s = timeout_s

    def create_topic(self, name: str) -> None:
        self._publisher.create_topic(request={"name": name}, timeout=self._timeout_s)

    def create_subscription(self, name: str, topic: str) -> None:
        self._subscriber.create_subscription(
            request={"name": name, "topic": topic, "ack_deadline_seconds": 60},
            timeout=self._timeout_s,
        )

    def delete_topic(self, name: str) -> None:
        self._publisher.delete_topic(request={"topic": name}, timeout=self._timeout_s)

    def delete_subscription(self, name: str) -> None:
        self._subscriber.delete_subscription(
            request={"subscription": name}, timeout=self._timeout_s
        )

    def publish(self, topic: str, data: bytes) -> None:
        self._publisher.publish(topic, data).result(timeout=self._timeout_s)

    def pull(self, subscription: str, *, max_messages: int = 100) -> list[bytes]:
        """One non-blocking-ish pull, acked. Callers poll under a deadline."""
        response = self._subscriber.pull(
            request={"subscription": subscription, "max_messages": max_messages},
            timeout=self._timeout_s,
        )
        received = list(response.received_messages)
        if received:
            self._subscriber.acknowledge(
                request={
                    "subscription": subscription,
                    "ack_ids": [message.ack_id for message in received],
                },
                timeout=self._timeout_s,
            )
        return [message.message.data for message in received]


def provision(resources: RunResources, *, pubsub: PubSubClient, ledger: RunLedger) -> None:
    """Create the run's topics and subscriptions, recording each as it appears.

    Order matters for teardown: a topic is recorded before its subscription is
    created, so a failure in between still leaves the topic in the ledger.
    """
    creator: Any = pubsub
    for topic, subscription in (
        (resources.events_topic, resources.events_subscription),
        (resources.outputs_topic, resources.outputs_subscription),
        (resources.intents_topic, resources.intents_subscription),
    ):
        ledger.topics.append(topic)
        creator.create_topic(topic)
        ledger.subscriptions.append(subscription)
        creator.create_subscription(subscription, topic)
