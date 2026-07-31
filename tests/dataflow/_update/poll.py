"""Deadline-driven waiting, and the three-way verdict a red run gets.

Design D5. `--update` has an asymmetric failure mode: when Dataflow's
compatibility check refuses the replacement graph, the *new* job fails while
the *old* job keeps running. A harness that reports that as "the job failed to
start" throws away the exact defect this gate exists to catch, so every failure
is classified before it is reported:

- `UpdateCompatibilityFailure` — the gate's primary red. The replacement was
  refused. Carries both resolved version strings, both job ids, and the
  service's stated reason.
- `StateLossFailure` — also red. The update took effect but the state did not
  survive: a suspension that restarted instead of resuming, a memory echo that
  came back empty, a fresh key that never completed.
- `InfraFailure` — not a verdict. Quota, worker-pool startup, PyPI, WIF. A red
  night triaged in minutes instead of an hour, and never retried into a green.

Nothing here sleeps for correctness: every wait is a predicate polled on an
interval under a hard deadline, with the clock and the sleep injected so the
offline unit tests drive them without wall time.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeVar

import httpx

DATAFLOW_API_ROOT = "https://dataflow.googleapis.com"

JOB_STATE_RUNNING = "JOB_STATE_RUNNING"
JOB_STATE_FAILED = "JOB_STATE_FAILED"
JOB_STATE_CANCELLED = "JOB_STATE_CANCELLED"
JOB_STATE_UPDATED = "JOB_STATE_UPDATED"
JOB_STATE_DONE = "JOB_STATE_DONE"
JOB_STATE_DRAINED = "JOB_STATE_DRAINED"

#: States in which a job still exists and still bills.
ACTIVE_STATES = frozenset(
    {
        JOB_STATE_RUNNING,
        "JOB_STATE_PENDING",
        "JOB_STATE_QUEUED",
        "JOB_STATE_DRAINING",
        "JOB_STATE_CANCELLING",
    }
)
TERMINAL_STATES = frozenset(
    {JOB_STATE_FAILED, JOB_STATE_CANCELLED, JOB_STATE_UPDATED, JOB_STATE_DONE, JOB_STATE_DRAINED}
)


class UpdateCompatibilityFailure(AssertionError):
    """The replacement graph was refused by Dataflow's compatibility check."""


class StateLossFailure(AssertionError):
    """The update succeeded but keyed state did not survive it."""


class InfraFailure(Exception):
    """The environment broke. Says nothing about compatibility."""


class PollTimeout(Exception):
    """A deadline elapsed with the awaited condition unmet."""


@dataclass(frozen=True)
class JobStatus:
    """One Dataflow job, as the harness needs it."""

    job_id: str
    name: str
    state: str
    message: str = ""
    create_time: str = ""
    labels: Mapping[str, str] = field(default_factory=dict)

    @property
    def is_active(self) -> bool:
        return self.state in ACTIVE_STATES

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    @property
    def is_healthy(self) -> bool:
        """Running, or on its way there — i.e. the old job survived the refusal."""
        return self.state in ACTIVE_STATES

    def describe(self) -> str:
        detail = f": {self.message}" if self.message else ""
        return f"{self.name} [{self.job_id}] {self.state}{detail}"


# -- REST shapes ----------------------------------------------------------------


def jobs_list_url(project: str, region: str) -> str:
    return f"{DATAFLOW_API_ROOT}/v1b3/projects/{project}/locations/{region}/jobs"


def job_url(project: str, region: str, job_id: str) -> str:
    return f"{jobs_list_url(project, region)}/{job_id}"


def cancel_body() -> dict[str, str]:
    """Force-cancel, never drain: draining waits on watermarks the gate does not
    care about, and a drain that stalls is a job that bills until morning.
    """
    return {"requestedState": JOB_STATE_CANCELLED}


def job_status_from_api(payload: Mapping[str, Any]) -> JobStatus:
    """Map one `projects.locations.jobs` resource onto `JobStatus`."""
    labels = payload.get("labels") or {}
    return JobStatus(
        job_id=str(payload.get("id", "")),
        name=str(payload.get("name", "")),
        state=str(payload.get("currentState", "")),
        message=str(payload.get("currentStateMessage", "") or payload.get("message", "")),
        create_time=str(payload.get("createTime", "")),
        labels={str(key): str(value) for key, value in labels.items()},
    )


# -- classification -------------------------------------------------------------

#: Phrases Dataflow uses when it refuses a replacement graph. Matched
#: case-insensitively against the job's state message and the launcher's stderr.
COMPATIBILITY_MARKERS: tuple[str, ...] = (
    "not compatible",
    "incompatible",
    "compatibility check",
    "transform_name_mapping",
    "unmatched step",
    "the coder or type for step",
    "cannot be updated",
    "does not match the original job",
)


def looks_like_compatibility_refusal(message: str) -> bool:
    """True when the service's own words name a compatibility refusal."""
    lowered = message.lower()
    return any(marker in lowered for marker in COMPATIBILITY_MARKERS)


def _versions(plan: Any) -> str:
    return f"launch={plan.launch_version} head={plan.head_version} mode={plan.label}"


def classify_update_failure(
    *,
    replacement: JobStatus | None,
    previous: JobStatus | None,
    plan: Any,
    error: BaseException | None = None,
) -> UpdateCompatibilityFailure | InfraFailure:
    """The verdict for a failed `--update`, per design D5.

    A refusal is recognized two ways, and either is sufficient: the service
    said so, or the replacement died while the original stayed healthy — which
    is precisely the shape of a refused replacement and of nothing else.
    """
    detail = [
        plan.report(),
        f"  versions: {_versions(plan)}",
        f"  replacement job: {replacement.describe() if replacement else '<never created>'}",
        f"  previous job:    {previous.describe() if previous else '<unknown>'}",
    ]
    if error is not None:
        detail.append(f"  error: {type(error).__name__}: {error}")

    messages = " ".join(
        part
        for part in (
            replacement.message if replacement else "",
            str(error) if error is not None else "",
        )
        if part
    )
    refused_by_message = looks_like_compatibility_refusal(messages)
    refused_by_shape = (
        replacement is not None
        and replacement.state == JOB_STATE_FAILED
        and previous is not None
        and previous.is_healthy
    )
    if refused_by_message or refused_by_shape:
        return UpdateCompatibilityFailure(
            "Dataflow refused the replacement job graph: the pipeline built from head "
            "is NOT --update-compatible with the running job. This is a state- or "
            "graph-compatibility defect, not infrastructure.\n" + "\n".join(detail)
        )
    return InfraFailure(
        "the --update leg failed for reasons that are not a compatibility verdict "
        "(quota, worker pool, provisioning, network); this run says nothing about "
        "compatibility.\n" + "\n".join(detail)
    )


def state_loss(detail: str, *, plan: Any, job_ids: Iterable[str]) -> StateLossFailure:
    """The second red: the update took effect, the state did not survive it."""
    return StateLossFailure(
        "keyed state did not survive the --update: "
        + detail
        + "\n"
        + plan.report()
        + f"\n  versions: {_versions(plan)}"
        + f"\n  jobs: {', '.join(job_ids)}"
    )


def infra(detail: str, *, plan: Any) -> InfraFailure:
    """An environment failure, reported so it is never mistaken for a verdict."""
    return InfraFailure(f"{detail}\n{plan.report()}\n  versions: {_versions(plan)}")


# -- polling --------------------------------------------------------------------

T = TypeVar("T")


def await_condition(
    observe: Callable[[], T | None],
    *,
    deadline_s: float,
    interval_s: float,
    description: str,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Poll `observe` until it returns a non-`None` value, or the deadline passes.

    The deadline is the correctness bound; the interval is only pacing. Nothing
    in this gate waits on a bare `sleep` for a condition to become true.
    """
    started = clock()
    while True:
        observed = observe()
        if observed is not None:
            return observed
        if clock() - started >= deadline_s:
            raise PollTimeout(f"timed out after {deadline_s:.0f}s waiting for {description}")
        sleep(interval_s)


def first_state(statuses: Sequence[JobStatus], *, states: frozenset[str]) -> JobStatus | None:
    """The first status in one of `states` — an `observe` for `await_condition`."""
    for status in statuses:
        if status.state in states:
            return status
    return None


# -- the Dataflow REST client ---------------------------------------------------


class DataflowJobs:
    """The handful of `projects.locations.jobs` calls the gate makes.

    Deliberately thin and REST-based: `google-cloud-dataflow-client` is not a
    dependency of this repo, and Beam's own apiclient is private. Credentials
    come from ADC, which the nightly job already provides via Workload Identity
    Federation.
    """

    _SCOPE = "https://www.googleapis.com/auth/cloud-platform"

    def __init__(self, project: str, region: str, *, timeout_s: float = 60.0) -> None:
        self._project = project
        self._region = region
        self._timeout_s = timeout_s
        # `Any`: google-auth's credentials object is unstubbed here.
        self._credentials: Any = None

    def _token(self) -> str:
        import google.auth
        import google.auth.transport.requests

        if self._credentials is None:
            self._credentials, _ = google.auth.default(scopes=[self._SCOPE])
        if not self._credentials.valid:
            self._credentials.refresh(google.auth.transport.requests.Request())
        token: str = self._credentials.token
        return token

    def _request(self, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
        response = httpx.request(
            method,
            url,
            headers={"Authorization": f"Bearer {self._token()}"},
            timeout=self._timeout_s,
            **kwargs,
        )
        response.raise_for_status()
        payload: dict[str, Any] = response.json() if response.content else {}
        return payload

    def list(self) -> list[JobStatus]:
        payload = self._request(
            "GET", jobs_list_url(self._project, self._region), params={"filter": "ACTIVE"}
        )
        return [job_status_from_api(job) for job in payload.get("jobs", [])]

    def get(self, job_id: str) -> JobStatus:
        return job_status_from_api(
            self._request(
                "GET",
                job_url(self._project, self._region, job_id),
                params={"view": "JOB_VIEW_SUMMARY"},
            )
        )

    def cancel(self, job_id: str) -> None:
        self._request("PUT", job_url(self._project, self._region, job_id), json=cancel_body())
