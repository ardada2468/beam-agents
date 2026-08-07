"""The Flex Template's nightly launch gate: does the packaging actually resolve?

The template spec built earlier in the same nightly run is launched exactly the
way a user launches it — `gcloud dataflow flex-template run`, parameters only —
with per-run topics, the fraud example's FakeLLM provider reference, and no
secret. The verdict is one state transition: reaching `JOB_STATE_RUNNING` proves
the whole chain this change owns resolved end to end — the spec in GCS, the
launcher container, the parameter parse, `AgentConfig`/`HitlPolicy`
construction, graph submission, and worker containers pulling the image and
booting the SDK harness.

It deliberately asserts nothing about message flow (design D7). Runtime
semantics are gated offline and on the Flink mini-cluster; a data-bearing
assertion here would add publishing, subscribing and draining against a live
streaming job to the most expensive test in the repo, for a property already
covered. Launch-only keeps a red night unambiguous: red means packaging broke.

Failures are classified, never blurred, the same way the `--update` gate
classifies its own (`tests/dataflow/test_update_compat.py`): a job that fails
before `RUNNING` is reported with the service's own error state so a launcher or
parameter defect reads differently from quota or an image pull. A launch the
service refuses on authorization or quota grounds is an `InfraFailure` naming
the grants to check rather than a `LaunchFailure`: the project is misconfigured
and the run is not a verdict on packaging. There is no retry and no
flake-tolerant skip.

Runs nightly only (`-m dataflow`) and skips visibly — never deselects — without
`GCP_PROJECT_ID`/`GCP_REGION`/`GCP_DATAFLOW_TEMP_BUCKET` and the spec path the
build step publishes.

Implements the `dataflow-flex-template` scenarios "Nightly launch reaches
RUNNING and is torn down" and "A launcher failure is reported as a packaging
defect".
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass

import pytest

from tests.dataflow._update.poll import (
    JOB_STATE_RUNNING,
    DataflowJobs,
    InfraFailure,
    JobStatus,
    PollTimeout,
    await_condition,
)
from tests.dataflow._update.resources import (
    PubSub,
    RunLedger,
    guaranteed_teardown,
    new_run_id,
)

#: The gs:// template spec the nightly build step published for this commit.
SPEC_ENV = "BEAM_AGENTS_FLEX_TEMPLATE_SPEC"

REQUIRED_ENV = ("GCP_PROJECT_ID", "GCP_REGION", "GCP_DATAFLOW_TEMP_BUCKET", SPEC_ENV)

#: The example's own scripted provider, named in the YAML provider's
#: `module:object` grammar: no credential, no egress, no smoke-tier traffic.
FAKE_MODEL = "examples.fraud_triage:make_provider"

#: Short enough that a wedged suspension cannot outlive the run.
HITL_TIMEOUT_MS = 120_000

#: Budget: launch submission ~2-4 min, worker startup to RUNNING ~5-8 min,
#: teardown ~1 min — comfortably inside the nightly job's `timeout-minutes`.
BUDGET_S = 20 * 60
RUNNING_DEADLINE_S = 900
POLL_INTERVAL_S = 15
LAUNCH_TIMEOUT_S = 300

LABEL = "beam-agents-test=flex-template"

pytestmark = [pytest.mark.dataflow, pytest.mark.slow, pytest.mark.timeout(BUDGET_S)]


class LaunchFailure(AssertionError):
    """The template did not launch: a packaging defect, this gate's purpose."""


@dataclass(frozen=True)
class GcpConfig:
    project: str
    region: str
    temp_bucket: str
    spec: str


@pytest.fixture(scope="module")
def gcp() -> GcpConfig:
    """Skip visibly — never deselect — when the lane is not configured."""
    missing = [name for name in REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        pytest.skip(
            "the Flex Template launch gate needs a configured GCP project and a "
            "template spec published by the nightly build step; missing " + ", ".join(missing)
        )
    return GcpConfig(
        project=os.environ["GCP_PROJECT_ID"],
        region=os.environ["GCP_REGION"],
        temp_bucket=os.environ["GCP_DATAFLOW_TEMP_BUCKET"],
        spec=os.environ[SPEC_ENV],
    )


@dataclass(frozen=True)
class RunTopics:
    """Per-run Pub/Sub topics, so concurrent nights never collide."""

    run_id: str
    project: str

    def _topic(self, suffix: str) -> str:
        return f"projects/{self.project}/topics/beam-agents-flex-{self.run_id}-{suffix}"

    @property
    def events(self) -> str:
        return self._topic("events")

    @property
    def approvals(self) -> str:
        return self._topic("approvals")

    @property
    def decisions(self) -> str:
        return self._topic("decisions")

    @property
    def intents(self) -> str:
        return self._topic("intents")

    @property
    def all(self) -> tuple[str, ...]:
        return (self.events, self.approvals, self.decisions, self.intents)

    def uri(self, topic: str) -> str:
        """The sink resolver's URI form of a topic path — the template's grammar."""
        return f"pubsub://{self.project}/{topic.rsplit('/', 1)[-1]}"


def launch_command(gcp: GcpConfig, topics: RunTopics, job_name: str) -> list[str]:
    """The exact `gcloud` invocation the template's README documents.

    Shelling out rather than calling the REST API is deliberate (design, Open
    Question 6): this gate's job is to prove the *documented user path* works,
    so it walks that path.
    """
    parameters = ",".join(
        [
            f"input_topic={topics.uri(topics.events)}",
            f"approvals_topic={topics.uri(topics.approvals)}",
            f"output_topic={topics.uri(topics.decisions)}",
            f"intents_topic={topics.uri(topics.intents)}",
            f"model={FAKE_MODEL}",
            f"hitl_timeout_ms={HITL_TIMEOUT_MS}",
        ]
    )
    return [
        "gcloud",
        "dataflow",
        "flex-template",
        "run",
        job_name,
        f"--template-file-gcs-location={gcp.spec}",
        f"--project={gcp.project}",
        f"--region={gcp.region}",
        f"--temp-location=gs://{gcp.temp_bucket}/flex-template/{job_name}",
        # `--temp-location` defaults *from* `--staging-location`, never the other
        # way round: with staging unset the service falls back to the per-project
        # default bucket `dataflow-staging-<region>-<project-number>` and tries to
        # create it, which a least-privilege CI principal may not do (it holds
        # object access on this bucket, not `storage.buckets.create` on the
        # project). Naming it keeps every byte this run stages under the run's own
        # prefix, the same way design D6 treats every other resource.
        f"--staging-location=gs://{gcp.temp_bucket}/flex-template/{job_name}/staging",
        f"--parameters={parameters}",
        f"--additional-user-labels={LABEL}",
        "--num-workers=1",
        "--max-workers=1",
        "--format=value(job.id)",
    ]


#: Authorization and quota vocabulary in gcloud's output: the project is
#: misconfigured, and the run is not a verdict on packaging. Deliberately narrow
#: — a bare `FAILED_PRECONDITION` is *not* here, because the launcher rejecting
#: the template's own preconditions is exactly the defect this gate exists to
#: catch, and a classifier that reads a real refusal as noise discards it.
INFRA_MARKERS = (
    "permission_denied",
    "permission denied",
    "does not have permission",
    "unauthorized",
    "unauthenticated",
    "resource_exhausted",
    "quota",
)


def looks_like_environment_failure(output: str) -> bool:
    """Does gcloud's output name an authorization or quota problem?"""
    lowered = output.lower()
    return any(marker in lowered for marker in INFRA_MARKERS)


def _run_gcloud(command: list[str]) -> str:
    completed = subprocess.run(  # fixed argv, no shell
        command,
        capture_output=True,
        text=True,
        timeout=LAUNCH_TIMEOUT_S,
        check=False,
    )
    if completed.returncode != 0:
        context = (
            f"command: {' '.join(command)}\nstdout: {completed.stdout}\nstderr: {completed.stderr}"
        )
        if looks_like_environment_failure(completed.stderr):
            raise InfraFailure(
                "`gcloud dataflow flex-template run` was refused for an "
                "authorization or quota reason (exit "
                f"{completed.returncode}), so this run says nothing about "
                "packaging. Check the nightly service account's grants — see "
                "docs/ci.md, 'What the nightly service account must be able to "
                f"do'.\n{context}"
            )
        raise LaunchFailure(
            "`gcloud dataflow flex-template run` refused the launch (exit "
            f"{completed.returncode}); this is a packaging or parameter defect, "
            f"not infrastructure.\n{context}"
        )
    job_id = completed.stdout.strip().splitlines()[-1].strip() if completed.stdout.strip() else ""
    if not job_id:
        raise LaunchFailure(f"no job id on gcloud's output; got:\n{completed.stdout!r}")
    return job_id


def test_the_published_template_launches_and_reaches_running(gcp: GcpConfig) -> None:
    """Launch the nightly's own spec, wait for RUNNING, cancel, clean up."""
    run_id = new_run_id()
    topics = RunTopics(run_id=run_id, project=gcp.project)
    job_name = f"beam-agents-flex-{run_id}"
    jobs = DataflowJobs(gcp.project, gcp.region)
    pubsub = PubSub()
    ledger = RunLedger()

    with guaranteed_teardown(ledger, jobs=jobs, pubsub=pubsub):
        for topic in topics.all:
            pubsub.create_topic(topic)
            ledger.topics.append(topic)

        job_id = _run_gcloud(launch_command(gcp, topics, job_name))
        ledger.job_ids.append(job_id)
        print(f"[flex-template] launched {job_id} from {gcp.spec}", flush=True)

        try:
            await_condition(
                lambda: _running_or_raise(jobs.get(job_id)),
                deadline_s=RUNNING_DEADLINE_S,
                interval_s=POLL_INTERVAL_S,
                description=f"the template job {job_id} to reach RUNNING",
            )
        except PollTimeout as exc:
            raise LaunchFailure(
                f"the template job {job_id} never reached {JOB_STATE_RUNNING} within "
                f"{RUNNING_DEADLINE_S}s: {jobs.get(job_id).describe()}. A job stuck "
                "before RUNNING with no error state is usually quota or an image "
                "pull; an error state names the launcher defect."
            ) from exc


def _running_or_raise(status: JobStatus) -> JobStatus | None:
    """Poll predicate: RUNNING satisfies it, a terminal state fails immediately.

    Distinguishing the two is the point of the scenario "A launcher failure is
    reported as a packaging defect": waiting out the full deadline on a job that
    already failed would report a timeout and hide the service's own reason.
    """
    if status.state == JOB_STATE_RUNNING:
        return status
    if status.is_terminal:
        raise LaunchFailure(
            f"the template job reached {status.state} before {JOB_STATE_RUNNING}: "
            f"{status.describe()}. The launcher container ran and the service "
            "rejected what it submitted — read the job's error state above for the "
            "parameter or graph defect."
        )
    return None
