"""Dataflow `--update` compatibility: live keyed state survives a release hop.

The release gate correctness invariant 7 is named after and nothing has ever
run: a streaming job on **real Dataflow** at the previous released version, put
into a state that matters — one key suspended mid-activation with a persisted
`Continuation` and a pending `APPROVAL` intent, one key with populated working
memory — and then replaced in place with `--update` at current head. Everything
is asserted from *outside* the pipeline, off its output topic:

- the suspended key resumes when its approval is injected **after** the update,
  emitting the continuation snapshot's nonce — a value recoverable only from
  the pre-update `Continuation`, never from a restarted key;
- the memory key's next activation echoes the marker written by the previous
  release's job, proving `MemoryBlob` bytes written by release N decoded under
  head;
- a previously unseen key completes normally on the updated job.

The two graphs come from one launcher module (`_update/pipeline.py`) executed by
two interpreters: the previous release installed from PyPI into its own venv,
head built from the checkout with `uv build`. They differ only in library
versions, which is the variable under test.

Failures are classified, never blurred (design D5): a replacement refused by
Dataflow's compatibility check — the new job fails while the old one keeps
running — is an `UpdateCompatibilityFailure` and is this gate's whole purpose;
a surviving update whose state did not survive is a `StateLossFailure`; quota,
worker pools, PyPI and WIF are `InfraFailure` and are not a verdict. There is
no `xfail`, no flake-tolerant skip, and no retry anywhere in this module: a
flaky step gets fixed in the harness, not tolerated in the verdict.

Runs nightly only (`-m dataflow`) and skips visibly without
`GCP_PROJECT_ID`/`GCP_REGION`/`GCP_DATAFLOW_TEMP_BUCKET`, following the smoke
tier's credential-skip pattern — a reported skip, never a silent deselection.
Cost is bounded by design D6: one Streaming Engine worker per job, per-phase
deadlines inside a 35-minute budget, unconditional teardown, and a sweeper that
force-cancels labelled jobs a crashed run left behind.

Implements the `state-guarantees` scenarios "A suspension survives the update",
"Working memory survives the update", "A refused compatibility check is
reported as the defect it is", "Before any PyPI release exists, the gate runs a
labelled self-update leg", "A failing run leaves nothing behind" and "A crashed
run is bounded to one night".
"""

from __future__ import annotations

import os
import secrets
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

from beam_agents._protos import ToolIntent
from tests.dataflow._update import pipeline as launcher
from tests.dataflow._update.poll import (
    JOB_STATE_RUNNING,
    JOB_STATE_UPDATED,
    DataflowJobs,
    JobStatus,
    PollTimeout,
    await_condition,
    classify_update_failure,
    infra,
    state_loss,
)
from tests.dataflow._update.resources import (
    SWEEP_MAX_AGE_S,
    PubSub,
    RunLedger,
    RunResources,
    guaranteed_teardown,
    new_run_id,
    provision,
    sweep_targets,
)
from tests.dataflow._update.versions import (
    Leg,
    VersionPlan,
    describe,
    head_version,
    resolve_plan,
    run_command,
)
from tests.dataflow._update.versions import provision as provision_versions

REPO_ROOT = Path(__file__).resolve().parents[2]
LAUNCHER = REPO_ROOT / "tests" / "dataflow" / "_update" / "pipeline.py"

#: Total budget (design D6): launch ~5-8, phase 1 ~5, update ~5-10, phase 2 ~5,
#: teardown ~2 — inside the nightly job's 50-minute `timeout-minutes`.
BUDGET_S = 35 * 60

LAUNCH_DEADLINE_S = 900
PHASE_ONE_DEADLINE_S = 420
UPDATE_DEADLINE_S = 900
PHASE_TWO_DEADLINE_S = 420
POLL_INTERVAL_S = 15

KEY_SUSPEND = b"K-suspend"
KEY_MEMORY = b"K-memory"
KEY_CANARY = b"K-canary"
KEY_POST = b"K-post"

REQUIRED_ENV = ("GCP_PROJECT_ID", "GCP_REGION", "GCP_DATAFLOW_TEMP_BUCKET")

pytestmark = [pytest.mark.dataflow, pytest.mark.slow, pytest.mark.timeout(BUDGET_S)]


@dataclass(frozen=True)
class GcpConfig:
    project: str
    region: str
    temp_bucket: str


@pytest.fixture(scope="module")
def gcp() -> GcpConfig:
    """Skip visibly — never deselect — when the project is not configured."""
    missing = [name for name in REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        pytest.skip(
            "Dataflow --update gate needs a configured GCP project; missing " + ", ".join(missing)
        )
    return GcpConfig(
        project=os.environ["GCP_PROJECT_ID"],
        region=os.environ["GCP_REGION"],
        temp_bucket=os.environ["GCP_DATAFLOW_TEMP_BUCKET"],
    )


def _now_ms() -> int:
    return int(time.time() * 1000)


def _launch(
    leg: Leg,
    *,
    phase: str,
    resources: RunResources,
    ledger: RunLedger,
) -> str:
    """Run one leg's interpreter over the shared launcher; record its job id."""
    command = [
        str(leg.python),
        str(LAUNCHER),
        *launcher.launcher_argv(
            phase=phase,
            project=resources.project,
            region=resources.region,
            job_name=resources.job_name,
            temp_location=resources.temp_location,
            events_subscription=resources.events_subscription,
            outputs_topic=resources.outputs_topic,
            intents_topic=resources.intents_topic,
            extra_package=str(leg.wheel),
        ),
    ]
    stdout = run_command(command, cwd=REPO_ROOT)
    job_id = launcher.parse_job_id(stdout)
    ledger.job_ids.append(job_id)
    return job_id


def _await_state(
    jobs: DataflowJobs, job_id: str, *, states: frozenset[str], deadline_s: float, what: str
) -> JobStatus:
    return await_condition(
        lambda: status if (status := jobs.get(job_id)).state in states else None,
        deadline_s=deadline_s,
        interval_s=POLL_INTERVAL_S,
        description=what,
    )


class _Outputs:
    """A buffered reader over the run's outputs subscription.

    Pub/Sub `pull` returns an arbitrary subset per call, so every read is
    accumulated and predicates run against the accumulated set — message counts
    are never the property, identity is.
    """

    def __init__(self, pubsub: PubSub, subscription: str) -> None:
        self._pubsub = pubsub
        self._subscription = subscription
        self.records: list[launcher.OutputRecord] = []
        self.undecodable: list[bytes] = []

    def _drain(self) -> None:
        for raw in self._pubsub.pull(self._subscription):
            try:
                self.records.append(launcher.decode_output(raw))
            except ValueError:
                self.undecodable.append(raw)

    def find(self, key: bytes, verb: str) -> launcher.OutputRecord | None:
        self._drain()
        for record in self.records:
            if record.key == key and record.verb == verb:
                return record
        return None

    def await_record(self, key: bytes, verb: str, *, deadline_s: float) -> launcher.OutputRecord:
        return await_condition(
            lambda: self.find(key, verb),
            deadline_s=deadline_s,
            interval_s=POLL_INTERVAL_S,
            description=f"a {verb!r} output for key {key!r}",
        )


def _await_approval_intent(
    pubsub: PubSub, subscription: str, key: bytes, *, deadline_s: float
) -> ToolIntent:
    """The APPROVAL intent the suspension staged, read off the intents topic.

    Read rather than derived: `intent_id` is a deterministic function of
    `(key, seq, step)`, but recomputing it here would make the gate agree with
    the runtime by construction instead of observing what it actually emitted.
    """
    seen: list[ToolIntent] = []

    def observe() -> ToolIntent | None:
        for raw in pubsub.pull(subscription):
            intent = ToolIntent()
            intent.ParseFromString(raw)
            seen.append(intent)
        for intent in seen:
            if intent.entity_key == key and intent.kind == ToolIntent.APPROVAL:
                return intent
        return None

    return await_condition(
        observe,
        deadline_s=deadline_s,
        interval_s=POLL_INTERVAL_S,
        description=f"the APPROVAL intent staged for key {key!r}",
    )


def _sweep(jobs: DataflowJobs, plan: VersionPlan) -> None:
    """Force-cancel labelled jobs a crashed run left behind (design D6)."""
    try:
        stale = sweep_targets(jobs.list(), now_s=time.time(), max_age_s=SWEEP_MAX_AGE_S)
    except Exception as exc:
        raise infra(f"could not list Dataflow jobs to sweep: {exc!r}", plan=plan) from exc
    for job in stale:
        print(f"[sweep] force-cancelling leaked job {job.describe()}")
        try:
            jobs.cancel(job.job_id)
        except Exception as exc:  # a leaked job we cannot cancel is not a verdict
            print(f"[sweep] could not cancel {job.job_id}: {exc!r}")


def test_update_preserves_a_live_suspension_and_working_memory(
    gcp: GcpConfig, tmp_path: Path
) -> None:
    """Launch at the previous release, `--update` to head, assert state survived."""
    plan = resolve_plan(head_version=head_version())
    print(plan.report(), flush=True)

    resources = RunResources(
        run_id=new_run_id(),
        project=gcp.project,
        region=gcp.region,
        temp_bucket=gcp.temp_bucket,
    )
    jobs = DataflowJobs(gcp.project, gcp.region)
    pubsub = PubSub()
    _sweep(jobs, plan)

    marker = f"marker-{secrets.token_hex(4)}"
    nonce = f"nonce-{secrets.token_hex(4)}"
    ledger = RunLedger()

    with guaranteed_teardown(ledger, jobs=jobs, pubsub=pubsub):
        provision(resources, pubsub=pubsub, ledger=ledger)
        provisioned = provision_versions(plan, workdir=tmp_path, repo_root=REPO_ROOT)
        print(describe(provisioned), flush=True)

        # -- phase 1: the previous release's job, holding live state ----------
        launch_job_id = _launch(
            provisioned.launch, phase=launcher.PHASE_LAUNCH, resources=resources, ledger=ledger
        )
        _await_state(
            jobs,
            launch_job_id,
            states=frozenset({JOB_STATE_RUNNING}),
            deadline_s=LAUNCH_DEADLINE_S,
            what=f"the {plan.launch_version} job to reach RUNNING",
        )

        now_ms = _now_ms()
        for key, command in (
            (KEY_CANARY, launcher.CMD_PING),
            (KEY_MEMORY, f"{launcher.CMD_REMEMBER}{marker}"),
            (KEY_SUSPEND, f"{launcher.CMD_SUSPEND}{nonce}"),
        ):
            pubsub.publish(
                resources.events_topic,
                launcher.event_envelope(key, command, event_time_ms=now_ms).SerializeToString(),
            )

        outputs = _Outputs(pubsub, resources.outputs_subscription)
        # The canary proves the job is genuinely live before the update begins.
        outputs.await_record(KEY_CANARY, launcher.VERB_PONG, deadline_s=PHASE_ONE_DEADLINE_S)
        written = outputs.await_record(
            KEY_MEMORY, launcher.VERB_REMEMBERED, deadline_s=PHASE_ONE_DEADLINE_S
        )
        assert written.detail == marker
        # The suspension is live once its APPROVAL intent is on the wire: the
        # Continuation and PENDING entry committed in the same bundle.
        pending = _await_approval_intent(
            pubsub, resources.intents_subscription, KEY_SUSPEND, deadline_s=PHASE_ONE_DEADLINE_S
        )

        # -- the update -------------------------------------------------------
        try:
            update_job_id = _launch(
                provisioned.head,
                phase=launcher.PHASE_UPDATE,
                resources=resources,
                ledger=ledger,
            )
        except Exception as exc:
            # Submission itself can carry the refusal; classify before reporting.
            raise classify_update_failure(
                replacement=None, previous=jobs.get(launch_job_id), plan=plan, error=exc
            ) from exc

        try:
            _await_state(
                jobs,
                update_job_id,
                states=frozenset({JOB_STATE_RUNNING}),
                deadline_s=UPDATE_DEADLINE_S,
                what="the head replacement job to reach RUNNING",
            )
            _await_state(
                jobs,
                launch_job_id,
                states=frozenset({JOB_STATE_UPDATED}),
                deadline_s=UPDATE_DEADLINE_S,
                what=f"the {plan.launch_version} job to hand over (JOB_STATE_UPDATED)",
            )
        except PollTimeout as exc:
            raise classify_update_failure(
                replacement=jobs.get(update_job_id),
                previous=jobs.get(launch_job_id),
                plan=plan,
                error=exc,
            ) from exc

        # -- phase 2: the same state, under head ------------------------------
        pubsub.publish(
            resources.events_topic,
            launcher.approval_envelope(
                KEY_SUSPEND, pending.intent_id, approved=True, decided_at_ms=_now_ms()
            ).SerializeToString(),
        )
        try:
            resumed = outputs.await_record(
                KEY_SUSPEND, launcher.VERB_RESUMED, deadline_s=PHASE_TWO_DEADLINE_S
            )
        except PollTimeout as exc:
            raise state_loss(
                f"{KEY_SUSPEND!r} never resumed after its approval was injected "
                f"post-update (intent {pending.intent_id}); the persisted Continuation "
                "did not survive the job replacement",
                plan=plan,
                job_ids=(launch_job_id, update_job_id),
            ) from exc
        if resumed.detail != nonce:
            raise state_loss(
                f"{KEY_SUSPEND!r} resumed with snapshot {resumed.detail!r}, expected the "
                f"pre-update continuation nonce {nonce!r}: the key restarted rather "
                "than resuming",
                plan=plan,
                job_ids=(launch_job_id, update_job_id),
            )

        pubsub.publish(
            resources.events_topic,
            launcher.event_envelope(
                KEY_MEMORY, launcher.CMD_ECHO, event_time_ms=_now_ms()
            ).SerializeToString(),
        )
        try:
            echoed = outputs.await_record(
                KEY_MEMORY, launcher.VERB_ECHO, deadline_s=PHASE_TWO_DEADLINE_S
            )
        except PollTimeout as exc:
            raise state_loss(
                f"{KEY_MEMORY!r} produced no echo on the updated job",
                plan=plan,
                job_ids=(launch_job_id, update_job_id),
            ) from exc
        if echoed.detail != marker:
            raise state_loss(
                f"{KEY_MEMORY!r} echoed {echoed.detail!r}, expected the marker "
                f"{marker!r} written by the {plan.launch_version} job: the MemoryBlob "
                "did not survive the job replacement",
                plan=plan,
                job_ids=(launch_job_id, update_job_id),
            )

        # A previously unseen key proves the updated job handles new work too.
        pubsub.publish(
            resources.events_topic,
            launcher.event_envelope(
                KEY_POST, launcher.CMD_PING, event_time_ms=_now_ms()
            ).SerializeToString(),
        )
        try:
            outputs.await_record(KEY_POST, launcher.VERB_PONG, deadline_s=PHASE_TWO_DEADLINE_S)
        except PollTimeout as exc:
            raise state_loss(
                f"the updated job never completed the fresh key {KEY_POST!r}",
                plan=plan,
                job_ids=(launch_job_id, update_job_id),
            ) from exc

        assert not outputs.undecodable, (
            f"the pipeline emitted outputs this gate cannot parse: {outputs.undecodable!r}"
        )
