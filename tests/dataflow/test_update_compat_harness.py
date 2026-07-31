"""Unit tests for the `--update` gate's harness — the plumbing that could lie.

The gate itself needs real Dataflow, so it runs once a night and nowhere else.
Everything *around* it is ordinary code with ordinary bugs, and each such bug
turns the gate into a liar rather than a failure: a resolver that silently
falls back would report a self-update run as cross-version evidence, a
classifier that reads a refused replacement as infrastructure noise would
discard the exact defect the gate exists to catch, a teardown that stops at the
first error would leave a streaming job billing overnight, and a sweeper with a
sloppy predicate would cancel someone else's job.

Offline (default tier): pure functions, injected clocks, fake clients,
`tmp_path`. No GCP, no network, no docker — and no `dataflow` marker, so these
run in the required unit lane while the gate module beside them is deselected.

Derived from the `state-guarantees` scenarios "A refused compatibility check is
reported as the defect it is", "A failing run leaves nothing behind", "A
crashed run is bounded to one night", and "Before any PyPI release exists, the
gate runs a labelled self-update leg".
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest

from beam_agents._protos import AgentEnvelope
from tests.dataflow._update import pipeline as launcher
from tests.dataflow._update.poll import (
    InfraFailure,
    JobStatus,
    PollTimeout,
    StateLossFailure,
    UpdateCompatibilityFailure,
    await_condition,
    cancel_body,
    classify_update_failure,
    job_status_from_api,
    job_url,
    jobs_list_url,
    looks_like_compatibility_refusal,
    state_loss,
)
from tests.dataflow._update.resources import (
    LABEL_KEY,
    LABEL_VALUE,
    RunLedger,
    RunResources,
    guaranteed_teardown,
    new_run_id,
    parse_rfc3339,
    sweep_targets,
)
from tests.dataflow._update.resources import provision as provision_resources
from tests.dataflow._update.versions import (
    BOOTSTRAP_LABEL,
    CROSS_VERSION_LABEL,
    VersionPlan,
    build_head_wheel_command,
    create_venv_command,
    download_wheel_command,
    find_wheel,
    install_wheel_command,
    latest_release,
    provision,
    resolve_plan,
    venv_python,
)

# -- version resolution: the bootstrap leg must never masquerade as evidence ----


def _pypi(*versions: str, yanked: tuple[str, ...] = ()) -> dict[str, Any]:
    """A PyPI JSON payload shaped like the real one, for the versions given."""
    return {
        "info": {"name": "beam-agents"},
        "releases": {
            version: [{"filename": f"beam_agents-{version}-py3-none-any.whl", "yanked": False}]
            if version not in yanked
            else [{"filename": f"beam_agents-{version}-py3-none-any.whl", "yanked": True}]
            for version in versions
        },
    }


def test_the_resolver_returns_the_latest_release_at_or_below_head() -> None:
    payload = _pypi("0.1.0", "0.2.0", "0.10.0")
    assert latest_release(payload, head_version="0.10.0") == "0.10.0"
    # Numeric, not lexicographic: 0.10.0 > 0.2.0.
    assert latest_release(payload, head_version="0.9.0") == "0.2.0"


def test_the_resolver_ignores_yanked_prerelease_and_fileless_entries() -> None:
    payload = _pypi("0.1.0", "0.2.0", yanked=("0.2.0",))
    payload["releases"]["0.3.0rc1"] = [{"filename": "x.whl", "yanked": False}]
    payload["releases"]["0.4.0"] = []  # a version with every file removed
    assert latest_release(payload, head_version="1.0.0") == "0.1.0"


def test_a_release_newer_than_head_is_never_chosen() -> None:
    """The promise is forward-only: updating head *from* a newer release is a
    downgrade, which the policy declares unsupported.
    """
    assert latest_release(_pypi("0.5.0"), head_version="0.4.0") is None


def test_no_release_yields_a_prominently_labelled_bootstrap_plan() -> None:
    """Scenario: "Before any PyPI release exists, the gate runs a labelled
    self-update leg"."""
    plan = resolve_plan(head_version="0.1.0", fetch=_pypi)

    assert plan.is_bootstrap
    assert plan.previous_version is None
    assert plan.label == BOOTSTRAP_LABEL
    assert BOOTSTRAP_LABEL.upper() == BOOTSTRAP_LABEL  # capitals, per design D7
    assert BOOTSTRAP_LABEL in plan.report()
    assert "0.1.0" in plan.report()


def test_a_pypi_outage_falls_back_loudly_rather_than_silently() -> None:
    """Resolution failure is classified and *named* in the report, never swallowed."""

    def explode() -> dict[str, Any]:
        raise OSError("pypi.org unreachable")

    plan = resolve_plan(head_version="0.1.0", fetch=explode)

    assert plan.is_bootstrap
    assert "pypi.org unreachable" in plan.reason
    assert BOOTSTRAP_LABEL in plan.report()


def test_a_found_release_arms_the_cross_version_leg_with_both_versions_named() -> None:
    plan = resolve_plan(head_version="0.2.0", fetch=lambda: _pypi("0.1.0"))

    assert not plan.is_bootstrap
    assert plan.previous_version == "0.1.0"
    assert plan.label == CROSS_VERSION_LABEL
    report = plan.report()
    assert "0.1.0" in report and "0.2.0" in report


# -- provisioning: two interpreters, one launcher source ------------------------


class _RecordingRunner:
    """Records the commands `provision` issues and materializes their artifacts."""

    def __init__(self) -> None:
        self.commands: list[list[str]] = []

    def __call__(self, command: list[str], **_kwargs: object) -> str:
        self.commands.append(list(command))
        # `uv build` / `pip download` write a wheel into their --out-dir/--dest.
        for flag in ("--out-dir", "--dest"):
            if flag in command:
                out = Path(command[command.index(flag) + 1])
                out.mkdir(parents=True, exist_ok=True)
                version = "0.1.0" if flag == "--dest" else "0.2.0"
                (out / f"beam_agents-{version}-py3-none-any.whl").write_bytes(b"")
        return ""


def test_the_cross_version_plan_provisions_a_pypi_wheel_into_its_own_venv(
    tmp_path: Path,
) -> None:
    plan = VersionPlan(head_version="0.2.0", previous_version="0.1.0", reason="")
    runner = _RecordingRunner()

    provisioned = provision(plan, workdir=tmp_path, run=runner)

    flattened = [" ".join(command) for command in runner.commands]
    assert any("uv build" in command for command in flattened), flattened
    assert any(
        "pip download" in command and "beam-agents==0.1.0" in command for command in flattened
    )
    assert any("venv" in command for command in flattened)
    assert any("pip install" in command for command in flattened)
    # Both legs log their full resolution — a compat failure is meaningless
    # without knowing which two environments collided (design D3).
    assert sum("freeze" in command for command in flattened) == 2

    assert provisioned.launch.version == "0.1.0"
    assert provisioned.head.version == "0.2.0"
    # Genuinely two interpreters: prev runs from its own venv.
    assert provisioned.launch.python != provisioned.head.python
    assert provisioned.launch.wheel != provisioned.head.wheel


def test_the_bootstrap_plan_provisions_one_environment_used_for_both_legs(
    tmp_path: Path,
) -> None:
    plan = VersionPlan(head_version="0.2.0", previous_version=None, reason="no release")
    runner = _RecordingRunner()

    provisioned = provision(plan, workdir=tmp_path, run=runner)

    flattened = [" ".join(command) for command in runner.commands]
    assert not any("pip download" in command for command in flattened), flattened
    assert provisioned.launch == provisioned.head
    assert provisioned.launch.label == BOOTSTRAP_LABEL


def test_the_provisioning_commands_are_explicit_about_what_they_install(
    tmp_path: Path,
) -> None:
    assert "--no-deps" in download_wheel_command("0.1.0", tmp_path)
    assert "beam-agents==0.1.0" in download_wheel_command("0.1.0", tmp_path)
    assert "--wheel" in build_head_wheel_command(tmp_path)
    assert str(tmp_path) in create_venv_command(tmp_path)
    wheel = tmp_path / "beam_agents-0.1.0-py3-none-any.whl"
    wheel.write_bytes(b"")
    assert str(wheel) in install_wheel_command(venv_python(tmp_path), wheel)
    assert find_wheel(tmp_path, version="0.1.0") == wheel


def test_finding_a_wheel_for_a_version_that_was_not_built_is_an_error(tmp_path: Path) -> None:
    (tmp_path / "beam_agents-0.9.9-py3-none-any.whl").write_bytes(b"")
    with pytest.raises(FileNotFoundError, match=re.escape("0.1.0")):
        find_wheel(tmp_path, version="0.1.0")


# -- the launcher's builders: identical source, both interpreters ---------------


def test_event_envelopes_round_trip_through_agent_envelope_bytes() -> None:
    envelope = launcher.event_envelope(b"K-memory", "remember:marker-7", event_time_ms=1_700)

    decoded = AgentEnvelope()
    decoded.ParseFromString(envelope.SerializeToString())

    assert decoded.entity_key == b"K-memory"
    assert decoded.event_time_ms == 1_700
    assert decoded.external_event == b"remember:marker-7"


def test_approval_envelopes_round_trip_through_agent_envelope_bytes() -> None:
    envelope = launcher.approval_envelope(
        b"K-suspend", "intent-42", approved=True, decided_at_ms=9_000
    )

    decoded = AgentEnvelope()
    decoded.ParseFromString(envelope.SerializeToString())

    assert decoded.entity_key == b"K-suspend"
    assert decoded.WhichOneof("payload") == "approval"
    assert decoded.approval.intent_id == "intent-42"
    assert decoded.approval.approved is True
    assert decoded.approval.decided_at_ms == 9_000


def test_agent_outputs_round_trip_and_carry_the_key_verb_detail_and_seq() -> None:
    """Outputs are read off a Pub/Sub topic with no key, so they carry their own."""
    record = launcher.decode_output(
        launcher.encode_output(b"K-suspend", launcher.VERB_RESUMED, "nonce-1", seq=3)
    )

    assert record.key == b"K-suspend"
    assert record.verb == launcher.VERB_RESUMED
    assert record.detail == "nonce-1"
    assert record.seq == 3


def test_a_malformed_output_is_an_error_not_a_silently_empty_record() -> None:
    with pytest.raises(ValueError, match="unparseable"):
        launcher.decode_output(b"not-an-output")


def test_the_update_phase_carries_update_and_reuses_the_job_name() -> None:
    """Dataflow replaces a job by name; a fresh name would launch a second job
    and quietly test nothing.
    """
    common: dict[str, Any] = {
        "project": "p",
        "region": "us-central1",
        "job_name": "ba-update-compat-run1",
        "temp_location": "gs://bucket/run1",
        "events_subscription": "projects/p/subscriptions/run1-events",
        "outputs_topic": "projects/p/topics/run1-outputs",
        "intents_topic": "projects/p/topics/run1-intents",
        "extra_package": "/tmp/beam_agents-0.1.0-py3-none-any.whl",
    }

    launch = launcher.beam_args(phase=launcher.PHASE_LAUNCH, **common)
    update = launcher.beam_args(phase=launcher.PHASE_UPDATE, **common)

    assert "--update" not in launch
    assert "--update" in update
    assert "--job_name=ba-update-compat-run1" in launch
    assert "--job_name=ba-update-compat-run1" in update
    # Everything else is identical: the job graph must differ only by version.
    assert [arg for arg in update if arg != "--update"] == launch


def test_the_job_shape_is_one_small_streaming_engine_worker() -> None:
    """Cost bound (design D6), asserted where it is actually expressed."""
    args = launcher.beam_args(
        phase=launcher.PHASE_LAUNCH,
        project="p",
        region="us-central1",
        job_name="j",
        temp_location="gs://bucket/j",
        events_subscription="projects/p/subscriptions/e",
        outputs_topic="projects/p/topics/o",
        intents_topic="projects/p/topics/i",
        extra_package="/tmp/w.whl",
    )

    assert "--streaming" in args
    assert "--enable_streaming_engine" in args
    assert "--max_num_workers=1" in args
    assert f"--labels={LABEL_KEY}={LABEL_VALUE}" in args
    assert "--extra_package=/tmp/w.whl" in args
    assert "--save_main_session" in args


def test_the_launcher_reports_its_job_id_on_a_parseable_line() -> None:
    stdout = f"noise\n{launcher.JOB_ID_PREFIX}2026-07-31_00_11_22-1234\nmore noise\n"
    assert launcher.parse_job_id(stdout) == "2026-07-31_00_11_22-1234"


def test_a_launcher_that_printed_no_job_id_is_an_error() -> None:
    with pytest.raises(ValueError, match="no job id"):
        launcher.parse_job_id("Traceback (most recent call last): ...")


# -- failure classification: the gate's verdict must survive triage ------------


_PLAN = VersionPlan(head_version="0.2.0", previous_version="0.1.0", reason="")


def _job(job_id: str, state: str, *, message: str = "") -> JobStatus:
    return JobStatus(job_id=job_id, name="ba-update-compat", state=state, message=message)


def test_a_refused_replacement_beside_a_healthy_original_is_a_compatibility_failure() -> None:
    """Scenario: "A refused compatibility check is reported as the defect it is".

    The asymmetry is the whole point: Dataflow fails the *new* job and leaves
    the old one running. A naive harness calls that "job failed to start".
    """
    failure = classify_update_failure(
        replacement=_job("job-new", "JOB_STATE_FAILED", message="Workflow failed."),
        previous=_job("job-old", "JOB_STATE_RUNNING"),
        plan=_PLAN,
    )

    assert isinstance(failure, UpdateCompatibilityFailure)
    text = str(failure)
    assert "0.1.0" in text and "0.2.0" in text  # both resolved versions named
    assert "job-new" in text and "job-old" in text
    assert "Workflow failed." in text


def test_a_refusal_named_by_the_service_is_a_compatibility_failure_however_it_ends() -> None:
    """The message is authoritative even when job states are ambiguous."""
    failure = classify_update_failure(
        replacement=_job(
            "job-new",
            "JOB_STATE_CANCELLED",
            message="The new job is not compatible with the running job: step RunAgent/Activate",
        ),
        previous=_job("job-old", "JOB_STATE_CANCELLED"),
        plan=_PLAN,
    )

    assert isinstance(failure, UpdateCompatibilityFailure)


@pytest.mark.parametrize(
    "message",
    [
        "The new job is not compatible with the running job.",
        "The Coder or type for step RunAgent/Agent has changed.",
        "Job graph is incompatible: unmatched step RunAgent/KeyByEntity",
        "Provide a transform_name_mapping to rename the step.",
    ],
)
def test_the_service_vocabulary_for_a_refusal_is_recognized(message: str) -> None:
    assert looks_like_compatibility_refusal(message)


@pytest.mark.parametrize(
    "message",
    [
        "Startup of the worker pool in zone us-central1-a failed to bring up any workers.",
        "Quota exceeded for quota metric 'CPUs'.",
        "",
    ],
)
def test_infrastructure_messages_are_not_read_as_refusals(message: str) -> None:
    assert not looks_like_compatibility_refusal(message)


def test_a_replacement_that_died_alongside_its_original_is_infrastructure() -> None:
    """Both jobs down with no compatibility vocabulary: the environment broke,
    and the run says nothing about compatibility.
    """
    failure = classify_update_failure(
        replacement=_job(
            "job-new", "JOB_STATE_FAILED", message="Quota exceeded for quota metric 'CPUs'."
        ),
        previous=_job("job-old", "JOB_STATE_FAILED", message="Workers lost."),
        plan=_PLAN,
    )

    assert isinstance(failure, InfraFailure)
    assert not isinstance(failure, UpdateCompatibilityFailure)


def test_a_provisioning_error_is_infrastructure_and_never_a_verdict() -> None:
    failure = classify_update_failure(
        replacement=None,
        previous=_job("job-old", "JOB_STATE_RUNNING"),
        plan=_PLAN,
        error=RuntimeError("pip download failed: connection reset"),
    )

    assert isinstance(failure, InfraFailure)
    assert "pip download failed" in str(failure)


def test_a_phase_two_assertion_timeout_is_state_loss_not_infrastructure() -> None:
    """Scenario: the update succeeded, so a missing resume/echo means the state
    did not survive — the gate's second red, distinct from a refusal.
    """
    failure = state_loss(
        "K-suspend never produced a resumed output after the approval was injected",
        plan=_PLAN,
        job_ids=("job-old", "job-new"),
    )

    assert isinstance(failure, StateLossFailure)
    assert not isinstance(failure, UpdateCompatibilityFailure)
    assert not isinstance(failure, InfraFailure)
    assert "0.1.0" in str(failure) and "0.2.0" in str(failure)
    assert "K-suspend" in str(failure)


# -- deadline-driven polling: no sleep-based correctness -----------------------


class _FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def test_a_poller_returns_the_first_satisfying_observation() -> None:
    clock = _FakeClock()
    observations = iter([None, None, "RUNNING"])

    result = await_condition(
        lambda: next(observations),
        deadline_s=60,
        interval_s=5,
        description="job to start",
        clock=clock,
        sleep=clock.sleep,
    )

    assert result == "RUNNING"
    assert clock.now == 10  # two intervals, no wall-clock waiting


def test_a_poller_gives_up_at_its_deadline_naming_what_it_waited_for() -> None:
    clock = _FakeClock()

    with pytest.raises(PollTimeout, match="job to start"):
        await_condition(
            lambda: None,
            deadline_s=30,
            interval_s=5,
            description="job to start",
            clock=clock,
            sleep=clock.sleep,
        )

    assert clock.now <= 30 + 5  # bounded: it never polls unboundedly


# -- Dataflow REST shapes ------------------------------------------------------


def test_job_urls_are_regional_and_the_cancel_body_force_cancels() -> None:
    assert jobs_list_url("proj", "us-central1").endswith(
        "/v1b3/projects/proj/locations/us-central1/jobs"
    )
    assert job_url("proj", "us-central1", "job-1").endswith(
        "/v1b3/projects/proj/locations/us-central1/jobs/job-1"
    )
    # Cancel, not drain: draining waits on watermarks the gate does not care about.
    assert cancel_body() == {"requestedState": "JOB_STATE_CANCELLED"}


def test_a_job_status_is_read_off_the_api_payload_including_its_labels() -> None:
    status = job_status_from_api(
        {
            "id": "job-1",
            "name": "ba-update-compat-run1",
            "currentState": "JOB_STATE_RUNNING",
            "createTime": "2026-07-31T01:02:03.456Z",
            "labels": {LABEL_KEY: LABEL_VALUE},
        }
    )

    assert status.job_id == "job-1"
    assert status.state == "JOB_STATE_RUNNING"
    assert status.is_active
    assert not status.is_terminal
    assert status.labels[LABEL_KEY] == LABEL_VALUE


# -- resources: naming, sweeping, and total teardown ---------------------------


def test_every_provisioned_name_embeds_the_run_id() -> None:
    resources = RunResources(
        run_id="20260731-ab12cd", project="proj", region="us-central1", temp_bucket="my-bucket"
    )

    names = [
        resources.job_name,
        resources.events_topic,
        resources.events_subscription,
        resources.outputs_topic,
        resources.outputs_subscription,
        resources.intents_topic,
        resources.intents_subscription,
        resources.temp_location,
    ]
    for name in names:
        assert "20260731-ab12cd" in name, name
    assert resources.labels == {LABEL_KEY: LABEL_VALUE}
    assert resources.temp_location.startswith("gs://my-bucket/")


def test_run_ids_are_dated_and_unique_per_run() -> None:
    first = new_run_id(now=1_785_000_000.0)
    second = new_run_id(now=1_785_000_000.0)

    assert first.startswith("2026")
    assert first != second


def test_the_sweeper_selects_only_labelled_active_jobs_older_than_the_threshold() -> None:
    """Scenario: "A crashed run is bounded to one night"."""
    now = parse_rfc3339("2026-07-31T06:00:00Z")
    stale = JobStatus(
        job_id="stale",
        name="ba-update-compat-old",
        state="JOB_STATE_RUNNING",
        create_time="2026-07-30T06:00:00Z",
        labels={LABEL_KEY: LABEL_VALUE},
    )
    fresh = JobStatus(
        job_id="fresh",
        name="ba-update-compat-now",
        state="JOB_STATE_RUNNING",
        create_time="2026-07-31T05:30:00Z",
        labels={LABEL_KEY: LABEL_VALUE},
    )
    someone_else = JobStatus(
        job_id="theirs",
        name="production-pipeline",
        state="JOB_STATE_RUNNING",
        create_time="2026-01-01T00:00:00Z",
        labels={},
    )
    already_done = JobStatus(
        job_id="done",
        name="ba-update-compat-done",
        state="JOB_STATE_CANCELLED",
        create_time="2026-07-30T06:00:00Z",
        labels={LABEL_KEY: LABEL_VALUE},
    )

    targets = sweep_targets(
        [stale, fresh, someone_else, already_done], now_s=now, max_age_s=2 * 3600
    )

    assert [job.job_id for job in targets] == ["stale"]


class _FakeJobs:
    def __init__(self, *, failing: frozenset[str] = frozenset()) -> None:
        self.cancelled: list[str] = []
        self._failing = failing

    def cancel(self, job_id: str) -> None:
        self.cancelled.append(job_id)
        if job_id in self._failing:
            raise RuntimeError(f"cancel {job_id} failed")


class _FakePubSub:
    def __init__(self, *, failing: frozenset[str] = frozenset()) -> None:
        self.deleted_topics: list[str] = []
        self.deleted_subscriptions: list[str] = []
        self._failing = failing

    def delete_topic(self, name: str) -> None:
        self.deleted_topics.append(name)
        if name in self._failing:
            raise RuntimeError(f"delete topic {name} failed")

    def delete_subscription(self, name: str) -> None:
        self.deleted_subscriptions.append(name)
        if name in self._failing:
            raise RuntimeError(f"delete subscription {name} failed")


def _ledger() -> RunLedger:
    return RunLedger(
        job_ids=["job-old", "job-new"],
        topics=["t-events", "t-outputs"],
        subscriptions=["s-events", "s-outputs"],
    )


class _TimeoutSignal(BaseException):
    """Stands in for pytest-timeout's asynchronous interruption."""


@pytest.mark.parametrize(
    "raised",
    [None, AssertionError("the suspension never resumed"), _TimeoutSignal("timeout")],
    ids=["success", "failure", "timeout"],
)
def test_teardown_cancels_and_deletes_everything_on_every_exit_path(
    raised: BaseException | None,
) -> None:
    """Scenario: "A failing run leaves nothing behind" — on pass, fail, and timeout."""
    jobs, pubsub, ledger = _FakeJobs(), _FakePubSub(), _ledger()

    def body() -> None:
        if raised is not None:
            raise raised

    if raised is None:
        with guaranteed_teardown(ledger, jobs=jobs, pubsub=pubsub):
            body()
    else:
        with pytest.raises(type(raised)), guaranteed_teardown(ledger, jobs=jobs, pubsub=pubsub):
            body()

    assert jobs.cancelled == ["job-old", "job-new"]
    assert pubsub.deleted_subscriptions == ["s-events", "s-outputs"]
    assert pubsub.deleted_topics == ["t-events", "t-outputs"]


def test_teardown_is_total_even_when_one_cleanup_step_fails() -> None:
    """One dead resource must not strand the rest — that is how a job bills
    overnight."""
    jobs = _FakeJobs(failing=frozenset({"job-old"}))
    pubsub = _FakePubSub(failing=frozenset({"s-events"}))
    ledger = _ledger()

    with guaranteed_teardown(ledger, jobs=jobs, pubsub=pubsub):
        pass

    assert jobs.cancelled == ["job-old", "job-new"]
    assert pubsub.deleted_subscriptions == ["s-events", "s-outputs"]
    assert pubsub.deleted_topics == ["t-events", "t-outputs"]
    assert ledger.teardown_errors  # recorded, not swallowed


def test_teardown_never_replaces_the_failure_that_caused_it() -> None:
    """A cleanup error must not mask the assertion that made the gate red."""
    jobs = _FakeJobs(failing=frozenset({"job-old"}))
    pubsub = _FakePubSub()

    with (
        pytest.raises(StateLossFailure, match="memory echo"),
        guaranteed_teardown(_ledger(), jobs=jobs, pubsub=pubsub),
    ):
        raise state_loss("memory echo missing", plan=_PLAN, job_ids=("job-old",))


def test_a_run_only_tears_down_what_it_actually_created() -> None:
    """The ledger is appended to as resources come up, so a crash halfway
    through provisioning cancels exactly what exists.
    """
    jobs, pubsub = _FakeJobs(), _FakePubSub()
    ledger = RunLedger()
    ledger.topics.append("t-events")

    with guaranteed_teardown(ledger, jobs=jobs, pubsub=pubsub):
        ledger.job_ids.append("job-old")

    assert jobs.cancelled == ["job-old"]
    assert pubsub.deleted_topics == ["t-events"]
    assert pubsub.deleted_subscriptions == []


def test_the_launcher_command_line_names_every_resource_the_leg_needs() -> None:
    """Both interpreters get the same argv shape; only `--phase` differs."""
    argv = launcher.launcher_argv(
        phase=launcher.PHASE_UPDATE,
        project="p",
        region="us-central1",
        job_name="ba-update-compat-run1",
        temp_location="gs://bucket/run1",
        events_subscription="projects/p/subscriptions/run1-events",
        outputs_topic="projects/p/topics/run1-outputs",
        intents_topic="projects/p/topics/run1-intents",
        extra_package="/tmp/w.whl",
    )

    assert f"--phase={launcher.PHASE_UPDATE}" in argv
    assert "--events-subscription=projects/p/subscriptions/run1-events" in argv
    assert "--outputs-topic=projects/p/topics/run1-outputs" in argv
    assert "--intents-topic=projects/p/topics/run1-intents" in argv
    assert "--extra-package=/tmp/w.whl" in argv


def test_an_unknown_phase_is_refused_rather_than_silently_launching() -> None:
    with pytest.raises(ValueError, match="unknown phase"):
        launcher.beam_args(
            phase="drain",
            project="p",
            region="us-central1",
            job_name="j",
            temp_location="gs://b/j",
            events_subscription="s",
            outputs_topic="o",
            intents_topic="i",
            extra_package="w.whl",
        )


class _RecordingPubSub(_FakePubSub):
    def __init__(self) -> None:
        super().__init__()
        self.created_topics: list[str] = []
        self.created_subscriptions: list[tuple[str, str]] = []

    def create_topic(self, name: str) -> None:
        self.created_topics.append(name)

    def create_subscription(self, name: str, topic: str) -> None:
        self.created_subscriptions.append((name, topic))


def test_provisioning_records_each_resource_in_the_ledger_as_it_creates_it() -> None:
    """The ledger is what makes teardown total, so it must be written eagerly."""
    resources = RunResources(
        run_id="20260731-ab12cd", project="proj", region="us-central1", temp_bucket="b"
    )
    pubsub = _RecordingPubSub()
    ledger = RunLedger()

    provision_resources(resources, pubsub=pubsub, ledger=ledger)

    assert ledger.topics == [
        resources.events_topic,
        resources.outputs_topic,
        resources.intents_topic,
    ]
    assert ledger.subscriptions == [
        resources.events_subscription,
        resources.outputs_subscription,
        resources.intents_subscription,
    ]
    assert pubsub.created_topics == ledger.topics
    assert [name for name, _topic in pubsub.created_subscriptions] == ledger.subscriptions
