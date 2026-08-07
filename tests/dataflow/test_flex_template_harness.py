"""Unit tests for the Flex Template gate's harness — the parts that can lie.

The gate itself needs real Dataflow and runs once a night, but the launch
command it builds and the verdict it renders are ordinary code, and a bug in
either turns a nightly into a misleading report rather than a failure: a launch
missing a location flag sends the service reaching for a bucket the CI
principal may not create, and a classifier that files an IAM refusal under
"packaging defect" points triage at the template for a problem that is entirely
in the project's IAM policy.

Offline, like `test_update_compat_harness.py` beside it: pure functions only, no
GCP, and deliberately **no `dataflow` marker**, so these run in the required
unit lane while the gate module they import from is deselected.
"""

from __future__ import annotations

import pytest

from tests.dataflow.test_flex_template_launch import (
    GcpConfig,
    RunTopics,
    launch_command,
    looks_like_environment_failure,
)

_GCP = GcpConfig(
    project="MY_PROJECT",
    region="us-east1",
    temp_bucket="MY_BUCKET",
    spec="gs://MY_BUCKET/templates/fraud-flex-abc123.json",
)


def _flag(command: list[str], name: str) -> str:
    """The single `--name=value` in `command`, or a failed assertion."""
    matches = [arg for arg in command if arg.startswith(f"{name}=")]
    assert len(matches) == 1, f"expected exactly one {name} in {command}"
    return matches[0].split("=", 1)[1]


def test_the_launch_names_its_own_staging_location() -> None:
    """`--temp-location` defaults *from* `--staging-location`, not the reverse.

    With staging unset the service falls back to the per-project default bucket
    and tries to create it, which failed the 2026-08-07 night outright: a CI
    principal holding object access on the temp bucket does not hold
    `storage.buckets.create` on the project. Both locations must therefore be
    named, and both must sit under this run's own prefix.
    """
    command = launch_command(_GCP, RunTopics(run_id="20260807-abc", project="MY_PROJECT"), "job-1")

    staging = _flag(command, "--staging-location")
    temp = _flag(command, "--temp-location")
    assert staging.startswith("gs://MY_BUCKET/flex-template/job-1")
    assert temp.startswith("gs://MY_BUCKET/flex-template/job-1")


@pytest.mark.parametrize(
    "stderr",
    [
        # Verbatim from the 2026-08-07 nightly, the failure that motivated this.
        "ERROR: (gcloud.dataflow.flex-template.run) FAILED_PRECONDITION: "
        "(ce0815e1474a7cb6): Default temp bucket "
        "dataflow-staging-us-east1-47892149884 cannot be created due to error "
        "PERMISSION_DENIED.",
        "ERROR: Quota exceeded for quota metric 'CPUs'",
        "The caller does not have permission to act as the worker service account",
    ],
)
def test_an_authorization_or_quota_refusal_is_an_environment_failure(stderr: str) -> None:
    assert looks_like_environment_failure(stderr)


@pytest.mark.parametrize(
    "stderr",
    [
        # A bare FAILED_PRECONDITION is the launcher rejecting the template's own
        # preconditions: the packaging defect this gate exists to catch, and it
        # must never be filed as noise.
        "ERROR: FAILED_PRECONDITION: the template spec names no image",
        "ERROR: Invalid value for [--parameters]: unknown parameter 'modle'",
        "ERROR: unable to import 'examples.fraud_triage:make_provider'",
    ],
)
def test_a_packaging_or_parameter_refusal_is_not_reclassified_as_environment(
    stderr: str,
) -> None:
    assert not looks_like_environment_failure(stderr)
