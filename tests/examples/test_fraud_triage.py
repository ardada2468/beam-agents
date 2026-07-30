"""`examples/fraud_triage.py`, exercised — the doc-example-verified-by-test pattern.

`docs/examples/fraud-triage.md` renders `examples/fraud_triage.py` by snippet
inclusion, so the module under test here is byte-identical to the code the site
shows. These assertions pin the two outcomes that page documents — the approved
account's freeze decision and the unanswered account's fail-closed deny — by
driving the module's own scripted `TestStream` under a streaming `TestPipeline`.
"""

from __future__ import annotations

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from examples.fraud_triage import ACCOUNT_A, ACCOUNT_B, APPROVAL_INTENT_ID, build

from beam_agents._protos import ToolIntent
from beam_agents.hitl import HITL_TIMEOUT_OUTPUT


def _streaming_pipeline() -> BeamTestPipeline:
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    return BeamTestPipeline(options=options)


# --- Requirement: the fraud-triage example demonstrates suspension, approval
# --- resume, and the fail-closed timeout ---------------------------------------


def test_approved_account_resumes_to_a_freeze_decision() -> None:
    # Scenario: Approved account resumes to a freeze decision. Account A's
    # scripted approval re-enters on the same key before the deadline; the
    # resumed activation emits the documented freeze output, and exactly one
    # approval intent for that account appears on `.intents`.
    with _streaming_pipeline() as p:
        out = build(p)
        freezes = out.output | "freezes" >> beam.Filter(lambda o: o.startswith(b"freeze:"))
        assert_that(freezes, equal_to([b"freeze:" + ACCOUNT_A]), label="freeze-output")
        approvals_a = (
            out.intents
            | "acct-a-intents" >> beam.Filter(lambda i: i.entity_key == ACCOUNT_A)
            | "identity" >> beam.Map(lambda i: (i.intent_id, i.kind))
        )
        assert_that(
            approvals_a,
            equal_to([(APPROVAL_INTENT_ID, ToolIntent.APPROVAL)]),
            label="one-approval-intent",
        )
        assert_that(out.errors, equal_to([]), label="no-errors")


def test_unanswered_account_fails_closed_at_the_deadline() -> None:
    # Scenario: Unanswered account fails closed at the deadline. No decision
    # ever arrives for account B; the scripted processing-time advance elapses
    # its deadline, the default deny route emits its deterministic fallback
    # output, and no freeze decision is ever emitted for that key.
    with _streaming_pipeline() as p:
        out = build(p)
        denials = out.output | "denials" >> beam.Filter(lambda o: o == HITL_TIMEOUT_OUTPUT)
        assert_that(denials, equal_to([HITL_TIMEOUT_OUTPUT]), label="deny-fallback")
        freezes_b = out.output | "freezes-b" >> beam.Filter(lambda o: o == b"freeze:" + ACCOUNT_B)
        assert_that(freezes_b, equal_to([]), label="no-freeze-for-b")
