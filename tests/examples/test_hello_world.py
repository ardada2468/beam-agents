"""`examples/hello_world.py`, exercised — the doc-example-verified-by-test pattern.

`docs/examples/hello-world.md` renders `examples/hello_world.py` by snippet
inclusion, so the module under test here is byte-identical to the code the
site shows. These assertions pin the outputs that page documents: changing the
example so they no longer hold is a defect in the example, never in this test.
"""

from __future__ import annotations

from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to

from examples.hello_world import GREETING, build

# --- Requirement: the hello-world example demonstrates the minimal fast path ---


def test_one_event_in_one_output_out() -> None:
    # Scenario: One event in, one output out. `.output` carries exactly the
    # FakeLLM-scripted response; `.intents` and `.errors` are empty.
    with BeamTestPipeline() as p:
        out = build(p)
        assert_that(out.output, equal_to([GREETING]), label="output")
        assert_that(out.intents, equal_to([]), label="no-intents")
        assert_that(out.errors, equal_to([]), label="no-errors")
