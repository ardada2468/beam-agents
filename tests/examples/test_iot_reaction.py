"""`examples/iot_reaction.py`, exercised — the doc-example-verified-by-test pattern.

`docs/examples/iot-reaction.md` renders `examples/iot_reaction.py` by snippet
inclusion, so the module under test here is byte-identical to the code the site
shows. These assertions pin the page's two claims: quiet readings grow the
per-device memory window without a single model call, and a threshold breach
triggers exactly one scripted reaction.

The recording factory below is module-level (not a closure) so the DoFn holding
it pickles by reference for the DirectRunner; the created `FakeLLM`s land in a
module-level list this in-process runner shares with the test, making the
fake's recorded call count directly assertable.
"""

from __future__ import annotations

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to

from beam_agents.model.fake import FakeLLM
from examples import iot_reaction
from examples.iot_reaction import DEVICE_HOT, DEVICE_QUIET, QUIET_READINGS, build

_PROVIDERS: list[FakeLLM] = []


def recording_provider() -> FakeLLM:
    """The example's own scripted provider, with every instance kept for counting."""
    provider = iot_reaction.make_provider()
    _PROVIDERS.append(provider)
    return provider


def _fake_calls() -> int:
    return sum(provider.call_count for provider in _PROVIDERS)


def _streaming_pipeline() -> BeamTestPipeline:
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    return BeamTestPipeline(options=options)


# --- Requirement: the IoT-reaction example demonstrates keyed rolling memory ---


def test_quiet_readings_accumulate_memory_without_model_calls() -> None:
    # Scenario: Quiet readings accumulate memory without model calls. Each
    # activation completes, the rolling window grows per reading (window=1,2,3),
    # and the FakeLLM records zero calls.
    _PROVIDERS.clear()
    with _streaming_pipeline() as p:
        out = build(p, readings=QUIET_READINGS, provider_factory=recording_provider)
        quiet = DEVICE_QUIET.decode()
        assert_that(
            out.output,
            equal_to(
                [
                    f"ok:{quiet}:window=1".encode(),
                    f"ok:{quiet}:window=2".encode(),
                    f"ok:{quiet}:window=3".encode(),
                ]
            ),
            label="window-growth",
        )
        assert_that(out.errors, equal_to([]), label="no-errors")
    assert _fake_calls() == 0, f"expected zero model calls, got {_fake_calls()}"


def test_a_threshold_breach_triggers_exactly_one_reaction() -> None:
    # Scenario: A threshold breach triggers exactly one reaction. The hot
    # device's rolling average crosses the threshold once; the agent makes
    # exactly one scripted model call and emits the documented reaction output
    # on that device's key.
    _PROVIDERS.clear()
    with _streaming_pipeline() as p:
        out = build(p, provider_factory=recording_provider)
        reactions = out.output | "reactions" >> beam.Filter(lambda o: o.startswith(b"reaction:"))
        assert_that(
            reactions,
            equal_to([b"reaction:" + DEVICE_HOT + b":throttle"]),
            label="one-reaction",
        )
        assert_that(out.errors, equal_to([]), label="no-errors")
    assert _fake_calls() == 1, f"expected exactly one model call, got {_fake_calls()}"
