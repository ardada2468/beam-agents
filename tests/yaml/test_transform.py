"""Tests for the `yaml-provider` capability's row boundary and multi-output naming.

Requirements: "A fully-qualified YAML-facing transform constructor wraps
RunAgent", "Input rows are keyed and enveloped; malformed rows dead-letter
instead of crashing", and "The four outputs are addressable by name from YAML"
— every scenario, driven on a DirectRunner `TestPipeline` with `FakeLLM`.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock

import apache_beam as beam
import pytest
from apache_beam.pvalue import Row

# Aliased: a bare "TestPipeline" name would be mis-collected by pytest.
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.python_callable import PythonCallableWithSource

import beam_agents.yaml
import beam_agents.yaml._config
import beam_agents.yaml._refs
import beam_agents.yaml.transform
from beam_agents._protos import AgentEnvelope, ToolIntent, TraceEvent
from beam_agents.core.dofn import ActivationError
from beam_agents.core.error_records import activation_error_to_row
from beam_agents.core.transform import DefaultSinkResolver
from beam_agents.yaml import run_agent
from beam_agents.yaml._config import CONFIG_KEYS
from beam_agents.yaml.transform import (
    OUTPUT_NAMES,
    REASON_MALFORMED_ROW,
    RunAgentFromYaml,
    _RowsToEnvelopes,
)
from tests.yaml import _fixtures

FIXTURES = "tests.yaml._fixtures"
PROVIDER = f"{FIXTURES}:make_fake_llm"
_EVENT_TYPE_NAMES = tuple(TraceEvent.EventType.keys())


def _row(**fields: Any) -> Row:
    return Row(**fields)


def _envelopes(**overrides: Any) -> _RowsToEnvelopes:
    defaults: dict[str, Any] = {
        "key_field": "key",
        "payload_field": "payload",
        "event_time_field": None,
    }
    return _RowsToEnvelopes(**{**defaults, **overrides})


# --- Requirement: a fully-qualified constructor wraps RunAgent ----------------


def test_constructor_is_reachable_by_its_fully_qualified_name() -> None:
    # Scenario: Constructor is reachable by its fully-qualified name.
    # Exactly how Beam YAML's `type: python` provider resolves a constructor
    # (apache_beam/yaml/yaml_provider.py::python -> InlineProvider).
    constructor = PythonCallableWithSource.load_from_source("beam_agents.yaml.run_agent")
    transform = constructor(agent=f"{FIXTURES}:echo_agent", provider=PROVIDER)
    assert isinstance(transform, beam.PTransform)
    assert isinstance(transform, RunAgentFromYaml)


def test_beam_agents_yaml_never_imports_apache_beam_yaml() -> None:
    # Scenario: ... and no module under `apache_beam.yaml` has been imported by
    # `beam_agents.yaml`. Checked against every module's *import statements*
    # (design D1's dependency direction is Beam YAML -> us), so an import added
    # later fails here even if some other test already imported Beam's package,
    # and a docstring merely citing Beam's source does not read as one.
    modules: tuple[ModuleType, ...] = (
        beam_agents.yaml,
        beam_agents.yaml._refs,
        beam_agents.yaml._config,
        beam_agents.yaml.transform,
    )
    for module in modules:
        assert module.__file__ is not None
        tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
        for imported in _imported_modules(tree):
            assert not imported.startswith("apache_beam.yaml"), (
                f"{module.__name__} imports {imported}"
            )


def _imported_modules(tree: ast.AST) -> list[str]:
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            names.append(node.module)
            names.extend(f"{node.module}.{alias.name}" for alias in node.names)
    return names


def test_importing_beam_agents_yaml_pulls_in_no_beam_yaml_module() -> None:
    assert beam_agents.yaml.run_agent.__module__ == "beam_agents.yaml.transform"
    assert not any(name.startswith("apache_beam.yaml") for name in sys.modules)


# --- Scenario: Rows are keyed and enveloped by the configured fields ----------


def test_rows_are_keyed_and_enveloped_by_the_configured_fields() -> None:
    with BeamTestPipeline() as p:
        outputs = (
            p
            | beam.Create([_row(key="k1", payload=b"hello"), _row(key="k2", payload=b"world")])
            | run_agent(agent=f"{FIXTURES}:echo_agent", provider=PROVIDER)
        )
        assert_that(
            outputs["output"] | beam.Map(lambda r: r.output),
            equal_to([b"HELLO", b"WORLD"]),
        )


def test_the_wrapped_run_agent_receives_kv_bytes_agent_envelope() -> None:
    # The keying step in isolation: a str key is UTF-8 encoded and the payload
    # lands on `external_event`, so `RunAgent`'s KV validation sees the shape it
    # requires without the caller keying upstream.
    with BeamTestPipeline() as p:
        tagged = p | beam.Create([_row(key="k1", payload=b"hello")]) | _envelopes()
        assert_that(
            tagged["keyed"],
            equal_to([(b"k1", AgentEnvelope(entity_key=b"k1", external_event=b"hello"))]),
        )


def test_bytes_keys_pass_through_and_str_payloads_are_encoded() -> None:
    with BeamTestPipeline() as p:
        tagged = p | beam.Create([_row(key=b"raw", payload="text")]) | _envelopes()
        assert_that(
            tagged["keyed"],
            equal_to([(b"raw", AgentEnvelope(entity_key=b"raw", external_event=b"text"))]),
        )


def test_custom_key_and_payload_fields_are_honored() -> None:
    with BeamTestPipeline() as p:
        tagged = (
            p
            | beam.Create([_row(entity="k9", body=b"z")])
            | _envelopes(key_field="entity", payload_field="body")
        )
        assert_that(tagged["keyed"] | beam.Map(lambda kv: kv[0]), equal_to([b"k9"]))


def test_event_time_field_overrides_the_element_timestamp() -> None:
    with BeamTestPipeline() as p:
        tagged = (
            p
            | beam.Create([TimestampedValue(_row(key="k1", payload=b"a", ts_ms=7_000), 1.0)])
            | _envelopes(event_time_field="ts_ms")
        )
        assert_that(tagged["keyed"] | beam.Map(lambda kv: kv[1].event_time_ms), equal_to([7_000]))


def test_the_element_timestamp_is_used_when_no_event_time_field_is_configured() -> None:
    with BeamTestPipeline() as p:
        tagged = (
            p | beam.Create([TimestampedValue(_row(key="k1", payload=b"a"), 5.0)]) | _envelopes()
        )
        assert_that(tagged["keyed"] | beam.Map(lambda kv: kv[1].event_time_ms), equal_to([5_000]))


# --- Scenario: A row missing the key field dead-letters -----------------------


def test_a_row_missing_the_key_field_dead_letters_naming_the_field() -> None:
    with BeamTestPipeline() as p:
        # Two Creates, flattened: `beam.Create` coerces a heterogeneous element
        # list onto the first element's schema, which would silently drop the
        # `key` field from the good row too.
        good = p | "Good" >> beam.Create([_row(key="k1", payload=b"good")])
        orphan = p | "Orphan" >> beam.Create([_row(payload=b"orphan")])
        outputs = (
            (good, orphan)
            | beam.Flatten()
            | run_agent(agent=f"{FIXTURES}:echo_agent", provider=PROVIDER)
        )
        # The malformed element produced no activation; the rest of the bundle
        # was processed normally (the bundle did not fail).
        assert_that(
            outputs["output"] | beam.Map(lambda r: r.output),
            equal_to([b"GOOD"]),
            label="main",
        )
        assert_that(
            outputs["errors"] | beam.Map(lambda r: (r.reason, "key" in r.detail)),
            equal_to([(REASON_MALFORMED_ROW, True)]),
            label="errors",
        )


def test_a_row_missing_the_payload_field_dead_letters_naming_the_field() -> None:
    with BeamTestPipeline() as p:
        outputs = (
            p
            | beam.Create([_row(key="k1")])
            | run_agent(agent=f"{FIXTURES}:echo_agent", provider=PROVIDER, payload_field="body")
        )
        assert_that(
            outputs["errors"] | beam.Map(lambda r: (r.reason, "body" in r.detail)),
            equal_to([(REASON_MALFORMED_ROW, True)]),
        )


# --- Scenario: A downstream step consumes a non-main output by name ----------


def test_the_four_outputs_are_addressable_by_name() -> None:
    transform = run_agent(agent=f"{FIXTURES}:echo_agent", provider=PROVIDER)
    with BeamTestPipeline() as p:
        outputs = p | beam.Create([_row(key="k1", payload=b"x")]) | transform
        # A `dict[str, PCollection]` return is exactly Beam YAML's multi-output
        # declaration mechanism (design D6, verified against
        # apache_beam/yaml/yaml_transform.py::expand_leaf_transform, which turns
        # a dict return into named outputs addressable as `Transform.name`).
        assert isinstance(outputs, dict)
        assert tuple(OUTPUT_NAMES) == ("output", "intents", "traces", "errors")
        assert set(outputs) == set(OUTPUT_NAMES)
        for name, pcoll in outputs.items():
            assert isinstance(pcoll, beam.pvalue.PCollection)
            # Each one is independently consumable by a downstream step.
            pcoll | f"Consume-{name}" >> beam.Map(lambda element: element)


# --- Scenario: Tagged streams surface as rows, not protos --------------------


def test_tagged_streams_surface_as_rows_not_protos() -> None:
    with BeamTestPipeline() as p:
        outputs = (
            p
            | beam.Create([_row(key="k1", payload=b"x")])
            | run_agent(agent=f"{FIXTURES}:acting_agent", provider=PROVIDER)
        )
        assert_that(
            outputs["intents"]
            | beam.Map(lambda r: (r.tool_name, r.args_json, bool(r.intent_id), r.kind)),
            equal_to([("notify", '{"channel":"ops"}', True, "TOOL")]),
            label="intents",
        )
        assert_that(
            outputs["intents"] | "NoProtoIntents" >> beam.Map(_is_not_proto),
            equal_to([True]),
            label="no-proto-i",
        )
        # The trace rows keep the shipped trace-row shape (hex ids, enum names).
        assert_that(
            outputs["traces"]
            | "TraceShape" >> beam.Map(lambda r: _is_not_proto(r) and _is_trace_row(r))
            | beam.Distinct(),
            equal_to([True]),
            label="traces",
        )


def _is_not_proto(element: Any) -> bool:
    return not isinstance(element, (ToolIntent, TraceEvent, ActivationError))


def _is_trace_row(row: Any) -> bool:
    fields = row._asdict()
    return fields["event_type"] in _EVENT_TYPE_NAMES and isinstance(fields["trace_id"], str)


def test_error_rows_keep_the_shipped_activation_error_row_shape() -> None:
    expected_fields = set(
        activation_error_to_row(ActivationError(entity_key=b"k", reason="r", detail="d"))
    )
    with BeamTestPipeline() as p:
        outputs = (
            p
            | beam.Create([_row(payload=b"orphan")])
            | run_agent(agent=f"{FIXTURES}:echo_agent", provider=PROVIDER)
        )
        assert_that(
            outputs["errors"] | beam.Map(lambda r: set(r._asdict())),
            equal_to([expected_fields]),
        )


# --- Scenario: A configured sink leaves the named output addressable ---------


def test_a_configured_sink_leaves_the_named_output_addressable() -> None:
    # The sink resolves through the real `DefaultSinkResolver` seam, stubbed to
    # a no-op writer so the run stays offline (no Kafka client, no network).
    with (
        mock.patch.object(DefaultSinkResolver, "resolve", _fixtures.stub_sink_resolve),
        BeamTestPipeline() as p,
    ):
        outputs = (
            p
            | beam.Create([_row(key="k1", payload=b"x")])
            | run_agent(
                agent=f"{FIXTURES}:acting_agent",
                provider=PROVIDER,
                intents_to="kafka://broker:9092/intents",
            )
        )
        assert set(outputs) == set(OUTPUT_NAMES)
        assert_that(outputs["intents"] | beam.Map(lambda r: r.tool_name), equal_to(["notify"]))


def test_sink_resolver_is_not_part_of_the_yaml_config_surface() -> None:
    # `sink_resolver` is an advanced Python seam (design D4): a YAML document
    # naming it is rejected like any other unknown key.
    assert "sink_resolver" not in CONFIG_KEYS
    with pytest.raises(ValueError, match="sink_resolver"):
        run_agent(
            agent=f"{FIXTURES}:echo_agent",
            provider=PROVIDER,
            sink_resolver="not-a-resolver",
        )


# --- Scenario: The constructor is usable directly from Python ----------------


def test_the_constructor_is_usable_directly_from_python_with_a_model_call() -> None:
    with BeamTestPipeline() as p:
        outputs = (
            p
            | beam.Create([_row(key="k1", payload=b"evt")])
            | run_agent(
                agent=f"{FIXTURES}:model_agent",
                provider=f"{FIXTURES}:make_scripted_llm",
                provider_config={"payload": "pong"},
            )
        )
        assert_that(outputs["output"] | beam.Map(lambda r: r.output), equal_to([b"evt:pong"]))


def test_the_agent_and_config_are_exposed_for_inspection() -> None:
    transform = run_agent(agent=f"{FIXTURES}:echo_agent", provider=PROVIDER)
    resolved: object = transform.agent
    assert resolved is _fixtures.echo_agent
    assert transform.config.provider_factory() is not None
