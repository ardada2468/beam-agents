"""End-to-end test for the `yaml-provider` capability.

Requirement: "An end-to-end YAML pipeline runs RunAgent offline with FakeLLM".
A complete Beam YAML document — a `providers:` block mapping `RunAgent` to
`beam_agents.yaml.run_agent`, an agent named by `module:object`, and a
`FakeLLM`-backed provider factory named the same way — is parsed and executed on
DirectRunner with no docker, no network, and no real provider import.

The document is executed by `tests/yaml/_yaml_driver.py` (see its docstring for
why Beam's own `apache_beam.yaml` package cannot be imported in the offline
lane, and for the three Beam mechanisms the driver mirrors);
`test_beam_yaml_contract_still_matches_this_driver` pins those mechanisms
against Beam upgrades.
"""

from __future__ import annotations

from pathlib import Path

import apache_beam as beam
import pytest

# Aliased: a bare "TestPipeline" name would be mis-collected by pytest.
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms import combiners
from apache_beam.utils.python_callable import PythonCallableWithSource

import yaml
from beam_agents.yaml import PROVIDER_LISTING, run_agent
from tests.yaml import _yaml_driver
from tests.yaml._yaml_driver import (
    BEAM_DICT_OUTPUT_RETURN,
    BEAM_INLINE_CONSTRUCTION,
    BEAM_PYTHON_PROVIDER_TYPE,
    expand_document,
)

# A complete Beam YAML pipeline document. `Create` stands in for a real source
# so the run stays offline; everything downstream of it is the shipped surface.
PIPELINE_YAML = """
pipeline:
  transforms:
    - type: Create
      name: Events
      config:
        elements:
          - key: "cust-1"
            payload: "suspicious-login"
          - key: "cust-2"
            payload: "password-reset"

    - type: RunAgent
      name: Triage
      input: Events
      config:
        agent: "tests.yaml._fixtures:triage_agent"
        provider: "tests.yaml._fixtures:make_recording_llm"
        activation_timeout_s: 20

providers:
  - type: python
    transforms:
      RunAgent: "beam_agents.yaml.run_agent"
"""


# --- Scenario: A YAML document drives an agent activation end to end ---------


def test_a_yaml_document_drives_an_agent_activation_end_to_end() -> None:
    with BeamTestPipeline() as p:
        outputs = expand_document(p, PIPELINE_YAML)["Triage"]
        # The pipeline completes offline and the agent's output is observed on
        # the `output` stream. `make_recording_llm` is fail-closed — it matches
        # only the model id `triage_agent` requests and `FakeLLM` raises
        # `UnmatchedRequestError` otherwise — so an `escalate` suffix here is
        # proof that the FakeLLM recorded and served exactly that model call.
        assert_that(
            outputs["output"] | beam.Map(lambda row: row.output),
            equal_to(
                [
                    b"cust-1|suspicious-login:escalate",
                    b"cust-2|password-reset:escalate",
                ]
            ),
            label="output",
        )
        # Nothing dead-lettered, and the model call is recorded in the traces.
        assert_that(outputs["errors"], equal_to([]), label="errors")
        assert_that(
            outputs["traces"]
            | beam.Filter(lambda row: row.event_type == "LLM_CALL")
            | combiners.Count.Globally(),
            equal_to([2]),
            label="llm-calls",
        )


def test_the_document_exposes_all_four_named_outputs() -> None:
    with BeamTestPipeline() as p:
        outputs = expand_document(p, PIPELINE_YAML)["Triage"]
        assert sorted(outputs) == ["errors", "intents", "output", "traces"]
        # A downstream YAML step addresses one by qualified name.
        assert (
            _yaml_driver._resolve_input({"Triage": outputs}, "Triage.errors") is outputs["errors"]
        )


# --- A bad reference in the document fails at expansion, not in a bundle -----


def test_a_bad_agent_reference_in_the_document_fails_at_expansion() -> None:
    document = PIPELINE_YAML.replace(
        '"tests.yaml._fixtures:triage_agent"', '"no_such_pkg.agents:fraud_agent"'
    )
    with BeamTestPipeline() as p, pytest.raises(ValueError) as excinfo:
        expand_document(p, document)
    message = str(excinfo.value)
    assert "no_such_pkg.agents" in message
    assert "installed" in message


def test_an_unknown_config_key_in_the_document_fails_at_expansion() -> None:
    document = PIPELINE_YAML.replace("activation_timeout_s: 20", "activation_timeut_s: 20")
    with BeamTestPipeline() as p, pytest.raises(ValueError) as excinfo:
        expand_document(p, document)
    assert "activation_timeut_s" in str(excinfo.value)


# --- The document's provider block is the shipped listing --------------------


def test_the_documents_provider_block_matches_the_packaged_listing() -> None:
    declared = yaml.safe_load(PIPELINE_YAML)["providers"]
    listing = yaml.safe_load(Path(PROVIDER_LISTING).read_text(encoding="utf-8"))
    assert isinstance(listing, list), "a provider listing file is a list of provider specs"
    assert [spec["transforms"] for spec in listing] == [spec["transforms"] for spec in declared]
    assert {spec["type"] for spec in listing} == {BEAM_PYTHON_PROVIDER_TYPE}


def test_the_listed_constructor_resolves_to_the_shipped_run_agent() -> None:
    listing = yaml.safe_load(Path(PROVIDER_LISTING).read_text(encoding="utf-8"))
    for spec in listing:
        for path in spec["transforms"].values():
            assert PythonCallableWithSource.load_from_source(path) is run_agent


# --- Drift guard: Beam still behaves the way the driver mirrors --------------


def test_beam_yaml_contract_still_matches_this_driver() -> None:
    """Fail if a Beam upgrade moves any mechanism `_yaml_driver` mirrors.

    Read from the *installed* Beam source rather than by import: the modules
    below need Beam's `yaml` extra, which the offline lane does not install.
    """
    yaml_pkg = Path(beam.__file__).parent / "yaml"
    provider_source = (yaml_pkg / "yaml_provider.py").read_text(encoding="utf-8")
    transform_source = (yaml_pkg / "yaml_transform.py").read_text(encoding="utf-8")

    # 1. `type: python` is still a registered provider type whose no-`packages:`
    #    arm resolves fully-qualified names in-process.
    assert f"register_provider_type('{BEAM_PYTHON_PROVIDER_TYPE}')" in provider_source
    assert "PythonCallableWithSource.load_from_source(" in provider_source
    # 2. An inline provider still constructs a transform as `factory(**config)`.
    assert BEAM_INLINE_CONSTRUCTION in provider_source
    # 3. A dict return still becomes named outputs, addressed as `Name.output`.
    assert BEAM_DICT_OUTPUT_RETURN in transform_source
    assert "transform, output = name.rsplit('.', 1)" in transform_source
