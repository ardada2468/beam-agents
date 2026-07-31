"""The Flex Template launcher, exercised offline — no docker, no GCP, no keys.

Everything the launcher does *before* it submits a job graph is a pure function
of its arguments, and that is deliberate: a Flex Template's whole failure budget
is "did the parameters make sense", and the answer has to arrive before
Dataflow spins up workers. So these tests drive `build_launch_plan` directly —
parameter parsing, the sink-resolver grammar check, the HITL mapping, the C36
reference resolution, and the Secret Manager rule — and assert that a rejected
parameter never reaches `run_pipeline`.

The secret tests are the sharp ones. `model_api_key_secret` carries a *resource
name*; the value is fetched on the worker, at provider construction, and must
appear nowhere in the launch surface. A fake Secret Manager client stands in for
the real one so the deferral is observable: constructing the plan must perform
zero accesses, and the serialized provider factory must carry the resource name
and nothing else.

Implements the `dataflow-flex-template` scenarios "Topic parameters map onto the
pipeline", "A malformed topic URI fails before workers start", "HITL timeout
parameter reaches HitlPolicy", "A valid model string selects the provider and
model", "An invalid model string fails at launch", "The FakeLLM selection
launches without credentials", "Secret value never transits the launch surface",
"Credentialed provider without a secret is rejected at launch", and "Errors
never echo secret material".
"""

from __future__ import annotations

import pickle
from typing import Any

import apache_beam as beam
import pytest

from beam_agents.hitl import DEFAULT_HITL_TIMEOUT_MS
from beam_agents.model.anthropic import AnthropicProvider
from beam_agents.model.fake import FakeLLM
from examples.fraud_triage import make_provider, triage
from examples.fraud_triage_dataflow import launch

FAKE_MODEL = "examples.fraud_triage:make_provider"
CREDENTIALED_MODEL = "beam_agents.model.anthropic:AnthropicProvider"
SECRET_NAME = "projects/my-project/secrets/anthropic-key/versions/3"
# The stand-in for a Secret Manager payload. Every assertion below is
# `SECRET_VALUE not in <pickled pipeline / message / argv>`, so the value needs
# to be *distinctive* and nothing else — no entropy and no credential shape, or
# a secret scanner flags this file on every PR for a constant that was never a
# credential.
SECRET_VALUE = "placeholder-secret-payload-not-a-credential"

BASE_ARGV = [
    "--input_topic=pubsub://my-project/tx-events",
    "--approvals_topic=pubsub://my-project/fraud-approvals",
    "--output_topic=pubsub://my-project/fraud-decisions",
    "--intents_topic=pubsub://my-project/fraud-intents",
    f"--model={FAKE_MODEL}",
]


def argv(*extra: str, without: str = "") -> list[str]:
    """`BASE_ARGV` plus `extra`, optionally dropping one named parameter."""
    kept = BASE_ARGV
    if without:
        kept = [arg for arg in BASE_ARGV if not arg.startswith(f"--{without}=")]
    return [*kept, *extra]


def forbid_submission(monkeypatch: pytest.MonkeyPatch) -> list[object]:
    """Record submissions instead of making them; the list must stay empty."""
    submitted: list[object] = []
    monkeypatch.setattr(launch, "run_pipeline", submitted.append)
    return submitted


class FakeSecretClient:
    """Stands in for `SecretManagerServiceClient`: records names, serves values."""

    def __init__(self, *, value: str | None = SECRET_VALUE, error: Exception | None = None) -> None:
        self._value = value
        self._error = error
        self.requested: list[str] = []

    def access_secret_version(self, *, request: dict[str, str]) -> Any:
        self.requested.append(request["name"])
        if self._error is not None:
            raise self._error
        return _Response(self._value or "")


class _Response:
    def __init__(self, value: str) -> None:
        self.payload = _Payload(value)


class _Payload:
    def __init__(self, value: str) -> None:
        self.data = value.encode("utf-8")


@pytest.fixture
def no_secret_access(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make any Secret Manager access an outright failure, not a mock."""

    def forbidden() -> Any:
        raise AssertionError("the launcher reached Secret Manager when it must not")

    monkeypatch.setattr(launch, "secret_client", forbidden)


# --- Requirement: the template declares a validated parameter surface ----------


def test_topic_parameters_map_onto_the_pipeline(no_secret_access: None) -> None:
    # Scenario: Topic parameters map onto the pipeline. The four topic URIs
    # land where the runtime expects them: two Pub/Sub reads, the intents outbox
    # on `AgentConfig.intents_to`, and terminal decisions on the output topic.
    plan = launch.build_launch_plan(argv())

    assert plan.input_topic_path == "projects/my-project/topics/tx-events"
    assert plan.approvals_topic_path == "projects/my-project/topics/fraud-approvals"
    assert plan.output_topic_path == "projects/my-project/topics/fraud-decisions"
    # The intents sink keeps its URI form: `RunAgent` resolves it to the keyed
    # outbox writer, which is the whole point of routing it through the config.
    assert plan.agent_config.intents_to == "pubsub://my-project/fraud-intents"

    pipeline = beam.Pipeline()
    launch.build(pipeline, plan=plan)
    wired = _pubsub_endpoints(pipeline)
    assert wired == {
        plan.input_topic_path,
        plan.approvals_topic_path,
        plan.output_topic_path,
    }


def _pubsub_endpoints(pipeline: beam.Pipeline) -> set[str]:
    """Every Pub/Sub topic the applied graph actually reads or writes.

    `WriteToPubSub` publishes `full_topic`; `ReadFromPubSub` keeps it on its
    private source object, which is the only place Beam exposes it.
    """
    found: set[str] = set()

    class Visitor(beam.pipeline.PipelineVisitor):
        def visit_transform(self, node: Any) -> None:
            self.enter_composite_transform(node)

        def enter_composite_transform(self, node: Any) -> None:
            transform = node.transform
            topic: str | None = None
            if isinstance(transform, beam.io.WriteToPubSub):
                topic = transform.full_topic
            elif isinstance(transform, beam.io.ReadFromPubSub):
                topic = transform._source.full_topic
            if topic is not None:
                found.add(topic)

    pipeline.visit(Visitor())
    return found


def test_optional_sink_parameters_inherit_the_full_resolver_surface(
    no_secret_access: None,
) -> None:
    # Design D3: `errors_to`/`traces_to` pass through verbatim, so a user who
    # wrote them in Python writes the same string in the launch form.
    plan = launch.build_launch_plan(
        argv(
            "--errors_to=bigquery://my-project/agents/errors",
            "--traces_to=otlp://collector:4318",
        )
    )
    assert plan.agent_config.errors_to == "bigquery://my-project/agents/errors"
    assert plan.agent_config.traces_to == "otlp://collector:4318"


def test_optional_sink_parameters_default_to_unset(no_secret_access: None) -> None:
    plan = launch.build_launch_plan(argv())
    assert plan.agent_config.errors_to is None
    assert plan.agent_config.traces_to is None


@pytest.mark.parametrize(
    "parameter",
    ["input_topic", "approvals_topic", "output_topic", "intents_topic"],
)
def test_a_malformed_topic_uri_fails_before_workers_start(
    parameter: str, monkeypatch: pytest.MonkeyPatch, no_secret_access: None
) -> None:
    # Scenario: A malformed topic URI fails before workers start. The sink
    # resolver's own actionable message names the parameter and the offending
    # URI, and nothing is submitted.
    submitted = forbid_submission(monkeypatch)

    bad = f"--{parameter}=pubsub://my-project"
    with pytest.raises(ValueError) as excinfo:
        launch.build_launch_plan(argv(bad, without=parameter))
    message = str(excinfo.value)
    assert parameter in message
    assert "pubsub://my-project" in message
    assert "expected pubsub://<project>/<topic>" in message

    assert launch.main(argv(bad, without=parameter)) != 0
    assert submitted == []


def test_a_non_pubsub_topic_uri_is_refused(no_secret_access: None) -> None:
    # The four topic parameters are Pub/Sub endpoints, not the resolver's whole
    # scheme set: a well-formed `kafka://` URI is still the wrong thing here.
    with pytest.raises(ValueError, match="input_topic"):
        launch.build_launch_plan(
            argv("--input_topic=kafka://broker:9092/tx-events", without="input_topic")
        )


def test_hitl_timeout_parameter_reaches_hitl_policy(no_secret_access: None) -> None:
    # Scenario: HITL timeout parameter reaches HitlPolicy.
    plan = launch.build_launch_plan(argv("--hitl_timeout_ms=60000"))
    assert plan.agent_config.hitl_policy.timeout_ms == 60000
    # And it is the deadline the suspension actually carries: the example's
    # agent supplies its own per-suspension `timeout_ms`, so a policy default
    # alone would leave this parameter inert (design D1 revision).
    assert plan.agent.timeout_ms == 60000


def test_an_omitted_hitl_timeout_takes_the_runtime_default(no_secret_access: None) -> None:
    # Scenario: HITL timeout parameter reaches HitlPolicy — the omission half.
    plan = launch.build_launch_plan(argv())
    assert plan.agent_config.hitl_policy.timeout_ms == DEFAULT_HITL_TIMEOUT_MS
    assert plan.agent.timeout_ms == DEFAULT_HITL_TIMEOUT_MS


def test_a_non_positive_hitl_timeout_surfaces_hitl_policys_own_error(
    no_secret_access: None,
) -> None:
    with pytest.raises(ValueError) as excinfo:
        launch.build_launch_plan(argv("--hitl_timeout_ms=0"))
    message = str(excinfo.value)
    assert "hitl_timeout_ms" in message
    assert "HitlPolicy.timeout_ms must be positive" in message


def test_the_launchers_agent_delegates_to_the_examples_triage() -> None:
    # The template packages C24's example; it must not fork it. The wrapper
    # exists only to carry the configured deadline onto the suspension.
    assert launch.TriageWithDeadline(timeout_ms=1).delegate is triage


def test_the_launchers_agent_pickles(no_secret_access: None) -> None:
    # The runtime serializes the agent into the runner, so a wrapper that did
    # not pickle would fail deep inside job submission — after the launcher has
    # already reported success. A frozen dataclass at module scope is the point.
    plan = launch.build_launch_plan(argv("--hitl_timeout_ms=45000"))
    restored = pickle.loads(pickle.dumps(plan.agent))
    assert restored == plan.agent
    assert restored.timeout_ms == 45000


# --- Requirement: model and provider selection is a single config string
# --- shared with the YAML provider ---------------------------------------------


def test_a_valid_model_string_selects_the_provider(no_secret_access: None) -> None:
    # Scenario: A valid model string selects the provider and model. The
    # resolved factory is exactly what the YAML provider's parser produces for
    # that reference — the template defines no grammar of its own.
    plan = launch.build_launch_plan(argv())
    factory = plan.agent_config.provider_factory
    assert factory.func is make_provider  # type: ignore[attr-defined]
    assert factory.keywords == {}  # type: ignore[attr-defined]
    assert isinstance(factory(), FakeLLM)


def test_an_invalid_model_string_fails_at_launch(
    monkeypatch: pytest.MonkeyPatch, no_secret_access: None
) -> None:
    # Scenario: An invalid model string fails at launch. The message is the YAML
    # provider's own — `module:object`, quoting the reference — prefixed by the
    # parameter that carried it, and no job graph is submitted.
    submitted = forbid_submission(monkeypatch)

    bad = "--model=examples.fraud_triage.make_provider"
    with pytest.raises(ValueError) as excinfo:
        launch.build_launch_plan(argv(bad, without="model"))
    message = str(excinfo.value)
    assert "model" in message
    assert "module.path:attribute" in message

    assert launch.main(argv(bad, without="model")) != 0
    assert submitted == []


def test_a_model_reference_naming_a_missing_module_fails_at_launch(
    no_secret_access: None,
) -> None:
    with pytest.raises(ValueError, match="model"):
        launch.build_launch_plan(argv("--model=no_such_pkg.agents:provider", without="model"))


def test_the_fakellm_selection_launches_without_credentials(no_secret_access: None) -> None:
    # Scenario: The FakeLLM selection launches without credentials. The
    # `no_secret_access` fixture makes any Secret Manager call an assertion
    # failure, so reaching the end of this test is the assertion.
    plan = launch.build_launch_plan(argv())
    assert plan.parameters.model_api_key_secret is None
    assert isinstance(plan.agent_config.provider_factory(), FakeLLM)


# --- Requirement: provider API keys are supplied via Secret Manager resource
# --- names, never as parameter values ------------------------------------------


def test_secret_value_never_transits_the_launch_surface(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Scenario: Secret value never transits the launch surface. Building the
    # plan performs zero accesses; the serialized factory carries the resource
    # name and no value; the fetch happens only when the worker constructs the
    # provider.
    client = FakeSecretClient()
    monkeypatch.setattr(launch, "secret_client", lambda: client)

    plan = launch.build_launch_plan(
        argv(
            f"--model={CREDENTIALED_MODEL}",
            f"--model_api_key_secret={SECRET_NAME}",
            without="model",
        )
    )
    assert client.requested == []

    pickled = pickle.dumps(plan.agent_config.provider_factory)
    assert SECRET_NAME.encode() in pickled
    assert SECRET_VALUE.encode() not in pickled
    for arg in plan.beam_args:
        assert SECRET_VALUE not in arg

    provider = plan.agent_config.provider_factory()
    assert isinstance(provider, AnthropicProvider)
    assert client.requested == [SECRET_NAME]


def test_credentialed_provider_without_a_secret_is_rejected_at_launch(
    monkeypatch: pytest.MonkeyPatch, no_secret_access: None
) -> None:
    # Scenario: Credentialed provider without a secret is rejected at launch.
    submitted = forbid_submission(monkeypatch)

    bad = f"--model={CREDENTIALED_MODEL}"
    with pytest.raises(ValueError) as excinfo:
        launch.build_launch_plan(argv(bad, without="model"))
    message = str(excinfo.value)
    assert "model" in message
    assert "model_api_key_secret" in message

    assert launch.main(argv(bad, without="model")) != 0
    assert submitted == []


def test_errors_never_echo_secret_material(monkeypatch: pytest.MonkeyPatch) -> None:
    # Scenario: Errors never echo secret material. A denied or missing version
    # is reported by resource name and failing operation — never by value.
    client = FakeSecretClient(error=PermissionError("caller lacks secretmanager.versions.access"))
    monkeypatch.setattr(launch, "secret_client", lambda: client)

    plan = launch.build_launch_plan(
        argv(
            f"--model={CREDENTIALED_MODEL}",
            f"--model_api_key_secret={SECRET_NAME}",
            without="model",
        )
    )
    with pytest.raises(RuntimeError) as excinfo:
        plan.agent_config.provider_factory()
    message = str(excinfo.value)
    assert SECRET_NAME in message
    assert "access_secret_version" in message
    assert SECRET_VALUE not in message


def test_a_malformed_secret_resource_name_is_rejected(no_secret_access: None) -> None:
    with pytest.raises(ValueError, match="model_api_key_secret"):
        launch.build_launch_plan(
            argv(
                f"--model={CREDENTIALED_MODEL}",
                "--model_api_key_secret=projects/my-project/secrets/anthropic-key",
                without="model",
            )
        )


def test_a_credential_valued_flag_is_refused_outright(
    monkeypatch: pytest.MonkeyPatch, no_secret_access: None
) -> None:
    # The mutual-exclusion rule is only half the guard: unrecognized flags flow
    # on to `PipelineOptions`, so a key smuggled in as `--api_key=...` would
    # otherwise ride into the serialized pipeline. It is refused by name — the
    # value never reaches the guard, so `SECRET_VALUE` stands in for one here.
    submitted = forbid_submission(monkeypatch)
    smuggled = f"--api_key={SECRET_VALUE}"

    with pytest.raises(ValueError, match="api_key"):
        launch.build_launch_plan(argv(smuggled))
    assert launch.main(argv(smuggled)) != 0
    assert submitted == []


def test_no_declared_parameter_is_a_credential_slot() -> None:
    # The parameter surface itself can never grow a credential value: every
    # declared name is checked against the same rule the flag guard applies.
    for parameter in launch.PARAMETERS:
        assert not launch.is_credential_parameter(parameter.name)


@pytest.mark.parametrize(
    ("reference", "expected"),
    [
        ("beam_agents.model.anthropic:AnthropicProvider", True),
        ("beam_agents.model.openai_compat:OpenAICompatProvider", True),
        # `api_key` defaults to None: an unauthenticated endpoint is legal.
        ("beam_agents.model.vllm:VllmEndpointProvider", False),
        ("examples.fraud_triage:make_provider", False),
    ],
)
def test_credential_requirement_is_read_off_the_providers_signature(
    reference: str, expected: bool
) -> None:
    assert launch.requires_api_key(launch.resolve_model(reference)) is expected


# --- Requirement: the template image and spec build reproducibly ---------------


def test_beam_args_request_streaming_and_the_template_image(
    monkeypatch: pytest.MonkeyPatch, no_secret_access: None
) -> None:
    # The image cannot know its own published tag, so the build bakes it into
    # `BEAM_AGENTS_TEMPLATE_IMAGE` and the launcher forwards it as the job's
    # `sdk_container_image` — one image for launcher and workers (design D2).
    image = "us-central1-docker.pkg.dev/my-project/beam-agents/fraud-flex:abc123"
    monkeypatch.setenv(launch.TEMPLATE_IMAGE_ENV, image)
    plan = launch.build_launch_plan(argv())
    assert "--streaming" in plan.beam_args
    assert f"--sdk_container_image={image}" in plan.beam_args


def test_beam_args_omit_the_container_image_when_unset(
    monkeypatch: pytest.MonkeyPatch, no_secret_access: None
) -> None:
    monkeypatch.delenv(launch.TEMPLATE_IMAGE_ENV, raising=False)
    plan = launch.build_launch_plan(argv())
    assert not any(arg.startswith("--sdk_container_image") for arg in plan.beam_args)


def test_dataflows_own_pipeline_options_pass_through(no_secret_access: None) -> None:
    # The Flex Template launcher appends the job's pipeline options after the
    # template parameters; the launcher must forward, not swallow, them.
    plan = launch.build_launch_plan(
        argv("--project=my-project", "--region=us-central1", "--num_workers=2")
    )
    assert "--project=my-project" in plan.beam_args
    assert "--region=us-central1" in plan.beam_args
    assert "--num_workers=2" in plan.beam_args
