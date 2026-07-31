"""The Flex Template launcher: template parameters in, a streaming job out.

A Dataflow Flex Template turns a pipeline into a form: a platform team supplies
topics, a model reference and a HITL deadline, and `gcloud dataflow
flex-template run` does the rest. This module is what that form calls. It runs
inside the launcher container, before any worker exists, and its whole job is to
turn nine strings into `AgentConfig`/`HitlPolicy` — or to refuse them with an
actionable message while refusing is still cheap.

Three rules shape it:

* **Validation is borrowed, never re-implemented.** Topic URIs go through
  `DefaultSinkResolver`, the HITL deadline through `HitlPolicy`, and the `model`
  reference through the YAML provider's own resolver
  (`beam_agents.yaml._config.build_provider_factory`). A value that is valid in
  a YAML pipeline is valid here, with the same error text — one grammar
  everywhere, and no second opinion about what is well-formed. The regexes in
  `metadata.json` are a console-side prefilter; this module is authoritative,
  and `tests/examples/test_flex_template_metadata.py` fails offline if the two
  drift apart.
* **The agent is imported, not forked.** `examples/fraud_triage.py` owns the
  triage logic, its suspension/resume flow, and its scripted provider. What that
  module hardcodes is exactly what a template must parameterize — its `build()`
  wires a `TestStream`, not Pub/Sub — so the source/sink wiring is assembled
  here and nothing else is (design D1). `TriageWithDeadline` wraps the example's
  agent solely to carry the configured deadline onto the suspension it stages;
  without that, `hitl_timeout_ms` would be inert, since the example supplies its
  own per-suspension `timeout_ms` and `HitlPolicy.timeout_ms` is only the
  default for suspensions that do not.
* **A key passed as a parameter is a key logged.** Template parameters appear in
  the launch request, the job description and the console, so no parameter
  carries a credential — `model_api_key_secret` names a Secret Manager *version*
  and the value is fetched on the worker, at provider construction, under the
  job service account's own credentials (design D5). Anything that looks like a
  credential flag is refused outright, including in the pipeline options that
  pass through to Beam.

Everything a pipeline references is module level and picklable, and nothing is
defined in `__main__` — `main.py` is a shim for exactly that reason, because
Beam pickles DoFns and agents by module reference.

Importing this module has no side effects.
"""

from __future__ import annotations

import argparse
import functools
import inspect
import os
import re
import sys
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Protocol, cast
from urllib.parse import urlparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_agents import AgentConfig, RunAgent
from beam_agents._protos import AgentEnvelope
from beam_agents.core.agent import Complete, Suspend
from beam_agents.core.transform import DefaultSinkResolver
from beam_agents.hitl import HitlPolicy
from beam_agents.yaml._config import build_provider_factory
from beam_agents.yaml._refs import parse_reference, resolve_callable
from examples.fraud_triage import triage

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from beam_agents.core.agent import Agent
    from beam_agents.core.context import ActivationContext
    from beam_agents.model.client import LLMClient

# -- the parameter surface ------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Parameter:
    """One template parameter: its flag name and whether a launch must supply it."""

    name: str
    required: bool


#: The template's complete parameter surface, in `metadata.json` order. This
#: tuple and the committed document are two descriptions of one thing, and
#: `tests/examples/test_flex_template_metadata.py` asserts they agree.
#: Growing it is additive-only: renaming or retyping a parameter breaks every
#: recorded launch invocation and needs its own change proposal.
PARAMETERS: tuple[Parameter, ...] = (
    Parameter("input_topic", required=True),
    Parameter("approvals_topic", required=True),
    Parameter("output_topic", required=True),
    Parameter("intents_topic", required=True),
    Parameter("model", required=True),
    Parameter("hitl_timeout_ms", required=False),
    Parameter("errors_to", required=False),
    Parameter("traces_to", required=False),
    Parameter("model_api_key_secret", required=False),
)

#: The four Pub/Sub endpoints, all in the sink resolver's URI grammar (design D3).
TOPIC_PARAMETERS: tuple[str, ...] = (
    "input_topic",
    "approvals_topic",
    "output_topic",
    "intents_topic",
)

#: Optional sinks that pass through to `AgentConfig` verbatim, inheriting the
#: resolver's whole surface — `bigquery://` errors, `otlp://` traces, and so on.
SINK_PARAMETERS: tuple[str, ...] = ("errors_to", "traces_to")

#: The Secret Manager resource-name grammar, shared with `metadata.json`'s regex.
SECRET_RESOURCE_PATTERN = r"^projects/[^/]+/secrets/[^/]+/versions/[^/]+$"

#: Set by the image build to the image's own published URI, so the launcher can
#: pin worker containers to the same image it is running from (design D2).
TEMPLATE_IMAGE_ENV = "BEAM_AGENTS_TEMPLATE_IMAGE"

#: Substrings that mark a flag as carrying credential *material*. The rule is
#: deliberately blunt: a false positive costs a renamed flag, a false negative
#: costs a leaked key in a job description.
_CREDENTIAL_TOKENS: tuple[str, ...] = (
    "api_key",
    "apikey",
    "password",
    "passwd",
    "token",
    "credential",
    "secret",
)

#: The one parameter allowed to mention a secret: it carries a *reference* to
#: one, never a value.
_SECRET_REFERENCE_PARAMETERS = frozenset({"model_api_key_secret"})

_PROVIDER_EXPECTED = (
    "a model must be a module-level callable returning an LLMClient, named in the "
    "YAML provider's 'module.path:attribute' grammar (docs/yaml.md)"
)

_RESOLVER = DefaultSinkResolver()


def is_credential_parameter(name: str) -> bool:
    """Would a flag named `name` carry credential material?

    `model_api_key_secret` is exempt by name because it carries a Secret Manager
    resource name — a pointer, safe to log — and never a value.
    """
    if name in _SECRET_REFERENCE_PARAMETERS:
        return False
    lowered = name.lower().lstrip("-").replace("-", "_")
    return any(token in lowered for token in _CREDENTIAL_TOKENS)


# -- validation -----------------------------------------------------------------


def validate_parameter(name: str, value: str) -> None:
    """Run this parameter's own validation, raising `ValueError` naming it.

    Every arm delegates: the sink resolver owns URI grammar, `HitlPolicy` owns
    the deadline's range, and the YAML provider owns the reference grammar.
    """
    if name in TOPIC_PARAMETERS:
        _validate_pubsub_topic(name, value)
    elif name in SINK_PARAMETERS:
        _RESOLVER.validate(name, value)
    elif name == "model":
        parse_reference(value, field="model")
    elif name == "hitl_timeout_ms":
        parse_hitl_timeout(value)
    elif name == "model_api_key_secret":
        _validate_secret_resource_name(value)
    else:  # pragma: no cover - unreachable while PARAMETERS is the only source
        raise ValueError(f"{name}: not a template parameter")


def _validate_pubsub_topic(name: str, uri: str) -> None:
    """Grammar first (the resolver's message), then the scheme restriction.

    The resolver accepts every sink scheme; these four parameters are Pub/Sub
    endpoints specifically, so a well-formed `kafka://` URI is still wrong here.
    """
    _RESOLVER.validate(name, uri)
    scheme = urlparse(uri).scheme
    if scheme != "pubsub":
        raise ValueError(
            f"{name}: {uri!r} names a {scheme}:// sink, but this parameter is a "
            "Pub/Sub endpoint; expected pubsub://<project>/<topic>"
        )


def parse_hitl_timeout(value: str) -> int:
    """Parse `hitl_timeout_ms`, surfacing `HitlPolicy`'s own range error."""
    try:
        timeout_ms = int(value)
    except ValueError as exc:
        raise ValueError(
            f"hitl_timeout_ms: {value!r} is not an integer number of milliseconds"
        ) from exc
    try:
        HitlPolicy(timeout_ms=timeout_ms)
    except ValueError as exc:
        raise ValueError(f"hitl_timeout_ms: {exc}") from exc
    return timeout_ms


def _validate_secret_resource_name(value: str) -> None:
    if not re.fullmatch(SECRET_RESOURCE_PATTERN, value):
        raise ValueError(
            f"model_api_key_secret: {value!r} is not a Secret Manager version "
            "resource name; expected projects/<project>/secrets/<secret>/versions/"
            "<version> (a version, not a bare secret — the launcher never resolves "
            "'latest' on your behalf at build time)"
        )


def pubsub_topic_path(uri: str) -> str:
    """`pubsub://<project>/<topic>` -> the `projects/*/topics/*` form Beam reads."""
    parsed = urlparse(uri)
    topic = parsed.path.lstrip("/")
    return f"projects/{parsed.netloc}/topics/{topic}"


# -- provider selection and Secret Manager --------------------------------------


class _SecretPayload(Protocol):
    data: bytes


class _SecretVersion(Protocol):
    payload: _SecretPayload


class SecretAccessor(Protocol):
    """The one Secret Manager method this launcher uses."""

    def access_secret_version(self, *, request: Mapping[str, str]) -> _SecretVersion:
        """Fetch one secret version. Matches the Secret Manager client's method."""
        ...


def secret_client() -> SecretAccessor:
    """Build a Secret Manager client from the ambient credentials.

    Imported here rather than at module top: `google-cloud-secret-manager` is
    baked into the template image only (it is not a `beam-agents` dependency),
    and the offline tests must import this module without it.
    """
    from google.cloud import secretmanager  # noqa: PLC0415 - see docstring

    return cast("SecretAccessor", secretmanager.SecretManagerServiceClient())


def access_secret_version(name: str) -> str:
    """Read one secret version's value, on the worker, under the job's ADC.

    The failure message names the resource and the operation and stops there —
    it never interpolates the underlying exception's text, so no code path can
    carry a resolved value into a log line.
    """
    client = secret_client()
    try:
        version = client.access_secret_version(request={"name": name})
    except Exception as exc:
        raise RuntimeError(
            f"model_api_key_secret: access_secret_version failed for {name!r} "
            f"({type(exc).__name__}); the job's service account needs "
            "roles/secretmanager.secretAccessor on that secret version"
        ) from exc
    return version.payload.data.decode("utf-8")


def resolve_model(reference: str) -> Callable[..., object]:
    """Resolve the `model` reference through the YAML provider's own resolver."""
    return resolve_callable(reference, field="model", expected=_PROVIDER_EXPECTED)


def requires_api_key(provider: Callable[..., object]) -> bool:
    """Does constructing this provider require an `api_key`?

    Read off the callable's signature rather than a hardcoded provider list, so
    a provider added later is classified correctly with no edit here. A
    signature-less callable (C-implemented) is treated as not requiring one; the
    provider's own constructor stays the backstop.
    """
    try:
        signature = inspect.signature(provider)
    except (TypeError, ValueError):  # pragma: no cover - C-implemented callables
        return False
    parameter = signature.parameters.get("api_key")
    return parameter is not None and parameter.default is inspect.Parameter.empty


def provider_with_secret_api_key(*, provider: str, secret_name: str) -> LLMClient:
    """Worker-side provider construction: fetch the key, then build.

    Bound into a `functools.partial` by the launcher, so what ships to the
    runner is this function plus two strings — the reference and the resource
    name. No key material is in the serialized pipeline, because none has been
    fetched yet when the pipeline is serialized.
    """
    resolved = resolve_model(provider)
    return cast("LLMClient", resolved(api_key=access_secret_version(secret_name)))


def build_provider_factory_for(parameters: TemplateParameters) -> Callable[[], LLMClient]:
    """Map `model` (+ `model_api_key_secret`) onto `AgentConfig.provider_factory`."""
    resolved = resolve_model(parameters.model)
    needs_key = requires_api_key(resolved)
    secret = parameters.model_api_key_secret

    if needs_key:
        if secret is None:
            raise ValueError(
                f"model: the provider named by 'model' ({parameters.model!r}) requires an "
                "API key, so 'model_api_key_secret' must name a Secret Manager version "
                "(projects/<project>/secrets/<secret>/versions/<version>); it was not set. "
                "The key is never a template parameter — see the template README."
            )
        return functools.partial(
            provider_with_secret_api_key, provider=parameters.model, secret_name=secret
        )
    if secret is not None:
        raise ValueError(
            f"model_api_key_secret: was set, but the provider named by 'model' "
            f"({parameters.model!r}) takes no api_key, so the secret would never be "
            "read; drop the parameter or name a provider that needs it"
        )
    # The exact factory a YAML pipeline would get for this reference.
    return build_provider_factory(parameters.model, None)


# -- the agent ------------------------------------------------------------------


@dataclass(frozen=True)
class TriageWithDeadline:
    """The example's triage agent, carrying the template's HITL deadline.

    The example stages its approval and suspends with its own
    `Suspend.timeout_ms`, which wins over `HitlPolicy.timeout_ms` (the policy
    value is only the default for suspensions that name none). Re-timing the
    suspension here is what gives `hitl_timeout_ms` teeth without forking the
    agent: the triage logic, the model call and the resume path are all still
    `examples.fraud_triage.triage`, called unchanged.

    A frozen dataclass rather than a closure: the runtime serializes the agent
    into the runner, and a closure does not pickle.
    """

    timeout_ms: int

    @property
    def delegate(self) -> Agent:
        """The example's agent — imported, never copied (design D1)."""
        return triage

    async def __call__(self, ctx: ActivationContext) -> Complete | Suspend:
        """Run the shared triage agent, stamping the launch plan's HITL timeout."""
        outcome = await triage(ctx)
        if isinstance(outcome, Suspend):
            return replace(outcome, timeout_ms=self.timeout_ms)
        return outcome


# -- parameters -> a launch plan ------------------------------------------------


@dataclass(frozen=True, slots=True)
class TemplateParameters:
    """The nine template parameters, parsed and individually validated."""

    input_topic: str
    approvals_topic: str
    output_topic: str
    intents_topic: str
    model: str
    hitl_timeout_ms: int | None = None
    errors_to: str | None = None
    traces_to: str | None = None
    model_api_key_secret: str | None = None


@dataclass(frozen=True)
class LaunchPlan:
    """Everything resolved before submission — the launcher's whole verdict."""

    parameters: TemplateParameters
    agent: TriageWithDeadline
    agent_config: AgentConfig
    beam_args: list[str]
    input_topic_path: str
    approvals_topic_path: str
    output_topic_path: str


def build_parser() -> argparse.ArgumentParser:
    """An `--<name>=<value>` parser generated from `PARAMETERS`, not restated."""
    parser = argparse.ArgumentParser(
        prog="fraud-triage-flex-template",
        description="Dataflow Flex Template launcher for the fraud-triage example.",
    )
    for parameter in PARAMETERS:
        parser.add_argument(f"--{parameter.name}", required=parameter.required)
    return parser


def reject_credential_flags(arguments: list[str]) -> None:
    """Refuse a credential smuggled in as a pipeline option.

    Unrecognized flags flow on to `PipelineOptions` and from there into the
    serialized pipeline and the job description, so `--api_key=...` has to die
    at the launcher rather than be forwarded.
    """
    for argument in arguments:
        if not argument.startswith("--"):
            continue
        name = argument[2:].split("=", 1)[0]
        if is_credential_parameter(name):
            raise ValueError(
                f"{name}: credential values are never accepted as launch parameters "
                "or pipeline options — they end up in the job description and the "
                "console. Store the key in Secret Manager and pass its version "
                "resource name as 'model_api_key_secret'."
            )


def parse_parameters(argv: list[str]) -> tuple[TemplateParameters, list[str]]:
    """Split the launcher's argv into our parameters and Beam's options.

    The Flex Template launcher appends the job's own pipeline options after the
    template parameters, so the leftovers are Beam's and are forwarded verbatim.
    """
    known, rest = build_parser().parse_known_args(argv)
    reject_credential_flags(rest)

    supplied = {
        parameter.name: value
        for parameter in PARAMETERS
        if (value := getattr(known, parameter.name)) is not None
    }
    for name, value in supplied.items():
        validate_parameter(name, value)

    fields: dict[str, Any] = dict(supplied)
    if "hitl_timeout_ms" in fields:
        fields["hitl_timeout_ms"] = parse_hitl_timeout(fields["hitl_timeout_ms"])
    return TemplateParameters(**fields), rest


def beam_args(rest: list[str]) -> list[str]:
    """Beam's own options: Dataflow's, plus what the template must assert.

    `--sdk_container_image` pins workers to the image the launcher is running
    from; the image cannot know its own published URI, so the build bakes it in
    (design D2, spike findings).
    """
    args = [*rest, "--streaming"]
    image = os.environ.get(TEMPLATE_IMAGE_ENV)
    if image:
        args.append(f"--sdk_container_image={image}")
    return args


def build_launch_plan(argv: list[str]) -> LaunchPlan:
    """Parse, validate and resolve everything — before any job graph exists.

    Every failure below is a `ValueError` raised here, in the launcher
    container, with the offending parameter named. Nothing is submitted.
    """
    parameters, rest = parse_parameters(argv)
    timeout_ms = (
        parameters.hitl_timeout_ms
        if parameters.hitl_timeout_ms is not None
        else HitlPolicy().timeout_ms
    )
    config = AgentConfig(
        provider_factory=build_provider_factory_for(parameters),
        intents_to=parameters.intents_topic,
        errors_to=parameters.errors_to,
        traces_to=parameters.traces_to,
        hitl_policy=HitlPolicy(timeout_ms=timeout_ms),
    )
    return LaunchPlan(
        parameters=parameters,
        agent=TriageWithDeadline(timeout_ms=timeout_ms),
        agent_config=config,
        beam_args=beam_args(rest),
        input_topic_path=pubsub_topic_path(parameters.input_topic),
        approvals_topic_path=pubsub_topic_path(parameters.approvals_topic),
        output_topic_path=pubsub_topic_path(parameters.output_topic),
    )


# -- the pipeline ---------------------------------------------------------------


def parse_envelope(raw: bytes) -> AgentEnvelope:
    """Parse an ``AgentEnvelope`` off the wire."""
    return AgentEnvelope.FromString(raw)


def entity_key(envelope: AgentEnvelope) -> bytes:
    """The envelope's entity key, used to key the pipeline."""
    return envelope.entity_key


def build(pipeline: beam.Pipeline, *, plan: LaunchPlan) -> None:
    """The production shape of the fraud example: two topics in, decisions out.

    Events and approvals are separate Pub/Sub topics flattened onto one keyed
    stream, which is the re-injection path the runtime is built around: a human
    decision re-enters on the same key its suspension committed under. Intents
    leave through `AgentConfig.intents_to`, so the outbox writer handles them.
    """
    events = pipeline | "ReadTransactions" >> beam.io.ReadFromPubSub(topic=plan.input_topic_path)
    approvals = pipeline | "ReadApprovals" >> beam.io.ReadFromPubSub(
        topic=plan.approvals_topic_path
    )
    keyed = (
        (events, approvals)
        | "MergeSources" >> beam.Flatten()
        | "ParseEnvelope" >> beam.Map(parse_envelope)
        | "KeyByAccount" >> beam.WithKeys(entity_key).with_output_types(tuple[bytes, AgentEnvelope])
    )
    outputs = keyed | "Triage" >> RunAgent(plan.agent, config=plan.agent_config)
    _ = outputs.output | "WriteDecisions" >> beam.io.WriteToPubSub(topic=plan.output_topic_path)


def run_pipeline(plan: LaunchPlan) -> str:
    """Submit the streaming job and return its id.

    Streaming: `run()` submits and returns; Dataflow owns the job from here.
    """
    pipeline = beam.Pipeline(options=PipelineOptions(plan.beam_args))
    build(pipeline, plan=plan)
    result = pipeline.run()
    # `job_id()` is DataflowPipelineResult's, not on the `PipelineResult` base.
    job_id = str(result.job_id())  # type: ignore[attr-defined]
    print(f"Dataflow job id: {job_id}", flush=True)
    return job_id


def main(argv: list[str] | None = None) -> int:
    """Validate, then submit. A refused parameter exits non-zero, unsubmitted."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    try:
        plan = build_launch_plan(arguments)
    except ValueError as exc:
        print(f"flex template launch failed: {exc}", file=sys.stderr, flush=True)
        return 2
    run_pipeline(plan)
    return 0
