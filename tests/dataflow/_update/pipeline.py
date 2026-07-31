"""The one launcher both interpreters run — the `--update` gate's job graph.

Design D3: this module is executed *by file path* from two different Python
environments (the previous release's venv and the checkout's), so both Dataflow
job graphs come from byte-identical source and differ only in the installed
`beam-agents`/`apache-beam` versions — exactly the variable under test. That
constraint is load-bearing in three ways:

- it imports nothing from the harness (`tests.dataflow._update.*`), because the
  previous-release venv has no `tests` package on its path;
- it is restricted to API that must be stable across adjacent releases, which
  makes the gate a public-API-stability canary as a side effect;
- every function a pipeline references is module level, and the job runs with
  `--save_main_session`, so Dataflow workers can unpickle them.

The scripted agent (design D4) covers the two keyed-state cells whose loss is
unrecoverable. `suspend:<nonce>` stages an `APPROVAL` intent and suspends with
the nonce as its continuation snapshot — a nonce recoverable *only* from the
persisted `Continuation`, so an output echoing it proves the suspension
resumed rather than restarted. `remember:<marker>` writes a marker into working
memory; `echo` reads it back. `ping` completes trivially. Every output carries
the key, the verb, the detail and `seq`, so a reset activation counter is
visible from outside too.

The harness imports this module normally (head's interpreter) for its pure
builders — envelopes, output codec, argument construction — which is what the
offline unit tests exercise.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_agents import AgentConfig, HitlPolicy, RunAgent
from beam_agents._protos import AgentEnvelope, ToolIntent
from beam_agents.core.agent import Complete, Suspend
from beam_agents.core.context import ActivationContext
from beam_agents.model.client import LlmRequest
from beam_agents.model.fake import FakeLLM, match_any, respond_with

# -- the scripted scenario ------------------------------------------------------

#: Working-memory key the `remember`/`echo` commands read and write.
MEMORY_KEY = "update-compat-marker"
#: `Continuation.adapter` discriminator for the suspension this gate creates.
ADAPTER = "update-compat"
#: Approval channel the staged APPROVAL intent names.
APPROVAL_CHANNEL = "update-compat-approvals"

# Six hours, far beyond the ~35-minute test budget: the HITL timer must NOT
# fire during the run (a fail-closed timeout would deny the suspension and hide
# whether the continuation survived), and the effector-side `expires_at` guard
# must still admit the approval injected after the update.
SUSPEND_TIMEOUT_MS = 6 * 60 * 60 * 1000
INTENT_TTL_MS = 6 * 60 * 60 * 1000

CMD_SUSPEND = "suspend:"
CMD_REMEMBER = "remember:"
CMD_ECHO = "echo"
CMD_PING = "ping"

VERB_SUSPENDED = "suspended"
VERB_RESUMED = "resumed"
VERB_DENIED = "denied"
VERB_REMEMBERED = "remembered"
VERB_ECHO = "echo"
VERB_PONG = "pong"
VERB_UNKNOWN = "unknown"

#: What `echo` reports when working memory came back empty — i.e. the exact
#: state loss this gate exists to catch, spelled so a red run is unambiguous.
MEMORY_MISSING = "MEMORY-LOST"

_MODEL_ID = "update-compat-fake"
_FAKE_RESPONSE = b"ok"


def make_provider() -> FakeLLM:
    """Module-level (picklable) provider factory: an in-process scripted fake.

    Deliberately not FakeLLM-over-HTTP (design D4): this gate asserts on
    pipeline outputs only, never on provider call counts, so a deployed
    endpoint would add an egress path and its failure modes to the flakiest
    test in the repo for no assertion.
    """
    return FakeLLM([(match_any(), respond_with(_FAKE_RESPONSE))])


def _resume_output(ctx: ActivationContext) -> bytes:
    """A resumed activation's terminal output.

    `ctx.snapshot` comes from the persisted `Continuation` and from nowhere
    else: a key that restarted rather than resuming carries an empty snapshot,
    which is exactly the state loss the gate asserts against.
    """
    approval = ctx.resume_approval
    if approval is not None and approval.approved:
        return encode_output(ctx.entity_key, VERB_RESUMED, ctx.snapshot.decode(), seq=ctx.seq)
    return encode_output(ctx.entity_key, VERB_DENIED, "", seq=ctx.seq)


async def compat_agent(ctx: ActivationContext) -> Complete | Suspend:
    """The gate's agent: suspend, remember, echo, or pong — nothing else."""
    if ctx.is_resume:
        return Complete(output=_resume_output(ctx))

    command = ctx.event.decode()
    # One model call per fresh activation, so the LLM_CACHE cell is populated
    # and carried across the update alongside the cells under assertion.
    await ctx.call_model(
        LlmRequest(
            model_id=_MODEL_ID,
            messages=[{"role": "user", "content": command}],
            tools_schema=None,
            sampling_params=None,
        )
    )

    if command.startswith(CMD_SUSPEND):
        nonce = command[len(CMD_SUSPEND) :]
        ctx.request_approval(
            json.dumps({"action": "update-compat", "nonce": nonce}, sort_keys=True),
            channel=APPROVAL_CHANNEL,
            ttl_ms=INTENT_TTL_MS,
        )
        return Suspend(snapshot=nonce.encode(), adapter=ADAPTER, timeout_ms=SUSPEND_TIMEOUT_MS)

    if command.startswith(CMD_REMEMBER):
        marker = command[len(CMD_REMEMBER) :]
        ctx.memory.set(MEMORY_KEY, marker.encode())
        return Complete(output=encode_output(ctx.entity_key, VERB_REMEMBERED, marker, seq=ctx.seq))

    if command == CMD_ECHO:
        stored = ctx.memory.get(MEMORY_KEY)
        detail = stored.decode() if stored else MEMORY_MISSING
        return Complete(output=encode_output(ctx.entity_key, VERB_ECHO, detail, seq=ctx.seq))

    if command == CMD_PING:
        return Complete(output=encode_output(ctx.entity_key, VERB_PONG, "", seq=ctx.seq))

    return Complete(output=encode_output(ctx.entity_key, VERB_UNKNOWN, command, seq=ctx.seq))


# -- envelopes and the output codec (shared with the harness) -------------------


def event_envelope(key: bytes, payload: str, *, event_time_ms: int) -> AgentEnvelope:
    """An external event for `key`. `event_time_ms` is the activation clock the
    DoFn reads, so the harness passes real epoch milliseconds.
    """
    return AgentEnvelope(
        entity_key=key, event_time_ms=event_time_ms, external_event=payload.encode()
    )


def approval_envelope(
    key: bytes,
    intent_id: str,
    *,
    approved: bool,
    decided_at_ms: int,
    approver: str = "update-compat-harness",
) -> AgentEnvelope:
    """An approval decision re-entering the pipeline on `key`."""
    return AgentEnvelope(
        entity_key=key,
        event_time_ms=decided_at_ms,
        approval=AgentEnvelope.Approval(
            intent_id=intent_id,
            approved=approved,
            approver=approver,
            decided_at_ms=decided_at_ms,
        ),
    )


_OUTPUT_SEPARATOR = "|"
_OUTPUT_FIELDS = 4


@dataclass(frozen=True, slots=True)
class OutputRecord:
    """One terminal output, as read back off the outputs topic."""

    key: bytes
    verb: str
    detail: str
    seq: int


def encode_output(key: bytes, verb: str, detail: str, *, seq: int) -> bytes:
    """`key|verb|detail|seq` — outputs land on an unkeyed Pub/Sub topic, so they
    carry their own correlation, and `seq` rides along because a reset
    activation counter is itself a state-loss symptom (design D4).
    """
    return _OUTPUT_SEPARATOR.join((key.decode(), verb, detail, str(seq))).encode()


def decode_output(raw: bytes) -> OutputRecord:
    parts = raw.decode(errors="replace").split(_OUTPUT_SEPARATOR)
    if len(parts) != _OUTPUT_FIELDS or not parts[3].lstrip("-").isdigit():
        raise ValueError(f"unparseable gate output: {raw!r}")
    return OutputRecord(key=parts[0].encode(), verb=parts[1], detail=parts[2], seq=int(parts[3]))


# -- pipeline construction ------------------------------------------------------

PHASE_LAUNCH = "launch"
PHASE_UPDATE = "update"

#: Cost bound (design D6): one small worker, Streaming Engine on — the
#: configuration under which `--update` is Dataflow's supported upgrade path.
MACHINE_TYPE = "n1-standard-2"

#: The label every job carries so the sweeper can find a leaked one. Duplicated
#: here rather than imported from `resources.py`: this module must stay
#: importable by an interpreter that has no `tests` package.
JOB_LABEL = "beam-agents-test=update-compat"

#: `DataflowPipelineResult.job_id()` printed on its own line for the harness.
JOB_ID_PREFIX = "BEAM_AGENTS_UPDATE_COMPAT_JOB_ID="

# Stable transform labels. These names ARE the `--update` compatibility
# surface: Dataflow matches steps between the old and new graphs by name, so
# renaming one here breaks the gate exactly as renaming one inside RunAgent
# would (docs/state-compat.md, the graph-shape row).
STEP_READ = "ReadEvents"
STEP_PARSE = "ParseEnvelope"
STEP_KEY = "KeyByEntity"
STEP_AGENT = "RunAgent"
STEP_ENCODE_INTENTS = "EncodeIntents"
STEP_WRITE_INTENTS = "WriteIntents"
STEP_WRITE_OUTPUTS = "WriteOutputs"


def beam_args(
    *,
    phase: str,
    project: str,
    region: str,
    job_name: str,
    temp_location: str,
    events_subscription: str,
    outputs_topic: str,
    intents_topic: str,
    extra_package: str,
) -> list[str]:
    """The Beam pipeline options for one leg, as a flag list.

    Pure and total: the launch and update phases differ by exactly one flag
    (`--update`), and the job name is identical in both, because Dataflow
    replaces a running job *by name* — a fresh name would silently launch a
    second job and test nothing.

    `events_subscription`, `outputs_topic` and `intents_topic` are not options
    here (they are the launcher's own arguments) but are accepted so the
    signature is the single place a leg's parameters are named.
    """
    if phase not in (PHASE_LAUNCH, PHASE_UPDATE):
        raise ValueError(f"unknown phase {phase!r}; expected {PHASE_LAUNCH} or {PHASE_UPDATE}")
    del events_subscription, outputs_topic, intents_topic  # launcher args, not options
    args = [
        "--runner=DataflowRunner",
        f"--project={project}",
        f"--region={region}",
        f"--job_name={job_name}",
        f"--temp_location={temp_location}",
        "--streaming",
        "--enable_streaming_engine",
        "--num_workers=1",
        "--max_num_workers=1",
        f"--machine_type={MACHINE_TYPE}",
        f"--labels={JOB_LABEL}",
        f"--extra_package={extra_package}",
        "--save_main_session",
    ]
    if phase == PHASE_UPDATE:
        args.append("--update")
    return args


def launcher_argv(
    *,
    phase: str,
    project: str,
    region: str,
    job_name: str,
    temp_location: str,
    events_subscription: str,
    outputs_topic: str,
    intents_topic: str,
    extra_package: str,
) -> list[str]:
    """This module's own command line, as the harness spells it for each leg."""
    return [
        f"--phase={phase}",
        f"--project={project}",
        f"--region={region}",
        f"--job-name={job_name}",
        f"--temp-location={temp_location}",
        f"--events-subscription={events_subscription}",
        f"--outputs-topic={outputs_topic}",
        f"--intents-topic={intents_topic}",
        f"--extra-package={extra_package}",
    ]


def parse_job_id(stdout: str) -> str:
    """Read the launched job id back off a leg's stdout."""
    for line in stdout.splitlines():
        stripped = line.strip()
        if stripped.startswith(JOB_ID_PREFIX):
            job_id = stripped[len(JOB_ID_PREFIX) :].strip()
            if job_id:
                return job_id
    raise ValueError(f"no job id on the launcher's output; got:\n{stdout}")


def parse_envelope(raw: bytes) -> AgentEnvelope:
    envelope = AgentEnvelope()
    envelope.ParseFromString(raw)
    return envelope


def encode_intent(intent: ToolIntent) -> bytes:
    return intent.SerializeToString(deterministic=True)


def build(
    pipeline: beam.Pipeline, *, events_subscription: str, outputs_topic: str, intents_topic: str
) -> None:
    """Wire the graph. Every label here is part of the `--update` contract."""
    keyed = (
        pipeline
        | STEP_READ >> beam.io.ReadFromPubSub(subscription=events_subscription)
        | STEP_PARSE >> beam.Map(parse_envelope)
        | STEP_KEY
        >> beam.WithKeys(lambda envelope: envelope.entity_key).with_output_types(
            tuple[bytes, AgentEnvelope]
        )
    )
    outputs = keyed | STEP_AGENT >> RunAgent(
        compat_agent,
        config=AgentConfig(
            provider_factory=make_provider,
            hitl_policy=HitlPolicy(
                timeout_ms=SUSPEND_TIMEOUT_MS,
                intent_ttl_ms=INTENT_TTL_MS,
                approval_channel=APPROVAL_CHANNEL,
            ),
        ),
    )
    _ = outputs.output | STEP_WRITE_OUTPUTS >> beam.io.WriteToPubSub(topic=outputs_topic)
    _ = (
        outputs.intents
        | STEP_ENCODE_INTENTS >> beam.Map(encode_intent)
        | STEP_WRITE_INTENTS >> beam.io.WriteToPubSub(topic=intents_topic)
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", required=True, choices=(PHASE_LAUNCH, PHASE_UPDATE))
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--job-name", required=True)
    parser.add_argument("--temp-location", required=True)
    parser.add_argument("--events-subscription", required=True)
    parser.add_argument("--outputs-topic", required=True)
    parser.add_argument("--intents-topic", required=True)
    parser.add_argument("--extra-package", required=True)
    args = parser.parse_args(argv)

    options = PipelineOptions(
        beam_args(
            phase=args.phase,
            project=args.project,
            region=args.region,
            job_name=args.job_name,
            temp_location=args.temp_location,
            events_subscription=args.events_subscription,
            outputs_topic=args.outputs_topic,
            intents_topic=args.intents_topic,
            extra_package=args.extra_package,
        )
    )
    pipeline = beam.Pipeline(options=options)
    build(
        pipeline,
        events_subscription=args.events_subscription,
        outputs_topic=args.outputs_topic,
        intents_topic=args.intents_topic,
    )
    # Streaming: `run()` submits and returns; the harness owns every wait from
    # here, under its own deadlines.
    result = pipeline.run()
    # `job_id()` is DataflowPipelineResult's, not on the `PipelineResult` base
    # class Beam types `run()` as returning.
    job_id: str = result.job_id()  # type: ignore[attr-defined]
    print(f"{JOB_ID_PREFIX}{job_id}", flush=True)
    return 0


if __name__ == "__main__":  # pragma: no cover - executed as a subprocess, per leg
    sys.exit(main())
