# Fraud triage as a Dataflow Flex Template

[`examples/fraud_triage.py`](../fraud_triage.py) runs offline on a scripted
`TestStream`. This directory is the same agent, packaged so a platform team can
put it on the Dataflow managed service with one command and no checkout:

```sh
gcloud dataflow flex-template run "fraud-triage-$(date +%s)" \
  --template-file-gcs-location=gs://MY_BUCKET/templates/fraud-flex-SHA.json \
  --project=MY_PROJECT \
  --region=us-central1 \
  --parameters=input_topic=pubsub://MY_PROJECT/tx-events \
  --parameters=approvals_topic=pubsub://MY_PROJECT/fraud-approvals \
  --parameters=output_topic=pubsub://MY_PROJECT/fraud-decisions \
  --parameters=intents_topic=pubsub://MY_PROJECT/fraud-intents \
  --parameters=model=beam_agents.model.anthropic:AnthropicProvider \
  --parameters=model_api_key_secret=projects/MY_PROJECT/secrets/anthropic-key/versions/3 \
  --parameters=hitl_timeout_ms=900000
```

The agent is imported from the example, never copied. What this directory adds
is the wiring the example deliberately fakes: real Pub/Sub reads for events and
approvals, the intents outbox, and the decisions topic.

## Parameters

| Parameter | Required | Value |
| --- | --- | --- |
| `input_topic` | yes | `pubsub://<project>/<topic>` carrying `AgentEnvelope`-encoded transaction events. |
| `approvals_topic` | yes | `pubsub://<project>/<topic>` the effector publishes human decisions back onto. |
| `output_topic` | yes | `pubsub://<project>/<topic>` terminal decisions are written to. |
| `intents_topic` | yes | `pubsub://<project>/<topic>` for approval intents — becomes `AgentConfig.intents_to`. |
| `model` | yes | The provider, in the YAML provider's `module:object` grammar ([docs/yaml.md](../../docs/yaml.md)). |
| `hitl_timeout_ms` | no | Human-approval deadline in ms. Defaults to the runtime default (24 h). |
| `errors_to` | no | Sink URI for the errors stream: `kafka://`, `pubsub://`, or `bigquery://`. |
| `traces_to` | no | Sink URI for the traces stream; also accepts `otlp://<host>:<port>`. |
| `model_api_key_secret` | no | Secret Manager version resource name holding the provider's API key. |

Every topic parameter uses the sink resolver's URI grammar — the same string you
would write for `intents_to` in Python. One grammar everywhere.

Validation happens twice: `metadata.json`'s regexes let the console and `gcloud`
turn away a malformed value before a job exists, and the launcher re-validates
through the runtime's own construction-time checks (`DefaultSinkResolver`,
`HitlPolicy`, the YAML provider's reference resolver) before submitting a graph.
The launcher is authoritative; the regexes are a prefilter, and
`tests/examples/test_flex_template_metadata.py` fails offline if the two drift.

### The HITL deadline

`hitl_timeout_ms` sets `HitlPolicy.timeout_ms` *and* the deadline the
suspension itself carries. That second half matters: the example's agent stages
its approval with an explicit `Suspend(timeout_ms=...)`, which wins over the
policy default, so setting only the policy would leave the parameter inert.
Whatever you pass is the deadline after which the activation fails closed and
the default deny route emits its deterministic fallback output.

## Provider credentials

**The template never accepts an API key as a parameter.** Launch parameters
appear in the launch request, the job description, and the console — a key
passed there is a key logged. Instead, put the key in Secret Manager and pass
the *version resource name*:

```sh
printf 'sk-ant-...' | gcloud secrets create anthropic-key \
  --project=MY_PROJECT --data-file=-

gcloud secrets add-iam-policy-binding anthropic-key \
  --project=MY_PROJECT \
  --member="serviceAccount:MY_WORKER_SA@MY_PROJECT.iam.gserviceaccount.com" \
  --role=roles/secretmanager.secretAccessor
```

The value is fetched on the worker, at provider construction, under the job
service account's Application Default Credentials — once per worker, not once
per element. No key material transits the launcher, the pipeline options, or
Beam's serialized pipeline proto.

A `model` naming a provider whose constructor requires an `api_key` and no
`model_api_key_secret` is a launch-time error naming both parameters. The
example's own `examples.fraud_triage:make_provider` needs no credential, which
is what the nightly validation run uses.

## Building the image and the spec

The nightly `dataflow` workflow job does this on every green night, tagging both
artifacts with the commit SHA and publishing no `latest` alias. The local
equivalent:

```sh
IMAGE=REGION-docker.pkg.dev/MY_PROJECT/MY_REPO/fraud-flex:$(git rev-parse HEAD)

docker build -f examples/fraud_triage_dataflow/Dockerfile \
  --build-arg "TEMPLATE_IMAGE=$IMAGE" -t "$IMAGE" .
docker push "$IMAGE"

gcloud dataflow flex-template build \
  "gs://MY_BUCKET/templates/fraud-flex-$(git rev-parse HEAD).json" \
  --image="$IMAGE" \
  --sdk-language=PYTHON \
  --metadata-file=examples/fraud_triage_dataflow/metadata.json
```

One image serves as both the template launcher and the job's
`sdk_container_image`: Google's launcher base image *is* the Beam Python SDK
harness image with the launcher binary copied in, so launcher and workers share
one dependency closure and one protobuf pin. `TEMPLATE_IMAGE` is baked in
because a container cannot discover its own published URI; the launcher forwards
it as `--sdk_container_image`.

The build ends with an import self-check that also constructs a launch plan, so
a broken reference or a protobuf mismatch fails `docker build` rather than
surfacing at launch.

## What is gated, and where

| Gate | Where | What it proves |
| --- | --- | --- |
| `tests/examples/test_flex_template_metadata.py` | `ci`, offline | The document and the launcher describe one parameter surface, and no parameter is a credential slot. |
| `tests/examples/test_flex_template_launcher.py` | `ci`, offline | Parameter mapping, the sink-resolver grammar, the HITL deadline, provider resolution, and the Secret Manager deferral. |
| `tests/dataflow/test_flex_template_launch.py` | `nightly`, real GCP | The published spec launches and the job reaches `JOB_STATE_RUNNING`. |

The nightly gate validates packaging, not semantics: it asserts one state
transition and then cancels the job. Runtime correctness — effectively-once
side effects, HITL fail-closed, state compatibility — is gated by the semantics
tier and the Flink end-to-end gate, and is not re-litigated on a live Dataflow
job every night.
