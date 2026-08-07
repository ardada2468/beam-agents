# Example: fraud triage as a Dataflow Flex Template

The [fraud triage example](fraud-triage.md) runs offline on a scripted
`TestStream`: no credentials, no topics, no cloud. That is what makes it a good
example and a poor deployment. This page is the same agent, packaged as a
[Dataflow Flex Template](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates)
— a form a platform team fills in, with no checkout and no `uv` environment.

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

Source: `examples/fraud_triage_dataflow/`. Sample code — outside the wheel,
outside the public API, no compatibility promise beyond its own tests.

!!! note "The agent is imported, not copied"
    `triage` and its scripted provider stay in `examples/fraud_triage.py`.
    What this directory adds is the wiring the offline example deliberately
    fakes: real Pub/Sub reads for events *and* approvals, the intents outbox,
    and the decisions topic. There is exactly one copy of the agent.

## Parameters

| Parameter | Required | Value |
| --- | --- | --- |
| `input_topic` | yes | `pubsub://<project>/<topic>` carrying `AgentEnvelope`-encoded transaction events. |
| `approvals_topic` | yes | `pubsub://<project>/<topic>` the effector publishes human decisions back onto. |
| `output_topic` | yes | `pubsub://<project>/<topic>` terminal decisions are written to. |
| `intents_topic` | yes | `pubsub://<project>/<topic>` for approval intents — becomes `AgentConfig.intents_to`. |
| `model` | yes | The provider, in the [YAML provider's](../yaml.md) `module:object` reference grammar. |
| `hitl_timeout_ms` | no | Human-approval deadline in ms. Defaults to the runtime default (24 h). |
| `errors_to` | no | Sink URI for the [errors stream](../errors.md): `kafka://`, `pubsub://`, `bigquery://`. |
| `traces_to` | no | Sink URI for the [traces stream](../traces.md); also `otlp://<host>:<port>`. |
| `model_api_key_secret` | no | Secret Manager version resource name holding the provider's API key. |

Two things are deliberately *not* new vocabulary. Every topic is a
`pubsub://<project>/<topic>` sink-resolver URI — the same string you would pass
to `intents_to` in Python — and `model` is the same `module:object` reference a
YAML pipeline uses for `provider`. A value that is valid in one surface is valid
in all of them, and fails with the same message everywhere.

Validation happens twice. `metadata.json`'s regexes let the console and
`gcloud` turn a malformed value away before a job exists; the launcher then
re-validates through the runtime's own construction-time checks — the sink
resolver, `HitlPolicy`, and the reference resolver — before submitting a graph.
The launcher is authoritative, and an offline test fails if the two descriptions
of the parameter surface ever drift apart.

### The approval deadline

`hitl_timeout_ms` sets `HitlPolicy.timeout_ms` *and* the deadline the suspension
itself carries. The second half is what makes the parameter real: the example's
agent stages its approval with an explicit `Suspend(timeout_ms=...)`, which wins
over the policy default, so configuring only the policy would leave the
parameter inert. Whatever you pass is the deadline after which the activation
[fails closed](fraud-triage.md).

## Provider credentials never ride the parameters

Launch parameters appear in the launch request, the job description, and the
console. A key passed there is a key logged, so the template accepts none: put
the key in Secret Manager and pass its **version resource name**.

```sh
printf 'sk-ant-...' | gcloud secrets create anthropic-key \
  --project=MY_PROJECT --data-file=-

gcloud secrets add-iam-policy-binding anthropic-key \
  --project=MY_PROJECT \
  --member="serviceAccount:MY_WORKER_SA@MY_PROJECT.iam.gserviceaccount.com" \
  --role=roles/secretmanager.secretAccessor
```

The value is fetched **on the worker**, at provider construction, under the job
service account's Application Default Credentials — once per worker, not once
per element. No key material transits the launcher, the pipeline options, or
Beam's serialized pipeline proto, and a failed lookup is reported by resource
name and operation, never by value.

Naming a provider whose constructor requires an `api_key` without
`model_api_key_secret` is a launch-time error naming both parameters. The
example's own `examples.fraud_triage:make_provider` needs no credential at all,
which is what the nightly validation run uses.

## One image, launcher and workers

Google's Flex Template launcher base image *is* the Beam Python SDK harness
image with the launcher binary copied in — it installs `apache-beam[gcp]` and
keeps `/opt/apache/beam/boot` as its entrypoint. So a single image serves as
both the template launcher and the job's `sdk_container_image`: one dependency
closure, one protobuf pin, and no "the launcher resolved but the worker didn't"
failure mode. Building and publishing it is
[the nightly `dataflow` job's](../ci.md#the-fraud-triage-flex-template) work;
`examples/fraud_triage_dataflow/README.md` has the local equivalent.

## The launcher

The code below is included verbatim from
[`examples/fraud_triage_dataflow/launch.py`](https://github.com/ardada2468/beam-agents/blob/main/examples/fraud_triage_dataflow/launch.py)
— the same file
[`tests/examples/test_flex_template_launcher.py`](https://github.com/ardada2468/beam-agents/blob/main/tests/examples/test_flex_template_launcher.py)
exercises offline in CI, asserting the parameter mapping, the grammar checks,
and that a resolved secret never reaches the launch surface.

```python title="examples/fraud_triage_dataflow/launch.py"
--8<-- "examples/fraud_triage_dataflow/launch.py"
```
