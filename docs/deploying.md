# Wiring the image for real Dataflow and a real model

[The quickstart](quickstart.md) runs the same pipeline on your laptop, on local
Flink, and on Dataflow. This page is about the third one: what has to be true of
the **container image** before Dataflow workers can run your agent against a
real model provider.

Two images live in this repo and they are not interchangeable:

| Image | What it is for |
|---|---|
| `docker/sdk-harness.Dockerfile` | The **local** Beam-on-Flink worker, built by compose, used only by this repo's tests |
| `examples/fraud_triage_dataflow/Dockerfile` | The **Dataflow** Flex Template, published to Artifact Registry |
| `docker/console.Dockerfile` | The console. Not a pipeline image at all |

Copy the second one. The rest of this page explains the parts you must not
change and the parts you must.

## The base image is both roles

```dockerfile
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base@sha256:35e7280...
```

That image *is* the Beam Python 3.11 SDK harness — it installs
`apache-beam[gcp]` and keeps `ENTRYPOINT ["/opt/apache/beam/boot"]` — with the
template launcher copied on top. Dataflow overrides the entrypoint when it runs
the launcher container; worker containers exec the Beam boot binary already
there.

So one image serves the launcher **and** the workers. There is no multi-stage
split, and no class of "launcher resolved it, the worker didn't" failure that
would be invisible until a job started. Pin it by digest.

## Three things that will bite you

### 1. Pin protobuf

This repo's committed `_pb2.py` bindings are 6.x gencode. Stock Beam images ship
a 5.x runtime, and protobuf requires gencode and runtime to share a major
version, so `import beam_agents._protos` fails outright without:

```dockerfile
RUN pip install --no-cache-dir "protobuf==6.33.6" ...
```

Beam's own constraint is `protobuf<7.0.0`, so 6.33.6 satisfies both sides.

### 2. Bake the code in — never `--extra_package`

Staging `beam_agents` per job makes every SDK worker start pay a pip install
over the network. Baking it in makes a worker restart a process spawn.

```dockerfile
COPY pyproject.toml README.md /src/
COPY src /src/src
RUN pip install --no-cache-dir --no-deps /src
```

`--no-deps` is load-bearing: `apache-beam[gcp]` is already present at exactly
the version this image's boot binary matches, and letting pip resolve
`apache-beam[gcp]>=2.60` would fight the image's own install. Install
`beam_agents`' real import-time dependencies explicitly in the layer above, and
put third-party pins **before** any `COPY` of your source so a code-only change
reuses the cached layer.

### 3. Your pipeline module must import under the same name everywhere

Beam pickles by module reference. Whatever your agent lives in has to be
importable under an identical name on the launcher and on every worker:

```dockerfile
COPY examples /template/examples
ENV PYTHONPATH=/template
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/examples/fraud_triage_dataflow/main.py
```

Swap `examples` for your own package. If the name differs between launcher and
worker, the job fails at unpickling with an import error that names a module you
believe exists.

### The image must know its own URI

A container cannot discover the tag it was published under, and the launcher has
to forward it as the job's `--sdk_container_image` so workers run *this* image
rather than a stock Beam one:

```dockerfile
ARG TEMPLATE_IMAGE=""
ENV BEAM_AGENTS_TEMPLATE_IMAGE=${TEMPLATE_IMAGE}
```

```sh
docker build -f path/to/Dockerfile \
  --build-arg TEMPLATE_IMAGE=<registry>/my-flex:<git-sha> \
  -t <registry>/my-flex:<git-sha> .
```

The build context is the repository root.

## The model credential

**Never pass an API key as a template parameter.** Template parameters are
recorded with the job and visible to anyone who can describe it.

The pattern this repo uses is a parameter that carries a *reference*:
`model_api_key_secret` names a Secret Manager **version**, validated at launch
against the resource-name grammar

```
projects/<project>/secrets/<secret>/versions/<version>
```

and the launcher refuses any other parameter whose name looks credential-shaped
(`api_key`, `secret`, …). The key itself is fetched where it is used:

```python
resolved(api_key=access_secret_version(secret_name))
```

Set it up once:

```sh
printf %s "$ANTHROPIC_API_KEY" \
  | gcloud secrets create beam-agents-model-key --data-file=-

gcloud secrets add-iam-policy-binding beam-agents-model-key \
  --member="serviceAccount:$WORKER_SA" \
  --role="roles/secretmanager.secretAccessor"
```

Two details that cause most of the failures here:

- **Grant the accessor role on the secret, to the worker service account.** Not
  to your own account, and not on the project — the fetch happens on the worker.
- **`google-cloud-secret-manager` belongs in the template image, not in your
  library dependencies.** Worker-side credential resolution is the deployment's
  concern; making it a `beam-agents` dependency would put a GCP client in every
  install of the library.

If you would rather not use Secret Manager, the other supported shape is an
environment variable set on the worker — but it must be set **on the worker**,
not on the machine you submitted from. `provider_factory` runs in the worker
process, so `os.environ["ANTHROPIC_API_KEY"]` there reads the worker's
environment, which is empty unless you put it there.

## Fail at build, not at launch

The most valuable ten lines in the Dockerfile are the last ten: import
everything the launcher and the workers import on the happy path, and build a
plan from a complete parameter set.

```dockerfile
RUN python -c "\
import apache_beam; \
import beam_agents._protos; \
import beam_agents.core.transform; \
import examples.fraud_triage; \
from examples.fraud_triage_dataflow import launch; \
plan = launch.build_launch_plan([...]); \
print('flex template ready', apache_beam.__version__)"
```

A protobuf mismatch, a broken module reference, or a missing dependency stops
the build and nothing is pushed. Without it those failures surface as a Dataflow
job that starts, runs for a few minutes, and dies in a worker log.

## Getting telemetry back

`console://localhost:8787` means the *worker's* own loopback on Dataflow, which
is nothing. Two options:

1. **Export to a broker or warehouse and read a window locally.** Keep
   `traces_to="kafka://…"` or `"bigquery://…"` and point a console at that —
   see [the console's ingest paths](console.md#getting-records-in). This is the
   answer for anything beyond a trial, because a production-rate pipeline will
   outrun a single-writer SQLite file anyway.
2. **Run the console somewhere the workers can reach**, and point
   `traces_to`/`errors_to` at that host. The console has no authentication, so
   this belongs on a trusted network only — see
   [the caveats](console.md#what-this-is-not).

## Cost

Everything on the [quickstart](quickstart.md)'s first two rungs is free apart
from model tokens. A Dataflow job provisions workers and bills for them until it
is drained or cancelled. Nothing in this repo starts one for you.

## Where it lands

| Thing | Path |
|---|---|
| Flex Template image | `examples/fraud_triage_dataflow/Dockerfile` |
| Launcher and parameter validation | `examples/fraud_triage_dataflow/launch.py` |
| Parameter schema | `examples/fraud_triage_dataflow/metadata.json` |
| Worked example | [Fraud triage on Dataflow](examples/fraud-triage-dataflow.md) |
