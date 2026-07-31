## Context

What exists today, and what this template stands on:

- **The fraud example does not exist yet.** There is no `examples/` directory; sibling proposal `add-docs-site` (C24) creates `examples/fraud/` — a pipeline reading keyed transaction events, running `RunAgent` with a suspension/HITL approval flow, and emitting intents. This change packages that pipeline; it does not author it, and it must not duplicate its assembly logic. *(As merged, C24 shipped it as the single module `examples/fraud_triage.py`; see D1's revision note for what that changes.)*
- **Provider selection from a string does not exist yet either.** `AgentConfig.provider_factory` is a Python callable ([transform.py:403](../../../src/beam_agents/core/transform.py:403)), and the concrete providers take explicit constructor arguments including `api_key` ([anthropic.py:78](../../../src/beam_agents/model/anthropic.py:78)). Sibling proposal `add-yaml-provider` (C36) defines config-string conventions for naming a provider + model without Python — exactly what a Flex Template parameter needs, since template parameters are strings.
- **The sink resolver already defines the topic URI grammar.** `DefaultSinkResolver` validates `pubsub://<project>/<topic>` (and `kafka://`, `bigquery://`, `otlp://`) at `AgentConfig` construction, import-free, with actionable `ValueError`s ([transform.py:249](../../../src/beam_agents/core/transform.py:249), [transform.py:374](../../../src/beam_agents/core/transform.py:374)). The HITL timeout is `HitlPolicy.timeout_ms`, self-validating at construction ([hitl.py:136](../../../src/beam_agents/hitl.py:136)).
- **The nightly GCP lane exists but is empty.** The `dataflow` job in [nightly.yml:34](../../../.github/workflows/nightly.yml:34) authenticates via Workload Identity Federation and is gated on `vars.GCP_PROJECT_ID != ''`, but `make test-dataflow` ([Makefile:63](../../../Makefile:63)) currently collects zero tests and tolerates exit code 5 to stay green.
- **The custom-container problem is already solved once.** [sdk-harness.Dockerfile:23](../../../docker/sdk-harness.Dockerfile:23) documents the two traps a beam-agents worker image must avoid: the protobuf gencode/runtime major-version mismatch against stock Beam images (this repo's committed `_pb2.py` are 6.x gencode; `apache/beam_python3.11_sdk:2.72.0` ships runtime 5.29.5), and per-job `--extra_package` installs making worker (re)starts network-dependent. Both lessons carry over verbatim to the template's worker image.

A Dataflow Flex Template consists of (a) a Docker image containing a launcher that reads template parameters from environment variables and submits the pipeline, and (b) a template spec file in GCS, produced by `gcloud dataflow flex-template build`, that names the image and embeds `metadata.json`'s parameter declarations. Users launch with `gcloud dataflow flex-template run` or the console, supplying parameters only.

## Goals / Non-Goals

**Goals:**
- One-command deployment of the fraud example to Dataflow: topics, model string, and HITL timeout as template parameters; everything else defaulted.
- A parameter surface whose validation reuses the runtime's existing construction-time checks (`DefaultSinkResolver`, `HitlPolicy`, and C36's config-string parser) rather than inventing a parallel validation layer.
- Provider API keys via Secret Manager resource names, never as parameter values.
- Reproducible builds: digest-pinned base image, dependencies from the committed `uv.lock`, image and template spec tagged by git SHA.
- A nightly launch-validation gate riding the existing WIF-authenticated `dataflow` job — the first `-m dataflow` test in the repo.

**Non-Goals:**
- A full end-to-end Dataflow assertion (events in → intents out → approval → resume) — the effectively-once and HITL semantics are already gated offline and on the Flink mini-cluster; the template gate validates *packaging*, not runtime semantics (see D7).
- Templates for pipelines other than the fraud example, or a general template-generation mechanism.
- Deploying the effector: it is a separate service ([pyproject.toml:47](../../../pyproject.toml:47)) and stays out of the pipeline template.
- Publishing a public/stable template artifact — the package is version `0.0.0` with no releases; the template is rebuilt nightly from `main` (see Risks).
- Classic (legacy) Dataflow templates, `--update` compatibility guarantees for template-launched jobs, and Terraform/infra provisioning of the Artifact Registry repo (recorded as an Open Question).

## Decisions

### D1. Entrypoint: a Python launcher invoking the fraud pipeline's assembly function — not a Beam YAML pipeline

Two candidate entrypoints exist once C36 lands: point the Flex Template launcher at a Python main, or express the pipeline in Beam YAML and launch that via the C36 provider. This template uses the **Python entry**: the image sets `FLEX_TEMPLATE_PYTHON_PY_FILE` to `examples/fraud_triage_dataflow/main.py`, a shim over `launch.py`, which parses the template parameters, builds `AgentConfig`/`HitlPolicy`, wires the fraud example's agent, and runs it.

**What is imported vs. what is assembled here (revised at implementation).** C24 shipped the example as a single module, `examples/fraud_triage.py`, whose `build(pipeline)` wires a scripted `TestStream` — an offline, credential-free demonstration with no Pub/Sub reads and no `intents_to`. That function is *not* reusable as a Dataflow entrypoint: every parameter this template exists to supply is exactly what it hardcodes. What must never be duplicated is the load-bearing part — the triage agent, its suspension/resume logic, and its scripted provider — and those are imported verbatim (`triage`, `make_provider`, `APPROVAL_TIMEOUT_MS`). The launcher supplies only the source/sink wiring the example deliberately fakes. So the non-duplication rule is honored where it matters: there is exactly one copy of the agent, and it is C24's.

Rationale: the fraud example is authored (by C24) as a Python pipeline, and its load-bearing configuration is Python objects a YAML document cannot carry — `provider_factory` is a callable, `HitlPolicy.on_timeout` must be a picklable module-level function ([hitl.py:136](../../../src/beam_agents/hitl.py:136)), and the tool registry is code. Routing through YAML would mean C36 must first invent string encodings for all of those, putting this change behind a much larger dependency for zero user-visible gain. What *is* borrowed from C36 is precisely the piece that already has to be a string: the provider/model config grammar (D4). If a later change makes the fraud example fully YAML-expressible, the template's parameter surface (the spec's contract) survives an entrypoint swap unchanged.

### D2. One dual-purpose image: template launcher and SDK harness container

The Dockerfile produces a single image used both as the Flex Template launcher image and as the job's `sdk_container_image`, so worker containers and the launcher share one dependency closure. Construction mirrors [sdk-harness.Dockerfile:23](../../../docker/sdk-harness.Dockerfile:23): start from the digest-pinned Google-provided Python 3.11 Flex Template launcher base, install the pinned Beam SDK matching the repo's floor, pin `protobuf` to the 6.x runtime the committed gencode requires, install `beam_agents` with `--no-deps` plus its explicitly named runtime deps, bake in `examples/fraud/` on `PYTHONPATH`, and end with an import self-check (`import beam_agents.core.transform`, the fraud pipeline module, and the C36 provider module) so a broken image fails at build, not at nightly launch. Baking code in (rather than `--extra_package` staging) keeps worker starts network-free — the same reasoning the harness Dockerfile documents. `google-cloud-secret-manager` is installed here and only here (D5).

Trade-off: one image is larger than a minimal launcher, but two images means two protobuf-pinning surfaces and a class of "launcher resolved, worker didn't" failures the nightly gate could not distinguish. Whether launcher-base and SDK-harness responsibilities can actually coexist in one image without entrypoint conflicts is verified in the spike task; if they cannot, this decision splits into two images built from one Dockerfile via multi-stage targets (Open Question 2).

### D3. All topic parameters use the sink resolver's URI grammar

`input_topic`, `approvals_topic`, `output_topic`, and `intents_topic` all take `pubsub://<project>/<topic>` — the exact grammar `DefaultSinkResolver` already parses and error-messages ([transform.py:374](../../../src/beam_agents/core/transform.py:374)) — with matching regexes in `metadata.json` so the console/`gcloud` reject malformed values before a job is even created. The launcher converts read-side URIs to the `projects/<p>/topics/<t>` form `ReadFromPubSub` expects; write-side URIs pass through as `intents_to`/`errors_to`/`traces_to` unchanged, which means the optional sink parameters inherit the full resolver surface (e.g. `bigquery://` for errors) for free. One grammar everywhere: a user who has written `intents_to` in Python writes the same string in the launch form. Misconfiguration that slips past the metadata regex still fails at `AgentConfig` construction inside the launcher — before Dataflow spins up workers — with the resolver's actionable message.

### D4. Model selection is one config string, parsed by the C36 provider's parser

The `model` parameter is a single string in `add-yaml-provider`'s config-string grammar. **As shipped, C36's grammar for `provider` is a `module:object` reference** ([_refs.py](../../../src/beam_agents/yaml/_refs.py), `docs/yaml.md` "References"), not a `<provider>:<model-id>` vocabulary — so `model` takes exactly that: `beam_agents.model.anthropic:AnthropicProvider`, `examples.fraud_triage:make_provider`. The launcher resolves it through C36's own `build_provider_factory`/`resolve_callable` ([_config.py:114](../../../src/beam_agents/yaml/_config.py:114)) to obtain the `provider_factory` for `AgentConfig` ([transform.py:395](../../../src/beam_agents/core/transform.py:395)), so the template can never drift from the YAML surface: a reference valid in a YAML pipeline is valid in the template, and an invalid one fails in the launcher with C36's own error message naming the parameter. `decode` is not a template parameter (the spec's parameter surface is closed); a provider whose decoder matters is a follow-up additive parameter. The nightly gate launches with the fraud example's own FakeLLM factory reference, keeping real-provider traffic out of the `dataflow` tier per the testing-tier contract ([project.md](../../project.md)).

### D5. Provider API keys: Secret Manager resource names, resolved worker-side; never parameter values

Template parameters are visible in launch requests, job metadata, and the console; a key passed as a parameter is a key logged. The template therefore accepts `model_api_key_secret` — a Secret Manager **resource name** (`projects/*/secrets/*/versions/*`), optional because the FakeLLM path needs no credential. Resolution happens where the key is used: the provider factory produced by the launcher defers `SecretManagerServiceClient.access_secret_version` to worker-side provider construction (factories are constructed per DoFn instance in `setup()`, so this adds one metadata-server-authenticated call per worker, not per element), using the job service account's Application Default Credentials — no key material transits the launcher, pipeline options, or Beam's serialized pipeline proto. The launcher enforces the mutual-exclusion rule: a `model` string whose provider requires a credential plus a missing `model_api_key_secret` is a launch-time error naming both parameters; the error text echoes the secret's resource name (harmless) and never any resolved value. Rejected alternative — resolving in the launcher and passing the value through pipeline options — leaks the key into the serialized pipeline and Dataflow's job description.

### D6. Build/publish rides the nightly `dataflow` job, tagged by git SHA

The existing nightly `dataflow` job already holds the only GCP credentials in CI (WIF, gated on `vars.GCP_PROJECT_ID != ''`, [nightly.yml:37](../../../.github/workflows/nightly.yml:37)); the template build goes there rather than into a new credentialed surface. Steps, ahead of the existing `make test-dataflow` step: `docker build` the D2 image → push to Artifact Registry as `<region>-docker.pkg.dev/$GCP_PROJECT_ID/beam-agents/fraud-flex:<git-sha>` → `gcloud dataflow flex-template build gs://<staging-bucket>/templates/fraud-flex-<git-sha>.json --image ... --metadata-file examples/fraud_triage_dataflow/metadata.json --sdk-language PYTHON`. SHA tagging makes every nightly's template spec + image immutable and attributable; a `latest` alias is deliberately not published (nothing downstream should depend on a moving nightly artifact while the package is `0.0.0`). The `skip-notice` job keeps covering forks. Region, bucket, and repo names come from repo variables alongside `GCP_PROJECT_ID`; whether the Artifact Registry repo is pre-provisioned or CI-created, and how old SHAs are garbage-collected, are Open Questions.

### D7. The nightly gate validates launch, not end-to-end semantics

`tests/dataflow/test_flex_template_launch.py` (marker `dataflow`, per [project.md](../../project.md) nightly-only) launches the just-built template spec with per-run-suffixed topics, the FakeLLM model string, and a short `hitl_timeout_ms`; polls until the job reaches `JOB_STATE_RUNNING`; then cancels and deletes the run's topics. Reaching `RUNNING` proves the chain this change owns: template spec resolves → launcher container starts → parameters parse → `AgentConfig`/`HitlPolicy` construct → job graph submits → worker containers pull the image and boot the SDK harness. It deliberately does not assert message flow: runtime semantics are gated elsewhere (offline semantics tier, Flink e2e gate), a data-bearing assertion would multiply nightly cost and flake surface (publishing, subscribing, and draining against a live job), and a launch-only gate keeps failures unambiguous — red means packaging broke. Timeout classification follows the e2e gate's discipline: infrastructure failure (quota, image pull) is reported distinctly from a launcher error surfaced in the job's error state. Extending to a small data-bearing run is future work, noted in Open Questions.

## Risks / Trade-offs

- **Protobuf major-version mismatch in the launcher base image.** The same trap [sdk-harness.Dockerfile:23](../../../docker/sdk-harness.Dockerfile:23) documents for the Beam SDK image plausibly applies to Google's launcher base image. Mitigated by the explicit `protobuf` 6.x pin and the build-time import self-check (D2); if the launcher base hard-pins an incompatible runtime, the fallback is the multi-stage split in D2's trade-off note.
- **Nightly cost and quota.** Each nightly adds an image build/push, GCS spec write, and a short-lived streaming Dataflow job. The job is cancelled as soon as `RUNNING` is observed and the test enforces a hard deadline + guaranteed-cancel teardown so a wedged launch cannot leak a running job past the workflow's `timeout-minutes`.
- **Grammar drift between `metadata.json` regexes and Python validation.** The regexes are a UX convenience, not the contract; the launcher's construction-time validation is authoritative. The offline metadata test pins the parameter names/required-flags, and a launcher test asserts that a value accepted by each regex is accepted by the corresponding Python parser, so drift fails offline in `ci`, not at nightly launch.
- **Template freshness vs. version `0.0.0`.** With no releases, the template is only as current as the last green nightly, and there is no stable artifact to hand users. Accepted for now: SHA-tagged nightly artifacts are explicitly non-public; promoting a template build into the (future) release process is out of scope and flagged for the release-engineering roadmap item.
- **Dependency on two unlanded siblings.** C24's example layout and C36's grammar may shift before this implements. The spec here binds to their *names and responsibilities*, not their internals; tasks include a reconciliation step, and internals-level assumptions are quarantined in Open Questions.

## Migration Plan

Purely additive: no existing file changes semantics, no state schemas are touched, and no user is running a template today. Sequencing: land after C24 and C36 merge; first nightly on the merged tree exercises build → publish → launch. If the nightly gate is red on packaging, the template directory can be reverted without touching runtime or examples. Parameter-surface evolution after first landing is additive-only (new optional parameters); renaming or retyping a parameter is a breaking change to launch invocations and requires its own change proposal.

## Spike findings (tasks 2.1–2.3; answers Open Questions 1, 2, 6)

Resolved by reading the launcher base image's own registry metadata rather than
trusting prose — `gcr.io` serves manifests and config blobs over anonymous
HTTPS, so the questions below were answered from the artifact itself:

- **Launcher base image + digest.** `gcr.io/dataflow-templates-base/python311-template-launcher-base`,
  pinned at `sha256:35e7280471e19e0dfa4399944978569f5f4adee51d6d10191d613295cae8b75d`
  (label `com.google.cloud.dataflow.flex-templates.version=flex_templates_base_image_release_20260720_RC00`).
- **Open Question 2 — one image, and the base already is one.** The image's
  config blob shows `ENTRYPOINT ["/opt/apache/beam/boot"]` and a build history
  that installs `apache-beam[gcp]` from `/opt/apache/beam/tars/` *before*
  copying `/opt/google/dataflow/python_template_launcher` in. The launcher base
  **is** the Beam Python SDK harness image plus the launcher binary, so a single
  image serves both roles with no multi-stage split and no entrypoint conflict:
  Dataflow overrides the entrypoint with the launcher when it runs the launcher
  container, and `/opt/apache/beam/boot` is what worker containers exec. D2
  stands as written; its multi-stage fallback is not needed.
- **Entrypoint env var.** `FLEX_TEMPLATE_PYTHON_PY_FILE`, an absolute path to a
  `.py` file the launcher executes; the launched parameters arrive as
  `--<name>=<value>` argv, with Dataflow's own pipeline options appended. The
  launcher module therefore parses with `parse_known_args`: named parameters are
  ours, the remainder are `PipelineOptions`.
  Because Beam pickles by module reference, the file named by that variable is a
  three-line shim (`main.py`) that imports `launch.main` — nothing a pipeline
  references may be defined in the `__main__` module, or workers cannot unpickle it.
- **Worker image selection.** The image cannot know its own published tag, so the
  Dockerfile takes it as a build arg and bakes it into
  `BEAM_AGENTS_TEMPLATE_IMAGE`; the launcher appends
  `--sdk_container_image=<that>` when it is set. Explicit, and unit-testable
  offline via the launcher's Beam-argument builder.
- **`gcloud dataflow flex-template build` flags.** `<gcs-spec-path> --image
  <uri> --sdk-language PYTHON --metadata-file <path>` — the set D6 already
  names. No parameter-length limit is documented that a `pubsub://` URI or a
  Secret Manager resource name could approach.
- **Open Question 6 — launch mechanism.** `gcloud dataflow flex-template run`,
  shelled out, for fidelity with the documented user path (the base image ships
  the Cloud SDK and the runner has `gcloud`); polling and cancellation reuse
  C46's `DataflowJobs` REST client rather than a second CLI surface.

## Open Questions

1. ~~**Exact Flex Template launcher surface.**~~ Answered in Spike findings above.
2. ~~**Can one image serve as both launcher and `sdk_container_image`**~~ Yes — answered in Spike findings above.
3. **Artifact Registry provisioning and retention:** is the `beam-agents` repo pre-provisioned by an admin (one-time `gcloud artifacts repositories create`, documented in `docs/ci.md`) or created idempotently by the workflow? What GC policy for SHA-tagged images and GCS template specs?
4. **Read-side topic vs. subscription:** the launch test and simple deployments can read `topic=` (auto-created subscription), but production HITL re-injection may warrant explicit subscription parameters (`input_subscription`, `approvals_subscription`) for retention/replay control. Additive later if so.
5. ~~**C36 grammar finalization:**~~ Settled: C36 shipped `module:object` references plus a separate `provider_config` mapping (D4, revised). Sampling parameters and `openai-compat` base URLs live in `provider_config`, which the closed parameter surface does not expose; a template that needs them is an additive follow-up (`provider_config_json`, say), not a regrammar.
6. ~~**Launch mechanism in the test:**~~ `gcloud dataflow flex-template run` — answered in Spike findings above.
7. **Future data-bearing smoke:** whether a follow-up extends the gate to publish a handful of events and assert intents appear (requires FakeLLM-over-HTTP reachable from Dataflow workers — likely the FakeLLM served from the worker container itself or a config-string-selectable in-process fake). Out of scope here.
