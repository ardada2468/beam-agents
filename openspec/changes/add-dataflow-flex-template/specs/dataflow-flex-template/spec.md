## ADDED Requirements

### Requirement: The template declares a validated parameter surface covering topics, model, and HITL timeout

The Flex Template's `metadata.json` SHALL declare, as required parameters: `input_topic`, `approvals_topic`, `output_topic`, `intents_topic`, and `model`; and SHALL declare `hitl_timeout_ms`, `errors_to`, `traces_to`, and `model_api_key_secret` as optional parameters. Every topic parameter SHALL take the sink resolver's `pubsub://<project>/<topic>` URI grammar, enforced by a `metadata.json` regex AND re-validated by the launcher via the runtime's construction-time checks, so a malformed value is rejected before any Dataflow workers start. The launcher SHALL map `hitl_timeout_ms` onto `HitlPolicy.timeout_ms` (defaulting to the runtime default when omitted) and SHALL surface `HitlPolicy`'s own `ValueError` for non-positive values.

#### Scenario: metadata.json declares the full parameter surface

- **WHEN** the committed `metadata.json` is parsed
- **THEN** it declares exactly the parameters above, with `input_topic`, `approvals_topic`, `output_topic`, `intents_topic`, and `model` marked required and the rest optional, each carrying help text and (for topics) the `pubsub://` grammar regex

#### Scenario: Topic parameters map onto the pipeline

- **WHEN** the launcher is invoked with valid `pubsub://` values for all four topic parameters
- **THEN** the read side consumes `input_topic` and `approvals_topic`, the fraud pipeline's `AgentConfig.intents_to` is the given `intents_topic` URI, and terminal decisions are written to `output_topic`

#### Scenario: A malformed topic URI fails before workers start

- **WHEN** the launcher receives a topic parameter that is not a valid `pubsub://<project>/<topic>` URI
- **THEN** it exits with the sink resolver's actionable error naming the parameter and the offending URI, and no Dataflow job graph is submitted

#### Scenario: HITL timeout parameter reaches HitlPolicy

- **WHEN** the launcher is invoked with `hitl_timeout_ms=60000`
- **THEN** the constructed `HitlPolicy.timeout_ms` is 60000; and **WHEN** `hitl_timeout_ms` is omitted, **THEN** the runtime default applies

### Requirement: Model and provider selection is a single config string shared with the YAML provider

The `model` parameter SHALL be a single string in the config-string grammar defined by the `yaml-provider` capability (`add-yaml-provider`), and the launcher SHALL resolve it through that capability's public parser to obtain the `provider_factory` and paired `decode` for `AgentConfig`. The template SHALL NOT define its own provider-naming grammar. An unrecognized or malformed `model` string SHALL fail the launch with the parser's error naming the `model` parameter, before job submission. The grammar's FakeLLM selection SHALL be launchable so validation runs need no provider credentials.

#### Scenario: A valid model string selects the provider and model

- **WHEN** the launcher receives a `model` string that the YAML provider's parser accepts
- **THEN** the resulting `AgentConfig.provider_factory` and `decode` are exactly what the parser resolved for that string

#### Scenario: An invalid model string fails at launch

- **WHEN** the launcher receives a `model` string the parser rejects
- **THEN** the launcher exits with the parser's error message naming the `model` parameter, and no Dataflow job graph is submitted

#### Scenario: The FakeLLM selection launches without credentials

- **WHEN** the template is launched with the grammar's FakeLLM model string and no `model_api_key_secret`
- **THEN** the launch proceeds with no Secret Manager access and no provider credential anywhere in the parameters

### Requirement: Provider API keys are supplied via Secret Manager resource names, never as parameter values

The template SHALL NOT accept any provider API key value as a template parameter. Credentialed providers SHALL be configured via `model_api_key_secret`, a Secret Manager resource name (`projects/*/secrets/*/versions/*`); the secret value SHALL be resolved worker-side at provider construction using the job service account's Application Default Credentials. The resolved value SHALL NOT appear in pipeline options, the serialized pipeline, job metadata, or log/error output. When the selected provider requires a credential and `model_api_key_secret` is absent, the launcher SHALL fail before job submission with an error naming both the `model` and `model_api_key_secret` parameters.

#### Scenario: Secret value never transits the launch surface

- **WHEN** the template is launched with a credentialed `model` string and a `model_api_key_secret` resource name
- **THEN** the launch parameters and constructed pipeline options contain only the resource name, and the key value is fetched from Secret Manager on the worker at provider construction

#### Scenario: Credentialed provider without a secret is rejected at launch

- **WHEN** the `model` string selects a provider that requires an API key and `model_api_key_secret` is not set
- **THEN** the launcher exits with an error naming both parameters, and no Dataflow job graph is submitted

#### Scenario: Errors never echo secret material

- **WHEN** worker-side secret resolution fails (missing secret, denied access)
- **THEN** the surfaced error names the secret's resource name and the failing operation, and no output path contains a resolved secret value

### Requirement: The template image and spec build reproducibly from the committed tree

The template Dockerfile SHALL pin its base image by digest and install dependencies at versions locked by the repository (committed `uv.lock` / explicit pins), including a `protobuf` runtime pin matching the committed 6.x gencode. The image build SHALL fail if `beam_agents`, the fraud pipeline module, or the provider config-string parser fail to import (build-time self-check). The build workflow SHALL tag the pushed image and the GCS template spec with the git commit SHA they were built from, and SHALL NOT publish a mutable `latest` alias.

#### Scenario: Digest-pinned, lock-driven image build

- **WHEN** the template image is built twice from the same commit
- **THEN** both builds resolve the same base image digest and the same dependency versions

#### Scenario: A broken import fails the build, not the launch

- **WHEN** the image is built from a tree where the fraud pipeline module or provider parser cannot be imported
- **THEN** `docker build` fails at the self-check step and no image is pushed

#### Scenario: Artifacts are SHA-addressed

- **WHEN** the nightly build publishes the template
- **THEN** the Artifact Registry image tag and the GCS spec object name both carry the built commit's SHA, and no `latest` tag is updated

### Requirement: A nightly launch validation gates the template in the Dataflow lane

A test marked `dataflow` SHALL launch the template spec built in the same nightly run, using per-run uniquely named topics, the FakeLLM model string, and no secret parameter; SHALL assert the job reaches `JOB_STATE_RUNNING` within a bounded deadline; and SHALL then cancel the job and delete the run's topics, with teardown guaranteed on failure and timeout. The test SHALL run only in the nightly `dataflow` job (gated on `vars.GCP_PROJECT_ID`), riding the existing `make test-dataflow` step. A launch that fails before `RUNNING` SHALL be reported with the job's error state so packaging failures are distinguishable from infrastructure failures.

#### Scenario: Nightly launch reaches RUNNING and is torn down

- **WHEN** the nightly `dataflow` job runs after building and publishing the template
- **THEN** the launch test observes `JOB_STATE_RUNNING` within its deadline, cancels the job, and deletes the run's topics even if an assertion failed

#### Scenario: The gate is skipped, not failed, without a GCP project

- **WHEN** `vars.GCP_PROJECT_ID` is unset (e.g. a fork)
- **THEN** the nightly `dataflow` job does not run and the skip-notice path reports the skip, unchanged from today

#### Scenario: A launcher failure is reported as a packaging defect

- **WHEN** the launched job fails before reaching `JOB_STATE_RUNNING`
- **THEN** the test fails with the job's error details in its report, distinguishing a launcher/parameter error from quota or image-pull infrastructure failure
