## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 `tests/examples/test_flex_template_metadata.py`: parse the committed `metadata.json` and assert the exact parameter surface — names, required/optional flags, help text presence, and the `pubsub://` regexes on the four topic parameters ("metadata.json declares the full parameter surface"). Offline, no docker.
- [ ] 1.2 `tests/examples/test_flex_template_launcher.py`: launcher parameter mapping as pure-function tests — topic URIs map onto reads/`intents_to`/`output_topic` ("Topic parameters map onto the pipeline"); a malformed topic URI raises the sink resolver's error naming the parameter, with no pipeline built ("A malformed topic URI fails before workers start"); `hitl_timeout_ms` reaches `HitlPolicy.timeout_ms` and omission takes the default ("HITL timeout parameter reaches HitlPolicy"); regex/parser agreement — a value each metadata regex accepts is accepted by the corresponding Python validation (design Risks).
- [ ] 1.3 Model-string tests in the same module: a parser-accepted string yields that parser's `provider_factory`/`decode` ("A valid model string selects the provider and model"); a rejected string exits naming `model` ("An invalid model string fails at launch"); the FakeLLM string requires no secret ("The FakeLLM selection launches without credentials").
- [ ] 1.4 Secret-handling tests with a fake Secret Manager client: only the resource name appears in constructed pipeline options ("Secret value never transits the launch surface"); credentialed provider + missing secret parameter exits naming both parameters ("Credentialed provider without a secret is rejected at launch"); a resolution failure's message contains the resource name and never a value ("Errors never echo secret material").
- [ ] 1.5 `tests/dataflow/test_flex_template_launch.py`, marked `dataflow`: launch the SHA-addressed spec with unique topics + FakeLLM string, poll to `JOB_STATE_RUNNING`, cancel, delete topics in guaranteed teardown; report job error state on pre-RUNNING failure ("Nightly launch reaches RUNNING and is torn down", "A launcher failure is reported as a packaging defect"). Until nightly wiring lands this collects and skips outside the nightly env — the repo's first `-m dataflow` test.

## 2. Spike: launcher surface and image shape (design Open Questions 1, 2, 6)

- [ ] 2.1 Verify against current GCP docs: launcher base image + digest, the Python entrypoint env var, and the current `gcloud dataflow flex-template build` flag set; record findings in `design.md`, replacing Open Question 1 with answers.
- [ ] 2.2 Confirm one image can serve as both launcher and `sdk_container_image` (design D2); if not, restructure the Dockerfile as multi-stage two-target and update D2.
- [ ] 2.3 Decide gcloud CLI vs `google-cloud-dataflow-client` for the launch test (Open Question 6) and record the choice.

## 3. Launcher and metadata

- [ ] 3.1 Implement `examples/fraud/dataflow/launch.py`: parse parameters; validate topics via the sink-resolver grammar; resolve `model` through the C36 parser; build `HitlPolicy`/`AgentConfig`; call the fraud example's pipeline-assembly function (imported from C24's `examples/fraud/`, never duplicated); run with streaming Dataflow options.
- [ ] 3.2 Implement worker-side secret resolution in the launcher-built provider factory: `access_secret_version` at provider construction via ADC; enforce the credentialed-provider-requires-secret rule at launch time (design D5).
- [ ] 3.3 Author `examples/fraud/dataflow/metadata.json` with the full parameter surface, regexes derived from the sink-resolver grammar and C36's final model grammar (Open Question 5), and help text.
- [ ] 3.4 Reconcile with the merged C24 layout and C36 parser API; adjust imports/paths and update this change's design if either shifted (design Risks).

## 4. Image and template build

- [ ] 4.1 Write `examples/fraud/dataflow/Dockerfile` per design D2: digest-pinned base, locked deps, `protobuf` 6.x pin, `beam_agents` via `--no-deps` + named runtime deps, `google-cloud-secret-manager`, baked-in `examples/fraud/`, and the build-time import self-check covering `beam_agents.core.transform`, the fraud pipeline module, and the C36 parser ("A broken import fails the build, not the launch").
- [ ] 4.2 Script the build/publish: image → Artifact Registry `fraud-flex:<git-sha>`, `gcloud dataflow flex-template build` → GCS spec `fraud-flex-<git-sha>.json`; no `latest` alias ("Artifacts are SHA-addressed", "Digest-pinned, lock-driven image build").
- [ ] 4.3 Settle Artifact Registry provisioning + retention (Open Question 3); document the one-time setup in `docs/ci.md` if pre-provisioned.

## 5. Nightly CI wiring

- [ ] 5.1 Add build/publish steps to the `dataflow` job in `.github/workflows/nightly.yml` ahead of `make test-dataflow`, using the existing WIF auth and `vars.GCP_PROJECT_ID` gate; add region/bucket/repo repo-variables; leave `skip-notice` covering the unset-project path ("The gate is skipped, not failed, without a GCP project").
- [ ] 5.2 Pass the built spec path/SHA into the launch test via env; confirm the job's `timeout-minutes` budget covers build + push + launch + cancel.
- [ ] 5.3 Run the full nightly path once (workflow_dispatch) on a GCP-configured tree and confirm green end to end.

## 6. Docs

- [ ] 6.1 `examples/fraud/dataflow/README.md`: parameter table, one-command `gcloud dataflow flex-template run` example, Secret Manager setup for provider keys.
- [ ] 6.2 Cross-link from the C24 fraud example page; add the launcher-vs-local-stack image distinction note to `docker/README.md`.

## 7. Gates

- [ ] 7.1 `make lint` and `make type` clean (launcher and tests fully typed; `mypy --strict`).
- [ ] 7.2 `make test-unit` green offline with the new metadata/launcher/secret tests; coverage ratchet does not regress.
- [ ] 7.3 `uv run pre-commit run --all-files` clean.
- [ ] 7.4 `openspec validate add-dataflow-flex-template --strict` passes.
