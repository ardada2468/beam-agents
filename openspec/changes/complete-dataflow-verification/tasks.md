# Tasks: complete-dataflow-verification

Two rules carried over from `verify-live-infrastructure`, because they are what makes a result mean
something:

1. **Never weaken a test, threshold or gate to obtain a green run.** A failure is triaged, recorded,
   and filed.
2. **Record one of four verdicts per phase**: `pass`, `fail (defect)`, `fail (infra)`, `blocked`. A
   capacity or quota failure from the cloud provider is `fail (infra)`, is remediated, and is never
   recorded as a verdict on the gate.

## 1. Flex Template — provision the publishing chain

- [ ] 1.1 Create an Artifact Registry **docker** repository in the verification project and record its
  id, region and the full image path. Set `GCP_ARTIFACT_REGISTRY_REPO` (repository variable for CI;
  environment variable for a local run).
- [ ] 1.2 Grant the pushing identity `roles/artifactregistry.writer` and confirm
  `gcloud auth configure-docker <region>-docker.pkg.dev` succeeds.
- [ ] 1.3 Build and push the template image exactly as the nightly job does — build context is the
  **repository root**:
  ```
  docker build -f examples/fraud_triage_dataflow/Dockerfile \
    --build-arg TEMPLATE_IMAGE=<region>-docker.pkg.dev/<project>/<repo>/fraud-flex:<sha> \
    -t <region>-docker.pkg.dev/<project>/<repo>/fraud-flex:<sha> .
  docker push <same tag>
  ```
  Note the image is `linux/amd64`; building on an arm64 host needs `--platform linux/amd64` or the
  workers will fail to start. Record whether that was required.
- [ ] 1.4 Confirm the built image imports the package — `docker run --rm --entrypoint python <image>
  -c "import beam_agents, pydantic_ai"`. The image pins `protobuf==6.33.6` independently of
  `pyproject.toml`; verify the two agree rather than assuming it (this is the surface `D-11` came
  from — the constraint lived only in Dockerfiles).
- [ ] 1.5 Publish the spec:
  ```
  gcloud dataflow flex-template build gs://<bucket>/templates/fraud-flex-<sha>.json \
    --project=<project> --image=<image> --sdk-language=PYTHON \
    --metadata-file=examples/fraud_triage_dataflow/metadata.json
  ```

## 2. Flex Template — run the gate

- [ ] 2.1 `BEAM_AGENTS_FLEX_TEMPLATE_SPEC=<spec>` plus `GCP_PROJECT_ID`, `GCP_REGION`,
  `GCP_DATAFLOW_TEMP_BUCKET`, then `make test-dataflow`. The launch test must **run**, not skip —
  a reported skip means the spec variable did not reach it.
- [ ] 2.2 Confirm the verdict is the one state transition the test asserts: `JOB_STATE_RUNNING`.
  Per its design D7 it asserts nothing about message flow; do not add data-bearing assertions.
- [ ] 2.3 On a failure before `RUNNING`, record the service's own error state and classify it: a
  launcher/parameter defect reads differently from quota or an image-pull failure. There is no retry
  and no flake-tolerant skip.
- [ ] 2.4 Verify the secret-handling requirement of `add-dataflow-flex-template` explicitly: the
  launcher resolves its provider API key from Secret Manager and **no secret value appears in the
  template parameters, the job options, or the logs**. Check the launched job's parameters via
  `gcloud dataflow jobs describe` and grep the launcher logs. This is a specified requirement, not an
  incidental check.
- [ ] 2.5 Confirm teardown: the job is cancelled and no job created by the run remains active.
- [ ] 2.6 Record the verdict, the image digest, the spec path, and the observed cost/duration.

## 3. Cross-version `--update` — remove the `pip` dependency

- [ ] 3.1 Replace `download_wheel_command` (`tests/dataflow/_update/versions.py:197`), which shells out
  to `sys.executable -m pip download` and cannot work in the uv-managed venv the nightly job
  provisions. There is **no** `uv pip download`; the two viable shapes are:
  - `uv pip install --python <prev_venv_python> beam-agents==<version>` after creating the venv,
    dropping the separate download/`install_wheel_command` pair; or
  - reorder provisioning so `create_venv_command` runs first and the download executes through that
    venv's own `pip`, which `python -m venv` does provide.
  Pick one, and say in the code comment why — `freeze_command` above it already carries the
  equivalent explanation.
- [ ] 3.2 Keep the design D3 property that the previous release resolves **its own** pinned
  `apache-beam[gcp]`: whatever replaces the install must not force the two legs to equal versions.
  That Beam skew is the real user upgrade path.
- [ ] 3.3 `uv run pytest tests/dataflow/test_update_compat_harness.py` green (46 offline tests cover
  the command construction and the bootstrap/cross-version plan).

## 4. Cross-version `--update` — obtain the real evidence

- [ ] 4.1 **Blocked until a version is published.** The bootstrap leg engages only because
  `beam-agents` 404s on PyPI. Once a release exists, the harness selects the cross-version path
  automatically.
- [ ] 4.2 Run `make test-dataflow` against a tree whose previous released version is resolvable.
  Confirm from the banner that the run is `CROSS_VERSION`, **not** `BOOTSTRAP` — that string is the
  difference between evidence and a self-update.
- [ ] 4.3 Confirm the assertions hold across versions: the continuation nonce, the memory marker, and
  a fresh key all survive the update.
- [ ] 4.4 Record both legs' full `pip freeze` output. Per the harness's own docstring, a compat failure
  is meaningless without knowing which two environments collided, and the Beam skew inside the update
  is the first thing a triager asks about.
- [ ] 4.5 Record the verdict, and update `verification-report.md`'s phase-6 caveat — it currently
  states that cross-version compatibility is **not** proven.

## 5. Gates

- [ ] 5.1 `make lint` and `make type` clean.
- [ ] 5.2 `make test-unit` and `make test-semantics-offline` unaffected.
- [ ] 5.3 `make test-dataflow` green with **both** tests executing — neither skipped.
- [ ] 5.4 No Dataflow job left active in any region the run touched.
- [ ] 5.5 `openspec validate complete-dataflow-verification --strict` passes.
