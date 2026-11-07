# Tasks: complete-dataflow-verification

Two rules carried over from `verify-live-infrastructure`, because they are what makes a result mean
something:

1. **Never weaken a test, threshold or gate to obtain a green run.** A failure is triaged, recorded,
   and filed.
2. **Record one of four verdicts per phase**: `pass`, `fail (defect)`, `fail (infra)`, `blocked`. A
   capacity or quota failure from the cloud provider is `fail (infra)`, is remediated, and is never
   recorded as a verdict on the gate.

## 1. Flex Template — provision the publishing chain

- [x] 1.1 Create an Artifact Registry **docker** repository in the verification project and record its
  id, region and the full image path. Set `GCP_ARTIFACT_REGISTRY_REPO` (repository variable for CI;
  environment variable for a local run).
  — *Done (recorded 2026-11-07 from the nightly job's own log, run 40192263829 / job 121165584279, 2026-10-25, `main` @ `407edab`):* repository `beam-agents`, region `us-east1`, project `beamagents`; image path
  `us-east1-docker.pkg.dev/beamagents/beam-agents/fraud-flex:<sha>`. `GCP_ARTIFACT_REGISTRY_REPO=beam-agents` is set
  as a repository variable — the `Template build configuration` step resolved a non-empty image, which needs all
  three template variables set. `docs/ci.md` documents the repository, the variable, and the grants.

- [x] 1.2 Grant the pushing identity `roles/artifactregistry.writer` and confirm
  `gcloud auth configure-docker <region>-docker.pkg.dev` succeeds.
  — *Done, exercised rather than asserted:* the job's `gcloud auth configure-docker us-east1-docker.pkg.dev
  --quiet` logged `Adding credentials for: us-east1-docker.pkg.dev`, and the `docker push` that followed succeeded
  (digest under 1.3). The grant itself is the `roles/artifactregistry.writer` row in `docs/ci.md`'s IAM table.

- [x] 1.3 Build and push the template image exactly as the nightly job does — build context is the
  **repository root**:
  ```
  docker build -f examples/fraud_triage_dataflow/Dockerfile \
    --build-arg TEMPLATE_IMAGE=<region>-docker.pkg.dev/<project>/<repo>/fraud-flex:<sha> \
    -t <region>-docker.pkg.dev/<project>/<repo>/fraud-flex:<sha> .
  docker push <same tag>
  ```
  Note the image is `linux/amd64`; building on an arm64 host needs `--platform linux/amd64` or the
  workers will fail to start. Record whether that was required.
  — *Done, by the nightly job:* built from the repository root exactly as above and pushed as
  `fraud-flex:407edab7e9baa3b590f0e0a7e2dda7b068dde76c`, digest
  `sha256:e26192747974e08f8d6322c5872c9b49eb5a6746f2ee8442c184e693d7e1868f` (manifest size 5569). The builder is
  `ubuntu-latest` (amd64), so `--platform linux/amd64` was **not** required there; an arm64 developer host still
  needs it.

- [x] 1.4 Confirm the built image imports the package — `docker run --rm --entrypoint python <image>
  -c "import beam_agents, pydantic_ai"`. The image pins `protobuf==6.33.6` independently of
  `pyproject.toml`; verify the two agree rather than assuming it (this is the surface `D-11` came
  from — the constraint lived only in Dockerfiles).
  — *Done — the import indirectly, the pin check directly.* The job has no standalone `docker run … -c "import
  beam_agents, pydantic_ai"` step, but the launch gate (2.1–2.2) reaching `JOB_STATE_RUNNING` from this image is
  that import executed in anger: the Flex Template launcher runs the example's pipeline module inside the image to
  construct and submit the graph, and an import failure surfaces as a launcher error before `RUNNING`. The pin
  agreement is checked against the tree: the Dockerfile pins `protobuf==6.33.6`, and `pyproject.toml` bounds the
  runtime to the 6.x major (`<7`, matching Beam's own bound) for the same `D-11` reason both comments cite, so
  they agree. A direct `docker run` import step would still be a cheap addition to the job; it is not needed to
  close this box.

- [x] 1.5 Publish the spec:
  ```
  gcloud dataflow flex-template build gs://<bucket>/templates/fraud-flex-<sha>.json \
    --project=<project> --image=<image> --sdk-language=PYTHON \
    --metadata-file=examples/fraud_triage_dataflow/metadata.json
  ```
  — *Done, by the nightly job:* the command above, with `--project=beamagents`, logged `Template File GCS
  Location: gs://beamagents-dataflow-temp-use1/templates/fraud-flex-407edab7e9baa3b590f0e0a7e2dda7b068dde76c.json`,
  and that same path reached the test as `BEAM_AGENTS_FLEX_TEMPLATE_SPEC`.

## 2. Flex Template — run the gate

- [x] 2.1 `BEAM_AGENTS_FLEX_TEMPLATE_SPEC=<spec>` plus `GCP_PROJECT_ID`, `GCP_REGION`,
  `GCP_DATAFLOW_TEMP_BUCKET`, then `make test-dataflow`. The launch test must **run**, not skip —
  a reported skip means the spec variable did not reach it.
  — *Done:* the job sets all four variables and runs `make test-dataflow`;
  `tests/dataflow/test_flex_template_launch.py .` in run 40192263829 — a pass, not an `s` — and likewise on every
  green `dataflow` night since 2026-08-20. The tier's `2 passed, 2860 deselected` is both tests, nothing skipped.

- [x] 2.2 Confirm the verdict is the one state transition the test asserts: `JOB_STATE_RUNNING`.
  Per its design D7 it asserts nothing about message flow; do not add data-bearing assertions.
  — *Done:* `test_the_published_template_launches_and_reaches_running` passed, and it asserts exactly
  `JOB_STATE_RUNNING` (a terminal state before it fails immediately via `_running_or_raise`). No data-bearing
  assertion was added.

- [x] 2.3 On a failure before `RUNNING`, record the service's own error state and classify it: a
  launcher/parameter defect reads differently from quota or an image-pull failure. There is no retry
  and no flake-tolerant skip.
  — *Not triggered:* the run reached `RUNNING`. The rule stands for the next red night — the test's failure
  message carries the service's own error state (`jobs.get(job_id).describe()`), and there is no retry or skip.

- [ ] 2.4 Verify the secret-handling requirement of `add-dataflow-flex-template` explicitly: the
  launcher resolves its provider API key from Secret Manager and **no secret value appears in the
  template parameters, the job options, or the logs**. Check the launched job's parameters via
  `gcloud dataflow jobs describe` and grep the launcher logs. This is a specified requirement, not an
  incidental check.
  — **Open (2026-11-07):** not evidenced by the nightly job as written. No step runs `gcloud dataflow jobs
  describe` on the launched job or greps the launcher logs for the secret value, and the launch test asserts a
  state transition only. Closing it needs either a one-off manual check against a nightly's job id (the ids are
  in the `dataflow` job log) or a new assertion in the job; it is left open rather than checked on inference.

- [x] 2.5 Confirm teardown: the job is cancelled and no job created by the run remains active.
  — *Done, by the test itself and cross-checked nightly:* the launch test cancels the job it created in its
  teardown ("wait for RUNNING, cancel, clean up" is its own docstring, and it passed, so teardown ran without
  raising). Whether anything was *left* active is checked every night by the `--update` harness's pre-run sweep,
  which lists the region's Dataflow jobs and fails as `InfraFailure` when it cannot — the 2026-09-08 red night
  (run 34223185987) was exactly that sweep failing on a token refresh, so the sweep demonstrably runs.

- [x] 2.6 Record the verdict, the image digest, the spec path, and the observed cost/duration.
  — *Recorded:* verdict `pass` (run 40192263829, 2026-10-25). Image digest
  `sha256:e26192747974e08f8d6322c5872c9b49eb5a6746f2ee8442c184e693d7e1868f` for tag
  `407edab7e9baa3b590f0e0a7e2dda7b068dde76c`; spec
  `gs://beamagents-dataflow-temp-use1/templates/fraud-flex-407edab7e9baa3b590f0e0a7e2dda7b068dde76c.json`.
  Duration: image build and push 12:15:05–12:16:29 (≈ 1.5 min); launch test 12:16:43–12:20:42 (≈ 4 min to
  `RUNNING`, then cancelled); whole `dataflow` job 12:14:54–12:40:48 (≈ 26 min of its 80-min budget, the
  `--update` gate being the other 20). Cost: **not captured** — the job has no billing read access and none was
  taken manually; recorded as unknown rather than estimated.

## 3. Cross-version `--update` — remove the `pip` dependency

- [x] 3.1 Replace `download_wheel_command` (`tests/dataflow/_update/versions.py:197`), which shells out
  to `sys.executable -m pip download` and cannot work in the uv-managed venv the nightly job
  provisions. There is **no** `uv pip download`; the two viable shapes are:
  - `uv pip install --python <prev_venv_python> beam-agents==<version>` after creating the venv,
    dropping the separate download/`install_wheel_command` pair; or
  - reorder provisioning so `create_venv_command` runs first and the download executes through that
    venv's own `pip`, which `python -m venv` does provide.
  Pick one, and say in the code comment why — `freeze_command` above it already carries the
  equivalent explanation.
  — *Done (2026-11-07): the second shape.* `create_venv_command` is now `uv venv --seed --python <sys.executable>
  <dir>` and runs **first**; `download_wheel_command(python, version, dest)` runs `<prev_venv_python> -m pip
  download …` through that venv's seeded pip; `install_wheel_command` is unchanged. The code comment says why:
  the Dataflow workers need the previous release's *wheel file* as `--extra_package`, and `uv pip install
  --python` leaves no wheel behind; `--seed` is chosen over relying on the base interpreter's `ensurepip`. This is
  also the shape `verification-report.md` phase 6 anticipated.

- [x] 3.2 Keep the design D3 property that the previous release resolves **its own** pinned
  `apache-beam[gcp]`: whatever replaces the install must not force the two legs to equal versions.
  That Beam skew is the real user upgrade path.
  — *Kept:* `install_wheel_command` still installs the wheel without `--no-deps`, so the previous release
  resolves its own pinned `apache-beam[gcp]`; nothing forces the two legs equal.

- [x] 3.3 `uv run pytest tests/dataflow/test_update_compat_harness.py` green (46 offline tests cover
  the command construction and the bootstrap/cross-version plan).
  — `uv run pytest tests/dataflow/test_update_compat_harness.py`: **52 passed** (the suite has grown from the
  46 this task was written against). The cross-version provisioning test now also asserts that the venv is
  created *before* the download and that the download's interpreter is the venv's, not `sys.executable`.

## 4. Cross-version `--update` — obtain the real evidence

- [ ] 4.1 **Blocked until a version is published.** The bootstrap leg engages only because
  `beam-agents` 404s on PyPI. Once a release exists, the harness selects the cross-version path
  automatically.
  — *Still blocked (2026-11-07):* PyPI returns 404 for `beam-agents`; every nightly banner since the tier first
  ran reads `SELF-UPDATE (BOOTSTRAP)`, and `pyproject.toml` is at `1.0.0a1`, a prerelease `parse_version` would
  ignore by design even once published. Nothing in §4 can move until a plain release is published.

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

- [x] 5.1 `make lint` and `make type` clean.
  — 2026-11-07: `make lint` (ruff check + format) and `make type` (`mypy --strict`, 416 source files) clean on
  this tree.

- [x] 5.2 `make test-unit` and `make test-semantics-offline` unaffected.
  — 2026-11-07: `make test-unit` 2642 passed, 220 deselected (branch coverage 92.72 %, at the re-measured
  baseline); `make test-semantics-offline` 79 passed, 1 declared skip. Both unaffected by the §3 change, which lives
  under `tests/dataflow/` and is deselected by both tiers.

- [x] 5.3 `make test-dataflow` green with **both** tests executing — neither skipped.
  — *Evidenced by nightly run 40192263829:* `2 passed` with both tests executing; see 2.1.

- [ ] 5.4 No Dataflow job left active in any region the run touched.
  — *Not independently verified from this checkout (no GCP credentials here).* The launch test cancels its own
  job (2.5) and the `--update` harness sweeps the region before each run; the next green sweep after a run is the
  standing check. Left open rather than inferred.

- [x] 5.5 `openspec validate complete-dataflow-verification --strict` passes.
  — 2026-11-07: `npx @fission-ai/openspec@1.7.0 validate complete-dataflow-verification --strict` → "Change
  'complete-dataflow-verification' is valid".
