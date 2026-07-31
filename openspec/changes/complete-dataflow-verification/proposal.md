## Why

`verify-live-infrastructure` executed the Dataflow tier for the first time and got the
`--update` state-compatibility gate to pass against a real project (1 passed, 531 s, project
`beamagents`, region `us-east1`, teardown clean). Getting there required fixing two defects that made
the tier impossible to run at all — an unbounded `protobuf` requirement that left `beam_agents`
unimportable on a stock Dataflow worker, and a `pip freeze` call that could not work in the
uv-managed venv CI provisions with.

Three gaps remain before the Dataflow tier is genuinely proven. None is speculative; each is a
specific thing that has never executed or is known to be broken on a path that could not be reached.

**1. The Flex Template launch gate has never run.** `tests/dataflow/test_flex_template_launch.py`
skips without `BEAM_AGENTS_FLEX_TEMPLATE_SPEC`, and that spec is published by a nightly step that
requires `GCP_ARTIFACT_REGISTRY_REPO` — which is not configured. So the entire packaging chain the
`dataflow-flex-template` capability owns is unverified: the spec in GCS, the launcher container, the
parameter parse, `AgentConfig`/`HitlPolicy` construction, graph submission, and worker containers
pulling the image and booting the SDK harness. A user launching the published template exercises
every one of those, and none has been executed once.

**2. Cross-version `--update` compatibility is unproven.** The gate passed on its documented
bootstrap **head → head** leg, because no version has ever been tagged — the harness resolved
`beam-agents` on PyPI, got a 404, and said so itself:

```
NOTE: this is a SELF-UPDATE run. It proves the harness, the Dataflow update mechanics, and that
head's job graph is update-compatible with itself. It is NOT cross-version evidence.
```

That proves the mechanism. It does not prove the guarantee `add-dataflow-update-compat` actually
makes, which is that a *previously released* pipeline can be updated to head with live keyed state
intact. Until a release exists to update from, that guarantee has no evidence.

**3. A latent `pip` dependency will break the cross-version leg the moment it can run.**
`tests/dataflow/_update/versions.py:197`'s `download_wheel_command` shells out to
`sys.executable -m pip download`. `freeze_command` was fixed to `uv pip freeze` after it failed for
exactly this reason — the job's venv is uv-managed and has no `pip` — but `download_wheel_command`
runs **only on the cross-version path**, which has never been reachable, so it was left in place
rather than edited untested. It will fail on the first cross-version run.

## What Changes

- **Provision the Flex Template chain**: an Artifact Registry repository, the template image built and
  pushed from `examples/fraud_triage_dataflow/Dockerfile`, and a template spec published to GCS, so
  `BEAM_AGENTS_FLEX_TEMPLATE_SPEC` can be set and the launch gate can run.
- **Make the cross-version `--update` leg executable**: replace the `pip`-dependent
  `download_wheel_command` with a uv-driven equivalent (or reorder provisioning so the download runs
  through the pip-bearing previous-release venv), and prove it against a real published version once
  one exists.
- **Record the cross-version result** as the evidence `add-dataflow-update-compat`'s guarantee has
  been waiting for, replacing the bootstrap caveat currently standing in for it.

## Capabilities

### Modified Capabilities

- `dataflow-flex-template`: its launch scenarios gain their first execution. **No requirement
  changes** — the scenarios are already specified; they have simply never run, so no delta is
  proposed for them.
- `state-guarantees` (which owns the nightly Dataflow `--update` gate): gains two requirements that
  this run's failures showed were assumed rather than stated — that the gate's provisioning cannot
  depend on tooling absent from the project's own managed environment, and that a bootstrap
  self-update run is recorded distinctly from cross-version evidence.

## Impact

- **Depends on** a tagged release for gap 2. Gaps 1 and 3 are independent and can land first.
- **Modified code:** `tests/dataflow/_update/versions.py` (`download_wheel_command`). Possibly
  `.github/workflows/nightly.yml` if the template build needs adjusting once exercised.
- **New infrastructure:** one Artifact Registry repository in the verification project, plus the
  repository variable `GCP_ARTIFACT_REGISTRY_REPO`.
- **Cost:** the flex-template gate builds and pushes a container image and launches a streaming job to
  `JOB_STATE_RUNNING` before cancelling. Comparable to the `--update` gate's ~9 minutes.
- **Not in scope:** the Spark leg (see `record-spark-sdf-checkpoint-gap`) and the mutation survivors
  (see `close-core-mutation-gaps`).
