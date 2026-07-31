## 1. Tests (written first, must fail for the right reason)

- [x] 1.1 `tests/examples/test_flex_template_metadata.py`: parse the committed `metadata.json` and assert the exact parameter surface — names, required/optional flags, help text presence, and the `pubsub://` regexes on the four topic parameters ("metadata.json declares the full parameter surface"). Offline, no docker. — 22 tests; drift guard verified by deleting `traces_to` from the document, which reddened 4 tests including `test_metadata_parameters_match_the_launchers_accepted_flags`.
- [x] 1.2 `tests/examples/test_flex_template_launcher.py`: launcher parameter mapping as pure-function tests — topic URIs map onto reads/`intents_to`/`output_topic` ("Topic parameters map onto the pipeline"); a malformed topic URI raises the sink resolver's error naming the parameter, with no pipeline built ("A malformed topic URI fails before workers start"); `hitl_timeout_ms` reaches `HitlPolicy.timeout_ms` and omission takes the default ("HITL timeout parameter reaches HitlPolicy"); regex/parser agreement — a value each metadata regex accepts is accepted by the corresponding Python validation (design Risks). — the wiring assertion walks the applied graph's Pub/Sub endpoints, not just the plan; "no job graph submitted" is asserted by a `run_pipeline` spy that must stay empty.
- [x] 1.3 Model-string tests in the same module: a parser-accepted string yields that parser's `provider_factory`/`decode` ("A valid model string selects the provider and model"); a rejected string exits naming `model` ("An invalid model string fails at launch"); the FakeLLM string requires no secret ("The FakeLLM selection launches without credentials"). — `decode` is not a template parameter; see Revision 2.
- [x] 1.4 Secret-handling tests with a fake Secret Manager client: only the resource name appears in constructed pipeline options ("Secret value never transits the launch surface"); credentialed provider + missing secret parameter exits naming both parameters ("Credentialed provider without a secret is rejected at launch"); a resolution failure's message contains the resource name and never a value ("Errors never echo secret material"). — plus two guards beyond the scenarios: a `--api_key=` flag is refused even as a pipeline option, and no declared parameter may read as a credential slot.
- [x] 1.5 `tests/dataflow/test_flex_template_launch.py`, marked `dataflow`: launch the SHA-addressed spec with unique topics + FakeLLM string, poll to `JOB_STATE_RUNNING`, cancel, delete topics in guaranteed teardown; report job error state on pre-RUNNING failure ("Nightly launch reaches RUNNING and is torn down", "A launcher failure is reported as a packaging defect"). Until nightly wiring lands this collects and skips outside the nightly env — the repo's first `-m dataflow` test. — `make test-dataflow` now collects 2 tests, both reporting skips; C46's `DataflowJobs`/`PubSub`/`guaranteed_teardown` harness is reused rather than re-grown. Never executed against real GCP here (blocked: needs cloud).

## 2. Spike: launcher surface and image shape (design Open Questions 1, 2, 6)

- [x] 2.1 Verify against current GCP docs: launcher base image + digest, the Python entrypoint env var, and the current `gcloud dataflow flex-template build` flag set; record findings in `design.md`, replacing Open Question 1 with answers. — answered from the artifact rather than prose: `gcr.io`'s anonymous manifest/blob API gave the digest (`sha256:35e7280…`) and the image config. Recorded under "Spike findings".
- [x] 2.2 Confirm one image can serve as both launcher and `sdk_container_image` (design D2); if not, restructure the Dockerfile as multi-stage two-target and update D2. — confirmed: the launcher base's config blob shows `ENTRYPOINT ["/opt/apache/beam/boot"]` and a history that installs `apache-beam[gcp]` before copying the launcher binary in. It *is* the SDK harness image. No multi-stage split; D2 stands.
- [x] 2.3 Decide gcloud CLI vs `google-cloud-dataflow-client` for the launch test (Open Question 6) and record the choice. — `gcloud dataflow flex-template run` for path fidelity (the base image ships the Cloud SDK); polling/cancel reuse C46's REST client.

## 3. Launcher and metadata

- [x] 3.1 Implement `examples/fraud_triage_dataflow/launch.py`: parse parameters; validate topics via the sink-resolver grammar; resolve `model` through the C36 parser; build `HitlPolicy`/`AgentConfig`; call the fraud example's pipeline-assembly function (imported from C24's `examples/fraud/`, never duplicated); run with streaming Dataflow options. — the example's *agent* is imported; its `build()` is a `TestStream` harness and could not be called. See Revision 1.
- [x] 3.2 Implement worker-side secret resolution in the launcher-built provider factory: `access_secret_version` at provider construction via ADC; enforce the credentialed-provider-requires-secret rule at launch time (design D5). — the factory is `functools.partial(provider_with_secret_api_key, provider=<ref>, secret_name=<name>)`: two strings ship to the runner, no key material. "Credentialed" is read off the provider's own signature (a required `api_key`), so a provider added later classifies itself.
- [x] 3.3 Author `examples/fraud_triage_dataflow/metadata.json` with the full parameter surface, regexes derived from the sink-resolver grammar and C36's final model grammar (Open Question 5), and help text. — parsed and every regex compiled statically; agreement with the launcher is a test, not a comment.
- [x] 3.4 Reconcile with the merged C24 layout and C36 parser API; adjust imports/paths and update this change's design if either shifted (design Risks). — both shifted; see Revisions 1–3.

## 4. Image and template build

- [x] 4.1 Write `examples/fraud_triage_dataflow/Dockerfile` per design D2: digest-pinned base, locked deps, `protobuf` 6.x pin, `beam_agents` via `--no-deps` + named runtime deps, `google-cloud-secret-manager`, baked-in `examples/fraud/`, and the build-time import self-check covering `beam_agents.core.transform`, the fraud pipeline module, and the C36 parser ("A broken import fails the build, not the launch"). — written; the self-check also builds a complete launch plan, so a broken *parameter* surface fails the build too. **The image was never built** (blocked: needs docker), so "A broken import fails the build, not the launch" is unverified.
- [x] 4.2 Script the build/publish: image → Artifact Registry `fraud-flex:<git-sha>`, `gcloud dataflow flex-template build` → GCS spec `fraud-flex-<git-sha>.json`; no `latest` alias ("Artifacts are SHA-addressed", "Digest-pinned, lock-driven image build"). — scripted as nightly steps (see 5.1) rather than a standalone script, so there is one build path, not two. **Never executed** (blocked: needs docker/cloud); "Artifacts are SHA-addressed" is unverified at runtime.
- [x] 4.3 Settle Artifact Registry provisioning + retention (Open Question 3); document the one-time setup in `docs/ci.md` if pre-provisioned. — pre-provisioned by an admin, CI gets `artifactregistry.writer` only; commands and the retention stance are in `docs/ci.md`.

## 5. Nightly CI wiring

- [x] 5.1 Add build/publish steps to the `dataflow` job in `.github/workflows/nightly.yml` ahead of `make test-dataflow`, using the existing WIF auth and `vars.GCP_PROJECT_ID` gate; add region/bucket/repo repo-variables; leave `skip-notice` covering the unset-project path ("The gate is skipped, not failed, without a GCP project"). — three steps gated on `GCP_ARTIFACT_REGISTRY_REPO` alongside the existing variables; the job-level `if:` and `skip-notice` are untouched. Workflow parsed and step conditions inspected statically.
- [x] 5.2 Pass the built spec path/SHA into the launch test via env; confirm the job's `timeout-minutes` budget covers build + push + launch + cancel. — `BEAM_AGENTS_FLEX_TEMPLATE_SPEC` carries the run's own spec (a stale one can never be gated); `timeout-minutes` raised 50 → 80 to hold the `--update` gate's 35-minute budget plus this gate's 20 and the image build.
- [ ] 5.3 Run the full nightly path once (workflow_dispatch) on a GCP-configured tree and confirm green end to end. — (blocked: needs docker/cloud)

## 6. Docs

- [x] 6.1 `examples/fraud_triage_dataflow/README.md`: parameter table, one-command `gcloud dataflow flex-template run` example, Secret Manager setup for provider keys. — plus the local image/spec build commands and a table of what each gate proves.
- [x] 6.2 Cross-link from the C24 fraud example page; add the launcher-vs-local-stack image distinction note to `docker/README.md`. — also a new site page, `docs/examples/fraud-triage-dataflow.md`: C24's `test_every_example_module_is_rendered_by_a_page` requires every `examples/` package to be rendered by a page, and this directory is one. It renders `launch.py` by inclusion like every other example page.

## 7. Gates

- [x] 7.1 `make lint` and `make type` clean (launcher and tests fully typed; `mypy --strict`). — `ruff check`/`format --check` clean over 342 files; `mypy` clean over 336. Two pre-existing `unused-ignore` errors inherited from the merge (`effector/sources.py`, `effector/sinks.py`) were fixed the same way the merged integration commit fixed `actions/write_intents.py`.
- [x] 7.2 `make test-unit` green offline with the new metadata/launcher/secret tests; coverage ratchet does not regress. — 1623 passed, 9 skipped; `coverage-ratchet` reports "branch coverage 90.28% is at baseline" (no raise needed, so `coverage-baseline.toml` is untouched). `make test-semantics-offline` also green: 65 passed.
- [ ] 7.3 `uv run pre-commit run --all-files` clean. — (blocked: `pre-commit` is not in the synced dependency groups). Its hooks were run individually instead: `ruff`, `ruff-format`, `mypy`, the changelog-fragment check, and equivalents for `check-yaml`/`check-toml`/`end-of-file-fixer`/`trailing-whitespace` over every touched file — all clean.
- [x] 7.4 `openspec validate add-dataflow-flex-template --strict` passes. — "Change 'add-dataflow-flex-template' is valid".

## Revisions

Numbered corrections made during implementation, with the artifact edits they
produced.

### Revision 1 — the fraud example is one module with a `TestStream`, not a package with a reusable assembly function

`proposal.md` and `design.md` D1 assumed C24 would ship `examples/fraud/` with a
pipeline-assembly function the launcher could call. As merged, C24 shipped
`examples/fraud_triage.py`, whose `build(pipeline)` wires a scripted
`TestStream` and an `AgentConfig` with no sinks — every single thing this
template exists to parameterize is hardcoded in it, so calling it was not
possible and packaging it unchanged would have produced a job that reads no
topic.

Resolution: the launcher imports what must never be duplicated — `triage`, its
scripted provider, and the example's approval-timeout constant — and assembles
only the source/sink wiring the example deliberately fakes. There is still
exactly one copy of the agent, and it is C24's. The directory moved from
`examples/fraud/dataflow/` to `examples/fraud_triage_dataflow/` to sit beside
the module it packages. **Edits:** `design.md` D1 gained a revision paragraph
and the Context bullet a parenthetical; all three artifacts' path references
updated.

### Revision 2 — C36's config-string grammar is `module:object`, not `<provider>:<model-id>`

`design.md` D4 anticipated a provider+model vocabulary (illustratively
`anthropic:claude-…`). C36 shipped something different and simpler: `provider`
is a setuptools-style `module:object` reference resolved by import, with
constructor arguments in a separate `provider_config` mapping. There is no model
*id* in the grammar at all — `model_id` is a per-request field on `LlmRequest`.

Resolution: the `model` parameter takes C36's reference grammar verbatim and is
resolved through C36's own `build_provider_factory`/`resolve_callable`, so the
two surfaces cannot drift. `decode` is not exposed, because the spec's parameter
surface is closed at nine; a provider whose decoder matters wants an additive
parameter and its own change. **Edits:** `design.md` D4 rewritten; Open Question
5 closed with the settled answer.

### Revision 3 — `hitl_timeout_ms` would have been inert without re-timing the suspension

The spec requires the launcher to map `hitl_timeout_ms` onto
`HitlPolicy.timeout_ms`. Doing only that would have shipped a parameter with no
observable effect on this pipeline: `HitlPolicy.timeout_ms` is the default for
suspensions that name no deadline, and the example's agent always passes an
explicit `Suspend(timeout_ms=APPROVAL_TIMEOUT_MS)`, which wins
(`core/dofn.py`'s `default_hitl_timeout_ms`).

Resolution: the launcher's agent is a small frozen dataclass that awaits the
example's `triage` and, when it suspends, replaces the outcome's `timeout_ms`
with the configured one. The policy mapping is still exactly what the spec says;
the wrapper is what makes it true of the job. The spec's requirement text needed
no change — the scenario "HITL timeout parameter reaches HitlPolicy" holds as
written — so this revision is recorded here rather than as a spec edit.
**Edits:** `design.md` D1's revision paragraph notes the wrapper.

### Revision 4 — the launcher imports C36's `_config`/`_refs`, which are underscore-private

`design.md` D4 speaks of "C36's public parse/resolve entrypoint". C36's only
public surface is `beam_agents.yaml.run_agent`; the reference resolver and the
provider-factory builder live in `beam_agents.yaml._config` and
`beam_agents.yaml._refs`.

Resolution: the launcher imports those private modules deliberately, because the
alternative — restating the grammar — is what the spec forbids ("The template
SHALL NOT define its own provider-naming grammar"). This is sample code outside
the wheel, so it takes the coupling rather than the duplication. Promoting a
`resolve_provider` entrypoint to the public YAML surface would remove the
coupling and is a reasonable follow-up; it is not in this change's scope, which
names no `src/beam_agents/` changes.
