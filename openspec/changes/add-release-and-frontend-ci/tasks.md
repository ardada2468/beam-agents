## 1. Release workflow: attach the most recent green benchmark report

- [x] 1.1 Add a `Locate the most recent green benchmark report` step to `release.yml`'s `publish` job: walk `gh run list --workflow nightly.yml --branch main` (newest first, window of 30) and select the first run whose **`bench` job** conclusion is `success` via `gh api repos/.../actions/runs/<id>/jobs`; fail with `::error::` and a remediation message if none exists. Document in the step comment why neither the run conclusion (red `dataflow`/`smoke` siblings say nothing about latency) nor artifact existence (nightly uploads `if: always()`, so a red bench still uploads) is the right filter. *(Step lands before the PyPI publish step so the release fails closed while nothing irreversible has happened.)*
- [x] 1.2 Add a `Download the benchmark report` step: `gh run download <run_id> --name benchmark-report`, assert `bench-report.md` non-empty (`test -s`), zip `bench-results/` into `bench-results.zip`; extend `gh release create` with both assets (`bench-report.md` labeled as the most recent green nightly bench report, the zip as the raw pyperf JSON). *(Artifact layout mirrors nightly's upload paths; `test -s` and `zip` fail the job on a malformed artifact.)*
- [x] 1.3 Grant the `publish` job `actions: read` with a comment naming the two `gh` commands that need it. *(`GITHUB_TOKEN` in the same repository carries `actions: read` once declared; no PAT, consistent with the repo's no-long-lived-credential stance.)*

## 2. Frontend CI workflow

- [x] 2.1 Create `.github/workflows/frontend.yml`: `pull_request` + push-to-`main`, paths `frontend/**` and the workflow file itself; workflow-level concurrency (`cancel-in-progress: true`, matching `website.yml` — these are PR checks, not publishes); `permissions: contents: read`; single `frontend` job, `timeout-minutes: 15`, `defaults.run.working-directory: frontend`.
- [x] 2.2 Steps: checkout, `setup-node` (`node-version: '22'`, `cache: npm`, `cache-dependency-path: frontend/package-lock.json`), then `npm ci`, `npm run lint`, `npm run typecheck`, `npm run build` as separate named steps. *(Node 22 matches `website/.nvmrc`; comment in the workflow says to switch to `node-version-file` if `frontend/` grows an `.nvmrc`.)*

## 3. Verification

- [x] 3.1 Both workflow files parse (`yaml.safe_load`) and pass `yamllint` with the repo-appropriate relaxations (line-length for comment prose, GitHub's `on:` truthy key). *(release.yml jobs: `build-and-verify`, `publish`, `publish-testpypi`; frontend.yml jobs: `frontend`.)*
- [x] 3.2 Run the frontend scripts locally on the current tree: `npm ci && npm run lint && npm run typecheck && npm run build`. *(All four green: 184 packages installed, eslint clean at `--max-warnings 0`, `tsc -b --noEmit` clean, vite build completed in <1 s writing the hashed assets into `src/beam_agents/console/static/` unchanged.)*
- [x] 3.3 Confirm no changelog fragment is required: `changelog-fragment-required` (and `openspec-change-required`) gate `src/` edits; this change touches only `.github/workflows/` and this change folder. *(Per `.pre-commit-config.yaml` and `docs/releasing.md`'s compatibility surface: "CI workflow shape" is explicitly out of contract.)*
- [x] 3.4 `openspec validate add-release-and-frontend-ci --strict` passes.

## 4. First live runs (blocked: need CI / a tag)

- [ ] 4.1 Open the PR and confirm the `frontend` job triggers (the workflow file itself is in the paths filter), reports under the `frontend` context, and all four steps pass on the runner (blocked: needs CI run)
- [ ] 4.2 Add `frontend` to `main`'s required status-check contexts once it has reported at least once (`gh api repos/:owner/:repo/branches/main/protection`) — operational, user-performed (blocked: needs merge window)
- [ ] 4.3 The next `v*` tag (or a deliberate dry run on a scratch tag) exercises the locate/download/attach path end-to-end; confirm the Release carries `bench-report.md` and `bench-results.zip`, and that the chosen nightly run id printed in the log has a green `bench` job (blocked: needs a tag; `workflow_dispatch` rehearsals do not reach the `publish` job by design)
