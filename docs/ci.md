# CI workflow map

Four workflows under `.github/workflows/`, one per testing tier in
[`openspec/project.md`](../openspec/project.md):

| Workflow           | Trigger                          | Tier                          | Required for merge |
|---------------------|-----------------------------------|--------------------------------|---------------------|
| `ci.yml`            | push to `main`, pull request      | lint, type, unit (3.11–3.12 × ubuntu/macos) | yes |
| `integration.yml`   | push to `main`, pull request      | integration + semantics (docker compose) | yes |
| `quality.yml`       | push to `main`, pull request      | mutation (on touched `core/`) + coverage ratchet | yes |
| `nightly.yml`       | schedule `0 7 * * *` UTC, manual  | dataflow (real GCP via Workload Identity Federation) | no |

Every workflow step maps 1:1 to a `Makefile` target — see the
[`Makefile`](../Makefile) for the exact commands `ci-lint`, `ci-unit`, etc.
run locally.

## Triggering `nightly` manually

From the Actions tab, select the `nightly` workflow and use
**Run workflow**. It no-ops (via the `skip-notice` job) until the repository
variables `GCP_PROJECT_ID`, `GCP_WORKLOAD_IDENTITY_PROVIDER`, and
`GCP_SERVICE_ACCOUNT` are configured — no long-lived service-account key is
ever used.

## Making checks required

Once this repository has a GitHub remote, mark `ci`, `integration`, and
`quality` as required status checks on `main` under
**Settings → Branches → Branch protection rules**. `nightly` is intentionally
not required.
