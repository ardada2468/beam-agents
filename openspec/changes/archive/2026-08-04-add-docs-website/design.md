## Context

The repository already holds documentation of unusual density: `README.md`, `docs/{ci,effector,errors,metrics,traces}.md` (646 lines of operational reference), nine capability specs under `openspec/specs/`, twenty archived/active change folders, and module docstrings written as prose. None of it is reachable without a clone, and none of it is indexed.

Three facts about the current state constrain every decision below:

1. **The project is pre-release.** `pyproject.toml` declares `version = "0.0.0"`; `https://pypi.org/pypi/beam-agents/json` returns 404. `README.md` currently instructs `uv pip install 'beam-agents[langgraph]'`, which cannot work today.
2. **`openspec/project.md` describes the intended architecture, not the shipped one.** Its module map lists `memory/` stores (Bigtable/Redis/Firestore/SQL), `adapters/adk`, `adapters/pydantic_ai`, `model/vertex`, `model/vllm`, and a `yaml/` provider. On disk, `memory/` contains only `facade.py`, `adapters/` contains only `langgraph/`, and `model/` contains `anthropic.py`, `openai_compat.py`, `fake.py`, plus the client/cache/facade. A site built by paraphrasing `project.md` would be substantially false.
3. **The repo's culture is mechanical enforcement.** Protobuf regeneration must be diff-clean in CI; markers are a closed registry; a script fails the build if a semantics test escapes both partitions. The truthfulness guarantee should be built the same way — as a check that fails, not a convention that is remembered.

The stated goal is a site that reads like a serious infrastructure project's site (tensorflow.org, flink.apache.org) and is 100% truthful. Those two goals pull against each other: the visual language of a mature project's site is built from adopter logos, benchmark charts, release badges, and confident capability claims — exactly the things this project cannot honestly show yet. The design resolves this by making credibility come from *verifiability* (every claim traceable to a spec, test, symbol, or citation) rather than from social proof.

## Goals / Non-Goals

**Goals:**

- A Next.js site under `website/` whose every page is delivered as complete HTML by the server, indexable without client-side JavaScript.
- Deep documentation: concepts, guides, full operational reference, generated API reference, runnable examples, the capability specs, and a sourced comparison section.
- A mechanical truthfulness layer: page-level maturity status, claim verification against the repository, citation requirements for external and quantitative claims, and a prohibited-content check — all failing CI on violation.
- Examples that are real, executed programs, embedded by reference so prose and code cannot diverge.
- A Python developer loop that stays Node-free.

**Non-Goals:**

- Hosting/deployment wiring (domain, CDN, host). This change produces a build and verifies it; wiring a host is a follow-up.
- Docs versioning (`/v0.1/…` trees). There is one version and it is unreleased; a version switcher would be scaffolding for a problem that does not exist yet.
- Any change under `src/`. The site reads the runtime; it never edits it.
- Auto-generating narrative pages from docstrings. Generated output is confined to the API reference; concept and guide prose is written by humans and verified, not synthesized.
- i18n, blog, changelog automation, analytics, comment systems.
- Publishing performance numbers. `openspec/project.md` states a latency budget (p50 < 15 ms, p99 < 60 ms runtime overhead); no benchmark harness exists in-repo to substantiate it, so the site states it as a *design budget with no published measurement*, never as a result.

## Decisions

### D1. Next.js App Router, pre-rendered at build time, served by a Node server

Every content route is a React Server Component with `generateStaticParams` over the content tree, producing full HTML at build time. The site runs under `next start` (Node runtime) rather than `output: 'export'`.

*Why:* The requirement is that a crawler receives complete markup — build-time pre-rendering satisfies that as fully as request-time rendering, with better latency and no runtime dependency on the Python package. Keeping the Node server (rather than a static export) preserves `redirects`, `headers`, streaming, and future ISR without a migration. *Alternatives:* Docusaurus (the obvious default for OSS docs) was rejected because the truthfulness layer needs custom frontmatter validation, generated-content rendering, and build-time verification hooks that fight Docusaurus's plugin model; static export was rejected for the reasons above; request-time SSR of static content was rejected as cost with no indexing benefit.

### D2. Content is MDX under `website/content/`, with Zod-validated frontmatter

`website/content/<section>/<slug>.mdx`, loaded by a typed loader and rendered with `next-mdx-remote/rsc` in a server component. Frontmatter is parsed and validated by a Zod schema; an invalid or missing field is a build failure, not a warning.

Frontmatter carries the truthfulness contract:

```yaml
title: Effectively-once side effects
status: stable            # stable | experimental | partial | planned
summary: …                # used for <meta description> and search
verifies:                 # assertions checked against the repo
  - symbol: beam_agents.RunAgent
  - module: src/beam_agents/actions/write_intents.py
  - spec: openspec/specs/tool-registry/spec.md
  - test: tests/semantics/test_effectively_once_e2e.py
  - example: examples/outbox_intents.py
sources:                  # required for external/comparative/quantitative claims
  - claim: Flink Agents uses inline durable execution for side effects
    url: https://…
    retrieved: 2026-07-30
```

*Why frontmatter rather than a central registry file:* the claim lives next to the prose it backs, so deleting a page deletes its claims and a reviewer sees both in one diff. *Alternative rejected:* a central `claims.yaml` — it rots independently of the content.

### D3. The verifier is Python, and it imports the package

`scripts/verify_docs_claims.py` runs inside the repo's `uv` environment so it can `import beam_agents` and resolve `symbol:` assertions by real attribute access (including the lazy `__getattr__` path for `LangGraphAgent`), not by grepping. `module:`, `spec:`, `test:`, and `example:` assertions resolve against the filesystem; `test:` additionally requires the node id to be collectable by `pytest --collect-only`.

*Why Python:* the ground truth is a Python package. A TypeScript verifier could only pattern-match source text, which is exactly the kind of approximate check that lets a false claim through. The cost is that `make site-check` needs both toolchains; `make site-build` alone does not.

### D4. Maturity status is derived, then asserted

`status: planned` and `status: partial` are not decoration — the verifier enforces their meaning:

| Status | Meaning | Verifier enforces |
|---|---|---|
| `stable` | Implemented, spec'd, and covered by tests | every `verifies` entry resolves; at least one `spec:` and one `test:` present |
| `experimental` | Implemented and tested; interface may change | every `verifies` entry resolves; at least one `test:` present |
| `partial` | Partly implemented; the page must say what is missing | every `verifies` entry resolves; page body must contain a "Not yet implemented" section |
| `planned` | Not implemented | `verifies` MUST NOT contain `symbol:` or `module:` entries that resolve — a planned page that names existing code is a status error |

The status renders as a visible badge in the page header and in navigation, so a reader never has to infer maturity from tone. This is the mechanism that lets the site cover the full intended architecture (which is genuinely useful to an evaluator) without misrepresenting it.

### D5. The API reference is generated and drift-checked, mirroring the protobuf convention

`scripts/gen_api_reference.py` imports `beam_agents`, walks the public surface (`__all__` plus the modules the package documents as public), and emits `website/generated/api.json` — qualified name, kind, signature, resolved type annotations, docstring, source path and line. The file is **committed**, and `make site-check` regenerates and fails on any diff.

*Why committed + drift check:* it is the pattern this repo already uses for `_pb2.py`, it keeps the Node build independent of a Python environment, and it makes API changes visible in review diffs. *Alternative rejected:* generating at `next build` time — couples the site build to a Python env and hides API changes from review.

The generator records `beam_agents.__all__` verbatim, including `LangGraphAgent`, which resolves through `__getattr__` only when the `langgraph` extra is installed; the reference marks such symbols with their required extra rather than omitting them.

### D6. Examples are executed by the repo's offline unit tier

`website/examples/*.py` are standalone, runnable programs. `tests/docs/test_website_examples.py` discovers each file and executes it under `DirectRunner` with `FakeLLM`, in the default (no-docker, offline) tier. MDX embeds them with `<Example file="outbox_intents.py" />`, which reads the file at build time — prose never contains a retyped copy.

*Why in the repo's test tier rather than a website-side check:* it makes example breakage a `ci` failure on any `src/` change, which is the point. It also means the examples participate in the coverage ratchet's view of the public API.

Snippets too small to be programs (a three-line config) are embedded as *regions* of a real example file, delimited by `# region: <name>` comments, so even fragments come from executed code.

### D7. Comparison content is sourced or absent

The comparison section (`/comparison/*`) compares `beam-agents` with Apache Flink Agents and with running an agent framework outside a streaming runtime. Rules enforced by the verifier:

- Any statement about another project requires a `sources` entry with a URL and `retrieved` date, rendered as a visible footnote.
- Any statement about `beam-agents` in a comparison table requires a `spec:` or `test:` assertion.
- Cells with neither are rendered as "Not established" — a first-class value in the table component, not an empty cell.
- No numeric performance comparison is permitted anywhere on the site until a benchmark exists in-repo.

*Why this is worth the friction:* comparison pages are where OSS documentation lies most often, usually by accident and always in one direction. The rule makes "we don't know" cheap to publish.

### D8. Prohibited content is a lexical check

`scripts/check_docs_prose.py` scans rendered content for patterns that cannot be true of this project today or are unfalsifiable in general: adopter/testimonial language ("trusted by", "used in production by", "our customers"), unsourced superlatives ("fastest", "the only"), unsourced numerics matching `\d+(\.\d+)?\s*(x|%|ms|QPS)` outside fenced code and cited contexts, and phrasing implying Apache Software Foundation governance ("an Apache project", "ASF incubating"). Findings are errors with file and line.

The footer carries a standing disclaimer: `beam-agents` is licensed Apache-2.0 and builds on Apache Beam; it is not an Apache Software Foundation project. Apache, Apache Beam, and Apache Flink are trademarks of the ASF.

### D9. Install instructions describe reality

Until a release exists, the install page's primary path is source installation at a git ref, with an explicitly labelled "when released" section for the PyPI path. `README.md`'s current `uv pip install 'beam-agents[langgraph]'` line is corrected as part of this change. A `verifies: - released: false` assertion ties the page to the actual state: the verifier reads `project.version` from `pyproject.toml` and fails the page if it claims PyPI availability while the version is `0.0.0`.

### D10. Search: pre-built index, server-rendered fallback

`next build` emits a JSON index (title, section, status, summary, headings, body text) consumed by a client component using MiniSearch. A `/search?q=` route renders results server-side for no-JS clients and is marked `noindex`. If the index exceeds 1 MB it is split per section and loaded on demand.

### D11. Visual design: typographic, dense, no marketing furniture

A restrained token layer (CSS custom properties, light/dark via `prefers-color-scheme` plus an explicit toggle), one text face and one mono face, generous measure, high-contrast code blocks with Shiki highlighting at build time. Landing page leads with what the runtime does and a real code sample, not a hero gradient. Trust signals are: the license, the four CI workflows, the spec index, the test tiers, and the status badges — all of which are real.

### D12. Toolchain and CI

pnpm, Node 22 LTS, TypeScript `strict`, Tailwind CSS v4 with the token layer, ESLint + Prettier, Vitest for the loader/verifier TypeScript units. `.github/workflows/website.yml` triggers on `website/**`, `docs/**`, `openspec/specs/**`, `src/**`, and its own file; it runs `make site-check` (typecheck, lint, build, claim verification, prose check, API drift check, link check, SSR assertion). It is not added to the required-checks set by this change — that is a repository-settings decision for the maintainer.

`make site-build` requires only Node. `make site-check` additionally requires the `uv` environment. `make bootstrap`, `make lint`, `make type`, and `make test-unit` remain Node-free.

### D13. SSR is asserted, not assumed

`scripts/check_site_ssr.mjs` starts the built server, fetches every route from the sitemap, and asserts on the raw HTML response: status 200, a non-empty `<h1>`, the page's `summary` present as `<meta name="description">`, a `<link rel="canonical">`, and at least 200 characters of body text — all before any JavaScript executes. Any route failing is a build failure. This is what makes "indexable" a tested property rather than an intention.

## Risks / Trade-offs

- **The verifier proves existence, not accuracy.** `symbol: beam_agents.RunAgent` resolving says nothing about whether the surrounding paragraph describes it correctly. → Mitigated by requiring `spec:`/`test:` assertions for `stable` pages (pushing prose to be grounded in scenario text), by embedding all code from executed examples, and by generating the API reference. Residual risk is accepted and stated: the checks bound the failure mode, they do not eliminate it.
- **Node in a Python repo raises maintenance cost.** → Confined to `website/` and one workflow; the Python loop never touches it; lockfile committed and Node version pinned.
- **Committed `api.json` will produce drift-check failures on unrelated `src/` PRs.** → This is intended (it is the same trade the `_pb2.py` drift check makes), but the failure message must name the exact regeneration command; the workflow only triggers on paths that can cause it.
- **Status badges can go stale in the other direction** — a `planned` page whose feature ships stays `planned` and understates the project. → The `planned` rule (D4) inverts the check: once the named symbol or module exists, the page fails until it is re-classified. Staleness becomes a build error in both directions.
- **Rendering the intended architecture as `planned` pages could still read as overpromising** to a skimmer who ignores badges. → Planned pages are excluded from the primary nav (reachable from a single "Roadmap" index), are `noindex`, and open with a standard callout stating the feature does not exist.
- **The prose checker will produce false positives** (a legitimate `100 KiB` blob cap reads as an unsourced numeric). → Numerics inside code fences, tables citing a `spec:` source, and an explicit per-line `<!-- prose-check: ok <reason> -->` escape are exempt; the escape is greppable so reviewers can audit its use.
- **Scope.** This is a large surface for one change. → Tasks are ordered so that the site is publishable at the end of the shell + fidelity phases, with content sections layered on; each section is independently verifiable.

## Migration Plan

Additive only; nothing depends on the site. Rollback is deleting `website/`, the workflow, and the `site-*` targets. The one non-additive edit is the `README.md` install correction, which stands on its own merit.

## Open Questions

- Which host (Vercel, Cloudflare Pages, GitHub Pages via static export, self-managed) — deferred to the follow-up change; D1's choice of `next start` keeps every option except pure-static open.
- Whether `website.yml` becomes a required check. Deferred to the maintainer, since required checks are a repository setting.
- Whether the site's canonical domain is a subdomain of a project domain or a GitHub Pages URL; `NEXT_PUBLIC_SITE_URL` isolates the decision to one environment variable.
