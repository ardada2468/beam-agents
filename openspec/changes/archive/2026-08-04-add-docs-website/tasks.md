## 1. Scaffolding and toolchain

- [x] 1.1 Create `website/` with a Next.js App Router app: `package.json` (pnpm, pinned Node 22 in `.nvmrc` and `engines`), committed `pnpm-lock.yaml`, `tsconfig.json` with `strict: true`, `next.config.ts`, ESLint + Prettier config
- [x] 1.2 Add Tailwind CSS v4 and a design-token layer (CSS custom properties for color, type scale, spacing) with light/dark themes driven by `prefers-color-scheme` plus an explicit toggle
- [x] 1.3 Add `website/` entries to `.gitignore` (`node_modules/`, `.next/`, `out/`) and confirm `pnpm-lock.yaml` stays tracked
- [x] 1.4 Add `site-dev`, `site-build`, `site-check` targets to the `Makefile`; verify `make bootstrap lint type test-unit` still succeed with no Node on PATH and `make site-build` succeeds with no `.venv/`
- [x] 1.5 Add `.github/workflows/website.yml`: triggers on `website/**`, `src/**`, `docs/**`, `openspec/specs/**`, and itself; pinned Node, cached pnpm store, `uv sync` for the Python-side checks, single primary step `make site-check`

## 2. Content model and loader

- [x] 2.1 Define the frontmatter Zod schema (`title`, `summary`, `status` from the closed set, optional `verifies`, `sources`, `nav` ordering) and make validation failure a build error naming file and field
- [x] 2.2 Implement the content loader over `website/content/<section>/<slug>.mdx`: parse frontmatter, build the section tree and navigation, expose typed accessors for pages, sections, and status
- [x] 2.3 Wire MDX rendering in a server component with build-time Shiki highlighting; no client-side highlighter
- [x] 2.4 Add Vitest unit tests for the loader and schema: valid page, missing `status`, invalid `status` value, unknown `verifies` key, duplicate slug
- [x] 2.5 Implement the `<Example file=… region=… />` component reading from `website/examples/` at build time, failing the build on a missing file or region

## 3. Truthfulness infrastructure

- [x] 3.1 Write `scripts/verify_docs_claims.py`: parse every page's frontmatter, resolve `symbol:` by importing `beam_agents` (including the lazy `__getattr__` path), `module:`/`spec:`/`example:` against the filesystem, `test:` against `pytest --collect-only`; report file, line, and reproduction command on failure
- [x] 3.2 Add status-semantics enforcement to the verifier: `stable` requires a `spec:` and a `test:`; `experimental` requires a `test:`; `partial` requires a "Not yet implemented" section in the body; `planned` fails if any `symbol:`/`module:` assertion resolves
- [x] 3.3 Add the distribution check: read `project.version` from `pyproject.toml` and fail any page presenting a registry install as currently available while the version is `0.0.0`
- [x] 3.4 Add the citation check: every claim naming another project requires a `sources` entry with `claim`, `url`, `retrieved`; every `beam-agents` comparison cell requires a `spec:` or `test:` backing
- [x] 3.5 Write `scripts/check_docs_prose.py`: adopter/testimonial language, unsourced superlatives, unsourced performance numerics outside code fences, ASF-governance phrasing; implement the `<!-- prose-check: ok <reason> -->` escape with a mandatory reason and a single greppable token
- [x] 3.6 Add pytest coverage for both scripts under `tests/docs/` using fixture content trees: one passing tree and one failing case per rule, asserting exit code and message content
- [x] 3.7 Build the `StatusBadge` component and render it in page headers and navigation entries; exclude `planned` pages from primary navigation

## 4. Generated API reference

- [x] 4.1 Write `scripts/gen_api_reference.py`: import `beam_agents`, walk `__all__` and documented public modules, emit `website/generated/api.json` with qualified name, kind, signature with resolved annotations, verbatim docstring, source path and line, and `requires_extra`
- [x] 4.2 Make lazily-exported symbols explicit: `LangGraphAgent` documented with `requires_extra: "langgraph"`; exit non-zero naming the missing extra when it cannot be resolved; never silently omit an `__all__` entry
- [x] 4.3 Commit `website/generated/api.json` and add the drift check to `make site-check`, with a failure message naming the regeneration command
- [x] 4.4 Render the API section: index page plus one pre-rendered page per top-level symbol, stable per-member anchors, docstring rendered as prose, explicit "No docstring" marker where absent, source links into the repository
- [x] 4.5 Add link validation for generated source links and an assertion that no underscore-prefixed module or member appears in the index

## 5. Examples

- [x] 5.1 Create `website/examples/` with a fast-path activation example (DirectRunner + FakeLLM), module docstring stating what it demonstrates
- [x] 5.2 Add the intent/re-injection example: agent emits a `ToolIntent`, a scripted `ToolResult` re-enters on the same key, activation resumes
- [x] 5.3 Add the HITL example: suspension, approval channel, timeout fallback via `HitlPolicy` returning `Deny`/`Drop`/`Escalate`
- [x] 5.4 Add the four-outputs example: consuming `.output`, `.intents`, `.traces`, `.errors`, including an `ActivationError` record
- [x] 5.5 Add the LangGraph adapter example, labelled with the `langgraph` extra it requires
- [x] 5.6 Write `tests/docs/test_website_examples.py`: discover every file in `website/examples/`, execute it offline in the default tier, assert exit `0`; skip extra-dependent examples with a stated reason when the extra is absent
- [x] 5.7 Add the region-embedding convention (`# region:` / `# endregion:`) to the examples that supply fragments, and the check rejecting transcribed Python blocks in content
- [x] 5.8 Add the required-topic audit to `make site-check` and build the Examples index page with a one-line description per example

## 6. Site shell, routing, and SEO

- [x] 6.1 Build the root layout: header with section nav and search entry, sidebar navigation with status badges, in-page table of contents, footer carrying the Apache-2.0 / non-ASF / trademark statement
- [x] 6.2 Implement content routing with `generateStaticParams` over the content tree so a new MDX file is pre-rendered with no route wiring
- [x] 6.3 Implement `generateMetadata` per page: unique title, description from `summary`, Open Graph and Twitter tags, absolute canonical from `NEXT_PUBLIC_SITE_URL`; `noindex` for `/search` and `planned` pages
- [x] 6.4 Add `app/sitemap.ts` and `app/robots.ts`, excluding non-indexable routes; add JSON-LD structured data to content pages
- [x] 6.5 Build the search index at build time; implement the client search component over MiniSearch and the server-rendered `/search?q=` route that works without JavaScript
- [x] 6.6 Write `scripts/check_site_ssr.mjs`: start the built server, fetch every sitemap route, assert `200`, non-empty `<h1>`, matching `<meta name="description">`, canonical link, and ≥200 characters of body text
- [x] 6.7 Write the internal/external link checker covering site routes, in-page anchors, and repository-relative paths with optional line anchors
- [x] 6.8 Add the automated accessibility audit over one representative route per section in both themes; fix contrast, landmark, heading-order, and keyboard findings; verify no horizontal body scroll at 375px

## 7. Landing page and Learn section

- [x] 7.1 Build the landing page: what the runtime is, the `events | RunAgent(agent)` shape, a real embedded example, the pre-release banner, and links into Learn/Docs/API — no marketing furniture, no unverifiable claims
- [x] 7.2 Write `learn/what-is-beam-agents`: runtime-not-framework positioning, target workloads, the explicit non-goal of sub-second interactive chat
- [x] 7.3 Write `learn/architecture`: the dataflow shape, the two execution paths through `RunAgent`, why iterative loops cycle through the message bus rather than the DAG
- [x] 7.4 Write `learn/correctness-invariants`: atomic commit, deterministic intent IDs, replay cache, per-key serialization, effects only via intents, fail-closed timeouts, protobuf state — each backed by a `spec:` and a `test:` assertion
- [x] 7.5 Write `learn/state-and-memory`: keyed state layout, timers, blob and working-memory caps, TTL GC; mark the store backends that do not exist as `planned` roadmap pages, not as documentation
- [x] 7.6 Write `learn/glossary` from the domain glossary (activation, continuation, intent, effector, re-injection, replay cache, seq, fast path)
- [x] 7.7 Write the getting-started guide and the install page: source install at a git ref first, registry path under an explicit "when released" heading

## 8. Docs (operational reference) section

- [x] 8.1 Publish the errors reference covering `docs/errors.md`: `.errors` semantics, the reason table, sink configuration, encoding
- [x] 8.2 Publish the metrics reference covering `docs/metrics.md`: counters, distributions, the two identities worth alerting on, and the note that Beam distributions carry no percentiles
- [x] 8.3 Publish the traces reference covering `docs/traces.md`, including exporter configuration and the `otlp` extra
- [x] 8.4 Publish the effector reference covering `docs/effector.md`: deployment preconditions, lease/TTL budgets, dedup, and an explicit statement of what is and is not guaranteed
- [x] 8.5 Publish the CI/testing-tiers reference covering `docs/ci.md` and the four testing tiers, the marker registry, and the conformance matrix
- [x] 8.6 Publish the HITL reference: `HitlPolicy`, timeout routing, `Deny`/`Drop`/`Escalate`, fail-closed behavior at both layers
- [x] 8.7 Publish the runners and deployment reference stating exactly which runners are exercised by which test tier, with no claim beyond that
- [x] 8.8 Add the `make site-check` audit asserting every `docs/*.md` file maps to at least one published page

## 9. Specs, comparison, and community

- [x] 9.1 Publish the Specs section: an index plus a page per capability under `openspec/specs/`, rendered from the spec source, with the audit that fails when a capability has no page
- [x] 9.2 Write the spec-driven-development page explaining the change workflow, scenario→test→code traceability, and the archived-change history as evidence
- [x] 9.3 Build the comparison table component with a first-class "Not established" cell and rendered footnotes carrying source URL and retrieval date
- [x] 9.4 Write `comparison/flink-agents` with every competitor claim carried by a dated citation and every `beam-agents` claim backed by a `spec:` or `test:`; no numeric performance comparison
- [x] 9.5 Write `comparison/agent-framework-outside-a-runtime`: what the runtime provides that a framework alone does not, each claim spec- or test-backed
- [x] 9.6 Write the adapters page: LangGraph documented as implemented; ADK and Pydantic AI as `planned` roadmap pages carrying the not-implemented callout
- [x] 9.7 Write the roadmap index gathering every `planned` page, stating plainly that nothing on it exists in the current code
- [x] 9.8 Write the community page: license, repository, issue and PR workflow, `CONTRIBUTING.md` requirements, and the non-affiliation statement in full

## 10. Verification and close-out

- [x] 10.1 Run the full `make site-check` locally and drive it to green: typecheck, lint, build, claim verification, prose check, API drift, link check, SSR assertion, accessibility audit
- [x] 10.2 Run `make test-unit` and confirm the example tests execute (not skip) with docker down and no network
- [x] 10.3 Correct `README.md`: replace the `uv pip install 'beam-agents[langgraph]'` instructions with the source-install path and add a link to the site
- [x] 10.4 Deliberately break one claim (delete a `module:` target), confirm the workflow fails with a file, line, and reproduction command, then restore
- [x] 10.5 Audit every `<!-- prose-check: ok -->` escape in the tree and confirm each states a defensible reason
- [x] 10.6 Read every published page against the repository one final time for claims the automated checks cannot catch, and record any residual uncertainty on the page itself rather than removing the mention
