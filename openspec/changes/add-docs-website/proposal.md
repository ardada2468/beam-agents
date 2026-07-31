## Why

`beam-agents` has rigorous documentation — `README.md`, five `docs/*.md` pages, nine `openspec/specs/*/spec.md` capability specs, and dense module docstrings — but all of it is only reachable by cloning the repository. Nothing about the runtime is discoverable by search, and a reader evaluating it against Apache Flink Agents has no public surface to read. The project needs a documentation site with the same standard of care as its test suite: server-rendered so search engines index it, and mechanically prevented from claiming anything the code does not do.

The second half is the hard part. This project is pre-release (`version = "0.0.0"`, not published to PyPI — `https://pypi.org/pypi/beam-agents/json` returns 404), and several capabilities described in `openspec/project.md`'s architecture section are not implemented yet: there is no `memory/` store backend beyond `facade.py`, no `adapters/adk` or `adapters/pydantic_ai`, no `model/vertex.py` or `model/vllm.py`, no `yaml/` provider. A conventional marketing site would quietly present the architecture document as shipped reality. This change makes that failure mode impossible to reach.

## What Changes

- Add a new top-level `website/` directory containing a Next.js (App Router) documentation site, server-rendered/statically pre-rendered so every page is indexable without JavaScript execution.
- Site information architecture modelled on large OSS project sites (tensorflow.org, flink.apache.org): landing page, **Learn** (concepts + guides), **Docs** (operational reference), **API**, **Examples**, **Specs**, **Comparison**, **Community/Contributing**.
- **Every page carries a machine-checked maturity status** (`stable` / `experimental` / `partial` / `planned`) sourced from a claim registry that is validated against the actual repository in CI. A page describing a module that does not exist fails the build.
- **API reference is generated, not written**: a build-time introspection pass over the installed `beam_agents` package emits signatures, type annotations, and docstrings to JSON, which the site renders. It cannot drift from the code.
- **Every code sample is a real file that is executed by the test suite**: samples live in `website/examples/`, are run under `DirectRunner` with `FakeLLM` by a pytest module in the repo's offline unit tier, and are embedded into pages by file reference — never retyped into prose.
- **The comparison section states only sourced claims**: each row about Apache Flink Agents (or any other project) carries a citation URL and retrieval date, or is marked "not established". Self-claims must point at a spec scenario or a test. No benchmark numbers are published until a benchmark exists in-repo.
- **Prohibited content is enumerated and enforced**: no fabricated adopter logos, testimonials, download counts, or performance figures; no implication that this is an Apache Software Foundation project (it is Apache-2.0 licensed and built on Apache Beam — a disclaimer to that effect appears in the site footer); install instructions state the actual distribution reality (install from source at a git ref) until a release exists.
- Add `make site-*` targets and a `website.yml` GitHub Actions workflow running lint, typecheck, build, link check, and the claim-registry verification.

## Capabilities

### New Capabilities

- `docs-site`: The Next.js site itself — routing and information architecture, server-side rendering/static pre-rendering guarantees, per-page SEO metadata, sitemap/robots/structured data, client-side search over pre-built indexes, accessibility and no-JS baseline, and the build/serve contract.
- `docs-api-reference`: Generation of the API reference from the installed package by introspection, its JSON schema, coverage of the documented public surface, and failure behavior when the package and the committed reference disagree.
- `docs-examples`: Runnable example programs under `website/examples/`, their execution by the repo's offline test tier, and the rule that site pages embed example source by file reference rather than duplicating it.
- `docs-content-fidelity`: The truthfulness contract — the claim registry and its verification against the repository, the maturity-status taxonomy and where each page's status comes from, citation requirements for comparative and quantitative claims, the prohibited-content list, and the CI checks that enforce all of it.

### Modified Capabilities

- `repo-scaffolding`: The "GitHub Actions workflows mirror the testing tiers" requirement currently fixes the workflow set at exactly four (`ci`, `integration`, `quality`, `nightly`); it must admit a fifth, `website.yml`, scoped to `website/**` changes. The project-layout requirement must record `website/` as a recognized top-level directory, and the `Makefile` requirement must cover the new `site-*` targets — including the constraint that the Node toolchain is not required for `make bootstrap`, `make lint`, `make type`, or `make test-unit`.

## Impact

- **New**: `website/` (Next.js app, MDX content, examples, generation and verification scripts), `.github/workflows/website.yml`, `make site-dev|site-build|site-check` targets, one pytest module executing the website examples in the offline unit tier.
- **Modified**: `Makefile`, `openspec/specs/repo-scaffolding/spec.md`, `.gitignore` (Node artifacts), `README.md` (link to the site; correct the `uv pip install 'beam-agents[langgraph]'` instruction, which currently describes a package that is not published).
- **Not modified**: nothing under `src/`. The runtime is unchanged; this change only reads it.
- **New toolchain dependency**: Node.js and a package manager, required only for `website/` work and its CI job. The Python developer loop stays Node-free.
- **Deployment**: out of scope for this change beyond producing a deployable build. The site is built and verified in CI; choosing and wiring a host is a follow-up.
