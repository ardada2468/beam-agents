# The beam-agents documentation site

Next.js (App Router), pre-rendered at build time so every page is indexable
without client-side JavaScript.

```sh
make site-dev      # dev server
make site-build    # production build — Node only, no Python needed
make site-check    # every gate (needs Node *and* the uv environment)
```

## Why this is not an ordinary docs site

The project is pre-release: `project.version` is `1.0.0` but no `v1.0.0` tag
has been cut, so nothing is published to PyPI. A site written the usual way
would present the declared version as a shipped release. So the content
contract is enforced mechanically:

| Rule                                                                             | Enforced by                                                  |
| -------------------------------------------------------------------------------- | ------------------------------------------------------------ |
| Every page declares a maturity status from a closed set                          | `lib/schema.ts`, at build time                               |
| `stable` needs a spec and a test; `planned` fails once the code exists           | `scripts/verify_docs_claims.py`                              |
| `symbol:` claims resolve by **importing** the package, not by grepping           | `scripts/verify_docs_claims.py`                              |
| No adopters, superlatives, ASF-governance phrasing, or unsourced numbers         | `scripts/check_docs_prose.py`                                |
| Comparative claims carry a dated citation; unbacked cells read "Not established" | `scripts/verify_docs_claims.py`, `components/ClaimTable.tsx` |
| Install instructions match the real distribution state                           | `scripts/verify_docs_claims.py`                              |
| The API reference matches the installed package                                  | `scripts/gen_api_reference.py --check`                       |
| Examples are real programs that run                                              | `tests/docs/test_website_examples.py`                        |
| Every page is complete HTML before JavaScript                                    | `scripts/check_site_ssr.mjs`                                 |

The Python-side scripts live in the repository root `scripts/` because they
import `beam_agents`. That is the load-bearing choice: a TypeScript verifier
could only pattern-match source text, which is exactly the approximate check
that lets a false claim through.

## Layout

```
content/<section>/<slug>.mdx   Pages. Adding a file adds a route — no wiring.
examples/*.py                  Runnable programs, executed by the test suite.
generated/api.json             Generated + committed, drift-checked in CI.
lib/                           Content loader, schema, search, repo-doc reader.
components/                    Example, Callout, ClaimTable, RepoDoc, Spec, …
scripts/                       Link, SSR, and accessibility checks.
```

## Writing a page

Frontmatter is the contract:

```yaml
---
title: The errors output
summary: One sentence; becomes the meta description.
status: stable # stable | experimental | partial | planned
order: 1
verifies:
  - symbol: beam_agents.RunAgentOutputs
  - module: src/beam_agents/core/error_records.py
  - spec: openspec/specs/wire-schemas/spec.md
  - test: tests/core/test_error_records.py
  - example: four_outputs.py
sources: # required for comparative claims
  - claim: …
    url: https://…
    retrieved: 2026-07-30
---
```

Then:

- **Never retype code.** Use `<Example file="four_outputs.py" region="agent" />`.
  Regions are delimited in the Python file by `# region:` / `# endregion:`.
- **Never paraphrase repository docs.** Use `<RepoDoc file="docs/errors.md" />`
  or `<Spec capability="tool-registry" />` — they render the repository file
  itself, so there is no second copy to fall out of date.
- **Say what is missing.** A `partial` page must carry a "Not yet implemented"
  section; a `planned` page must open with
  `<Callout kind="not-implemented">`.

## Accessibility checking, honestly

`scripts/check_a11y.mjs` runs axe-core in jsdom for structural rules and
computes contrast **from the design tokens** rather than from rendered pixels.
jsdom has no layout engine, so axe reports colour-contrast as "incomplete"; a
check that trusted it would be theatre. The token pass is a narrower claim than
"every rendered pixel passes AA" — and it is one that is actually true. The
rules skipped in jsdom are named in the check's own output.

## Deployment

Not wired up. The site builds and is verified in CI; choosing a host is a
follow-up. `NEXT_PUBLIC_SITE_URL` is the single knob that sets canonical URLs
and the sitemap origin.
