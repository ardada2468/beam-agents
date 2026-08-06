## ADDED Requirements

### Requirement: The site lives in `website/` and builds with Node alone

The documentation site SHALL live in a top-level `website/` directory containing a Next.js App Router application with TypeScript in `strict` mode, a committed lockfile, and a pinned Node version. Building the site (`make site-build`) MUST NOT require a Python environment, the `beam_agents` package, or network access to a package registry beyond dependency installation.

#### Scenario: Site builds without a Python environment

- **WHEN** a contributor runs `make site-build` on a checkout with no `.venv/` present and `uv` unavailable
- **THEN** the build completes successfully and produces a runnable Next.js production build

#### Scenario: Python targets do not require Node

- **WHEN** a contributor runs `make bootstrap`, `make lint`, `make type`, and `make test-unit` on a machine with no Node installed
- **THEN** every target completes with the same result it had before this change

#### Scenario: TypeScript strict mode is enforced

- **WHEN** a contributor introduces an implicit `any` in `website/`
- **THEN** `make site-check` exits non-zero with a TypeScript error

### Requirement: Every content route is server-rendered as complete HTML

Every route the site exposes SHALL be delivered by the server as complete HTML containing the page's readable content, without requiring client-side JavaScript execution to become readable. Content routes MUST be pre-rendered at build time via `generateStaticParams` over the content tree.

#### Scenario: Raw HTML response carries page content

- **WHEN** a client issues `GET /docs/errors` against the built site and does not execute JavaScript
- **THEN** the response is `200` and its HTML body contains the page's `<h1>` text and at least 200 characters of body prose

#### Scenario: Every sitemap route passes the SSR assertion

- **WHEN** `make site-check` runs the SSR assertion against the built server
- **THEN** every URL listed in `sitemap.xml` returns `200` with a non-empty `<h1>`, a `<meta name="description">` matching the page's `summary`, and a `<link rel="canonical">`, and any route failing any assertion fails the check

#### Scenario: A new content file is pre-rendered without route wiring

- **WHEN** a contributor adds `website/content/guides/new-guide.mdx` with valid frontmatter and rebuilds
- **THEN** `/guides/new-guide` is pre-rendered, appears in the sitemap, and appears in the section navigation

### Requirement: Information architecture covers concepts, reference, API, examples, specs, and comparison

The site SHALL expose these top-level sections: a landing page, `Learn` (concepts and guides), `Docs` (operational reference), `API`, `Examples`, `Specs`, `Comparison`, and `Community`. The `Docs` section MUST cover the material in the repository's `docs/` directory, and the `Specs` section MUST cover every capability under `openspec/specs/`. Navigation MUST show each page's maturity status.

#### Scenario: Every repository doc page has a site counterpart

- **WHEN** `make site-check` compares `docs/*.md` in the repository against the site's `Docs` section
- **THEN** every repository doc file maps to at least one site page, and a repository doc with no counterpart fails the check

#### Scenario: Every capability spec is published

- **WHEN** `make site-check` compares `openspec/specs/*/spec.md` against the site's `Specs` section
- **THEN** every capability has a published page, and a capability with no page fails the check

#### Scenario: Navigation exposes status

- **WHEN** a reader views any section's navigation
- **THEN** each entry displays its page's maturity status, and entries with status `planned` are absent from primary navigation

### Requirement: Pages carry SEO metadata, canonical URLs, sitemap, robots, and structured data

Every page SHALL emit a unique `<title>`, a `<meta name="description">` derived from its frontmatter `summary`, Open Graph and Twitter card tags, and an absolute `<link rel="canonical">` built from the configured site URL. The site MUST serve `sitemap.xml` listing every indexable route and `robots.txt`. Content pages MUST embed JSON-LD structured data.

#### Scenario: Titles and descriptions are unique per page

- **WHEN** `make site-check` inspects the rendered HTML of every indexable route
- **THEN** no two routes share a `<title>` or a `<meta name="description">`, and any duplicate fails the check

#### Scenario: Canonical URLs are absolute and derived from configuration

- **WHEN** the site is built with `NEXT_PUBLIC_SITE_URL` set
- **THEN** every canonical link and every sitemap entry is an absolute URL under that origin

#### Scenario: Non-indexable routes are excluded

- **WHEN** the sitemap and the `/search` route's metadata are inspected
- **THEN** `/search` and every `planned` page carry `noindex` and are absent from `sitemap.xml`

### Requirement: Search works without client-side JavaScript

The site SHALL provide search over page titles, summaries, headings, and body text. A client-side index MUST be built at build time. A server-rendered `/search?q=` route MUST return matching results in its HTML response for clients that do not execute JavaScript.

#### Scenario: Server-rendered search returns results

- **WHEN** a client issues `GET /search?q=intent` without executing JavaScript
- **THEN** the HTML response lists matching pages with their titles, sections, and statuses as links

#### Scenario: Search index is built, not fetched at runtime

- **WHEN** the built site is served with no network egress
- **THEN** search returns results from the pre-built index with no outbound request

### Requirement: Content is MDX with schema-validated frontmatter

Content SHALL live in `website/content/<section>/<slug>.mdx`. Frontmatter MUST be validated against a schema requiring `title`, `summary`, and `status`, and permitting `verifies` and `sources`. A missing or invalid field MUST fail the build rather than produce a default.

#### Scenario: Missing required frontmatter fails the build

- **WHEN** a contributor adds a content file with no `status` field
- **THEN** `make site-build` exits non-zero naming the file and the missing field

#### Scenario: Unknown status value fails the build

- **WHEN** a content file declares `status: production-ready`
- **THEN** the build fails reporting the permitted values `stable`, `experimental`, `partial`, `planned`

### Requirement: The site is accessible and readable in light and dark themes

The site SHALL meet WCAG 2.1 AA contrast for text and interactive elements in both light and dark themes, expose a skip-to-content link, use semantic landmarks and a single `<h1>` per page, keep heading levels non-skipping, and remain fully navigable by keyboard. Code blocks MUST be highlighted at build time, not in the browser.

#### Scenario: Automated accessibility check passes

- **WHEN** `make site-check` runs the automated accessibility audit over a representative route from every section in both themes
- **THEN** no serious or critical violations are reported

#### Scenario: Wide content does not scroll the page horizontally

- **WHEN** a page containing a wide table or code block is viewed at a 375px viewport
- **THEN** the wide element scrolls within its own container and the document body does not scroll horizontally

### Requirement: Internal and external links are checked

`make site-check` SHALL verify every internal link resolves to an existing route or in-page anchor, and every link into the repository resolves to an existing file and (when a line or anchor is given) an existing target. Broken links MUST fail the check.

#### Scenario: Broken internal link fails the check

- **WHEN** a page links to `/docs/does-not-exist`
- **THEN** `make site-check` exits non-zero naming the source file and the dead link

#### Scenario: Stale repository link fails the check

- **WHEN** a page links to `src/beam_agents/memory/stores.py`, which does not exist
- **THEN** `make site-check` exits non-zero naming the missing path
