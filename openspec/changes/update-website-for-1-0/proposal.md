## Why

`pyproject.toml` declares `1.0.0`, but the documentation site still described the 0.x world — and worse, it described a *smaller* project than the one that ships. Five roadmap pages asserted that code which exists does not (`adapters/adk/`, `adapters/pydantic_ai/`, `memory/stores/`, `yaml/`, `model/vllm.py`); the adapters comparison said "Today one does" when three adapters sit on the conformance matrix; the runners page claimed "no benchmark harness" and "no deployment guide" while `benchmarks/`, `docs/benchmarks.md`, and `docs/deploying.md` all exist; and the install page counted three extras out of nine.

The verifier that exists to prevent exactly this had three holes that let it happen:

1. **`_check_planned` was vacuous on `verifies: []`.** A planned page that declared nothing could never fail when its feature shipped — which is precisely how five roadmap pages stayed "planned" after the code landed.
2. **`check_release_state` keyed off the version string.** It early-returned the moment `project.version` left `0.0.0` — i.e. it disarmed itself during exactly the window it exists for, between the version bump and the `v1.0.0` tag. Nothing is on PyPI, and the guard was off.
3. **The doc-coverage regex excluded hyphens**, so `docs/state-compat.md` and `docs/state-migration.md` could never be credited by prose mention.

Separately, the site's own version wiring was stale: `PACKAGE_VERSION` was hardcoded `'0.0.0'` (footer read "0.0.0 · unreleased", the hero badge "Pre-release · v0.0.0"), `IS_RELEASED` was derived from the version string (the same bug as hole 2, in TypeScript), and the site had no 404 page, no error boundary, no favicon, and no share card.

## What Changes

**Verifier (`scripts/verify_docs_claims.py`)**

- A `planned` page with no `symbol:`/`module:` assertion is now a hard failure: it must declare what it is waiting on, or the planned/shipped inversion is unenforceable. Correspondingly, `check_assertions` no longer faults a planned page's `symbol:`/`module:` entries for not existing — non-existence is the honest state there, and `_check_planned` fires when they appear.
- `check_release_state` now keys off `is_released()` — the existence of the `v{version}` git tag, fail-closed when git is unavailable — never off the version string. The no-registry-install guard stays armed across the bump-to-tag window.
- The coverage regex accepts hyphens in doc filenames.
- New `check_site_constants()`: `website/lib/site.ts` must declare the same `PACKAGE_VERSION` as `pyproject.toml`, and `IS_RELEASED` must be an explicit boolean literal matching the git-tag release state.
- `scripts/check_docs_prose.py`: the stale "no benchmark harness exists in this repository" docstring and finding message are corrected; the rule (numbers need a label or a source) is unchanged.
- `tests/docs/test_fidelity_checks.py` follows: the `unreleased` fixture pins release state rather than the version string, plus regression tests for the bump-to-tag window, the empty-`verifies` planned page, and the honest planned page declaring an absent module.

**Content — false pages made true (true at this commit; release-dependent facts stay conditional)**

- `roadmap/adk-adapter.mdx` and `roadmap/pydantic-ai-adapter.mdx` deleted; `comparison/adapters.mdx` rewritten to document all three shipped adapters (table, prose, diagram, per-adapter sections, dated citations for ADK and Pydantic AI), with `verifies` covering all three adapter packages, their specs, and the conformance tests.
- `roadmap/memory-stores.mdx` deleted; new `docs/memory-stores.mdx` documents the shipped long-term tier (four backends, seq-guarded upserts, compaction, the invariant-5 exception) with retention honestly listed as not implemented. `learn/state-and-memory.mdx` promoted to `stable`: its "does not exist" section replaced by an accurate long-term memory section grounded in `docs/memory.md` and the store conformance suite.
- `roadmap/yaml-provider.mdx` deleted; new `docs/yaml-provider.mdx` (stable) documents the shipped provider.
- `roadmap/more-providers.mdx` rewritten as the Vertex-only roadmap page — vLLM is shipped and said so — and now declares `module: src/beam_agents/model/vertex.py` as what it waits on.
- `docs/runners.mdx`: the "no benchmark harness" and "no deployment guide" bullets replaced with sections pointing at `benchmarks/` + `docs/benchmarks.md` and `docs/deploying.md`; the Spark row updated to the precise current state (a weekly leg exists; every conformance cell is a declared skip due to the recorded Beam Spark runner gap).
- `learn/install.mdx`: the extras table lists all nine extras; the "nothing published yet" framing is kept, as it is still true.
- `comparison/flink-agents.mdx`: four stale claim cells corrected (long-term store, adapters, Spark, benchmarks).
- New `specs/capability-index.mdx` lists, by path, every capability spec promoted at 1.0 that has no dedicated site page yet — same pattern as the repository-reference page — so the coverage check passes without weakening it.

**Version wiring and launch hygiene (`website/`)**

- `lib/site.ts`: `PACKAGE_VERSION = '1.0.0'`; `IS_RELEASED` is an explicit `false` with a comment stating it flips at tag time and is held to the real tag state by the verifier. `package.json` version follows; `README.md`'s stale "0.0.0" sentence corrected.
- Footer renders "1.0.0 · not yet published" (qualifier driven by `IS_RELEASED`); the hero badge renders "v1.0.0 · pre-release, not yet on PyPI".
- New `app/not-found.tsx`, `app/error.tsx`, `app/icon.tsx` (favicon), and `app/opengraph-image.tsx` (share card), all in the site's idiom and truthful about release state.
- `SITE_URL` keeps its `localhost` fallback — no production host exists for this site — behind a loud comment stating that choosing a host is a project decision.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `docs-content-fidelity`: the planned-status rule gains the mandatory-declaration clause; the distribution-state rule is re-keyed from the version string to the release tag; a new requirement holds the site's rendered version constants to the repository.

## Impact

- `scripts/verify_docs_claims.py`, `scripts/check_docs_prose.py`, `tests/docs/test_fidelity_checks.py` — verifier and its tests.
- `website/content/**` — pages listed above (4 deleted, 3 added, 8 edited).
- `website/lib/site.ts`, `website/components/Footer.tsx`, `website/app/page.tsx`, `website/app/{not-found,error,icon,opengraph-image}.tsx`, `website/package.json`, `website/README.md`.
- No `src/beam_agents/` changes, so no changelog fragment is required (the fragment hook triggers on `src/*` only).
- Gates: `uv run python scripts/verify_docs_claims.py`, `uv run python scripts/check_docs_prose.py`, `uv run pytest tests/docs/test_fidelity_checks.py tests/docs/test_website_examples.py`, and in `website/`: `pnpm typecheck && pnpm lint && pnpm test && pnpm build && pnpm check:links && pnpm check:ssr && pnpm check:a11y` — all green at authoring time.
