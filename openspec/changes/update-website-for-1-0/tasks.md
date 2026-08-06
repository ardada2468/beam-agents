## 1. Verifier holes

- [x] 1.1 `_check_planned`: fail a `planned` page whose `verifies` carries no `symbol:`/`module:` assertion; keep the exists-inversion check
- [x] 1.2 `check_assertions`: skip existence resolution for `symbol:`/`module:` on `planned` pages (the inversion owns that direction), so an honest planned declaration is expressible
- [x] 1.3 `check_release_state`: add cached `is_released()` (`git tag -l v{version}`, fail-closed on git errors) and key the registry-install guard off it; update the module docstring and the finding message
- [x] 1.4 Coverage regex: `docs/[a-z_]+\.md` → `docs/[a-z_-]+\.md` so `docs/state-compat.md` and `docs/state-migration.md` are creditable
- [x] 1.5 New `check_site_constants()`: `website/lib/site.ts` `PACKAGE_VERSION` equals `pyproject.toml`'s version; `IS_RELEASED` is a boolean literal equal to the tag state; wire into `main()`
- [x] 1.6 `check_docs_prose.py`: correct the stale "no benchmark harness" docstring and finding message without weakening the rule
- [x] 1.7 `tests/docs/test_fidelity_checks.py`: re-pin the `unreleased` fixture to `is_released`, add the bump-to-tag regression, the empty-`verifies` planned failure, and the absent-module planned pass

## 2. Content corrections

- [x] 2.1 Delete `roadmap/{adk-adapter,pydantic-ai-adapter,memory-stores,yaml-provider}.mdx`; rewrite `roadmap/more-providers.mdx` as the Vertex-only page declaring `module: src/beam_agents/model/vertex.py`
- [x] 2.2 Rewrite `comparison/adapters.mdx` for three shipped adapters: table, diagram, per-adapter sections, citations for ADK and Pydantic AI, `verifies` over all three packages + conformance tests + adapter specs + `docs/adapters.md`
- [x] 2.3 New `docs/memory-stores.mdx` (partial — retention is the honest gap), grounded in `docs/memory.md` and `tests/memory/stores/_conformance.py`
- [x] 2.4 Promote `learn/state-and-memory.mdx` to `stable`: replace the "does not exist" section with the shipped long-term tier, extend `verifies` with the store modules, compaction, `docs/memory.md`, the memory-stores spec, and the store/longterm tests
- [x] 2.5 New `docs/yaml-provider.mdx` (stable) from `docs/yaml.md` and the yaml test suite
- [x] 2.6 `docs/runners.mdx`: benchmark and deploying sections replace the false bullets; Spark stated precisely (weekly leg, all cells declared skips, runner-level SDF checkpoint gap)
- [x] 2.7 `learn/install.mdx`: nine-extras table; keep the unpublished-package framing
- [x] 2.8 `comparison/flink-agents.mdx`: correct the long-term-store, adapters, Spark, and benchmark cells
- [x] 2.9 Fix links that pointed at deleted roadmap pages (`specs/memory-facade.mdx`, `specs/model-client.mdx`); update `docs/repository-reference.mdx` (memory/yaml rows out, `docs/providers.md` row in); cover `docs/hitl.md` and the human-in-the-loop spec from `docs/human-in-the-loop.mdx`
- [x] 2.10 New `specs/capability-index.mdx` covering, by path, every 1.0-promoted capability spec without a dedicated page

## 3. Version wiring and launch hygiene

- [x] 3.1 `lib/site.ts`: `PACKAGE_VERSION = '1.0.0'`; `IS_RELEASED = false` as an explicit literal with the tag-time comment; loud no-host-chosen warning on the `SITE_URL` fallback
- [x] 3.2 `components/Footer.tsx` and `app/page.tsx`: render version + release qualifier from the constants; fix the "Not built" ledger's release line
- [x] 3.3 `website/package.json` version `1.0.0`; `website/README.md` stale `0.0.0` sentence corrected
- [x] 3.4 Add `app/not-found.tsx`, `app/error.tsx`, `app/icon.tsx`, `app/opengraph-image.tsx`

## 4. Verification

- [x] 4.1 `uv run python scripts/verify_docs_claims.py` — passes (40 pages)
- [x] 4.2 `uv run python scripts/check_docs_prose.py` — passes (40 pages)
- [x] 4.3 `uv run pytest tests/docs/test_fidelity_checks.py tests/docs/test_website_examples.py` — 44 passed
- [x] 4.4 `website/`: `pnpm typecheck`, `pnpm lint`, `pnpm test` (49 passed), `pnpm build`, `pnpm check:links`, `pnpm check:ssr`, `pnpm check:a11y` — all green
- [x] 4.5 `uv run ruff check` / `ruff format --check` / `mypy` over the edited Python — clean
- [ ] 4.6 At tag time: flip `IS_RELEASED` to `true` in `lib/site.ts` (the verifier fails the build in either direction if it disagrees with the tag)
- [ ] 4.7 When a production host is chosen: replace the `SITE_URL` localhost fallback and set `NEXT_PUBLIC_SITE_URL` in the deploy environment
