## 1. Version-string sweep (the mechanical truth fixes)

- [x] 1.1 `docs/replay.md`: the transcript's `beam-agents-replay 0.1.0` line → `1.0.0` (the CLI prints the installed `importlib.metadata` version, so the transcript must show the version the release ships)
- [x] 1.2 `src/beam_agents/yaml/providers.yaml` and `src/beam_agents/yaml/__init__.py`: the `beam-agents==0.1.0` pins in the listing comment and the module docstring → `==1.0.0`
- [x] 1.3 `uv run pytest tests/yaml/ -q` green after the `src/` edits — confirms nothing guards the old strings (78 passed)
- [x] 1.4 Changelog fragment `changelog.d/update-docs-for-1-0.internal.md` (the `src/` docstring edits require one; `internal` because nothing user-observable changes)

## 2. `docs/releasing.md` for the 1.0 regime

- [x] 2.1 Replace "Pre-1.0 versioning policy" with the post-1.0 policy: semver table, `public-surface.toml` as the frozen surface (snapshot-tested both directions by `tests/test_public_surface.py`), the deprecation window restated in full (CONTRIBUTING.md, CHANGELOG.md, and the website all send readers here for "the full policy"), and the 0.x rule scoped to a history subsection
- [x] 2.2 Delete the "no 1.0 API-stability commitment yet" paragraph
- [x] 2.3 Compatibility surface item 1: the 16 frozen root names of `beam_agents/__init__.py`, with `public-surface.toml` as the authority; item 4: the 9 extras and 3 console scripts; the TestPyPI rehearsal list likewise covers all nine extras and all three scripts
- [x] 2.4 Latency-budget checklist item: machine-checked by `make bench-gate` (`scripts/bench_gate.py`) against `benchmark-baseline.toml` (seeded 2026-08-03 from scheduled nightly run 30806138398, ubuntu-latest), plus the release workflow's fail-closed green-bench requirement
- [x] 2.5 "What the tag triggers": the publish job's bench-report step — locate the most recent green nightly `bench` *job* via the jobs API, download `benchmark-report`, fail the release if none exists in the last 30 nightly runs, attach `bench-report.md` and `bench-results.zip` to the GitHub Release (verified against `.github/workflows/release.yml`)
- [x] 2.6 New release-PR step: the version-coupled-reference sweep (`docs/yaml.md` pins, both `src/beam_agents/yaml/` pins, the `docs/replay.md` transcript, `website/lib/site.ts` `PACKAGE_VERSION`, `uv.lock`) — rediscovered at three consecutive milestones, now a checklist step

## 3. `CONTRIBUTING.md`, `docs/index.md`, `README.md`

- [x] 3.1 `CONTRIBUTING.md` "Releasing": "pre-1.0" paragraph → the 1.0 statement, consistent with 2.1; deprecation-window policy text untouched
- [x] 3.2 `CONTRIBUTING.md` "Pre-1.0 exemption" note → past tense ("The one historical exemption") — it records why the `add-1-0-api-freeze` sweep was legal
- [x] 3.3 `docs/index.md` Install: `pip install beam-agents` presented as available once `v1.0.0` is published; source install kept as the current path; extras pointer added
- [x] 3.4 `docs/index.md` "Start here": all seven example programs, split honestly into the four hermetic ones and the three that touch the world; `README.md`'s "three runnable examples" likewise corrected
- [x] 3.5 `docs/benchmarks.md` release-artifact section: name the consumer in `release.yml` (attach + fail-closed)

## 4. Navigation and the design-doc markers

- [x] 4.1 `mkdocs.yml`: add the 8 orphaned pages — `examples/slack-approval.md` and `continuous_eval.md` under Examples; `memory.md` and `sharding.md` under the new "Building on the runtime" group; `state-compat.md`, `state-migration.md`, `benchmarks.md`, `benchmarks/0.3.0-vs-flink-agents.md` under "Operating the runtime"
- [x] 4.2 Leave `docs/design/*` out of nav (drafts, deliberately), but fix the factual markers in `apache-beam-ml-agents.md`: "Describes: 0.1.0" → 1.0.0; both "0.3.0 has not shipped" pendings; "the medians table is deliberately empty" → seeded
- [x] 4.3 `uv run mkdocs build --strict` clean (design/* absence from nav is INFO, not a failure)

## 5. The three new pages

- [x] 5.1 `docs/hitl.md`: the suspension lifecycle (stage → suspend → resume on the same key), `HitlPolicy` and the purity contract on `on_timeout`, the three routes, fail-closed at both layers (`HITL_TIMER` routing and the effector's `refuse_expired`, non-positive expiry reads as expired), late answers as `orphaned_result` with the four admission details — sourced from `src/beam_agents/hitl.py`, `core/agent.py`, `core/dofn.py`, `core/context.py`
- [x] 5.2 `docs/adapters.md`: the three adapters with their extras and adoption steps, the reserved memory namespaces, protocol-is-the-seam (no base class), the conformance matrix and its collection-time registry guard — sourced from `src/beam_agents/adapters/`, `tests/conformance/_registry.py`, the README adapter sections
- [x] 5.3 `docs/providers.md`: `provider_factory`/`decode` wiring, the four shipped providers (no Vertex provider exists — stated, with the gateway/ADK paths that cover Gemini), `FakeLLM`, the cache-first path and invariant 3, the facade's resilience layers — sourced from `src/beam_agents/model/`
- [x] 5.4 All three in nav under "Building on the runtime"; every internal anchor verified against the built site
- [x] 5.5 Fix `docs/api.md`'s inverted `Drop`/`Escalate`/`REASON_HITL_TIMEOUT` rows against `src/beam_agents/hitl.py`

## 6. Verification

- [x] 6.1 `uv run mkdocs build --strict` clean
- [x] 6.2 `uv run pytest tests/yaml tests/docs -q` clean (docker/network-dependent tests skip cleanly)
- [x] 6.3 `openspec validate update-docs-for-1-0 --strict` clean
- [ ] 6.4 At archive time: nothing to promote (no delta specs); confirm the release checklist's new sweep step survives any concurrent `docs/releasing.md` edits from the release-workflow workstream
