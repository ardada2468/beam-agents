## Why

`pyproject.toml` already reads `version = "1.0.0"`, but the documentation still describes the repository the 0.x line shipped. The gap is not cosmetic: [docs/releasing.md](../../../docs/releasing.md) opens with a "Pre-1.0 versioning policy" and states outright that there is "**no 1.0 API-stability commitment yet**" — while `CONTRIBUTING.md`, `CHANGELOG.md`, and the website all point readers at that page for "the full policy". Its compatibility-surface list names 8 public root names where `beam_agents/__init__.py` freezes 16, and 4 extras plus one console script where `pyproject.toml` ships 9 extras and 3 scripts. `docs/index.md` says "The package is unreleased" and counts "three examples" where `examples/` holds seven programs, each with a doc page. Two `src/` docstrings and the replay transcript still pin `0.1.0`. Eight real, finished doc pages (`continuous_eval`, `sharding`, `memory`, `state-compat`, `state-migration`, `benchmarks`, the Flink Agents comparison, the Slack approval example) are reachable only by URL because they never joined the mkdocs nav. And the version-coupled-reference sweep that keeps rediscovering itself — the same class of stale pin was fixed at 0.3.0, 0.5.0, and again now — has no checklist step, so it will be rediscovered a fourth time at tag time unless the release process learns it.

Tagging `v1.0.0` with the site claiming there is no 1.0 commitment would make the release notes and the documentation contradict each other on the release's central claim.

## What Changes

- **`docs/releasing.md` rewritten for the 1.0 regime.** The pre-1.0 policy section is scoped to history; the post-1.0 policy is stated in its place: semver over the frozen surface, `public-surface.toml` as the authority (snapshot-tested both directions by `tests/test_public_surface.py`), and the deprecation window restated in full — this page is where every other document sends readers for the policy. The "no 1.0 commitment yet" paragraph is deleted. The compatibility surface is corrected to the 16 frozen root names, 9 extras, and 3 console scripts (also in the TestPyPI rehearsal list). The latency-budget checklist item now reflects that the budget is machine-checked: `make bench-gate` against the seeded `benchmark-baseline.toml`, plus the release workflow's own fail-closed requirement of a green nightly `bench` job (whose report it attaches to the GitHub Release). A new release-PR step enumerates every version-coupled reference (`docs/yaml.md` pins, the two `src/beam_agents/yaml/` docstring pins, the `docs/replay.md` transcript, `website/lib/site.ts` `PACKAGE_VERSION`, `uv.lock`).
- **`CONTRIBUTING.md`** — the "project is pre-1.0" paragraph becomes the 1.0 statement consistent with the above, and the "Pre-1.0 exemption" note moves to past tense (it documents why the `add-1-0-api-freeze` sweep was legal, which stays true as history). The deprecation-window policy itself is unchanged.
- **`docs/index.md` and `README.md`** — the Install section presents `pip install beam-agents` as the path once `v1.0.0` is published, keeping source install as the current one; the "three examples" undercount becomes the honest seven (four hermetic, three world-touching), each linked to its page.
- **Stale `0.1.0` strings** — `docs/replay.md`'s transcript now prints `beam-agents-replay 1.0.0` (the CLI prints the installed `importlib.metadata` version), and the `beam-agents==0.1.0` pins in `src/beam_agents/yaml/providers.yaml` and the `beam_agents.yaml` module docstring become `==1.0.0`.
- **mkdocs nav completed** — the 8 orphaned pages join the nav in sensible groups (`docs/design/*` stays out deliberately: drafts). The worst factual staleness inside `docs/design/apache-beam-ml-agents.md` is corrected minimally ("Describes: 0.1.0", "0.3.0 has not shipped", "the medians table is deliberately empty") without rewriting the draft.
- **Three new pages, sourced from `src/` and the existing scattered mentions**: `docs/hitl.md` (the suspension lifecycle, `HitlPolicy`, the three routes, the two fail-closed layers, orphaned results), `docs/adapters.md` (the three adapters, the protocol-is-the-seam contract, the conformance-matrix guarantee), `docs/providers.md` (the four shipped providers, `FakeLLM`, the replay-cache interaction, the decode seam). All three join the nav under a new "Building on the runtime" group.
- **`docs/benchmarks.md`** — the release-artifact section now names its consumer: `release.yml`'s publish job locates the most recent green nightly `bench` job, attaches its report to the GitHub Release, and fails closed without one.
- **One factual correction outside the version sweep**: `docs/api.md`'s HITL table said `Escalate` routes to `.errors` and `Drop` merely discards — inverted against `src/beam_agents/hitl.py`. Corrected to the source's semantics.

## Capabilities

### New Capabilities

None. This change corrects and completes documentation of behavior that already exists and is already gated; no runtime, packaging, or CI behavior changes.

### Modified Capabilities

None. The two `src/beam_agents/yaml/` edits touch only a comment and a docstring — no code path, no public name, no wire byte changes — so no spec delta is warranted.

## Impact

- **Docs**: `docs/releasing.md`, `docs/index.md`, `docs/benchmarks.md`, `docs/replay.md`, `docs/api.md`, `docs/design/apache-beam-ml-agents.md`, new `docs/{hitl,adapters,providers}.md`, `mkdocs.yml` nav, `README.md`, `CONTRIBUTING.md`.
- **Code**: `src/beam_agents/yaml/providers.yaml` (comment) and `src/beam_agents/yaml/__init__.py` (docstring) — the `0.1.0` pins. `tests/yaml/` passes unchanged; the change carries an `internal` changelog fragment.
- **Truth-at-tag discipline**: everything written is true at this commit; the one fact that flips at the tag (PyPI availability) is phrased conditionally ("once v1.0.0 is published"), so nothing needs re-editing between merge and tag.
- **Not changed**: `CHANGELOG.md` (assembled at release time), `website/` and `.github/` (owned by concurrent workstreams), the deprecation-window policy itself, and the `docs/design/*` drafts beyond the named factual markers.
