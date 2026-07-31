## Why

This is a milestone-gate change with no propose-command in the roadmap: 0.5.0 (roadmap C43) is defined purely by its dependencies. It exists to cut the release that ships the M3 adoption surface — the YAML provider, the Dataflow Flex Template, the replay CLI, the Pydantic AI adapter, the Slack-approval and eval-pipeline examples, and the upstream design doc — as one coherent, installable version. Without an explicit gate change, "M3 is done" has no owner, no checklist, and no archived record; individual changes could land while the milestone silently never ships.

The release machinery itself (changelog file, version-bump procedure, tag-driven publish workflow) is established by `add-0-1-0-release` and is not re-specified here. This change only defines what must be true before the `v0.5.0` tag may be cut, and executes the established process once those conditions hold.

## What Changes

- **Version bump.** `pyproject.toml` `version` moves to `0.5.0` (currently `0.0.0` in-tree; intermediate bumps land with the earlier release changes, so this edit is "set to 0.5.0", not "increment from 0.0.0").
- **Changelog entry.** A `0.5.0` section in the changelog introduced by `add-0-1-0-release`, covering every M3 change archived since the previous release — all seven dependency changes listed under Impact, plus any other change archived in the window.
- **Tag and publish** via the release process established by `add-0-1-0-release`, unchanged. No new release machinery, workflow, or packaging configuration is introduced.
- **An explicit release-gate checklist** (the spec delta of this change): the seven M3 dependency changes archived, the adapter conformance matrix green across all registered adapters on both legs, the benchmark regression gate on the runtime-overhead latency budget green, and the changelog complete — all verified before the tag is cut.

## Capabilities

### New Capabilities

- `release-0-5`: the release-gate contract for 0.5.0 — the conditions under which the `v0.5.0` tag may be cut, and the version/changelog state the released artifact must carry. Written as a standalone capability so it validates on its own without depending on the (concurrently pending) `add-0-1-0-release` spec text.

### Modified Capabilities

None. This change executes the release process defined elsewhere; it modifies no runtime, packaging, or CI behavior. The only file edits are the version line and the changelog entry, neither of which is governed by an existing capability's requirements.

## Impact

**Depends on** — all seven M3 adoption-surface changes must be implemented and archived before this change can complete: `add-yaml-provider`, `add-dataflow-flex-template`, `add-replay-cli`, `add-pydantic-ai-adapter`, `add-slack-approval-example`, `add-eval-pipeline-example`, `add-upstream-design-doc`. Also builds on `add-0-1-0-release`, which establishes the changelog and the tag/publish release process this change reuses.

**New code:** no `src/` code. The additions are the `0.5.0` changelog section, this change's planning artifacts, and one test file — `tests/release/test_release_0_5_0.py`, which holds the release-artifact half of the gate (version, lockfile, the seven-change enumeration, and the recorded archival verdict checked against the archive directory).

**Modified code:** the version bump touches four files, not one (amended — see tasks.md Revision 1): `pyproject.toml`'s version field (`→ 0.5.0`); `uv.lock`, which records the project's own version and which `docs/releasing.md` already says "always comes with" a bump; `docs/yaml.md`'s two `beam-agents==X.Y.Z` provider pins, which `tests/yaml/test_docs_example.py` asserts equal the installed version; and the changelog file established by `add-0-1-0-release`, which gains a section. Plus `tests/release/test_release_0_3_0.py`, whose two version assertions pinned equality with the *then-current* version and are amended to the durable floor-plus-agreement form (tasks.md Revision 2). No `src/` or proto changes.

**CI/build:** no workflow edits. The publish is triggered through the existing tag-driven process from `add-0-1-0-release`; `ci`, `integration`, and `quality` must be green on the release commit as on any commit.

**Gates:** this change *is* a gate. Release-blocking: the seven dependency changes archived; conformance matrix green (all registered adapters — including Pydantic AI per `add-pydantic-ai-adapter` — × both DirectRunner and Flink legs); benchmark regression gate on the runtime-overhead latency budget (p50 < 15 ms / p99 < 60 ms per activation, excluding LLM/tool time) green; changelog covering all M3 changes. Plus the standard change gates: `make lint`, `make type`, `make test-unit`, `openspec validate add-0-5-0-release --strict`.
