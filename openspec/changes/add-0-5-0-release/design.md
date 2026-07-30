## Context

0.5.0 is the M3 milestone release: it ships the adoption surface (YAML provider, Flex Template, replay CLI, Pydantic AI adapter, two worked examples, upstream design doc) on top of the runtime the earlier 0.1.0 and 0.3.0 releases cover. In the current tree `pyproject.toml` still reads `0.0.0` and no changelog or release workflow exists — all of that machinery is created by the concurrently pending `add-0-1-0-release` and reused verbatim by every later release. The roadmap deliberately gives C43 no propose-command: the release has no feature content of its own, so its entire substance is the gate — which conditions block the tag, and what the tagged artifact must contain.

The sibling release-gate proposals (0.1.0, 0.3.0) set the pattern this change follows: a short proposal, a standalone per-release capability delta stating the SHALL NOT-ship conditions, and a checklist-shaped tasks file. Divergence from that pattern would make release gates harder to compare across milestones for no benefit.

## Goals / Non-Goals

**Goals:**
- Define the complete, checkable set of conditions under which `v0.5.0` may be tagged.
- Ship all seven M3 adoption-surface changes as one version, with a changelog that accounts for each.
- Reuse the `add-0-1-0-release` process without modification, keeping release mechanics identical across 0.1.0 → 0.3.0 → 0.5.0.

**Non-Goals:**
- Any new release machinery: no workflow edits, no packaging changes, no changelog-format changes.
- Any feature or fix content. A defect found during gate verification is fixed in its own change; this change only waits on it.
- Re-specifying the internals of the `add-0-1-0-release` process (trigger, index, credentials). Those are that change's requirements; this change references the process by name only.
- Defining M4 scope or the next release gate.

## Decisions

### D1. All seven dependency changes block; none may slip to a 0.5.x

The alternative — ship 0.5.0 with whatever subset of M3 landed and let stragglers trail in patch releases — is rejected. The M3 batch is an *adoption surface*: the examples exercise the YAML provider and HITL flow, the conformance matrix's value at this milestone is precisely that it now covers the Pydantic AI adapter, and the design doc describes the surface as shipped. A partial 0.5.0 would ship documentation and examples referencing capabilities that are not in the artifact. If any of the seven cannot archive, 0.5.0 slips as a whole — the release date moves, the scope does not.

### D2. The gate checks archival, not merely merge

The blocking condition is that each dependency change is **archived** (implemented, gates passed, delta synced to main specs), not just that its PRs merged. Archival is the repo's only marker that a change's spec, tests, and code are mutually consistent; merged-but-unarchived means verification is still open. This also makes the gate mechanically checkable: seven named directories present under `openspec/changes/archive/`.

### D3. Quality gates are the existing ones, re-verified — not re-invented

The conformance matrix and the benchmark latency budget are already release-blocking by constitution (`openspec/project.md`: semantics gates "gate every release"; benchmark regressions on the latency budget "are release blockers"). This change does not define new thresholds or new jobs; it pins *which* runs satisfy the gate: the matrix and benchmark results on the release commit itself (the commit the tag will point at), not on some earlier green commit. By 0.5.0 the registered-adapter set includes Pydantic AI, and the matrix meta-test already fails collection if a registered adapter escapes the matrix, so "matrix green" automatically means "green including pydantic-ai" with no extra wiring.

### D4. The changelog is verified against the archive, not against memory

The 0.5.0 changelog section must name every change archived since the previous release tag. The verification is a diff of `openspec/changes/archive/` timestamps against the previous tag's date — mechanical, not recall-based — so a small fix change landing mid-window cannot be silently omitted.

## Risks / Trade-offs

- **Serialization risk: seven blockers means seven chances to slip.** Accepted deliberately (D1). Mitigation: the dependencies are siblings in one batch and mostly independent of each other, so they archive in parallel; this gate serializes only the final tag, not their development.
- **Dependency on unmerged machinery.** This proposal references the `add-0-1-0-release` process by name while that change is itself pending. If its design shifts (e.g., a different tag format), this change's tasks inherit the shift. Mitigated by referencing the process abstractly ("the release process established by `add-0-1-0-release`") rather than restating its internals.
- **Stale-green risk.** A gate satisfied on Monday can rot by Friday's tag if intervening commits land. D3's "on the release commit" rule closes this: the verification run and the tagged commit are the same SHA.
- **Version-line conflict.** Every release change edits the same `pyproject.toml` line. Trivially resolved by ordering: this change lands last, after all prior release bumps, and sets the value absolutely.

## Migration Plan

Nothing migrates: no wire schema, state schema, API, or dependency changes. Consumers upgrade with `pip install --upgrade beam-agents`; any behavioral migration notes belong to the individual M3 changes and are surfaced through their changelog entries. Rollback of a bad publish follows whatever yank/post-release procedure `add-0-1-0-release` established; this change adds nothing to it.

## Open Questions

- Does `add-upstream-design-doc` (a documentation change) produce anything the *published artifact* must carry, or is its archival alone sufficient? Assumed: archival alone — the doc lives in the repo, not the wheel. Resolve when its proposal is final.
- Exact set of "other changes archived in the window" for the changelog cannot be known until tag time; D4's mechanical diff resolves it then.
