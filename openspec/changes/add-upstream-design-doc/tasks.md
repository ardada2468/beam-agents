## 1. Tests (written first, must fail for the right reason)

This is a documents-only change, so the test surface is deliberately thin: prose quality is gated by review, and the mechanizable properties — invariant completeness, link integrity, no unbacked numbers — are exactly what the docs-consistency test checks. The test is written before the documents exist so it fails for the right reason (missing files / missing invariants), establishing the scenario→test chain even for docs.

- [ ] 1.1 Create `tests/docs/test_upstream_design_doc.py` implementing *Dropping an invariant from the doc fails the build*: assert `docs/design/apache-beam-ml-agents.md` exists and contains identifying phrases for all seven correctness invariants (atomic commit, deterministic intent IDs incl. "byte-identical", replay cache incl. zero-additional-provider-calls, per-key serialization, side effects only via intents, fail-closed timeouts incl. `orphaned_result`, protobuf-only state incl. `state_schema_version`) — phrase list defined as module constants sourced from `openspec/project.md` review, not from the doc itself
- [ ] 1.2 Add the link-integrity check from *Every relative link resolves* mechanics of the docs-consistency requirement: every relative markdown link in both `docs/design/` documents resolves to an existing in-repo file
- [ ] 1.3 Implement *A placeholder number cannot ride to thread-readiness*: parse the evidence section's thread-ready checklist; fail if quantitative claims (regex over the evidence section for numeric performance/adoption figures) coexist with pending checklist items
- [ ] 1.4 Run the new tests and confirm they fail because the documents do not exist yet (not because of collection or parsing errors); confirm they are collected by the offline unit lane (`make test-unit`) and carry no `semantics`/`integration` markers

## 2. Design document: distillation sections

- [ ] 2.1 Draft `docs/design/apache-beam-ml-agents.md` front matter and audience framing per the *distills the constitution* requirement: standalone-readable, vocabulary defined on first use, runtime-not-framework principle with the out-of-scope list, the two execution paths, and the no-DAG-cycles rationale
- [ ] 2.2 Write the invariants section: all seven correctness invariants restated for a Beam audience at equal-or-stronger force, then diff it line-by-line against `openspec/project.md` §Correctness invariants and record the diff review in the PR description
- [ ] 2.3 Write the state-layout section per the *Beam SDK realities* requirement: five state specs, three timers, each SDK limitation (no MapState, no async DoFn, KV input) paired with its design response; state-size discipline and the `--update` compatibility story (`state_schema_version`, additive-only protos, golden blobs)
- [ ] 2.4 Write the effectively-once section: outbox model, deterministic `intent_id`, the effector dedup boundary, and the honest duplicate-window statement sourced from the effectively-once e2e gate's documented semantics (`tests/semantics/test_effectively_once_e2e.py` and `docs/effector.md`)
- [ ] 2.5 Write the compatibility section presenting the adapter conformance matrix (`tests/conformance/`: scenarios × adapters × DirectRunner/Flink legs) as the verifiable bring-your-own-framework story, consistent with the archived `add-adapter-conformance-matrix` capability spec
- [ ] 2.6 Write the dependency-policy section per design D3: `httpx`/`pydantic` as the only non-Beam required deps, the zero-provider-SDK commitment evidenced by the existing httpx-based clients, extras mapping for langgraph/effector/otlp, Beam dependency review marked as an open ask

## 3. Design document: decision record

- [ ] 3.1 Write the move/stay decision record per design D2: one entry per top-level module (core, protos, model, tools, actions, memory, hitl, observability, adapters, effector), each with disposition + rationale; cross-check the module list against `src/beam_agents/` so no module is silently omitted
- [ ] 3.2 Write the effector entry per its spec scenario: stays external, intent/result protobuf contract named as the standardized boundary, post-donation home listed as open
- [ ] 3.3 Add the open-questions section mirroring design.md's Open Questions (package path, donation mechanics, effector home, dependency review, Beam version floor, sponsor) so the doc carries them into the community conversation

## 4. Evidence section and thread-ready checklist (gated on 0.3)

- [ ] 4.1 Write the evidence section skeleton with the thread-ready checklist per design D5: entries for the Flink Agents benchmark report (with the p50 < 15 ms / p99 < 60 ms runtime-overhead budget named as the standing bar), the conformance-matrix results, and design-partner usage — all marked pending, zero numeric claims
- [ ] 4.2 After `add-0-3-0-release` ships its artifacts: fill each evidence entry by reference to the shipped artifact, check off the checklist, and re-run the docs-consistency test to confirm the no-unbacked-numbers rule now passes with real references
- [ ] 4.3 Final consistency pass: re-diff the invariants and state-layout sections against the then-current `openspec/project.md`, and record in the doc which beam-agents version it describes

## 5. Thread plan

- [ ] 5.1 Draft `docs/design/apache-beam-ml-agents-thread-plan.md` announcement email per the thread-plan requirement: problem statement, one-paragraph proposal, links to the design doc and evidence artifacts, and the three explicit asks (design feedback; sponsoring committer/PMC member; donation-mechanics guidance)
- [ ] 5.2 Write the objections register covering the full D4 minimum set — SDF vs. stateful DoFn, RunInference overlap, inline durable execution vs. outbox, cross-language scope, dependency policy, `--update`/state compat, maintainership, governance/IP clearance — each entry answered or explicitly marked "open — asking the thread", none blank
- [ ] 5.3 Review every governance passage in both documents against the *ASF process uncertainty* requirement: each unverifiable mechanic phrased as a question/ask; remove or reframe anything asserting ASF procedure as settled fact
- [ ] 5.4 Add the announcement-sequencing note: sending is blocked on the thread-ready checklist (task 4.2) and is a human-owner action outside this change; record the D1 mirroring step (markdown → commentable shared doc at send time, repo file stays canonical)

## 6. Gates

- [ ] 6.1 `make lint`
- [ ] 6.2 `uv run pre-commit run --all-files`
- [ ] 6.3 `openspec validate add-upstream-design-doc --strict`
- [ ] 6.4 `make test-unit` green including `tests/docs/test_upstream_design_doc.py`, and `git status` confirms no changes outside `docs/design/`, `tests/docs/`, and this change folder
