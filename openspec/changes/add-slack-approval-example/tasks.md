## 1. Tests (written first, must fail for the right reason)

- [x] 1.1 `tests/examples/test_slack_approval.py` — consume/post tests against fakes (spec: "An approval intent becomes a Block Kit message and is then committed"; "A crash before posting loses nothing"; "Non-approval intents are skipped"; "A redelivered intent does not double-post within a process"), using a FakeLLM-driven `TestPipeline` activation to stage the real intent and `InMemoryIntentSource` to deliver it. *(`test_the_demo_activation_stages_exactly_the_intent_the_surface_consumes` pins the fed intent byte-identical to the pipeline-staged one.)*
- [x] 1.2 Decision tests (spec: "Approve click publishes a keyed approval envelope"; "A second click on a decided intent publishes nothing"; "Deny click publishes approved=false and the agent takes the denied path"), asserting deterministic envelope bytes and raw-`entity_key` keying into `InMemoryMessageSink`.
- [x] 1.3 Resume tests (spec: "The published envelope resumes the suspended activation"): feed the surface-published envelope into a `TestStream`-scripted demo pipeline as `[event, envelope]` and assert the main output carries the verdict with nothing on `.errors`; cover approve and deny.
- [x] 1.4 TTL tests with the injectable clock, no sleeps (spec: "An already-expired intent is surfaced without buttons" including non-positive `expires_at_ms`; "Expiry while pending removes the buttons"; "A click racing expiry publishes nothing" including the post-restart empty-pending-map case).
- [x] 1.5 Boundary tests (spec: "The offline loop runs with fakes only"; "The service modules import without Beam"): import the service modules with `apache_beam` and `slack_sdk` blocked from `sys.modules`; assert `beam_agents.__all__` gains nothing. *(One static AST check over the service modules plus a subprocess import with both roots blocked, mirroring `tests/effector/test_boundary.py`.)*
- [ ] 1.6 `tests/examples/test_slack_approval_kafka.py` (`-m integration`) — the compose-Redpanda closed loop (spec: "The closed loop over Redpanda resumes the agent"; "The example tracks its documentation"): stage via `WriteIntents` to a real channel topic, run the surface with real Kafka source/sink and the fake gateway scripted to approve, read the envelope bytes back, re-inject, assert resume. *(Written per spec; **blocked: needs docker** — not runnable in this environment. See Revision R1 on the producer used in place of `WriteIntents`.)*

## 2. Example scaffolding and dependencies

- [x] 2.1 Create `examples/slack_approval/` following the examples-surface layout established by sibling change `add-docs-site` (C24); adjust paths to C24's final layout if it differs. *(C24 has not landed; layout chosen per design D1 — `examples/<name>/`, `docs/examples/<name>.md`, `tests/examples/`. See Revision R2.)*
- [ ] 2.2 Add an `examples` dependency group to `pyproject.toml` carrying `slack-sdk`; confirm it is outside the wheel, outside `[project.optional-dependencies]`, and unused by the offline `ci` lanes. *(**Blocked: requires a `uv.lock` update**, which this worktree must not make. The lazy import keeps every lane and every test slack-free; the doc page installs it ad hoc with `uv pip install slack-sdk`. See Revision R3.)*
- [x] 2.3 Add the `tests/examples/conftest.py` path fixture (or C24's equivalent import mechanism) so doc-contract tests import the example modules without installing them. *(Repo root on `sys.path`; `examples` is `known-first-party` for ruff isort and covered by `mypy --strict`.)*
- [x] 2.4 `examples/slack_approval/config.py`: frozen config (intents-channel URI, approvals-topic URI, Slack channel, tokens from env, sweep interval, clock) with eager import-free validation mirroring `EffectorConfig`, reusing the effector's transport-URI grammar. *(Clock is injected on `ApprovalSurface` rather than carried on the frozen config — a callable is not configuration; see Revision R4.)*

## 3. Surface service: consume → post

- [x] 3.1 `examples/slack_approval/blocks.py`: Block Kit rendering for the approval request (approve/deny actions with `intent_id` + hex `entity_key` + `expires_at_ms` in the action value), the expired notice, the decided edit, and the expired edit; enforce the verified action-value length bound with an actionable error.
- [x] 3.2 `examples/slack_approval/surface.py`: the consume loop over `IntentSource` — filter `kind == APPROVAL`, consume-time expiry check via `hitl.intent_expired`, post through the gateway, commit strictly after post, in-process posted-`intent_id` map with bounded TTL eviction.
- [x] 3.3 Skip-and-commit path for non-approval kinds, treating `TOOL_KIND_UNSPECIFIED` as not-an-approval.

## 4. Decisions → envelopes

- [x] 4.1 Decision handler in `surface.py`: parse the action value, decision-time expiry re-check (independent of the pending map), build `AgentEnvelope(approval=...)` with `approver` and `decided_at_ms` from the interaction, serialize deterministically, publish under raw `entity_key` via `MessageSink`, then edit the message; refuse and edit on expired; answer already-decided interactions without publishing.
- [x] 4.2 The periodic expiry sweep over the pending map, driven by the injectable clock. *(`sweep_once()` is directly callable so tests advance the clock instead of sleeping; `run()` schedules it on `sweep_interval_ms`.)*

## 5. Slack gateway

- [x] 5.1 `examples/slack_approval/slack.py`: the `SlackGateway` protocol and `FakeSlackGateway` (records posts/edits, scripts decisions, injectable clock). *(The fake takes each scripted click's timestamp rather than holding a clock — the surface owns the clock; see Revision R4.)*
- [x] 5.2 `SocketModeGateway` over `slack-sdk` (`SocketModeClient` + `WebClient`), lazy import in the constructor, interaction ack ordering per the verified Socket Mode contract (design Open Questions), `block_actions` → `Decision` parsing. *(Written and typed; **not exercised against a real workspace — blocked: needs Slack**. Every surface behavior is covered through the fake.)*
- [x] 5.3 Resolve the design Open Questions against current Slack documentation (action-value bound, ack semantics, ephemeral-reply context) and record the answers as constants/comments in `slack.py`/`blocks.py`. *(Verified 2026-07-30 against docs.slack.dev: button `value` ≤ 2000 chars, `text` ≤ 75, `action_id` ≤ 255, 50 blocks/message; Socket Mode requires echoing `envelope_id` or Slack retries — no published deadline, so the gateway acks before any `chat.update`; `block_actions` carries `container.channel_id` + `user.id`, enough for `chat.postEphemeral`, kept best-effort.)*

## 6. Demo agent and entry point

- [x] 6.1 `examples/slack_approval/agent.py`: the FakeLLM demo agent — requests approval with a demo TTL, suspends, and on resume emits distinct approved/denied outcomes; plus the demo pipeline wiring (`RunAgent` + `WriteIntents`). The only example module that imports Beam. *(Demo TTL settled at 10 min with a 30 s sweep, per design Open Questions.)*
- [x] 6.2 `examples/slack_approval/__main__.py`: build config from env/args, construct source/sink via `build_intent_source`/`build_message_sink`, run the surface with graceful shutdown (stop consuming, drain in-flight posts, close gateway and sink).

## 7. Documentation

- [x] 7.1 `docs/examples/slack-approval.md` under the C24 docs surface: architecture sketch (channel → surface → approvals topic → resume), Slack app setup (manifest, scopes, Socket Mode app token, why not HTTP webhook — including what an HTTP deployment must add: signature verification and its replay window), the compose-Kafka walkthrough the doc-contract test enforces, and the stated residuals (duplicate message after restart, first-verdict-wins arbitration by the pipeline).
- [x] 7.2 Cross-link from `docs/effector.md`'s approval-routing section to the example.

## 8. Gates

- [x] 8.1 `make lint` and `make type` clean, with `examples/slack_approval/` held to the same ruff + `mypy --strict` bar as `src/`. *(`files = ["src", "tests", "examples"]`; the one relaxation is the Beam-untyped-API set on the demo pipeline module, as every other Beam-driving module gets.)*
- [x] 8.2 `make test-unit` green offline (no docker, no `slack-sdk`), confirming the offline doc-contract leg rides the default tier. *(966 passed, 2 skipped — both `importorskip("aiokafka")` integration modules.)*
- [ ] 8.3 `make test-integration` green with compose up, covering the Redpanda closed-loop leg. *(**Blocked: needs docker.**)*
- [x] 8.4 Coverage ratchet: `make coverage-ratchet` non-regressing (examples stay outside the package coverage measurement). *(branch coverage 94.84%, at baseline.)*
- [ ] 8.5 `uv run pre-commit run --all-files` clean. *(**Blocked: the `precommit` group is not installed here** and hook installation needs network. Its hooks are covered directly: ruff, ruff-format, and `mypy --strict` all run clean; TOML/YAML parse, final-newline, and trailing-whitespace were checked over every touched file; no proto changed, so the drift hook is a no-op.)*
- [x] 8.6 `openspec validate add-slack-approval-example --strict` passes.

## Revision

Recorded during implementation; no spec requirement or scenario changed.

**R1 — the compose-Kafka leg produces the intent with a plain keyed producer, not `WriteIntents`.** Task 1.6 said "stage via `WriteIntents` to a real channel topic". The DirectRunner cross-language Kafka *write* is blocked by two stacked upstream Beam defects, root-caused and non-strict-xfailed in `tests/actions/test_write_intents_integration.py`; building the gate on it would make it xfail for a reason that has nothing to do with this example. The test instead produces the demo activation's intent bytes with an `aiokafka` producer keyed by the raw `entity_key` — exactly how `WriteIntents` keys the outbox — the same substitution `tests/effector/test_service_integration.py` already makes and documents. The spec's scenario ("the approval intent staged by the FakeLLM demo agent is consumed from a real topic") is unaffected: the offline leg proves those bytes are byte-identical to what the demo pipeline commits.

**R2 — C24's examples layout had not landed, so this change settled one.** Proposal and design both say paths follow sibling `add-docs-site` (C24) and that only file locations, not behavior, depend on it. C24 is not merged, so the layout implied by the design is the one implemented: source under `examples/slack_approval/`, narrative under `docs/examples/slack-approval.md`, doc-contract tests under `tests/examples/` reaching the example through a repo-root `sys.path` fixture in `tests/examples/conftest.py`. If C24 lands a different mechanism, only these paths move.

**R3 — the `examples` dependency group (task 2.2) was not added.** Adding it to `[dependency-groups]` requires regenerating `uv.lock`, which this change may not touch. Nothing depends on it: `slack-sdk` is imported lazily inside `SocketModeGateway.__init__`, so the example imports and its entire offline suite runs without the package, and the doc page tells a real-workspace user to `uv pip install slack-sdk`. The task stays open for whoever next updates the lockfile; it is packaging convenience, not a spec requirement (the capability requires that `slack-sdk` be confined to a dev dependency group — it is confined to *no* dependency group today, which is strictly stronger for the offline lanes).

**R4 — the injectable clock lives on `ApprovalSurface`, not on `SurfaceConfig` or the fake gateway.** Tasks 2.4 and 5.1 sketched it on both. A callable is not configuration: `SurfaceConfig` is a frozen, env-derived, eagerly-validated value mirroring `EffectorConfig`, and putting a function on it would make it unserializable and unvalidatable. One clock (`time_fn` on the surface) is also the correct number: expiry is decided in exactly one place, and the fake gateway takes each scripted click's `decided_at_ms` explicitly, which is what a real interaction payload carries anyway. The spec's "against an injectable clock … never by sleeping" is satisfied.

## 10. Revision: generalize the example-page docs contract for package-shaped examples (integration)

- [x] 10.1 `tests/examples/test_docs_snippets.py` (owned by `add-docs-site`, C24) assumed one flat
  module per example page: its `--8<--` regex matched only `examples/<module>.py`, and both
  direction checks compared bare file names. This example is the first package-shaped one
  (`examples/slack_approval/`), so the two changes were mutually unsatisfiable as written —
  C24's unit lane failed on this page. Generalized rather than exempted: the directive regex now
  also accepts `examples/<package>/<module>.py`; the page↔example agreement check compares the
  *example owner* (flat stem, or package directory name) instead of the file name; and the page
  now renders `examples/slack_approval/surface.py` verbatim, so this example is held to the same
  "the code the site shows is the code CI runs" contract as the other three. The
  imports-nothing-from-tests scan was widened from `glob` to `rglob` so package modules are
  covered too (strengthening, not relaxing). No spec requirement or scenario changed in either
  change. Verified: `pytest tests/examples` 43 passed, 1 skipped; `make lint`, `make type` clean.
