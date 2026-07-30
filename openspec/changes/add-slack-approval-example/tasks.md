## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 `tests/examples/test_slack_approval.py` — consume/post tests against fakes (spec: "An approval intent becomes a Block Kit message and is then committed"; "A crash before posting loses nothing"; "Non-approval intents are skipped"; "A redelivered intent does not double-post within a process"), using a FakeLLM-driven `TestPipeline` activation to stage the real intent and `InMemoryIntentSource` to deliver it.
- [ ] 1.2 Decision tests (spec: "Approve click publishes a keyed approval envelope"; "A second click on a decided intent publishes nothing"; "Deny click publishes approved=false and the agent takes the denied path"), asserting deterministic envelope bytes and raw-`entity_key` keying into `InMemoryMessageSink`.
- [ ] 1.3 Resume tests (spec: "The published envelope resumes the suspended activation"): feed the surface-published envelope into a `TestStream`-scripted demo pipeline as `[event, envelope]` and assert the main output carries the verdict with nothing on `.errors`; cover approve and deny.
- [ ] 1.4 TTL tests with the injectable clock, no sleeps (spec: "An already-expired intent is surfaced without buttons" including non-positive `expires_at_ms`; "Expiry while pending removes the buttons"; "A click racing expiry publishes nothing" including the post-restart empty-pending-map case).
- [ ] 1.5 Boundary tests (spec: "The offline loop runs with fakes only"; "The service modules import without Beam"): import the service modules with `apache_beam` and `slack_sdk` blocked from `sys.modules`; assert `beam_agents.__all__` gains nothing.
- [ ] 1.6 `tests/examples/test_slack_approval_kafka.py` (`-m integration`) — the compose-Redpanda closed loop (spec: "The closed loop over Redpanda resumes the agent"; "The example tracks its documentation"): stage via `WriteIntents` to a real channel topic, run the surface with real Kafka source/sink and the fake gateway scripted to approve, read the envelope bytes back, re-inject, assert resume.

## 2. Example scaffolding and dependencies

- [ ] 2.1 Create `examples/slack_approval/` following the examples-surface layout established by sibling change `add-docs-site` (C24); adjust paths to C24's final layout if it differs.
- [ ] 2.2 Add an `examples` dependency group to `pyproject.toml` carrying `slack-sdk`; confirm it is outside the wheel, outside `[project.optional-dependencies]`, and unused by the offline `ci` lanes.
- [ ] 2.3 Add the `tests/examples/conftest.py` path fixture (or C24's equivalent import mechanism) so doc-contract tests import the example modules without installing them.
- [ ] 2.4 `examples/slack_approval/config.py`: frozen config (intents-channel URI, approvals-topic URI, Slack channel, tokens from env, sweep interval, clock) with eager import-free validation mirroring `EffectorConfig`, reusing the effector's transport-URI grammar.

## 3. Surface service: consume → post

- [ ] 3.1 `examples/slack_approval/blocks.py`: Block Kit rendering for the approval request (approve/deny actions with `intent_id` + hex `entity_key` + `expires_at_ms` in the action value), the expired notice, the decided edit, and the expired edit; enforce the verified action-value length bound with an actionable error.
- [ ] 3.2 `examples/slack_approval/surface.py`: the consume loop over `IntentSource` — filter `kind == APPROVAL`, consume-time expiry check via `hitl.intent_expired`, post through the gateway, commit strictly after post, in-process posted-`intent_id` map with bounded TTL eviction.
- [ ] 3.3 Skip-and-commit path for non-approval kinds, treating `TOOL_KIND_UNSPECIFIED` as not-an-approval.

## 4. Decisions → envelopes

- [ ] 4.1 Decision handler in `surface.py`: parse the action value, decision-time expiry re-check (independent of the pending map), build `AgentEnvelope(approval=...)` with `approver` and `decided_at_ms` from the interaction, serialize deterministically, publish under raw `entity_key` via `MessageSink`, then edit the message; refuse and edit on expired; answer already-decided interactions without publishing.
- [ ] 4.2 The periodic expiry sweep over the pending map, driven by the injectable clock.

## 5. Slack gateway

- [ ] 5.1 `examples/slack_approval/slack.py`: the `SlackGateway` protocol and `FakeSlackGateway` (records posts/edits, scripts decisions, injectable clock).
- [ ] 5.2 `SocketModeGateway` over `slack-sdk` (`SocketModeClient` + `WebClient`), lazy import in the constructor, interaction ack ordering per the verified Socket Mode contract (design Open Questions), `block_actions` → `Decision` parsing.
- [ ] 5.3 Resolve the design Open Questions against current Slack documentation (action-value bound, ack semantics, ephemeral-reply context) and record the answers as constants/comments in `slack.py`/`blocks.py`.

## 6. Demo agent and entry point

- [ ] 6.1 `examples/slack_approval/agent.py`: the FakeLLM demo agent — requests approval with a demo TTL, suspends, and on resume emits distinct approved/denied outcomes; plus the demo pipeline wiring (`RunAgent` + `WriteIntents`). The only example module that imports Beam.
- [ ] 6.2 `examples/slack_approval/__main__.py`: build config from env/args, construct source/sink via `build_intent_source`/`build_message_sink`, run the surface with graceful shutdown (stop consuming, drain in-flight posts, close gateway and sink).

## 7. Documentation

- [ ] 7.1 `docs/examples/slack-approval.md` under the C24 docs surface: architecture sketch (channel → surface → approvals topic → resume), Slack app setup (manifest, scopes, Socket Mode app token, why not HTTP webhook — including what an HTTP deployment must add: signature verification and its replay window), the compose-Kafka walkthrough the doc-contract test enforces, and the stated residuals (duplicate message after restart, first-verdict-wins arbitration by the pipeline).
- [ ] 7.2 Cross-link from `docs/effector.md`'s approval-routing section to the example.

## 8. Gates

- [ ] 8.1 `make lint` and `make type` clean, with `examples/slack_approval/` held to the same ruff + `mypy --strict` bar as `src/`.
- [ ] 8.2 `make test-unit` green offline (no docker, no `slack-sdk`), confirming the offline doc-contract leg rides the default tier.
- [ ] 8.3 `make test-integration` green with compose up, covering the Redpanda closed-loop leg.
- [ ] 8.4 Coverage ratchet: `make coverage-ratchet` non-regressing (examples stay outside the package coverage measurement).
- [ ] 8.5 `uv run pre-commit run --all-files` clean.
- [ ] 8.6 `openspec validate add-slack-approval-example --strict` passes.
