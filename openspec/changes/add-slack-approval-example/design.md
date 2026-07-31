## Context

The approval loop's runtime half is complete. `ActivationContext.request_approval` ([context.py:559](../../../src/beam_agents/core/context.py:559)) stages a `ToolIntent` with `kind = APPROVAL`, `tool_name` = the approval channel ([`DEFAULT_APPROVAL_CHANNEL = "approval"`](../../../src/beam_agents/hitl.py:49)), and a mandatory positive `expires_at_ms`; the DoFn suspends with `deadline_ms = min(timeout, earliest intent expiry)` and a fail-closed default route ([`deny`](../../../src/beam_agents/hitl.py:130), [`DEFAULT_HITL_TIMEOUT_MS`](../../../src/beam_agents/hitl.py:40) = 24h). The effector consumes the outbox and [routes approval-kind intents](../../../src/beam_agents/effector/service.py:360) — serialized verbatim, keyed by `entity_key` — to its configured `approvals_to` channel, publishing the notification *before* marking it terminal, so the channel's consumer must tolerate duplicates. The resume half is `AgentEnvelope.Approval` ([beam_agents.proto:132](../../../protos/beam_agents.proto:132)): re-injected on the same key it either resumes the suspended activation or is orphaned by `_resume`'s live-and-unexpired admission check.

What is missing is a worked consumer of that channel. This change builds it as an *example*, not a runtime feature: a Slack surface in the examples surface being established by sibling change `add-docs-site` (C24), held to its documentation by a doc-contract test in `tests/examples/` — the pattern [`test_failure_streak_alarm.py`](../../../tests/examples/test_failure_streak_alarm.py:1) already sets for `docs/errors.md`.

Constraints inherited from the project:

- Unit tests pass offline with no docker and no optional dependencies; FakeLLM is the only model in tests.
- The example must not enter the wheel, the public API, or the pipeline process; like the effector, it imports no Beam in its service modules (the demo *pipeline* wiring in `agent.py` is the one file that does, and it is not imported by the service).
- Everything the surface publishes must respect the runtime's determinism and keying rules: envelopes keyed by raw `entity_key`, deterministic proto serialization, no invented IDs.

## Goals / Non-Goals

**Goals:**

- A runnable, honest reference for fronting the approval channel: consume approval-kind intents, post one interactive Slack message each, publish one `AgentEnvelope.Approval` per human verdict, keyed so the suspended activation resumes.
- Fail-closed TTL behavior at the surface: expired intents are visibly marked and never actionable, and a verdict that races expiry publishes nothing.
- The full loop demonstrable two ways: offline (FakeLLM + in-memory transports + fake Slack gateway, no docker) and against compose Redpanda with real topics (the doc-contract test).
- Teach the contract in code: `entity_key` keying, duplicate-notification tolerance, first-verdict-wins arbitration by the pipeline, deterministic serialization.

**Non-Goals:**

- Not a supported runtime surface. Nothing here enters `src/beam_agents/` or `beam_agents/__init__.py`; no compatibility promise beyond the example's own tests.
- No approval persistence or audit store. The Slack channel history is the demo's audit trail; a production surface would add its own store.
- No multi-workspace Slack app, OAuth distribution flow, or HTTP Events API implementation (documented, not built — see D4).
- No exactly-once posting to Slack. The effector's channel is at-least-once by design; the surface collapses duplicates best-effort in process memory and the docs state the residual (a duplicate message after a surface restart, harmless because both messages resolve to the same `intent_id` and the pipeline admits one verdict).
- No changes to the effector, HITL machinery, protos, or `WriteIntents`.

## Decisions

### D1. The example lives in `examples/slack_approval/`, outside the package

The surface is deliberately *not* `src/beam_agents/slack/`. It is sample code demonstrating a contract, and shipping it in the wheel would create a supported API out of a demo, drag `slack-sdk` toward the dependency tree, and blur the module map ("no hosted effector" generalizes: the runtime does not own approval surfaces). It lands in the examples surface established by sibling change C24 (`add-docs-site`): source under `examples/slack_approval/`, narrative under `docs/examples/slack-approval.md`, doc-contract tests under `tests/examples/`. Tests reach the example through a small path fixture in `tests/examples/conftest.py` (the example is not installed); if C24 settles a different layout or import mechanism, this change adopts it — the decision here is "examples surface, not package", not a specific path.

`slack-sdk` goes in a new `examples` dependency group (dev-side, like `docs`), never in `[project.optional-dependencies]`: extras are for installable package features (effector, langgraph, otlp); this is not one.

*Alternative rejected:* a separate installable example package. Heavier than the thing it demonstrates, and C24's examples surface exists precisely so examples do not need packaging.

### D2. Reuse the effector's transport seams; do not write a standalone consumer

The surface consumes intents through the effector's [`IntentSource`](../../../src/beam_agents/effector/sources.py:44) protocol and publishes envelopes through its [`MessageSink`](../../../src/beam_agents/effector/sinks.py:26) protocol, constructed via [`build_intent_source`](../../../src/beam_agents/effector/sources.py:269) / [`build_message_sink`](../../../src/beam_agents/effector/sinks.py:174). The fit is exact, not approximate:

- The channel carries serialized `ToolIntent`s keyed by `entity_key` — precisely what `IntentSource` yields as `DeliveredIntent`s, with per-key order inherited from the transport (Kafka consumer group / Pub/Sub ordered subscription) and explicit commit-after-processing, which is the crash-safety shape the surface needs (post before commit).
- The envelope must be raw bytes under the raw `entity_key` — precisely `MessageSink.publish(key, payload)`, with the Kafka key / Pub/Sub hex ordering-key conventions already matching `WriteIntents` on the input side.
- Both protocols ship in-memory implementations ([`InMemoryIntentSource`](../../../src/beam_agents/effector/sources.py:59), [`InMemoryMessageSink`](../../../src/beam_agents/effector/sinks.py:44)) that make the offline lane free.

A standalone consumer would re-implement all of that — lazy client imports, commit plumbing, ordering, fakes — to end up with a worse copy, and would silently drift from the conventions the rest of the loop enforces. The costs of reuse are acceptable and stated: the example imports `beam_agents.effector` (a documented, importable-without-Beam-or-clients package, even though it is outside the `__init__.py` public API) and needs the `effector` extra for real transports (which any deployment running this loop has installed anyway, since the effector is what feeds the channel). Kafka and Pub/Sub both work for free; the demo and doc-contract test use Kafka because compose provides Redpanda.

*Alternative rejected:* a minimal `aiokafka`-only consumer inside the example. Smaller at first glance, but it loses Pub/Sub parity, loses the in-memory fakes (so the offline doc-contract leg would need its own), and teaches adopters to bypass the seams the project maintains.

### D3. The surface filters on `kind == APPROVAL`; both channel wirings work

Primary wiring: point the surface at the effector's `approvals_to` channel ([config.py:117](../../../src/beam_agents/effector/config.py:117)), where every message is already an approval-kind intent. The surface still checks `intent.kind == ToolIntent.APPROVAL` and skips-and-commits anything else, which buys a second wiring for the minimal demo: point the surface directly at the outbox topic and run no effector at all (sound when the demo agent stages only approval intents, as `examples/slack_approval/agent.py` does). The filter is cheap, makes misconfiguration inert instead of destructive (a TOOL intent can never become a Slack button that fabricates a verdict), and mirrors the effector's own treatment of `TOOL_KIND_UNSPECIFIED` as TOOL — here, as not-an-approval.

Duplicate deliveries (the effector publishes the notification before marking it terminal, per [`_route_approval`](../../../src/beam_agents/effector/service.py:360)) are collapsed by an in-process `intent_id` → posted-message map, bounded and TTL-evicted alongside the sweep state (D6). Across a surface restart the map is empty and a redelivered intent may post a second message; both messages carry the same `intent_id`, so whichever is answered first wins and the other is edited on decision or expiry. Documented as the residual, not engineered away — a durable posted-set is exactly the dedup-store machinery the effector already demonstrates, and duplicating it here would bloat the example past its teaching purpose.

### D4. Slack delivery mode: Socket Mode, with the HTTP alternative documented

The surface receives interactions over **Socket Mode** (`slack-sdk`'s `SocketModeClient` over an app-level `xapp-` token, with `WebClient` for `chat.postMessage` / `chat.update`). Rationale:

- **Local runnability is the whole point.** The doc-contract demo runs on a laptop against docker compose; Socket Mode needs no public HTTPS endpoint, no tunnel, no TLS termination.
- **Less security code to get wrong.** An HTTP interactivity endpoint must verify `X-Slack-Signature` (an HMAC-SHA256 over a versioned concatenation of the request timestamp and body using the app's signing secret, with a freshness window against replay). That is real code with real failure modes, and an example that implements it becomes the thing people copy — including its bugs. Socket Mode authenticates at connection time via the app token, and Slack delivers interactions over that authenticated socket, so there is no request-signature path in the example at all. The docs page states this trade explicitly and sketches what an HTTP-mode deployment must add (signature verification, the ack-within-Slack's-deadline rule, retry semantics), with the API-level details flagged in Open Questions rather than asserted from memory.
- **Bolt not used.** Plain `slack-sdk` keeps the event flow visible (an example should show the wiring, not hide it) and keeps the dependency single.

All Slack I/O sits behind a `SlackGateway` protocol — `post_approval(...) -> message_ref`, `update_message(message_ref, blocks)`, and an async iterator of `Decision(intent_id, entity_key_hex, approved, approver, decided_at_ms)` parsed from `block_actions` payloads. `slack-sdk` is imported lazily inside the real gateway's constructor (the same pattern as the effector's adapters), and `FakeSlackGateway` scripts posts, edits, and clicks in memory, so every behavior of the surface is testable offline with no Slack workspace and no `slack-sdk` installed.

### D5. Verdict → envelope: the pipeline stays the arbiter

On an in-time decision the surface builds `AgentEnvelope(entity_key=<raw bytes>, event_time_ms=decided_at_ms, approval=Approval(intent_id, approved, approver, decided_at_ms))`, serializes with `SerializeToString(deterministic=True)`, and publishes via `MessageSink` under the raw `entity_key` — landing on the same partition/ordering key as the rest of the key's traffic, which is what re-injection requires. `approver` is the Slack user id from the interaction payload; `decided_at_ms` comes from the payload's action timestamp so the envelope is a function of the interaction, not of when the surface got around to it.

The surface does **not** try to enforce at-most-one verdict globally. Racing clicks (two approvers, or a click racing a redelivered duplicate message) may publish two envelopes; `_resume` admits the first against the live continuation and orphans the rest to `.errors` — fail-closed arbitration the runtime already guarantees and the conformance suite already tests. The surface's job is only UX-level: after publishing, it edits the message to show the verdict and drops the intent from its pending map so later clicks on a stale message are answered with an "already decided" ephemeral rather than another envelope. Publish-then-edit order matters — the envelope is the effect, the edit is cosmetic; a crash between them re-posts nothing (the intent's offset was committed at post time) and leaves a decided-but-live-looking message whose next click is refused by the decided/expired checks.

### D6. TTL: expired means not actionable, checked at three points

The surface honors `expires_at_ms` with the runtime's own pure guard ([`intent_expired`](../../../src/beam_agents/hitl.py:63)) and an injectable clock (`time_fn`), so expiry is testable without sleeping:

1. **At consume time** — an intent already expired is posted as a non-interactive "expired before it could be surfaced" notice (no buttons), committed, and never enters the pending map. Posting a notice rather than silently dropping keeps the channel an honest log of what was requested.
2. **While pending** — a periodic sweep (interval configurable, default well under the demo's TTL) walks the pending map and edits any message whose expiry has passed: buttons removed, "expired" status shown. The layer-1 HITL timer is what actually resolves the suspension (deny by default); the sweep only keeps the UI from soliciting clicks that can no longer matter.
3. **At decision time** — every decision is re-checked against `expires_at_ms` before an envelope is built. A click that races expiry (or arrives for an intent the sweep hasn't reached, or lands after a surface restart rebuilt an empty pending map) publishes nothing and edits the message to expired. This is the load-bearing check: 1 and 2 are UX, 3 is the fail-closed gate, and it does not depend on the in-memory map — the expiry rides in the button's action value, so a decision on a message posted by a previous process is still checked.

Even if the surface got all three wrong, `_resume` refuses a past-deadline approval — the example demonstrates *cooperating with* fail-closed layers, not being the only one.

### D7. The doc-contract test: offline loop plus a compose-Kafka closed loop

Two legs, one contract (`docs/examples/slack-approval.md` describes exactly what the tests prove):

- **Offline leg** (default tier, no docker, no `slack-sdk`): a FakeLLM-driven activation is run under `TestPipeline` to stage a real approval intent (real `intent_id` derivation, real expiry stamping); the surface consumes it from an `InMemoryIntentSource`, posts to the `FakeSlackGateway`, a scripted click flows back, and the envelope lands in an `InMemoryMessageSink`. Assertions cover the Block Kit payload, keying, deterministic bytes, expiry behaviors, skip-non-approval, and duplicate collapse. Fed back into a `TestStream`-scripted pipeline, the published envelope resumes the suspended activation — provable in one pipeline run by scripting `[event, envelope]`, because the intent the surface answered was minted by the same deterministic derivation the in-pipeline activation re-mints (`intent_id_for(entity_key, seq, step_index)`), so the pre-computed envelope matches the pending intent at resume time.
- **Compose-Kafka leg** (`-m integration`): the loop crosses real topics on Redpanda. Phase 1 stages the intent via the demo pipeline and `WriteIntents` onto a real approval-channel topic; phase 2 runs the surface (real `KafkaIntentSource`/`KafkaMessageSink`, fake gateway scripted to approve) which publishes the envelope to a real approvals topic; phase 3 reads those bytes back off Kafka and drives them into the demo pipeline, asserting the suspended activation resumes with the approved verdict. The envelope that resumes the agent is byte-for-byte the one the surface put on the wire — that is the claim the example exists to make, and this leg is what "runs with FakeLLM + docker compose kafka locally" means.

The doc page and the test reference the same example modules (the test imports them; the doc links and excerpts them), so drift between the two is a test failure, same as `docs/errors.md`.

## Risks / Trade-offs

- **Slack API details drift.** Block Kit shapes, payload fields, and Socket Mode framing are Slack's to change. → All Slack specifics live in `slack.py`/`blocks.py` behind the gateway seam; the surface's correctness properties (keying, TTL, envelope bytes) are tested against the fake and hold regardless. A live-workspace smoke run is manual, documented in the doc page, and not a CI gate.
- **Duplicate Slack messages after a surface restart** (D3 residual). → Harmless to correctness (same `intent_id`; pipeline admits one verdict) and bounded by the channel's redelivery window; documented rather than solved with a durable store the example doesn't need.
- **In-memory pending map lost on restart.** → The decision-time expiry check (D6.3) rides on the button value, not the map, so fail-closed behavior survives restarts; only the sweep's cosmetic edits are lost until re-delivery or decision.
- **Reusing `beam_agents.effector` couples the example to a non-public surface.** → Accepted deliberately (D2): the example is in-repo and versioned with that surface; the doc states that out-of-tree copies should pin the version they copied from.
- **Button action value size.** Slack bounds action values; hex `entity_key` doubles the key's length. → The demo's keys are short; the surface raises an actionable error at post time when the composed value exceeds the documented bound (limit constant in `blocks.py`, verified during implementation — see Open Questions).
- **C24 lands with a different examples layout.** → Paths here follow C24; this change tracks whatever layout that sibling settles (proposal Impact states the same). Behavior, capability scope, and tests are unaffected.
- **A second `AgentEnvelope` producer must match the pipeline's decode expectations.** → The surface only ever serializes the generated proto bindings deterministically; the compose-Kafka leg decodes with the same bindings the pipeline uses, so an encoding mismatch fails the gate, not production.

## Migration Plan

None — additive example code, no schema change, no runtime change, no data. Rollout for a user: create the approval-channel and approvals topics (the compose demo script does this), create the Slack app (manifest included: `chat:write`, interactivity, Socket Mode app token), export the two tokens, run `examples/slack_approval` alongside the effector. Rollback: stop the surface; pending approvals simply time out through the layer-1 `HitlPolicy` route, which is the system's designed behavior when nobody answers.

## Open Questions

- **Slack signature-verification specifics for the documented HTTP alternative.** The doc will describe v0 signing (HMAC-SHA256 of a versioned `timestamp:body` string with the signing secret, compared against `X-Slack-Signature`, with a short freshness window) — exact header names, the version prefix, and the recommended window must be verified against current Slack docs at implementation time, not asserted from memory.
- **Block Kit limits.** The action `value` length bound (believed 2000 characters) and any `blocks`-per-message constraints the expired-notice/verdict edits must respect — verify against current docs; the constants live in one module either way.
- **Socket Mode acknowledgement semantics.** Whether an interaction must be acked on the socket within a deadline before `chat.update` is issued (and what Slack does on a missed ack) affects the gateway's internal ordering; verify against `slack-sdk`'s `SocketModeClient` contract during implementation.
- **Ephemeral "already decided/expired" replies.** `chat.postEphemeral` needs a channel + user; whether the interaction payload always carries enough context for it in a multi-channel install is unverified — the fallback is editing the original message only.
- **Demo TTL defaults.** The demo wants a TTL short enough to demonstrate expiry live but long enough for a human demo (candidate: 10 minutes, sweep every 30 s, versus [`DEFAULT_INTENT_TTL_MS`](../../../src/beam_agents/hitl.py:45) = 1h). To be settled when the doc walkthrough is written.
