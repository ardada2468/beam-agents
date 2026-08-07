# Example: a Slack approval surface

The approval loop's runtime half ships with beam-agents: an activation calls
`ctx.request_approval(...)`, suspends with a fail-closed deadline, and its
`kind = APPROVAL` intent lands on the outbox; the effector routes it verbatim
to `--approvals-to` and never executes it. What closes the loop is an
`AgentEnvelope.Approval` arriving back on the pipeline's approvals topic, on
the same key.

Nothing in the runtime fronts that channel — approval surfaces are yours, not
the runtime's. This example is a worked one, so the contract is code you can
read rather than a paragraph you re-derive.

<figure class="diagram" markdown="1">
<div class="diagram-scroll">
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 720 252" role="img" aria-labelledby="slack-t slack-d">
<title id="slack-t">The approval round trip through a Slack surface</title>
<desc id="slack-d">A cycle that starts and ends at RunAgent. The activation calls request_approval, suspends, and its approval intent leaves the graph on dot intents to an outbox topic, where the effector routes it verbatim to the approval channel without executing it. Everything from there is the reader's own code: a Slack approval surface posts the intent to a channel, a human decides, and the verdict returns to the surface, which publishes one AgentEnvelope.Approval to the approvals topic under the same key. That topic is read back into the pipeline — the only solid edge in the diagram — and the suspended activation resumes on the same key. If no verdict arrives before the deadline, the runtime denies.</desc>
<defs>
<marker id="slack-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,1 L7,4 L0,7 z" fill="var(--rule-2, #8e8e87)"/></marker>
<marker id="slack-a-int" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,1 L7,4 L0,7 z" fill="var(--s-intents, #8a5205)"/></marker>
</defs>
<rect x="448" y="26" width="258" height="170" rx="2" fill="none" stroke="var(--rule-2, #8e8e87)" stroke-width="1" stroke-dasharray="4 3"/>
<g fill="none" stroke="var(--s-intents, #8a5205)" stroke-width="1.25" stroke-dasharray="4 3">
<path d="M94,118 V77 H198" marker-end="url(#slack-a-int)"/>
<path d="M304,77 H338" marker-end="url(#slack-a-int)"/>
<path d="M424,77 H462" marker-end="url(#slack-a-int)"/>
</g>
<g fill="none" stroke="var(--rule-2, #8e8e87)" stroke-width="1.25" stroke-dasharray="4 3">
<path d="M540,98 V128" marker-end="url(#slack-a)"/>
<path d="M606,144 H628" marker-end="url(#slack-a)"/>
<path d="M660,130 V100" marker-end="url(#slack-a)"/>
<path d="M478,98 V218 H438" marker-end="url(#slack-a)"/>
</g>
<path d="M298,218 H16 V154 H26" fill="none" stroke="var(--rule-2, #8e8e87)" stroke-width="1.25" marker-end="url(#slack-a)"/>
<g fill="var(--paper, #ffffff)" stroke="var(--ink, #0b0c0e)" stroke-width="1.25">
<rect x="28" y="118" width="132" height="72" rx="2"/>
<rect x="300" y="204" width="136" height="28" rx="2"/>
</g>
<g fill="var(--paper-2, #f6f6f4)" stroke="var(--rule-2, #8e8e87)" stroke-width="1">
<rect x="200" y="63" width="104" height="28" rx="2"/>
<rect x="340" y="63" width="84" height="28" rx="2"/>
<rect x="464" y="56" width="226" height="42" rx="2"/>
<rect x="506" y="130" width="100" height="28" rx="2"/>
<rect x="630" y="130" width="60" height="28" rx="2"/>
</g>
<g font-family="'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, monospace" font-size="11" fill="var(--ink-2, #4a4e54)" text-anchor="middle">
<text x="252" y="81">outbox topic</text>
<text x="382" y="81">effector</text>
<text x="577" y="73">Slack approval surface</text>
<text x="556" y="148">#approvals</text>
<text x="660" y="148">human</text>
<text x="368" y="222">approvals topic</text>
</g>
<text x="94" y="150" text-anchor="middle" font-family="'Instrument Sans', ui-sans-serif, system-ui, sans-serif" font-size="14" font-weight="600" fill="var(--ink, #0b0c0e)">RunAgent</text>
<text x="94" y="168" text-anchor="middle" font-family="'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, monospace" font-size="9.5" letter-spacing="0.06" fill="var(--ink-3, #63676d)">suspended · same key</text>
<text x="102" y="98" font-family="'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, monospace" font-size="11" fill="var(--s-intents, #8a5205)">.intents</text>
<g font-family="'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, monospace" font-size="9.5" letter-spacing="0.06" fill="var(--ink-3, #63676d)">
<text x="577" y="91" text-anchor="middle">examples/slack_approval</text>
<text x="382" y="104" text-anchor="middle">routes it verbatim</text>
<text x="462" y="44">your code, not the runtime's</text>
<text x="548" y="118">post</text>
<text x="648" y="118" text-anchor="end">verdict</text>
<text x="486" y="180">publishes AgentEnvelope.Approval</text>
<text x="176" y="148">no verdict before the deadline</text>
<text x="176" y="162">resolves the suspension as denied</text>
<text x="157" y="208" text-anchor="middle">resumes on the same key</text>
</g>
</svg>
</div>
<figcaption markdown="1">
Everything inside the dashed region is code you write; the runtime's half is the
amber leg, `.intents` to the outbox and an effector that routes the request
without executing it. The verdict returns as one `AgentEnvelope.Approval` on the
approvals topic — the only hop here that is an edge in the Beam graph, and the
re-injection that resumes the suspended activation on the same key.
</figcaption>
</figure>

Source: `examples/slack_approval/`. Sample code — outside the wheel, outside
the public API, no compatibility promise beyond its own tests. Copy it into
your own service and pin the beam-agents version you copied from.

## What the surface does

| Module | Role |
|---|---|
| `surface.py` | the consume → post → decide → publish loop; no Beam, no slack-sdk |
| `slack.py` | the `SlackGateway` seam: protocol, `FakeSlackGateway`, `SocketModeGateway` |
| `blocks.py` | Block Kit rendering and the button action value |
| `config.py` | URIs, channel, tokens, sweep interval — validated eagerly, import-free |
| `agent.py` | the FakeLLM demo agent and its pipeline wiring (the one module importing Beam) |
| `__main__.py` | `python -m examples.slack_approval` — real transports, graceful shutdown |

**Consume → post.** Intents arrive through the effector's `IntentSource` seam
(`kafka://`, `pubsub://`, or in-memory), so the example inherits per-key order,
explicit commits, and the offline fakes instead of re-implementing them. Each
live `kind == APPROVAL` intent becomes exactly one Block Kit message with
approve/deny buttons; anything else is skipped and committed, so pointing the
surface at the wrong topic is inert rather than destructive. **The delivery is
committed strictly after the post succeeds** — a crash before posting
re-delivers rather than loses.

**Decide → publish.** A verdict becomes one `AgentEnvelope` carrying
`Approval(intent_id, approved, approver, decided_at_ms)`, serialized with
`SerializeToString(deterministic=True)` and published under the **raw
`entity_key`** — the same keying `WriteIntents` and the effector's result sink
use, so it lands on the suspended key's partition. `approver` is the Slack user
id; `decided_at_ms` is the interaction's own timestamp, so the envelope is a
function of the click and not of when the surface got around to it. Publish
first, edit the message after: the envelope is the effect, the edit is
cosmetic.

**The pipeline stays the arbiter.** The surface does not try to enforce
at-most-one verdict globally. Racing clicks may publish two envelopes; the
runtime's resume admission takes the first against the live continuation and
orphans the rest to `.errors`. All the surface does is stop *itself* from
re-publishing and answer later clicks as already decided.

## TTL: expired means not actionable

Expiry uses the runtime's own guard, `hitl.intent_expired`, against an
injectable clock — the same function the effector calls, with the same reading
that a non-positive `expires_at_ms` is **expired**, never unbounded. It is
applied at three points:

1. **At consume time** — an already-expired intent is posted as a
   non-interactive "expired before it could be surfaced" notice and committed.
   Posting rather than dropping keeps the channel an honest log of what was
   requested.
2. **While pending** — a periodic sweep (default 30 s) edits any message whose
   expiry passed: buttons removed, expired status shown. Cosmetic only; the
   layer-1 HITL timer is what actually resolves the suspension (deny by
   default).
3. **At decision time** — every interaction is re-checked against the
   `expires_at_ms` carried *in the button's action value* before an envelope is
   built. This is the load-bearing check, and it deliberately does not read
   in-process state: a click on a message posted by a previous process is still
   refused correctly.

Even a surface that got all three wrong is backstopped — `_resume` refuses a
past-deadline approval. The example demonstrates *cooperating with* fail-closed
layers, not being the only one.

## Slack app setup (Socket Mode)

Socket Mode, not an HTTP Events endpoint: it needs no public HTTPS URL, so the
demo runs on a laptop against docker compose, and there is no request-signature
code in the example to copy wrong (see the trade-off below).

Create an app from this manifest, install it to your workspace, and invite the
bot to the channel:

```yaml
display_information:
  name: beam-agents approvals
features:
  bot_user:
    display_name: beam-agents
settings:
  interactivity:
    is_enabled: true
  socket_mode_enabled: true
oauth_config:
  scopes:
    bot:
      - chat:write
```

Then export the two tokens:

```sh
export SLACK_BOT_TOKEN=xoxb-...   # bot token, chat:write
export SLACK_APP_TOKEN=xapp-...   # app-level token, connections:write (Socket Mode)
```

Interactions arrive over the app-token-authenticated WebSocket. Slack requires
each envelope to be acknowledged by echoing its `envelope_id` "so that Slack
knows whether to retry", so `SocketModeGateway` acks **first**, before any
`chat.update`; a redelivered interaction surfaces as a duplicate decision,
which the surface collapses (first verdict wins).

### If you deploy the HTTP alternative instead

Not implemented here, but stated honestly, because a production surface may
need it. An HTTP interactivity endpoint must additionally:

- **Verify the request signature.** Slack signs each request with your app's
  signing secret: an HMAC-SHA256 over a versioned concatenation of the request
  timestamp and the raw body, compared in constant time against the
  `X-Slack-Signature` header, with a freshness window on `X-Slack-Request-
  Timestamp` to bound replay. Check the current Slack documentation for the
  exact version prefix, header names, and recommended window — do not copy
  these details from memory, including this paragraph's.
- **Acknowledge within Slack's deadline**, before doing the publish, and handle
  Slack's retries of anything it considers unacked (the same duplicate-decision
  tolerance the surface already has).
- **Terminate TLS on a public endpoint** and keep the raw body around for
  signature verification (frameworks that parse the body first make this
  subtly wrong).

## Running the compose demo

```sh
make compose-up   # Redpanda on localhost:19092

python -m examples.slack_approval \
  --intents-from  kafka://localhost:19092/approval-requests \
  --approvals-to  kafka://localhost:19092/approvals \
  --slack-channel '#approvals'
```

Point `--intents-from` at the effector's `--approvals-to` channel (the primary
wiring). For a minimal demo you can point it directly at the outbox and run no
effector at all — sound because the demo agent stages only approval intents,
and the surface's kind filter makes any stray intent inert.

The demo agent (`agent.py`) guards a pretend refund: it requests approval with
a 10-minute TTL and suspends; approved it emits `refund-issued`, denied — or
timed out — it emits `refund-declined` and the guarded action never happens.
FakeLLM is the model, so no provider credentials are involved anywhere.

`slack-sdk` is only needed to run against a real workspace:

```sh
uv pip install slack-sdk
```

The offline tests never need it: `SocketModeGateway` imports it lazily in its
constructor, exactly like the effector's transport adapters.

## The surface, in full

`ApprovalSurface` is the whole out-of-pipeline loop: consume an approval
intent, post it, take the verdict, publish the envelope back. This is the
module itself, rendered from source — the code below is the code the tests
below execute.

```python title="examples/slack_approval/surface.py"
--8<-- "examples/slack_approval/surface.py"
```

## What the tests hold this page to

`tests/examples/test_slack_approval.py` imports the same modules this page
describes, so a change that invalidates the documented demo fails the tests.

- **Offline leg** (`make test-unit`; no docker, no workspace, no slack-sdk):
  the FakeLLM demo activation stages a real intent under `TestPipeline`, and
  the surface consumes it from `InMemoryIntentSource`, posts to
  `FakeSlackGateway`, takes a scripted click, and publishes into
  `InMemoryMessageSink`. Asserted: the Block Kit action values, raw-`entity_key`
  keying, byte-exact envelope serialization, commit-after-post, skip-and-commit
  for non-approval kinds, within-process duplicate collapse, all three TTL
  points (including the post-restart click), and — fed back through a
  `TestStream`-scripted pipeline — that the published envelope **resumes the
  suspended activation** on both the approve and the deny path.
- **Compose-Kafka leg** (`tests/examples/test_slack_approval_kafka.py`,
  `-m integration`): the same loop over real Redpanda topics, with the envelope
  read back off the wire and re-injected byte-for-byte.

## Residuals (stated, not engineered away)

- **A duplicate message after a surface restart.** The channel is at-least-once
  by design (the effector publishes the approval notification *before* marking
  it terminal). Within one process the surface collapses redeliveries by
  `intent_id`; across a restart its map is empty, so a redelivered intent may
  post a second message. Harmless: both carry the same `intent_id`, whichever
  is answered first wins, and the pipeline admits at most one verdict. A
  durable posted-set would be the effector's dedup machinery again — more than
  this example is for.
- **The pending map is lost on restart.** Only the sweep's cosmetic edits are
  affected; the decision-time expiry check rides on the button value, so
  fail-closed behavior survives.
- **No approval audit store.** The Slack channel history is the demo's audit
  trail. A production surface would add its own.
- **No exactly-once posting to Slack**, and no at-most-one-verdict enforcement
  at the surface — see "the pipeline stays the arbiter" above.
