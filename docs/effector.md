# Running the reference effector

The effector is the external service that closes the effects loop:

<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 720 288" width="100%" role="img" aria-labelledby="eff-t eff-d">
<title id="eff-t">The effects loop closed by the effector</title>
<desc id="eff-d">RunAgent stages tool intents but never executes them. Its dot intents output leaves the Beam graph as a dashed line into an outbox topic. The effector consumes that topic in key order and executes each intent exactly once per deterministic intent id, following the phase order verify, refuse-expired, claim, execute, complete, publish, commit. It publishes one ToolResult to a results topic, which re-enters RunAgent on the same entity key. The outbox topic, the effector and the results topic all sit outside the pipeline.</desc>
<defs>
<marker id="eff-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,1 L7,4 L0,7 z" fill="var(--rule-2, #8e8e87)"/></marker>
<marker id="eff-a-int" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,1 L7,4 L0,7 z" fill="var(--s-intents, #8a5205)"/></marker>
</defs>
<rect x="20" y="140" width="680" height="114" rx="2" fill="none" stroke="var(--rule, #e2e2dd)" stroke-width="1"/>
<g fill="var(--paper, #ffffff)" stroke="var(--ink, #0b0c0e)" stroke-width="1.25">
<rect x="276" y="40" width="168" height="58" rx="2"/>
</g>
<g fill="var(--paper-2, #f6f6f4)" stroke="var(--rule-2, #8e8e87)" stroke-width="1">
<rect x="272" y="170" width="176" height="56" rx="2"/>
<rect x="48" y="184" width="126" height="28" rx="2"/>
<rect x="546" y="184" width="126" height="28" rx="2"/>
</g>
<g fill="none" stroke-width="1.25">
<path d="M276,69 H111 V182" stroke="var(--s-intents, #8a5205)" stroke-dasharray="4 3" marker-end="url(#eff-a-int)"/>
<path d="M174,198 H270" stroke="var(--s-intents, #8a5205)" stroke-dasharray="4 3" marker-end="url(#eff-a-int)"/>
<path d="M448,198 H544" stroke="var(--rule-2, #8e8e87)" stroke-dasharray="4 3" marker-end="url(#eff-a)"/>
<path d="M609,184 V69 H446" stroke="var(--rule-2, #8e8e87)" stroke-dasharray="4 3" marker-end="url(#eff-a)"/>
</g>
<text x="360" y="63" text-anchor="middle" font-family="'Instrument Sans', ui-sans-serif, system-ui, sans-serif" font-size="14" font-weight="600" fill="var(--ink, #0b0c0e)">RunAgent</text>
<text x="360" y="81" text-anchor="middle" font-family="'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, monospace" font-size="9.5" letter-spacing="0.06" fill="var(--ink-3, #63676d)">stages, never executes</text>
<text x="360" y="192" text-anchor="middle" font-family="'Instrument Sans', ui-sans-serif, system-ui, sans-serif" font-size="14" font-weight="600" fill="var(--ink, #0b0c0e)">effector</text>
<text x="360" y="210" text-anchor="middle" font-family="'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, monospace" font-size="9.5" letter-spacing="0.06" fill="var(--ink-3, #63676d)">once per intent_id</text>
<g font-family="'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, monospace" font-size="11" fill="var(--ink-2, #4a4e54)" text-anchor="middle">
<text x="111" y="202">outbox topic</text>
<text x="609" y="202">results topic</text>
</g>
<text x="118" y="118" font-family="'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, monospace" font-size="11" fill="var(--s-intents, #8a5205)">.intents</text>
<g font-family="'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, monospace" font-size="9.5" letter-spacing="0.06" fill="var(--ink-3, #63676d)">
<text x="595" y="110" text-anchor="end">one ToolResult per intent</text>
<text x="595" y="126" text-anchor="end">re-injected on the same key</text>
<text x="222" y="188" text-anchor="middle">in key order</text>
<text x="497" y="188" text-anchor="middle">publishes</text>
<text x="360" y="244" text-anchor="middle">verify → refuse-expired → claim → execute → complete → publish → commit</text>
<text x="360" y="272" text-anchor="middle">outside the pipeline</text>
</g>
</svg>

*Every edge in the loop is dashed because none of it is an edge in the Beam
graph: the cycle closes through the message bus, outside the pipeline. The
phase order under the effector is the crash argument — each boundary is
explained in [Behavior reference](#behavior-reference).*

It consumes `ToolIntent`s, refuses expired ones, dedups on `intent_id`,
executes side-effecting tools from your `ToolRegistry`, and publishes one
`ToolResult` per intent keyed by `entity_key`. It runs **outside** the
pipeline: it imports no Beam and no `beam_agents.core`.

```sh
uv pip install 'beam-agents[effector]'

beam-agents-effector \
  --registry myapp.agent:TOOLS \
  --intents-from kafka://broker:9092/intents \
  --results-to   kafka://broker:9092/results \
  --approvals-to kafka://broker:9092/approvals \
  --dedup        redis://redis:6379 \
  --consumer-group effector
```

Every flag also reads an environment variable (`EFFECTOR_REGISTRY`,
`EFFECTOR_INTENTS_FROM`, …). `--registry` is an import path
(`module:attribute`) naming the same `ToolRegistry` your agent declares its
tools from — the effector executes *your* tools, so it has to load them.

Anything security-relevant — intent-signature verification (`--verify-intents`,
`--signing-keys`, `--dead-letters-to`), broker SASL/TLS, and the rule that
secrets never travel in tool arguments — lives in [Security](security.md).
Credentials belong in environment variables rather than flags: `argv` is visible
in process listings.

## Deployment preconditions

The effector cannot enforce these; a deployment that skips them loses
guarantees silently.

| Precondition | Why |
|---|---|
| Intents are written keyed by `entity_key` | `WriteIntents` does this. It is what confines a key to one partition, which is what makes per-key order survive scale-out. |
| Pub/Sub subscriptions have **message ordering enabled** | Without it Pub/Sub delivers a key's intents concurrently and per-key order is gone. The effector logs a warning at startup when it can read the flag and it is off. |
| The dedup store is shared by every replica | Two replicas pointed at different stores dedup independently, so an `intent_id` can execute twice. `memory://` is single-process only. |
| Bigtable: the table has column family `d` with a `maxage` GC rule | Space reclamation only — expiry itself is decided at read time against the record's own `rexp` column, so a lagging GC never serves an expired result. Set `maxage` comfortably above `--result-ttl-ms` so GC never removes a record the store still considers live. |
| Kafka replicas share one `--consumer-group` | Partition assignment is the exclusivity mechanism. Separate groups each get the whole topic and both execute every intent (dedup collapses it, but only if they share a store). |

Bigtable table provisioning, for reference:

```sh
cbt createtable effector-dedup families="d:maxage=25h"
```

## Budgets

| Flag | Default | Rule |
|---|---|---|
| `--tool-timeout-ms` | 60s | Wall-clock budget for one tool invocation. Exceeding it publishes `ERROR`, not `REJECTED`: the effect is *unknown*, not un-attempted. |
| `--lease-ms` | 5min | Claim lifetime. **Must exceed `--tool-timeout-ms`** (validated at startup) so an unexpired lease implies a live owner. |
| `--result-ttl-ms` | 24h | Terminal-record lifetime. A redelivery arriving after this window re-executes, so set it above your worst-case redelivery lag. |
| `--max-concurrent-partitions` | 8 | How many partitions execute at once. Within a partition, intents are strictly sequential. |

## What is guaranteed, and what is not

**Guaranteed:** at most one *dispatch* per `intent_id`, one agreed terminal
result per `intent_id`, per-key execution order, and no intent lost (offsets
commit only after the result is published).

**Not guaranteed by the effector alone:** exactly-once *effects*. Two windows
remain, and both are inherent rather than fixable inside the effector:

1. **Lease expiry.** If a worker is alive but partitioned from the dedup store
   for longer than `--lease-ms`, another worker can re-claim and re-run its
   intent. The budget rule above makes this require a genuine partition, not
   just a slow tool.
2. **Crash mid-tool.** A worker killed after invoking a tool cannot know
   whether the effect landed. Its claim is left to expire (never handed back),
   and the redelivery re-executes.

### The exactly-once contract

The honest statement is two-sided. The runtime guarantees:

- **Deterministic intent IDs.** `intent_id` is uuid5-derived from
  `entity_key + seq + step_index`; a replayed bundle, a duplicated sink write,
  and a lease-expiry re-execution all carry the byte-identical id.
- **At most one *completed* execution per `intent_id`.** The dedup store
  admits one terminal result; every other delivery is collapsed or handed the
  stored result.

What that adds up to depends on the tool:

- A tool that keys its downstream effect on the intent id — a Stripe
  `Idempotency-Key`, a Redis `SETNX`, a keyed upsert, an
  `INSERT … ON CONFLICT DO NOTHING` — gets **exactly-once effects**: any
  crash-window re-invocation replays the same key and the downstream
  deduplicates it.
- A tool that does not is **at-least-once across crash recovery**: zero lost
  effects, duplicates only within the two windows above, strict exactly-once
  when no worker dies mid-tool.

To opt in, declare a keyword-only `intent: IntentInfo` parameter — the
effector injects the executing intent's identity (`intent_id`, `entity_key`,
`seq`, `step_index`, `attempt`) at invocation time. The parameter never
appears in the tool's provider-facing schema and cannot be supplied through
`args_json` (a spoofed `intent` key is rejected before the callable runs):

```python
from beam_agents.tools import IntentInfo, tool


@tool(side_effect=True)
async def charge(customer_id: str, amount_cents: int, *, intent: IntentInfo) -> str:
    # Replays and crash-window re-executions re-mint the same intent_id,
    # so the payment provider performs the charge exactly once.
    return await payments.charge(
        customer_id, amount_cents, idempotency_key=intent.intent_id
    )
```

Declaring the parameter on a `side_effect=False` tool is a
`ToolDefinitionError` at decoration time: read-only tools run inline in the
pipeline, where no intent exists.

## Behavior reference

**Phase order per intent** — verify → refuse-expired → claim → execute →
complete → publish → commit. Each edge is a crash argument: an unauthenticated
message drives nothing at all, not even an `EXPIRED` result (see
[Security](security.md#verification-failures)); expiry is decided before the
store is touched (an outage cannot make a deadline fail open); the result is
durable before it is published (a crash republishes rather than re-executes);
the offset advances last (a crash redelivers rather than loses).

The verify step is inert until you turn it on: `--verify-intents` defaults to
`off`, which is byte-for-byte the pre-signing behavior.

**Statuses.** `REJECTED` means the callable never ran (unknown tool, a
`side_effect=False` tool reaching the outbox, argument validation failure).
`ERROR` means it ran and the effect is unknown (raised, timed out, or returned
something unencodable). `EXPIRED` means past `expires_at_ms` — including
`expires_at_ms == 0`, which reads as expired, never as unbounded. `OK` carries
the return value as canonical JSON.

**Approvals.** An intent with `kind = APPROVAL` is published verbatim to
`--approvals-to` and never executed; no `ToolResult` is published, because the
answer comes back as an `AgentEnvelope.Approval` on the approvals topic. It is
marked terminal so a redelivery cannot notify twice. Expired approvals are
refused like any other intent. Fronting that channel is your job, not the
runtime's — see [the Slack approval surface example](examples/slack-approval.md)
for a worked one (Block Kit buttons, verdict → envelope keyed by `entity_key`,
fail-closed TTL at the surface).

**A busy claim blocks, it does not skip.** If another worker holds a live
lease, the effector waits (bounded by the lease) rather than committing past
an intent it never executed. Skipping would be the one failure mode that
silently drops an effect.

## Shutdown

`SIGINT`/`SIGTERM` stops consumption and cancels in-flight partitions. Claims
whose tool had not yet been invoked are released immediately, so a restarting
replica does not wait out their leases; a claim whose tool *was* invoked is
left to expire, since the effect may already have happened. Nothing
uncommitted is lost — it redelivers.

## Testing

- **Offline** (`make test-unit`): the whole loop runs against in-memory source,
  sink, and dedup fakes. No docker, no network, no client libraries installed.
- **Semantics** (`make test-semantics-offline`): the effectively-once gate
  replays an intent stream with kills injected at every phase boundary.
- **Integration** (`make compose-up && make test-integration`): Redpanda +
  Redis end-to-end, the Pub/Sub emulator for ordered delivery, and the
  Bigtable emulator for the conditional-claim semantics. The `DedupStore`
  conformance suite runs against all three backends, so a store that passes is
  substitutable for the others.
