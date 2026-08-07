# Security baseline

The effects loop — `RunAgent .intents → outbox topic → effector → results topic
→ re-injection` — is asynchronous. The pipeline and the effector never hold a
connection to each other, so "authentication between them" is not a handshake.
It splits into two layers with two different owners, and the most important
thing this page does is say which is which.

| Layer | Question it answers | Who enforces it |
|---|---|---|
| **Broker access control** | Who may produce to / consume from a topic? | **The broker.** Kafka SASL/mTLS plus per-principal ACLs; Pub/Sub IAM. The library makes it *configurable* and cannot make it *true*. |
| **Application provenance** | Did *the pipeline* mint this exact intent? | **The library.** `WriteIntents` signs each `ToolIntent`; the effector verifies before anything else runs. |

Neither substitutes for the other. Signing alone does not stop topic flooding,
eavesdropping, or forged *results*. Broker auth alone makes every holder of a
producer credential a full execution authority: without signing, write access to
the outbox topic **is** the authority to run any registered `side_effect=True`
tool with attacker-chosen arguments. The dedup store is no defense there — it
collapses duplicates of an `intent_id`, and a forged intent carries a novel one.

## Intent signing

Each `ToolIntent` carries a signature envelope: `signature_scheme`,
`signing_key_id`, and `signature`, where the signature is
`HMAC-SHA256(key, deterministic_serialization(intent with those three fields
cleared))`. HMAC-SHA256 is stdlib, so neither the pipeline image nor the
`effector` extra gains a dependency, and it is deterministic — a replayed bundle
re-mints a byte-identical intent and re-signs it to byte-identical wire bytes,
which is what keeps the outbox replay-stable (correctness invariant 2).

Signing happens at the **outbox writer**, not in `ctx.act`. The signature is a
transport property: intents held in keyed state and the continuations that list
them are byte-for-byte what they were before signing existed, so there is no
state-compat question and no `state_schema_version` bump. The key is resolved
worker-side in `DoFn.setup()` from a reference, so key material never enters the
pipeline graph the runner stores.

### Provisioning and rotating keys

The keyring is `key_id=base64(key)` lines, delivered to both workloads by your
secret manager and named by reference:

```text
# /var/run/secrets/intent-keys
k-2026-07=aGVsbG8td29ybGQtdGhpcy1pcy0zMi1ieXRlcyEh
```

```python
from beam_agents.actions.write_intents import WriteIntents
from beam_agents.intent_signing import IntentSigner

intents | WriteIntents(
    "kafka://broker:9092/agent-intents",
    signer=IntentSigner(key_id="k-2026-07", key_reference="file:/var/run/secrets/intent-keys"),
)
```

```sh
EFFECTOR_SIGNING_KEYS=file:/var/run/secrets/intent-keys \
EFFECTOR_VERIFY_INTENTS=require \
EFFECTOR_DEAD_LETTERS_TO=kafka://broker:9092/agent-intents-dead \
  beam-agents-effector ...
```

Generate a key with 32 bytes from a CSPRNG (`openssl rand -base64 32`). Rotation
is three ordered steps and needs no downtime:

1. **Add** the new key to the effector's keyring.
2. **Switch** the pipeline's `signing_key_id` to it.
3. **Retire** the old key once the outbox retention window has passed.

The window matters: a retained intent signed with a retired key dead-letters as
`unknown_signing_key`.

Key material must never appear on a command line. `--signing-keys` and every
credential flag take a *reference* precisely because `argv` is visible in
process listings.

### The rollout dial

`verify_intents` has three settings and they are meant to be walked in order.

| Mode | Signed intent | Unsigned intent | Bad signature |
|---|---|---|---|
| `off` (default) | executes; signature ignored | executes | executes — the dial has not been turned |
| `permissive` | verified, executes | executes, counts `unsigned_intents_accepted` | **dead-lettered** |
| `require` | verified, executes | **dead-lettered** (`unsigned_intent`) | **dead-lettered** |

A bad signature is never acceptable in a verifying mode; `permissive` excuses
*absence*, not *failure*.

The full sequence, each step independently reversible:

1. Deploy the schema and the new effector build with `verify_intents=off`.
   Nothing changes at runtime.
2. Provision keys to both workloads and flip effectors to `permissive`.
3. Create and wire `dead_letters_to`, then enable the pipeline's signer and roll
   the pipeline. It is `--update`-compatible: no state schema change, no coder
   change, only trailing fields on the emitted outbox bytes.
4. Wait until `unsigned_intents_accepted` has been **zero for longer than the
   outbox retention window**, then flip effectors to `require`.

Rolling back reverses one step at a time and strands nothing: nothing in the
sequence changes `intent_id` derivation, dedup records, or state blobs.

### Verification failures

Verification is the effector's phase **zero** — ahead of the expiry check, the
dedup store, and any tool resolution. A delivery that fails it is published
verbatim (raw payload, raw key) to `dead_letters_to`, counted, and its offset is
committed.

No `ToolResult` is published for a verification failure — not `REJECTED`, not
`EXPIRED`. A result inherits the delivery's `entity_key`, `intent_id`, and `seq`,
which on an unverifiable message are attacker-chosen bytes; publishing one would
put those bytes on the keyed re-injection path, letting an attacker who learns a
genuine pending `intent_id` race a forged refusal against the real result.

Committing past the failure rather than stalling is equally deliberate: a
partition head wedged on a forged message is a denial of service an attacker can
produce at will. The cost is honest — a *genuine* intent corrupted in flight, or
signed with a key the effector no longer holds, is dead-lettered and its
activation waits until the HITL timer fires its fallback. That is fail-closed at
both layers, which is what correctness invariant 6 prescribes.

### Counters to alarm on

| Counter | Means | Alarm when |
|---|---|---|
| `unsigned_intents_accepted` | `permissive` admitted an unsigned intent | non-zero after the migration window — otherwise you have signing theater |
| `unsigned_intent` | `require` dead-lettered an unsigned intent | non-zero after step 4 — a producer was missed |
| `bad_signature` | a signature did not verify | **any** non-zero value: tampering, or a signing-input skew between builds |
| `unknown_signing_key` | the `signing_key_id` is not in the keyring | non-zero — almost always a mis-provisioned or prematurely retired key |
| `intents_dead_lettered` | total of the three above | use for a single "something is wrong" page |

`unknown_signing_key` is deliberately distinct from `bad_signature` so a bad
deploy is distinguishable from an attack at a glance.

### What signing adds on top of dedup

- **Forgery** (novel `intent_id`, attacker content): dedup executes it once —
  dedup is *no* defense. The signature refuses it. This is the headline gain.
- **Substitution** (genuine pending `intent_id`, altered `tool_name`/`args_json`/
  `expires_at_ms`): the signature covers every field, so any alteration
  invalidates it. Without signing, whichever copy the effector sees first wins
  the claim — the attacker's copy can beat the genuine one.
- **Verbatim replay** of a captured signed intent: the signature verifies (it is
  the genuine message), and collapsing it is exactly dedup's job. Within
  `result_ttl_ms` the replay returns the stored result; beyond it, the intent's
  own signed `expires_at_ms` refuses it (signing prevents extending expiry). The
  residual window is a replay landing after the terminal record's TTL but before
  `expires_at_ms`, which is reachable only when an intent's TTL exceeds
  `result_ttl_ms`. **Keep `result_ttl_ms` above the largest intent TTL in use** —
  a rule redelivery correctness already needs.
- **Duplicate delivery / bundle replay** (non-adversarial): unchanged.
  Deterministic ids plus dedup, exactly as before.

Not covered, deliberately: `ToolResult` and approval envelopes are not signed
(the re-injection path admits a result only against a live continuation with a
matching pending `intent_id`; everything else is `orphaned_result`), payloads are
not encrypted, and there is no per-tool authorization policy — one key, one trust
domain.

## Secrets never travel in intent payloads or tool arguments

**`args_json` MUST NOT carry secrets.** This is a contract, not a preference,
because of where `args_json` goes: it is persisted in keyed state (`PENDING`,
continuations), written to the outbox topic, copied verbatim into the effector's
dead-letter channel, and re-encodable into pipeline error records. A secret
placed there is a secret copied into more places than you can revoke.

A tool that needs a credential resolves it **effector-side, in its own body**,
from the environment the deployment provisions. Only non-secret identifiers
travel as arguments:

```python
import os

import httpx

from beam_agents.tools import IntentInfo, tool


@tool(side_effect=True)
async def charge(customer_id: str, amount_cents: int, *, intent: IntentInfo) -> str:
    # The credential is resolved here, from the effector's own environment.
    # `customer_id` and `amount_cents` are the only things on the wire.
    api_key = os.environ["PAYMENTS_API_KEY"]
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "https://payments.example/charges",
            headers={
                "Authorization": f"Bearer {api_key}",
                # Replays and crash-window re-executions re-mint the same
                # intent_id, so the provider performs the charge exactly once.
                "Idempotency-Key": intent.intent_id,
            },
            json={"customer_id": customer_id, "amount_cents": amount_cents},
        )
    return response.text
```

This is not enforced by a linter, on purpose. Scanning `args_json` for
secret-shaped strings has false positives on legitimate high-entropy arguments
(ids, hashes, compressed payloads) in the hot path of a correctness-critical
writer, false negatives that manufacture confidence, and in either case the
secret it "catches" has already been minted into the intent object. The
structural fix — credentials never enter the argument path — is enforceable by
review and stated here; a scanner is neither.

## Secrets by reference, never by value

Every credential the library reads is named by a **reference**:

| Form | Resolves to |
|---|---|
| `env:VAR_NAME` | the value of that environment variable |
| `file:/path` | the file's contents, stripped |

References are validated eagerly at configuration time (import-free — no client
library is loaded to check a string) and resolved once, at client construction.
The resolved value lives inside the client object and is never written back onto
the configuration, so it appears in no attribute, no `repr`, and no error
message.

The pattern in full: your secret manager (Google Secret Manager, Vault, SOPS,
sealed secrets — whichever you run) materializes the secret into the workload's
environment or a mounted file; the library only ever sees the name. The library
ships no secret-manager client and does not want one.

### URI credentials are redacted, not rejected

`redis://user:password@host:6379` keeps working. Its userinfo is masked
(`redis://***@host:6379`) in every configuration error message, in
`repr(EffectorConfig)`, and on the `beam-agents-effector` startup error path, so
a `repr` in a log or a traceback in CI output stops being a leak. Prefer the
reference forms anyway — redaction cannot reach the *client libraries'* own
logging, and aiokafka or redis-py may render a connection string you never
handed them. Keeping the credential out of the URI removes the material those
logs would leak.

Finally: never pass a credential as a command-line flag. `argv` is visible in
process listings to every user on the host. Every security-relevant flag on
`beam-agents-effector` also reads an environment variable for exactly this
reason.

## Kafka hardening

Use `SASL_SSL` (or mTLS) and per-principal topic ACLs. The library threads the
settings into every Kafka client it constructs — the effector's intent source,
its result/approval/dead-letter sinks, and `WriteIntents`' producer:

```sh
EFFECTOR_KAFKA_SECURITY_PROTOCOL=SASL_SSL \
EFFECTOR_KAFKA_SASL_MECHANISM=SCRAM-SHA-512 \
EFFECTOR_KAFKA_SASL_USERNAME_REFERENCE=env:KAFKA_USER \
EFFECTOR_KAFKA_SASL_PASSWORD_REFERENCE=env:KAFKA_PASSWORD \
EFFECTOR_KAFKA_SSL_CA=/etc/ssl/certs/kafka-ca.pem \
  beam-agents-effector ...
```

```python
from beam_agents.effector.config import TransportSecurity

security = TransportSecurity(
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_username_reference="env:KAFKA_USER",
    sasl_password_reference="env:KAFKA_PASSWORD",
    ssl_ca_location="/etc/ssl/certs/kafka-ca.pem",
)

intents | WriteIntents("kafka://broker:9092/agent-intents", transport_security=security)
```

One caveat, because it is a property of the clients rather than of this
configuration: the TLS material paths are read by two different client families.
The effector's Python clients (aiokafka) read **PEM** files. `WriteIntents` on a
`kafka://` URI is Beam's cross-language sink, which is the **Java** client and
reads **Java keystores** (`ssl.truststore.location` / `ssl.keystore.location`).
Point each side at the format its client speaks.

### Least-privilege ACLs

Neither principal gets cluster admin, and neither gets a wildcard topic pattern.

| Principal | Topic | Operations |
|---|---|---|
| pipeline | `agent-intents` | `Write`, `Describe` |
| pipeline | `agent-results`, `agent-approvals` | `Read`, `Describe` + `Read` on its consumer group |
| effector | `agent-intents` | `Read`, `Describe` + `Read` on the effector consumer group |
| effector | `agent-results`, `agent-approvals`, `agent-intents-dead` | `Write`, `Describe` |

```sh
kafka-acls --add --allow-principal User:beam-agents-pipeline \
  --operation Write --operation Describe --topic agent-intents
kafka-acls --add --allow-principal User:beam-agents-effector \
  --operation Read --operation Describe --topic agent-intents
kafka-acls --add --allow-principal User:beam-agents-effector \
  --operation Read --group beam-agents-effector
```

## Pub/Sub hardening

Pub/Sub authentication is Application Default Credentials, so there is nothing
to configure in the library — only IAM roles to grant. Bind roles per resource,
never at the project level, and grant neither principal `roles/pubsub.admin`.

| Principal | Resource | Role |
|---|---|---|
| pipeline | topic `agent-intents` | `roles/pubsub.publisher` |
| pipeline | subscriptions `agent-results-sub`, `agent-approvals-sub` | `roles/pubsub.subscriber` |
| effector | subscription `agent-intents-sub` | `roles/pubsub.subscriber` |
| effector | topics `agent-results`, `agent-approvals`, `agent-intents-dead` | `roles/pubsub.publisher` |

```sh
gcloud pubsub topics add-iam-policy-binding agent-intents \
  --member=serviceAccount:beam-agents-pipeline@PROJECT.iam.gserviceaccount.com \
  --role=roles/pubsub.publisher

gcloud pubsub subscriptions add-iam-policy-binding agent-intents-sub \
  --member=serviceAccount:beam-agents-effector@PROJECT.iam.gserviceaccount.com \
  --role=roles/pubsub.subscriber
```

The intents subscription must additionally have **message ordering enabled** —
a correctness precondition, documented with the others in
[Running the effector](effector.md#deployment-preconditions).
