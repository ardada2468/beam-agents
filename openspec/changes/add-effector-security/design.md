## Context

The effects loop is asynchronous: the pipeline and the effector never hold a connection to each other. `RunAgent` stages `ToolIntent`s whose `intent_id` is `uuid5(NAMESPACE, entity_key|seq|step_index)` ([agent.py:43](../../../src/beam_agents/core/agent.py:43)), `WriteIntents` lands them on a Kafka/Pub/Sub outbox keyed by `entity_key`, and the effector consumes, dedups, executes, and publishes `ToolResult`s that re-enter the pipeline. "Authentication between pipeline and effector" therefore cannot mean a channel handshake — there is no channel. It decomposes into two layers with different owners:

- **Broker-level access control** (who may produce/consume which topic) is enforced by the broker — Kafka SASL/mTLS + ACLs, Pub/Sub IAM. The library's job is to make it configurable and to document the baseline; it cannot enforce it.
- **Application-level provenance** (did *the pipeline* mint this exact intent?) is enforceable by the library: sign each `ToolIntent` at the outbox writer, verify at the effector before anything else runs.

The threat this defends is the topic-writer boundary: today, write access to the outbox topic *is* the authority to execute any registered side-effect tool with arbitrary arguments. The dedup store does not narrow that authority — it collapses duplicates per `intent_id` ([dedup.py:69](../../../src/beam_agents/effector/dedup.py:69)), and a forged intent carries a fresh id.

Fixed constraints this design lives under: the effector imports no Beam and no `core/` (add-reference-effector D1), so anything shared between signer and verifier must be Beam-free; state is protobuf and additive-only (invariant 7); replayed bundles must produce byte-identical outbox messages (invariant 2); unit tests run offline with no docker.

## Goals / Non-Goals

**Goals:**

- Every `ToolIntent` on the outbox can carry a signature binding all of its fields — identity, tool, arguments, expiry — to a key held by the pipeline, and the effector can refuse anything that does not verify, before touching the dedup store or a tool.
- A staged rollout in which unsigned and signed intents coexist without losing effects, and a rollback that never strands intents.
- A configuration surface for Kafka SASL/mTLS on every client the library constructs (effector source, effector sinks, outbox writer), with secrets supplied by reference (env/file), never by value.
- No secret ever appears in an exception message, a config `repr`, or the documented deployment flow's `argv`; a written, testable statement that secrets do not belong in intent payloads or tool arguments.
- Honest documentation of what the library enforces versus what the deployment must provide.

**Non-Goals:**

- Signing `ToolResult`s or approval envelopes on the return path. The re-injection side already admits a result only against a live continuation with a matching pending `intent_id` (everything else is `orphaned_result`), and broker ACLs on the results topic are the primary control. Revisited as an open question, not silently assumed safe.
- A secrets-manager client integration. The pattern documented is "secret manager → deployment env/file → library reads a reference"; adding `google-cloud-secret-manager` or Vault SDKs to the dependency tree is out of scope.
- Payload encryption. Confidentiality of `args_json` on the broker is TLS-in-transit plus broker-side encryption at rest; field-level encryption is not proposed.
- Authorization policy (which tools a given pipeline may invoke). One key, one trust domain; per-tool or per-principal policy is future work.
- Scanning/linting tool arguments for secret-shaped content (rejected in D8).

## Decisions

### D1. Enforce provenance in the library; document broker authn — and say which is which

The library enforces what it can see end to end: intent signing/verification, secret redaction, and credential plumbing. It documents what only the deployment can decide: broker authentication (SASL_SSL or mTLS for Kafka, IAM for Pub/Sub), topic ACLs, and the least-privilege role matrix (pipeline principal: produce intents, consume results/approvals; effector principal: consume intents, produce results/approvals/dead-letters; neither gets admin). `docs/security.md` states this split explicitly, in the same "the effector cannot enforce these" register that [docs/effector.md](../../../docs/effector.md) already uses for its deployment preconditions. The failure mode this avoids is a deployment believing intent signing substitutes for broker auth (it does not: signing stops forged *execution*, not topic flooding, eavesdropping, or forged *results*) or vice versa (broker auth alone makes every writer-credential holder a full execution authority).

### D2. HMAC-SHA256 with key ids, behind a scheme-tagged seam; asymmetric rejected for now

The signature is `HMAC-SHA256(key, deterministic_serialization(intent with signature fields cleared))`, tagged with a `signing_key_id` and a `signature_scheme` enum on the wire.

Why symmetric, considering key distribution honestly: the signer (pipeline workers) and verifier (effector replicas) are two halves of one operator-deployed runtime, provisioned by the same deployment machinery. Key distribution is one secret delivered to two workloads via the secret-manager pattern of D7 — no PKI, no trust store. The classic argument for asymmetric — the verifier cannot mint — buys little here, because the effector is *already* the maximally trusted component: it holds the credentials for every downstream system its tools touch. An attacker who owns the effector does not need to mint intents. What signing defends is the topic-writer boundary (D6), and HMAC defends it fully.

What tips the decision: HMAC-SHA256 is stdlib (`hmac`, `hashlib`) — zero new dependencies for the core package and for the effector extra — and it is deterministic, which composes with correctness invariant 2: a retried bundle re-mints byte-identical intents, re-signs them to byte-identical signatures, and the outbox write remains replay-stable. (Ed25519 is also deterministic per RFC 8032, so determinism alone does not decide; the dependency does.)

*Alternative rejected:* Ed25519 via the `cryptography` package. Adds a compiled dependency to both the pipeline image and the effector extra, plus key-pair lifecycle, for a benefit (verifier cannot sign) that the effector's trust position mostly nullifies. The wire format keeps the door open: `signature_scheme` is an enum, `verify_intent` dispatches on it, and an asymmetric scheme can be added additively without touching this change's messages.

Key rotation: the verifier holds a keyring (`key_id → key`), the signer names its `signing_key_id` on each intent, so rotation is: add new key to effector keyring → switch pipeline signer to the new id → retire the old key after the outbox retention window.

### D3. Sign at `WriteIntents`, verify before everything; the signing input is the cleared-fields deterministic serialization

**Where to sign.** In the outbox writer (`_SerializeIntent`, [write_intents.py:224](../../../src/beam_agents/actions/write_intents.py:224)), not in `ctx.act`'s staging path ([context.py:586](../../../src/beam_agents/core/context.py:586)). Three reasons: (1) the signature is a transport property — intents staged in `PENDING` state and continuations stay byte-identical to today, so no state-compat or golden-blob churn and no `state_schema_version` question; (2) key material stays out of the agent DoFn entirely — the signer resolves its key in the writer DoFn's `setup()` from an env/file reference, and only the *reference* is pickled into the pipeline graph; (3) the atomic-commit invariant is untouched — signing is a pure function applied at emission, not a staged effect.

**Signing input.** `intent.SerializeToString(deterministic=True)` computed over a copy with the three signature fields cleared. The verifier parses the delivered bytes, extracts and clears the signature fields, re-serializes deterministically, and compares MACs (constant-time, `hmac.compare_digest`). The known hazard is that deterministic serialization is a per-implementation property, not a canonical form — but both ends are this library with committed bindings, and additive evolution gives the mitigation: every new `ToolIntent` field (the signature fields now, anything later) takes a new field number above all existing ones, so a verifier on older bindings sees a newer field as an unknown field that Python protobuf preserves and re-emits after the known fields — matching the signer's sorted-by-number layout. A compat test pins exactly this: a blob carrying an unknown (future-numbered) field must still verify. If a future protobuf runtime breaks unknown-field placement, that test — not production — is where it surfaces.

*Alternative rejected:* a `SignedToolIntent` wrapper message (`bytes intent` + signature fields), which would make the signing input the exact wire bytes with no re-serialization. It changes what the outbox topic *is*: every consumer breaks at once, old retained messages fail to parse as the wrapper, and mixed-mode rollout needs content sniffing. Additive fields keep one topic schema through the whole migration.

*Alternative rejected:* signing the canonical string `intent_id|tool_name|args_json|expires_at_ms|...` by hand. Every field added to `ToolIntent` would silently fall outside the signature unless someone remembers to extend the string — a substitution vulnerability by default. Serializing the whole message signs future fields by default.

**Where to verify.** As the effector's phase zero, before `refuse_expired` ([service.py:260](../../../src/beam_agents/effector/service.py:260)). An unauthenticated message must not drive any behavior: publishing `EXPIRED` for a forged intent would let an attacker emit attacker-keyed results (see D4), and touching the dedup store would let forged ids consume claims and store writes. The composed order is **verify → refuse-expired → claim → execute → complete → publish → commit**, and every crash-safety argument of add-reference-effector D3 is preserved because verification is a pure function of the delivered bytes — it acquires nothing that a crash could leak.

### D4. Verification failures dead-letter; they never become a `ToolResult`

A tampered, unsigned-under-`require`, or unknown-key intent is published to a configurable dead-letter channel (`dead_letters_to`, same `kafka://`/`pubsub://` grammar) as the raw delivered payload under the raw delivered key, counted per reason (`unsigned_intent`, `bad_signature`, `unknown_signing_key` — extending the reason vocabulary that `REASON_INTENT_DEAD_LETTER` ([dofn.py:127](../../../src/beam_agents/core/dofn.py:127)) established for the pipeline's own intent dead letters), and the offset is committed. When no dead-letter channel is configured, the failure is logged (identity fields only — never `args_json`, which may be attacker-chosen bait for log-injection or contain data worth not spraying into logs) and counted, then committed.

Why not `ToolResult(REJECTED)`, which is how the effector reports every other never-invoked failure? Because the result path is keyed re-injection: a `ToolResult` inherits `entity_key`, `intent_id`, and `seq` from the intent — which, for a message that failed verification, are attacker-chosen bytes. The pipeline's `orphaned_result` guard drops results with no matching pending intent, but an attacker who learns a *genuine* pending `intent_id` could forge a tampered copy and have the effector's own `REJECTED` race the genuine result to the continuation. Refusing to publish anything on verification failure closes that lever entirely.

Why commit rather than stall: a partition head wedged on a forged message is a denial of service an attacker can produce at will; committing past it keeps genuine traffic flowing. The cost is honest: a *genuine* intent corrupted in flight (or signed with a key the effector no longer holds) is dead-lettered, its agent's continuation waits, and the HITL timer fires the fallback path — fail-closed at both layers, exactly the shape correctness invariant 6 prescribes. The dead-letter channel plus the per-reason counters are the operator's detection surface; `docs/security.md` says to alarm on them.

### D5. Rollout modes `off` → `permissive` → `require`

`EffectorConfig` gains `verify_intents: off | permissive | require` (default `off`) and a keyring reference. `off` is byte-for-byte today's behavior — signature fields, if present, are ignored. `permissive` verifies any intent that carries a signature (dead-lettering tampered ones — a *bad* signature is never acceptable in any verifying mode) but accepts and counts unsigned ones (`unsigned_intents_accepted`). `require` dead-letters unsigned intents too. Startup validation rejects `permissive`/`require` without a resolvable keyring, and warns when `require` has no `dead_letters_to` (failures then exist only as logs and counters). The pipeline side is independent: `WriteIntents` signs whenever a signer spec is configured. The migration plan below sequences the two dials; the spec requires each mode's behavior separately so the coexistence window is a tested state, not an accident of deployment timing.

### D6. What signing adds on top of dedup — and what it does not

The dedup store and the signature solve different problems and compose:

- **Forgery** (novel `intent_id`, attacker content): dedup executes it once — dedup is *no* defense. Signature refuses it. This is the headline gain.
- **Substitution** (genuine pending `intent_id`, altered `tool_name`/`args_json`/`expires_at_ms`): the signature covers every field, so any alteration invalidates it. Without signing, whichever copy the effector processes first wins the claim and executes — the attacker's copy can beat the genuine one. Dedup alone cannot distinguish them.
- **Verbatim replay** of a captured signed intent: the signature verifies (it is the genuine message), and this is exactly dedup's job — within `result_ttl_ms` the replay collapses to `Done` and republishes the stored result. Beyond `result_ttl_ms`, the replayed intent's own signed `expires_at_ms` refuses it (signing prevents extending expiry). The residual window is a replay that lands after the terminal record's TTL but before `expires_at_ms` — only reachable when an intent's TTL exceeds `result_ttl_ms` (defaults: intent TTLs are minutes-to-hours, `result_ttl_ms` 24h, so the window is empty unless configured otherwise). `docs/security.md` states the rule: keep `result_ttl_ms` above the largest intent TTL in use, which the deployment already needs for redelivery correctness.
- **Duplicate delivery / bundle replay** (the non-adversarial case): unchanged — deterministic ids plus dedup, exactly as before; signatures are deterministic so replays are byte-identical and invisible to all of it.

### D7. Broker credentials and signing keys travel by reference, resolved at startup

A `TransportSecurity` block on `EffectorConfig` (and a mirrored parameter on `WriteIntents`) carries Kafka `security_protocol`, `sasl_mechanism`, CA/client cert paths, and username/password *references* of the form `env:VAR_NAME` or `file:/path` — resolved once at client construction (`setup()` pipeline-side, adapter constructor effector-side), held only in the client object, never stored back onto the config. The signing keyring uses the same reference forms (`BEAM_AGENTS_INTENT_KEYS=file:/var/run/secrets/intent-keys` with `key_id=base64(key)` lines). Pub/Sub needs no credential plumbing — auth is ADC — so its section of `docs/security.md` is purely the IAM role matrix. This is the secret-manager pattern in practice: the manager (GSM, Vault, SOPS — whichever the deployment runs) materializes secrets into the workload's env/files; the library only ever sees a name.

### D8. Secrets in URIs are redacted everywhere; secrets in tool args are forbidden by documented contract, not by linting

Code side: every place a transport/dedup URI is interpolated into an error ([config.py:88](../../../src/beam_agents/effector/config.py:88) and siblings, [\_\_main\_\_.py:152](../../../src/beam_agents/effector/__main__.py:152)'s stderr path) redacts URI userinfo first (`redis://:****@host`), and `EffectorConfig` gets an explicit `__repr__` applying the same redaction — so accidental `repr` in logs, tracebacks, and test output stops being a leak. Passing credentials inside URIs keeps working (redaction, not rejection) but the docs steer to D7's references.

Doctrine side: `args_json` is copied into keyed state (`PENDING`, continuations), onto the outbox topic, into the effector's dead-letter channel, and potentially into pipeline error records — a secret in tool arguments is unrecoverable from that many places. `docs/security.md` states the contract: tools MUST NOT receive secrets through arguments; a tool that needs a credential resolves it in its own body from the effector's environment (which D7 provisions), keyed if necessary by non-secret identifiers passed as arguments — the same shape as the existing `IntentInfo` idempotency pattern in [docs/effector.md](../../../docs/effector.md).

*Alternative rejected:* an `args_json` secret scanner (entropy heuristics / token-pattern regexes) at `ctx.act` or in the effector. False positives reject legitimate high-entropy arguments (ids, hashes, compressed payloads) in the hot path of a correctness-critical writer; false negatives manufacture confidence; and either way the secret a scanner catches has already been minted into the intent object. The structural fix — credentials simply never enter the argument path — is enforceable by review and stated by spec; a scanner is neither.

## Risks / Trade-offs

- **Cross-version signing-input drift.** Deterministic serialization is per-implementation; a protobuf runtime change or a mid-rollout bindings skew could make verifier bytes differ from signer bytes, dead-lettering genuine intents. → Fail-closed by design (HITL timers fire; nothing executes wrongly); pinned by the unknown-field compat test in D3; operational guidance: upgrade the effector before the pipeline when `ToolIntent` gains fields.
- **Shared HMAC key means a leaked effector keyring can mint valid intents.** → Accepted: the effector already holds strictly more power (every tool's downstream credentials). The scheme enum keeps an asymmetric upgrade additive if the trust model ever changes (e.g., third-party effectors).
- **`permissive` forever.** A deployment that never flips to `require` has signing theater. → `unsigned_intents_accepted` is a first-class counter and `docs/security.md` prescribes alarming on it staying nonzero after the migration window.
- **Dead-lettering commits past unverifiable messages**, so a mis-provisioned keyring silently drains genuine intents to the dead-letter channel. → Startup validation refuses verifying modes without a keyring; unknown-key dead letters carry their own counter (`unknown_signing_key`) distinct from tampering, making mis-provisioning distinguishable from attack at a glance; dead letters retain the raw payload, so recovery is re-publishing to the outbox after fixing keys.
- **Redaction cannot reach client-library logs.** aiokafka/redis may log their own connection strings. → Documented residual; the by-reference pattern of D7 keeps secrets out of URIs, which removes the material those logs would leak.
- **A `TransportSecurity` block adds config surface that must stay import-free.** → Same rule as `EffectorConfig.validate()` today: reference syntax is validated eagerly with no client imports; resolution happens at adapter construction.

## Migration Plan

All steps are independently reversible; unsigned and signed intents coexist safely throughout.

1. **Schema first.** Land the additive `ToolIntent` fields + regen (no `state_schema_version` bump; golden blobs extended). Old readers ignore the fields as unknowns; nothing changes at runtime.
2. **Upgrade effectors** with `verify_intents=off`. Behavior identical; the new code path is dormant.
3. **Provision keys** (secret manager → env/file on both workloads) and flip effectors to `permissive`. Signed intents (none yet) would verify; unsigned traffic flows and is counted.
4. **Enable signing in the pipeline**: configure the `WriteIntents` signer spec and roll the pipeline (`--update`-compatible: no state schema change, no coder change; only the outbox writer's emitted bytes gain trailing fields). From here every new intent is signed; retained unsigned intents continue to drain and execute under `permissive`.
5. **Flip effectors to `require`** once `unsigned_intents_accepted` has been zero for longer than the outbox retention window. Create/wire `dead_letters_to` before this step.
6. **Broker hardening** (SASL/mTLS/IAM/ACLs) is orthogonal and can proceed at any point via the new `TransportSecurity` config; the docs recommend doing it first, since it needs no code coordination at all.

**Rollback:** each step reverses independently — `require`→`permissive` re-admits unsigned intents; disabling the pipeline signer under `permissive` is safe; `permissive`→`off` ignores signatures entirely. No step strands an intent: nothing in the plan changes `intent_id` derivation, dedup records, or state blobs.

## Open Questions

- **Result-path signing.** Should `ToolResult`s (and approval envelopes) be signed by the effector and verified at re-injection? The `orphaned_result` guard plus results-topic ACLs cover the forgery case less absolutely than intent signing covers the outbox. If pursued, the same envelope pattern applies (`ToolResult` gets high-numbered signature fields); deferred until the trust model demands it.
- **KMS-backed signing.** `sign_intent` currently takes key bytes; a deployment wanting keys that never leave a KMS would need a signer interface async enough for a KMS RPC per bundle (or a data-key cache). Deferred; the seam (scheme enum + signer callable) admits it.
- **Per-tool signing policy.** Should `require` be scoped (e.g., only `side_effect` tools above some sensitivity tier require signatures)? One dial for the whole outbox is simpler and matches "one key, one trust domain"; revisit with multi-tenant registries.
- **Dead-letter re-drive tooling.** Recovering mis-keyed dead letters is manual re-publishing today. A `beam-agents-effector --redrive` verb is plausible future work once real operations demand it.
