## ADDED Requirements

### Requirement: ToolIntents are signed at the outbox writer

`WriteIntents` SHALL accept an optional signer specification naming a signature scheme, a `signing_key_id`, and a key *reference* (`env:VAR` or `file:/path`); when configured, it SHALL stamp each intent's signature fields before serialization so every message written to the outbox carries `signature_scheme`, `signing_key_id`, and a `signature` computed as HMAC-SHA256 over the deterministic serialization of the intent with its signature fields cleared. Key material SHALL be resolved worker-side at `DoFn.setup()` from the reference; the key bytes SHALL NOT be serialized into the pipeline graph. Signing SHALL be deterministic: the same intent and key always produce the same signed bytes. Intents staged in keyed state SHALL be unaffected — signing applies only at the outbox writer.

#### Scenario: A signed intent verifies against the signing key

- **WHEN** `WriteIntents` with a configured signer serializes an intent
- **THEN** the written message carries the configured scheme and `signing_key_id`, and its `signature` verifies as HMAC-SHA256 of the message's deterministic serialization with the signature fields cleared, using the referenced key

#### Scenario: A retried bundle re-signs to byte-identical messages

- **GIVEN** a bundle whose outbox write is retried
- **WHEN** the same intents are signed and serialized again
- **THEN** the signed wire bytes are byte-identical to the first attempt's

#### Scenario: No signer configured preserves today's output

- **WHEN** `WriteIntents` is constructed without a signer specification
- **THEN** the written messages carry no signature fields and are byte-identical to the pre-signing behavior

#### Scenario: Key bytes never enter the pipeline graph

- **WHEN** a `WriteIntents` with a signer specification is pickled (as the runner does when submitting the pipeline)
- **THEN** the pickled bytes contain the key reference but not the key material

### Requirement: The effector verifies intent signatures before any other phase

When intent verification is enabled, the effector SHALL verify each delivered intent's signature before evaluating expiry, before touching the dedup store, and before resolving any tool: the per-intent phase order becomes verify → refuse-expired → claim → execute → complete → publish → commit. Verification SHALL recompute the MAC over the delivered message's deterministic serialization with the signature fields cleared, look up the key by the intent's `signing_key_id` in a configured keyring, and compare in constant time. A verification failure SHALL prevent every later phase for that delivery.

#### Scenario: A validly signed intent executes normally

- **GIVEN** the effector in `require` mode with the signer's key in its keyring
- **WHEN** a correctly signed, unexpired `TOOL` intent is delivered
- **THEN** it passes verification and proceeds through claim, execution, completion, publish, and commit exactly as an intent does today

#### Scenario: A tampered intent never reaches the dedup store or a tool

- **GIVEN** a signed intent whose `args_json` (or any other field) was altered after signing
- **WHEN** the effector processes the delivery in `permissive` or `require` mode
- **THEN** verification fails, no tool is invoked, no `ToolResult` is published, and the dedup store records no claim for that `intent_id`

#### Scenario: An unknown signing key id fails verification distinctly

- **WHEN** a signed intent names a `signing_key_id` absent from the effector's keyring
- **THEN** the delivery is dead-lettered with a reason distinguishing unknown-key from bad-signature, so operators can tell mis-provisioned keys from tampering

#### Scenario: Verification precedes the expiry check

- **GIVEN** a tampered intent whose `expires_at_ms` is in the past
- **WHEN** the effector processes it in a verifying mode
- **THEN** it is dead-lettered for the signature failure and no `ToolResult` with status `EXPIRED` is published

### Requirement: Verification failures are dead-lettered and never produce a ToolResult

A delivery that fails verification SHALL NOT result in a published `ToolResult` of any status, because its `entity_key`, `intent_id`, and `seq` are unauthenticated bytes and a published result would enter the keyed re-injection path. Instead the effector SHALL publish the raw delivered payload under the raw delivered key to a configured dead-letter channel (`dead_letters_to`, same transport URI grammar as the other channels), increment a per-reason counter (`unsigned_intent`, `bad_signature`, `unknown_signing_key`), and commit the offset so a stream of unverifiable messages cannot wedge the partition. When no dead-letter channel is configured, the effector SHALL log the failure using identity fields only — never `args_json` content — count it, and commit.

#### Scenario: A dead-lettered delivery is preserved verbatim and the partition continues

- **GIVEN** a configured `dead_letters_to` channel
- **WHEN** a tampered intent is followed by a valid one on the same partition
- **THEN** the tampered delivery's exact payload bytes appear on the dead-letter channel, its offset is committed, and the valid intent executes without waiting on a lease or a retry

#### Scenario: No result of any status is published for a verification failure

- **WHEN** a delivery fails verification for any reason
- **THEN** the result sink receives nothing for it — not `REJECTED`, not `EXPIRED` — and the per-reason dead-letter counter increments

#### Scenario: Without a dead-letter channel the failure is logged and counted

- **GIVEN** no `dead_letters_to` configured
- **WHEN** a delivery fails verification
- **THEN** a log record identifying the delivery (partition, claimed `intent_id`, reason) is emitted without including `args_json`, the counter increments, and the offset is committed

### Requirement: Verification mode governs unsigned intents across the rollout

The effector SHALL support three verification modes: `off` (default — signature fields are ignored and behavior is identical to the pre-signing effector), `permissive` (intents carrying a signature are verified and dead-lettered on failure; unsigned intents are accepted and counted via `unsigned_intents_accepted`), and `require` (unsigned intents are dead-lettered with reason `unsigned_intent`). A bad signature SHALL be dead-lettered in every verifying mode — `permissive` never excuses a signature that fails to verify. Startup validation SHALL reject `permissive` or `require` without a resolvable keyring, and SHALL warn when `require` is configured without a dead-letter channel.

#### Scenario: Permissive mode accepts signed and unsigned intents side by side

- **GIVEN** the effector in `permissive` mode during a rollout
- **WHEN** an unsigned intent and a validly signed intent are delivered
- **THEN** both execute, and `unsigned_intents_accepted` increments exactly once

#### Scenario: Permissive mode still refuses a tampered signature

- **WHEN** an intent whose signature does not verify is delivered in `permissive` mode
- **THEN** it is dead-lettered with reason `bad_signature` and never executes

#### Scenario: Require mode dead-letters unsigned intents

- **WHEN** an unsigned intent is delivered in `require` mode
- **THEN** it is dead-lettered with reason `unsigned_intent`, no tool is invoked, and the offset is committed

#### Scenario: A verifying mode without a keyring fails at startup

- **WHEN** the effector is configured with `verify_intents=require` and no resolvable keyring reference
- **THEN** startup fails with an actionable `ValueError` before any client is constructed

### Requirement: Broker transport security is configurable with credentials by reference

The library SHALL provide a transport-security configuration block — Kafka `security_protocol`, SASL mechanism, TLS material paths, and username/password given as `env:VAR` or `file:/path` references — threaded into every Kafka client the library constructs: the effector's intent source and message sinks, and `WriteIntents`' producer. References SHALL be validated eagerly without importing any client library and resolved only at client construction; resolved secret values SHALL NOT be stored on the configuration object. Pub/Sub authentication SHALL remain Application Default Credentials, with the required IAM role split documented rather than configured.

#### Scenario: SASL settings reach the Kafka clients

- **GIVEN** a transport-security block specifying `SASL_SSL` with a password reference `env:KAFKA_PASSWORD`
- **WHEN** the effector's Kafka source and sinks are constructed
- **THEN** each underlying client is configured with the SASL mechanism, protocol, and the password resolved from the environment at construction time

#### Scenario: A malformed credential reference fails eagerly and import-free

- **WHEN** a transport-security block carries a reference in neither `env:` nor `file:` form
- **THEN** configuration validation raises an actionable `ValueError` without importing any transport client library

#### Scenario: Resolved secrets are absent from the configuration object

- **WHEN** a config carrying credential references is constructed and its clients built
- **THEN** the secret value appears in no attribute, `repr`, or error message of the configuration object — only the reference does

### Requirement: Secrets embedded in URIs never appear in errors or reprs

Every error message that interpolates a transport or dedup URI, and every `repr` of the effector configuration, SHALL redact URI userinfo (passwords and usernames) before rendering. The `beam-agents-effector` startup error path SHALL print only redacted configuration content to stderr.

#### Scenario: A malformed credentialed URI is reported redacted

- **WHEN** `EffectorConfig` is constructed with a malformed dedup URI containing `redis://user:secret@host`
- **THEN** the raised error names the URI with its userinfo masked and the string `secret` appears nowhere in the exception or its chain

#### Scenario: The config repr masks credentials

- **WHEN** `repr()` is taken of an `EffectorConfig` whose dedup URI embeds a password
- **THEN** the output shows the URI with userinfo masked and the password value appears nowhere in it

### Requirement: The security baseline is documented, including the prohibition on secrets in intent payloads

The project SHALL ship `docs/security.md` covering: the enforce-versus-document boundary (intent signing enforced by the library; broker authentication enforced by the broker); Kafka hardening (SASL_SSL or mTLS plus per-principal topic ACLs) and the Pub/Sub IAM least-privilege role matrix for the pipeline and effector principals; the secret-manager pattern (secrets materialized to workload env/files, referenced — never valued — in configuration); key provisioning and rotation for intent signing; the signed rollout sequence and the counters to alarm on; and the contract that secrets MUST NOT be placed in intent payloads or tool arguments — a tool needing a credential resolves it effector-side from its own environment, with only non-secret identifiers passed as arguments.

#### Scenario: The security document states the secrets-in-args contract

- **WHEN** `docs/security.md` is consulted
- **THEN** it states that tool arguments and intent payloads must never carry secrets, explains that `args_json` is copied into keyed state, broker topics, and dead letters, and shows the effector-side credential-resolution pattern

#### Scenario: The security document gives the least-privilege matrix

- **WHEN** `docs/security.md` is consulted
- **THEN** it lists, per principal, the exact produce/consume (or IAM role) grants needed for the intents, results, approvals, and dead-letter channels, and grants neither principal broker-admin rights
