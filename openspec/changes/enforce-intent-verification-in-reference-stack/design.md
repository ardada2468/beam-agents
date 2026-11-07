## Context

The effectively-once gate already runs the production effector binary as real OS processes against real Kafka and Redis, with the pipeline on the Flink mini-cluster inside the `beam-sdk-harness` container. The only part of the signed effects loop it did not exercise was the signature: the harness's outbox stand-in published unsigned bytes and the pool ran at `off`. Two constraints shape the design: the pipeline half and the effector half live in different filesystems (container vs. host), and the effector workers are SIGKILLed on purpose, so they cannot be compose services.

## Decisions

### D1: Key material travels through the spool bind mount, per run, by `file:` reference

The signer DoFn runs inside the harness container and the effector runs on the host. They already share exactly one filesystem: the `docker/e2e-spool` bind mount. The run's keyring is written there (`<run>/intent-keys`, not `*.seg`, so the spool source never reads it), referenced as `file:/var/beam-agents/spool/<run>/intent-keys` by the signer and `file:<repo>/docker/e2e-spool/<run>/intent-keys` by the pool, and removed by teardown's `rmtree`. This is the production provisioning shape — a reference in configuration, bytes resolved worker-side — with no key in the pipeline graph, on argv, or in git.

Rejected: a fixed dev key in `docker/compose.yaml`. There is no compose effector to attach it to, the harness container's environment would then have to be mirrored by the host-side pool (two copies to drift), and a committed key is exactly what `docs/security.md` tells operators not to have.

### D2: Sign in the harness's outbox, with the production signer objects

`WriteIntents` cannot run on this stack (upstream `kafka_write:v2` defect, no Java SDK environment), which is why the harness has its own publisher. The signing step added to it is a line-for-line mirror of `WriteIntents._SerializeIntent`: `IntentSigner` (reference only) → `resolve_key()` in `setup()` → `sign_intent` on a copy → deterministic serialization. The gate therefore exercises the shipped signer and verifier across the real container/process boundary, with only the broken Kafka writer swapped out — the same substitution the gate already documents.

### D3: One unsigned forgery, injected between phases, asserted exactly once

The offline signing gate already covers tampered, bad-signature, and unsigned deliveries under kills. On real infrastructure the gate adds the single case that distinguishes `require` from both other modes: an **unsigned** intent (`permissive` would execute it; `off` would execute it). It is well-formed in every other respect — registered tool, canonical arguments, the runtime's own deterministic `intent_id`, an hour of TTL — so nothing but the missing signature can refuse it, and an `EXPIRED` or `REJECTED` result would be the wrong reason to pass.

It is injected after phase A's strong-form assertion and before the TaskManager kill: the chaos schedule is complete and joined, so no worker is mid-replacement and no rebalance is in flight, which is what makes "exactly one record on the dead-letter topic" a clean assertion rather than an at-least-once count. It is published from the host, never through the pipeline, so phase B's replay cannot re-mint it.

### D4: The dial fails loudly in both directions

Turning the pool back to `off` or `permissive` makes the forgery execute. The gate catches that twice: the in-phase dead-letter wait times out with a message naming the dial and the ledger count it found, and the ledger's existing unknown-member check (`_assert_strong_form`) fires on the forged `intent_id`. Removing the signer from the pipeline while leaving the pool at `require` dead-letters every genuine intent and the gate fails on lost effects. There is no setting of either half under which the gate stays green without enforcement.

### D5: Default stays `off`; evidence is the deployment, not the default

`EffectorConfig.verify_intents = "off"` is part of the 1.0 compatibility surface: flipping it would make every upgrade without provisioned keys dead-letter its entire intent stream. The gate condition asks for *enforcement in a deployment*, and the repository's reference deployment is the one deployment this repository can make claims about. `docs/security.md` says so in as many words.

## Open Questions

None. The strictness of the single-dead-letter assertion is deliberate (see D3); if a CI run ever shows a duplicate there with healthy infrastructure, that is a finding about the effector's dead-letter publish path, to be root-caused rather than loosened.
