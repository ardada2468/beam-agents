# Security policy

This page is about reporting a vulnerability in **beam-agents** itself: the
Python package, the `beam-agents-effector` service, the Console, and the
examples and Flex Template in this repository. The deployment-side baseline
(broker access control, intent signing, key rotation, what must never travel in
an intent payload) lives in [`docs/security.md`](docs/security.md); read that
first if your question is "how do I run this safely" rather than "I found a
bug".

## Reporting a vulnerability

**Do not open a public issue for a suspected vulnerability.** Use GitHub's
private vulnerability reporting instead: open the repository's **Security** tab
and choose **Report a vulnerability**. The report reaches the maintainers
privately, and the conversation (triage, fix, advisory) stays in that thread
until an advisory is published.

A useful report says which component is affected (pipeline transform,
effector, a memory-store backend, the Console, a provider adapter, the Flex
Template launcher), the version or commit, what an attacker can do, and the
smallest reproduction you have. Proof-of-concept code is welcome; captured
secrets, customer data, or anything from a system you do not own are not.

You should hear back within seven days. If you do not, or if private
reporting is unavailable to you, contact a maintainer directly through their
GitHub profile and say only that you have a security report; do not include
the details in that first message.

## What we consider in scope

Anything that breaks a guarantee this project states it provides:

- **Intent provenance.** Executing a `ToolIntent` the pipeline did not mint
  while the effector runs with `verify_intents=require` (or executing one with
  a bad signature under `permissive`): a signature bypass, a verification
  ordering flaw, a dead-letter path that still publishes a `ToolResult`.
- **Exactly-once side effects.** A way to make the effector run a
  `side_effect=True` tool twice for one `intent_id`, or to make a forged
  result land on the keyed re-injection path against a live continuation.
- **Secret handling.** Any code path in this repository that copies a
  credential into keyed state, the outbox, the dead-letter channel, error
  records, logs, Flex Template parameters, or a command line.
- **State and replay safety.** A `--update`-compatible change that silently
  corrupts or discards keyed state, or a replay bundle that re-executes a side
  effect.
- **The usual software classes** in the package or the Console: injection,
  path traversal, unsafe deserialization, dependency vulnerabilities with a
  reachable path.

## What is out of scope

- **Broker and cloud access control.** Who may produce to or consume from a
  topic is enforced by Kafka or Pub/Sub, not by this library; a deployment
  with no ACLs is a configuration issue, and `docs/security.md` says so.
- **Deployments that have not turned the dial.** `verify_intents` defaults to
  `off` and the rollout is a documented operator sequence. "Unsigned intents
  execute with verification off" is the documented behavior, not a finding.
- **Secrets passed in `args_json` by user code.** The contract is that tool
  arguments never carry secrets; a tool that puts one there is outside the
  project's control.
- **Denial of service that needs a producer credential** on the outbox topic.
  Holding one already makes you a trusted party at the broker layer.
- Findings against the documentation website or the examples that require a
  deliberately misconfigured environment to reach.

If you are unsure, report it privately anyway and say you are unsure.

## Supported versions

The package has not yet published a release: `pyproject.toml` carries a
pre-release version and nothing is on PyPI. Until the first tagged release,
fixes land on `main` and there is nothing to backport to.

From the first stable release on, the policy follows the versioning rules in
[`CONTRIBUTING.md`](CONTRIBUTING.md) and [`docs/releasing.md`](docs/releasing.md):
semver over the frozen public surface, with fixes shipped as `PATCH` releases.

| Version | Receives security fixes |
| --- | --- |
| The latest `MINOR` of the current `MAJOR` | Yes, as a `PATCH` release |
| Earlier `MINOR` releases of the current `MAJOR` | No; upgrade to the latest `MINOR` (the public surface is frozen, so the upgrade is additive) |
| Earlier `MAJOR` releases | No |
| Pre-releases (`aN`, `bN`, `rcN`) | No; they exist to be superseded |

Supported Python versions are those `requires-python` names in
`pyproject.toml` (currently 3.11 and 3.12). Apache Beam and runner versions
are the ones the release's `uv.lock` pins; a vulnerability in a dependency is
handled by updating the pin in a `PATCH` release.

## Disclosure

Fixes are developed privately, released as a `PATCH`, and announced through a
GitHub security advisory and a `fixed` changelog fragment (the fragment type
registry in `changelog.d/README.md` is closed, and security fixes are fixes)
whose rendered entry names the affected versions and links the advisory. Reporters are credited in the advisory unless
they ask not to be. We ask for a reasonable embargo while a fix is prepared;
ninety days from acknowledgement is the default, shorter when a fix ships
sooner.
