`docs/security.md` now states that the repository's reference deployment — the docker compose
stack plus the effectively-once end-to-end gate's pipeline and effector pool — runs the effector
at `--verify-intents require` with a per-run signing key, and that the gate proves the setting
bites by injecting an unsigned intent that must be dead-lettered verbatim, never executed, and
never answered with a `ToolResult`. `EffectorConfig.verify_intents` still defaults to `off`.
