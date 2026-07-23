## Context

The `repo-scaffolding` capability pins the supported interpreter range and the CI test matrix. Both currently include Python 3.10. This change raises the floor to 3.11 for the reasons in the proposal (dependency-chain EOL pressure, a real 3.10-only `asyncio.TimeoutError`/`TimeoutError` divergence bug caught in CI this week). There is no architectural decision to make here — it's a version-floor bump across a handful of already-identified files.

## Goals / Non-Goals

**Goals:**
- `uv sync` refuses to resolve on Python 3.10, matching the existing upper-bound behavior for 3.13.
- CI no longer runs a 3.10 leg; `ci.yml`'s matrix is `["3.11", "3.12"]` × `[ubuntu-latest, macos-latest]`.
- Every doc/spec that states the supported range or CI matrix agrees with the new floor.

**Non-Goals:**
- No change to the upper bound (`<3.13`) — Beam's Python SDK support for 3.13 is a separate, unrelated question.
- No source-code changes — nothing in `src/beam_agents` uses 3.10-only syntax, and no 3.11+-only syntax is being adopted as part of this change either.
- No change to dependency versions themselves (e.g. `apache-beam[gcp]>=2.60` floor is untouched).

## Decisions

### D1. Floor moves to 3.11, not 3.12
3.11 is the oldest interpreter this repo already exercises in CI beyond the one being dropped, so raising the floor to 3.11 removes 3.10 with zero loss of tested coverage (3.11 and 3.12 remain exactly as tested as before). Jumping straight to 3.12 would drop a currently-supported, currently-tested version for no stated reason. *Alternative rejected:* floor at 3.12 — no justification for dropping 3.11 support was given, and doing so would be a second, separate breaking change bundled into this one.

## Risks / Trade-offs

- **Breaking change for any contributor or CI runner still on 3.10** → Mitigation: `uv sync` already fails loudly and clearly on an out-of-range interpreter (this is the same UX as the existing 3.13 upper-bound rejection), so the failure mode is well-understood and not silent.
- **Any external consumer pinning to `>=3.10` in their own tooling** → Out of scope: this is a v0.x internal repo (`version = "0.0.0"`), not a published package with external consumers to negotiate a deprecation window with.

## Open Questions

None.
