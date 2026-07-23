## Why

Python 3.10 is on borrowed time for this project's own dependency chain — `google-api-core` (pulled in via `apache-beam[gcp]`) already warns it will stop supporting 3.10 for `google.pubsub_v1` past 2026-10-04 — and it is actively costing us interpreter-specific bugs today: this week's CI run caught a real 3.10-only failure where `asyncio.TimeoutError` and the builtin `TimeoutError` are distinct classes (they were unified starting in 3.11), silently breaking a `pytest.raises(TimeoutError)` assertion only on the oldest supported interpreter. Dropping 3.10 removes both the looming dependency cliff and this class of footgun, at the cost of one CI matrix leg we were already trimming down to.

## What Changes

- Raise the minimum supported interpreter from `3.10` to `3.11`: **BREAKING** for any contributor or downstream consumer still on 3.10.
- `pyproject.toml`: `requires-python` becomes `>=3.11,<3.13`; `[tool.mypy]` `python_version` becomes `"3.11"`.
- `.github/workflows/ci.yml`: drop `"3.10"` from the `python-version` matrix, leaving `["3.11", "3.12"]` × `[ubuntu-latest, macos-latest]`.
- Documentation and process notes that state the supported Python range or CI matrix are updated to match: `README.md`, `docs/ci.md`, `openspec/project.md`.
- No `src/beam_agents` code changes — nothing in the current source tree depends on 3.10-only syntax or behavior; this is a floor-raise, not a feature change.

## Capabilities

### New Capabilities
<!-- None -->

### Modified Capabilities
- `repo-scaffolding`: the "Python and Beam version floors match project constraints" requirement's `requires-python` floor moves from `3.10` to `3.11`; the "GitHub Actions workflows mirror the testing tiers" requirement's `ci.yml` Python matrix drops `3.10`.

## Impact

- Affected files: `pyproject.toml`, `.github/workflows/ci.yml`, `README.md`, `docs/ci.md`, `openspec/project.md`.
- No runtime code, dependency, or public-API impact.
- Any contributor or CI runner still provisioning a 3.10 interpreter for this repo will need to upgrade to 3.11+; `uv sync` will refuse to resolve on 3.10 once `requires-python` is tightened, exactly as the existing "Install rejects Python 3.13" scenario already demonstrates for the upper bound.
