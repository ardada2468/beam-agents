# Tasks: drop-python-3-10-support

## 1. Update the version floor and CI matrix

- [x] 1.1 `pyproject.toml`: change `requires-python` from `">=3.10,<3.13"` to `">=3.11,<3.13"`, and `[tool.mypy]` `python_version` from `"3.10"` to `"3.11"`.
- [x] 1.2 `.github/workflows/ci.yml`: drop `"3.10"` from the `python-version` matrix, leaving `["3.11", "3.12"]`.
- [x] 1.3 Run `uv lock` if the lockfile records a `requires-python` marker, and confirm `uv sync --all-groups` still resolves cleanly on 3.11/3.12.

## 2. Update docs and process notes to match

- [x] 2.1 `README.md`: update the "Requires Python" line to `>=3.11,<3.13`.
- [x] 2.2 `docs/ci.md`: update the CI matrix description from `3.10–3.12` to `3.11–3.12`.
- [x] 2.3 `openspec/project.md`: update the Python version constraint (`Python ≥ 3.10` → `Python ≥ 3.11`) and the CI matrix mention (`3.10–3.12` → `3.11–3.12`).

## 3. Verify

- [x] 3.1 `openspec validate drop-python-3-10-support --type change --strict` passes.
- [x] 3.2 Confirm no other references to Python `3.10` remain outside `openspec/changes/archive/` (historical archive content is left untouched).
