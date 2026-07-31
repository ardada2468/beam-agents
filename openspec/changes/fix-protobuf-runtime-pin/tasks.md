# Tasks: fix-protobuf-runtime-pin

## 1. Fix

- [x] 1.1 Bound the core `protobuf` dependency to `>=6,<7` in `pyproject.toml`, with the reason
  recorded inline (gencode major must match runtime major; the Dataflow worker base image supplies
  5.29.5).
- [x] 1.2 Regenerate `uv.lock`.

## 2. Verify

- [x] 2.1 `uv sync --locked --all-groups` succeeds.
- [x] 2.2 A Dataflow worker imports `beam_agents` — evidenced by `make test-dataflow` getting past
  worker startup (previously every SDK worker crashed in `_protos/__init__.py`).
- [x] 2.3 `make lint`, `make type`, `make test-unit`, `make test-semantics-offline` unaffected.
- [x] 2.4 `docker/sdk-harness.Dockerfile`'s exact `protobuf==6.33.6` pin left in place — the range in
  package metadata and the exact pin in the image serve different purposes (compatibility vs. image
  reproducibility).
