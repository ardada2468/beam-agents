#!/usr/bin/env bash
set -euo pipefail

shopt -s nullglob
protos=(protos/*.proto)
if [[ ${#protos[@]} -eq 0 ]]; then
  exit 0
fi

uv run python -m grpc_tools.protoc \
  -I protos \
  --python_out=protos \
  --pyi_out=protos \
  "${protos[@]}"
