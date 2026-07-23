#!/usr/bin/env bash
set -euo pipefail

# Regenerate the committed protobuf bindings under src/beam_agents/_protos/.
#
# The generated message classes must be importable AND picklable under their
# real dotted path (`beam_agents._protos.beam_agents_pb2`): Beam pickles inlined
# `Create` data, side inputs, and DoFn closures at pipeline submission and on
# Dataflow workers, and protobuf derives a class's `__module__` from the proto
# file's path relative to the protoc include root. Compiling `protos/foo.proto`
# directly would stamp `__module__ = "foo_pb2"` (a bare, unimportable name).
#
# So we stage each `protos/*.proto` under `beam_agents/_protos/` inside a temp
# include root before compiling. The proto sources stay flat in `protos/`; only
# the compile-time layout is nested.

shopt -s nullglob
protos=(protos/*.proto)
if [[ ${#protos[@]} -eq 0 ]]; then
  exit 0
fi

out=src/beam_agents/_protos
mkdir -p "$out"

stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
pkg_dir="$stage/beam_agents/_protos"
mkdir -p "$pkg_dir"

rel_protos=()
for proto in "${protos[@]}"; do
  cp "$proto" "$pkg_dir/$(basename "$proto")"
  rel_protos+=("beam_agents/_protos/$(basename "$proto")")
done

uv run python -m grpc_tools.protoc \
  -I "$stage" \
  --python_out=src \
  --pyi_out=src \
  "${rel_protos[@]}"
