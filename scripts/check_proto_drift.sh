#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
"$script_dir/gen_proto.sh"

git diff --exit-code -- 'src/beam_agents/_protos/*_pb2.py' 'src/beam_agents/_protos/*_pb2.pyi'
