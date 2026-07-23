#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
"$script_dir/gen_proto.sh"

git diff --exit-code -- 'protos/*_pb2.py' 'protos/*_pb2.pyi'
