## Why

`beam_agents` cannot be imported on a stock Dataflow worker. The
`verify-live-infrastructure` run launched the `--update` compatibility gate against a real Dataflow
project and every SDK worker crashed on startup:

```
Error message from worker: generic::aborted: SDK harness sdk-0-0 disconnected.
Could not load main session.
  File ".../site-packages/beam_agents/__init__.py", line 30, in <module>
  File ".../site-packages/beam_agents/_protos/__init__.py", line 8, in <module>
  File ".../site-packages/beam_agents/_protos/beam_agents_pb2.py", line 12, in <module>
google.protobuf.runtime_version.VersionError: Detected mismatched Protobuf Gencode/Runtime
major versions when loading beam_agents/_protos/beam_agents.proto:
gencode 6.33.5 runtime 5.29.5. Same major version is required.
```

The committed `_pb2.py` bindings are 6.x gencode. Protobuf requires gencode and runtime to share a
**major** version. `pyproject.toml` declared a bare, unbounded `"protobuf"`, which any preinstalled
protobuf satisfies — and `apache/beam_python3.11_sdk:2.72.0`, the base image Dataflow workers run,
ships runtime **5.29.5**. pip therefore left 5.29.5 in place and the package became unimportable.

The project already knew this. `docker/sdk-harness.Dockerfile` has pinned `protobuf==6.33.6` since it
was written, and its header comment describes this precise failure — including the same
`VersionError` — as one of the two reasons that image is built rather than pulled. That constraint
was never propagated to the package's own dependency metadata, so it protected only the Flink
harness. Every Flink leg passed; Dataflow, which uses the same base image without the pin, did not.

This was invisible to every existing gate: the offline, unit, integration, semantics and conformance
tiers all run in environments where uv resolves protobuf 6.x, and the Flink harness bakes in the pin.
Only a real Dataflow launch reaches a worker that supplies its own protobuf.

## What Changes

- `pyproject.toml`'s core dependency `"protobuf"` becomes `"protobuf>=6,<7"`, matching the gencode's
  major version. `<7` matches Beam's own `protobuf<7.0.0.dev0` bound, so the two compose.
- `uv.lock` is regenerated.

## Capabilities

### Modified Capabilities

None. No specified behavior changes — the package's declared requirements are corrected so the
runtime it already requires is actually installed.

## Impact

- **Modified:** `pyproject.toml` (one dependency), `uv.lock`.
- **Runtime:** any environment that previously resolved protobuf < 6 alongside this package now
  installs a 6.x runtime. Environments already on 6.x are unaffected.
- **Gates:** unblocks the Dataflow tier, which could not start a worker. `docker/sdk-harness.Dockerfile`'s
  explicit `protobuf==6.33.6` pin is now redundant with the package metadata but is deliberately left
  in place: it pins an exact version for image reproducibility, which the range does not.
- **Severity:** this is a shipped-artifact defect. A user installing the wheel into a Dataflow
  environment would hit an unimportable package with a traceback pointing at protobuf internals
  rather than at anything they control.
