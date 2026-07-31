`beam-agents` now requires `protobuf>=6,<7`, matching the major version of its generated protobuf
bindings. Previously the requirement was unbounded, so environments that already provided an older
protobuf — including stock Dataflow worker images, which ship 5.29.5 — kept it and `import
beam_agents` failed with a gencode/runtime major-version mismatch.
