You can now get one entity's runtime state out of a running pipeline and re-run
its activation offline. Publish an `AgentEnvelope` carrying the new
`export_request` payload to the events topic and `RunAgent` answers with a
`StateSnapshot` on its new `.snapshots` output (routed by `AgentConfig
.snapshots_to`, exactly like `traces_to`), without running an activation or
mutating a single state cell. The new `beam-agents-replay` console script then
reconstructs that activation from the snapshot, its trace stream, and the
triggering envelope, and re-runs it locally against a provider that holds no
transport: every model call is served from the snapshot's replay cache, a miss
fails loudly naming the cache key instead of reaching a network, and the re-run
is diffed against the traced record with scriptable exit codes (0 reproduced,
1 diverged, 2 usage or version refusal, 3 irreproducible). Snapshots from older
schema versions migrate on load through the same migrations the pipeline
applies; newer ones are refused. See [docs/replay.md](https://github.com/ardada2468/beam-agents/blob/main/docs/replay.md).
