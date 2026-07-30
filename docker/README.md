# Local integration stack

`compose.yaml` brings up Redpanda (Kafka-compatible), Redis, and a Flink
job/task manager pair for integration and semantics tests. Every image is
pinned by digest so the stack behaves identically across machines and CI
runners.

## Ports

All ports are shifted off their defaults to avoid clashing with services a
contributor may already be running locally:

| Service                  | Container port | Host port |
|--------------------------|----------------|-----------|
| Redpanda (Kafka)         | 9092           | 19092     |
| Redis                    | 6379           | 16379     |
| Flink JobManager UI      | 8081           | 18081     |
| Beam job service         | 8099           | 18099     |
| Beam artifact service    | 8098           | 18098     |
| Beam expansion service   | 8097           | 18097     |

## The Beam-on-Flink path

`flink-jobserver` and `beam-sdk-harness` exist for the two Beam-on-Flink
suites: the effectively-once end-to-end semantics gate
(`make test-semantics`) and the adapter conformance matrix's Flink leg
(`make test-conformance-flink`). Nothing else in the suite submits a Beam
job, and neither service is needed by the rest of the `integration` tier —
which is why CI's base integration job brings up only the non-Flink services
via `make compose-up-core`.

Three constraints are load-bearing and were established empirically — changing
any of them silently breaks the gate:

- **`beam-sdk-harness` runs in the TaskManager's network namespace.** Beam hands
  an external worker pool the TaskManager's endpoints as `localhost:<port>`, so
  the pool is unreachable from any other network. Killing the TaskManager
  therefore also breaks the harness, which must be restarted with it.
- **The harness image is built, not pulled.** The stock Beam SDK image ships
  protobuf runtime 5.29.5 while this repo's committed `_pb2.py` is 6.x gencode,
  so `beam_agents._protos` cannot be imported there at all. See
  `sdk-harness.Dockerfile` for the pin and the reasoning.
- **The TaskManager runs with a 1 GiB metaspace.** At Flink's default the JVM
  exhausts metaspace after a handful of portable job submissions, and the
  failure surfaces to the client as a misleading gRPC negotiation error.

Cross-language IO (`ReadFromKafka`/`WriteToKafka`) does **not** work on this
stack: those transforms need a Java SDK harness, whose environment defaults to
`DOCKER`, and the Flink image has no docker CLI. Pipelines here must use
Python-native sources and sinks.

## Usage

```sh
make compose-up       # full stack: docker compose up -d --wait --build
make compose-up-core  # non-Flink services only (Redpanda, Redis, GCP emulators)
make compose-down     # tears the stack down
make compose-logs     # collect service logs + Flink diagnostics (LOGS_DIR=...)
```

Unit tests never require this stack. Only `integration`- and
`semantics`-marked tests do.
