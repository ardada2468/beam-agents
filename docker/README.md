# Local integration stack

`compose.yaml` brings up Redpanda (Kafka-compatible), Redis, and a Flink
job/task manager pair for integration and semantics tests. Every image is
pinned by digest so the stack behaves identically across machines and CI
runners.

## Ports

All ports are shifted off their defaults to avoid clashing with services a
contributor may already be running locally:

| Service              | Container port | Host port |
|-----------------------|----------------|-----------|
| Redpanda (Kafka)      | 9092           | 19092     |
| Redis                 | 6379           | 16379     |
| Flink JobManager UI   | 8081           | 18081     |

## Usage

```sh
make compose-up    # docker compose -f docker/compose.yaml up -d, waits for healthy
make compose-down  # tears the stack down
```

Unit tests never require this stack. Only `integration`- and
`semantics`-marked tests do.
