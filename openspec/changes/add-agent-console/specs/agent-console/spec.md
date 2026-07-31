## ADDED Requirements

### Requirement: The console stores telemetry records idempotently

The console SHALL persist `TraceEvent`, `ActivationErrorRecord`, and `StateSnapshot` records in a
single WAL-mode SQLite database identified by a filesystem path. Trace events SHALL be keyed on
`(trace_id, span_id, event_type)` — the dedup key the runtime publishes for at-least-once trace
delivery — and re-ingesting a record with an existing key SHALL merge its attributes rather than
inserting a duplicate row or discarding the newer copy.

The store SHALL create and migrate its own schema on open, so a fresh database file and an existing
one are both valid starting states.

#### Scenario: The same event ingested twice yields one row

- **WHEN** a `TraceEvent` is ingested, and then an identical copy of it is ingested again
- **THEN** the store holds exactly one event row for that `(trace_id, span_id, event_type)`, and
  every query counts it once

#### Scenario: A later copy carrying more attributes wins

- **WHEN** an event is ingested with a subset of its attributes, and a later copy of the same event
  carries additional attributes
- **THEN** the stored event carries the union of both attribute sets, and still occupies one row

#### Scenario: Opening a fresh path creates a usable store

- **WHEN** the store is opened on a path that does not exist
- **THEN** the database file and its full schema are created, and records can be written and read
  back without any further setup

### Requirement: Activation rollups are derived, never written

The console SHALL derive each activation's summary — status, kind, attempt count, token totals,
LLM-call count, tool-call count, intent count, and error count — from the trace events belonging to
`(entity_key, seq)`, recomputing it whenever an event for that activation is written. No producer
SHALL supply a rollup.

An activation with no `ACTIVATION_END` event SHALL be reported as in flight rather than assigned a
terminal status.

#### Scenario: A rollup is correct after a partial arrival

- **WHEN** only some of an activation's trace events have been ingested
- **THEN** the activation's rollup reflects exactly those events, and it is reported as in flight

#### Scenario: A rollup corrects itself when the rest arrives

- **WHEN** the remaining events for that activation are ingested afterwards, in any order
- **THEN** the rollup matches the rollup that would result from ingesting every event at once

#### Scenario: A suspend and resume are one activation

- **WHEN** an activation suspends and later resumes, producing two `ACTIVATION_START` events and
  two attempts under one `(entity_key, seq)`
- **THEN** the console reports one activation with two attempts, not two activations

### Requirement: The console serves a read-only HTTP API over the store

The console SHALL expose an HTTP API providing: an aggregate overview, a filterable and
cursor-paginated activation list, activation and trace detail including the span tree, an error
list grouped by the runtime's `reason` vocabulary, per-model and per-tool breakdowns, the pending
approval queue, per-entity-key timelines, and attribute search. Every endpoint SHALL be read-only
with respect to agent state; no endpoint SHALL write to a running pipeline.

The API SHALL expose a liveness endpoint that reports healthy without requiring any ingest to have
occurred.

#### Scenario: An empty store answers every endpoint

- **WHEN** the API is queried against a store with no records
- **THEN** every endpoint returns a well-formed empty result rather than an error, and the liveness
  endpoint reports healthy

#### Scenario: Filters narrow the activation list

- **WHEN** the activation list is requested with a filter on entity key, status, model, tool,
  error reason, or time range
- **THEN** exactly the activations matching every supplied filter are returned, in a stable order,
  with a cursor that resumes the same ordering

### Requirement: The console streams new records to connected clients

The console SHALL expose a server-sent-events endpoint that emits an event whenever a record is
ingested, so an open UI reflects a running pipeline without polling. A client that disconnects
SHALL NOT affect ingest, and a slow client SHALL be dropped rather than allowed to block a writer.

#### Scenario: An ingested record reaches an open stream

- **WHEN** a client is connected to the stream and a trace event is ingested
- **THEN** the client receives an event identifying the affected activation

#### Scenario: A disconnected client does not block ingest

- **WHEN** a connected client disconnects mid-stream while records continue to arrive
- **THEN** ingest continues to succeed and the remaining clients keep receiving events

### Requirement: The console is started by a documented command with an offline default

The console SHALL ship a `beam-agents-console` console_script that starts the service, with every
flag falling back to an environment variable, matching the effector CLI's convention. It SHALL exit
`2` on a configuration error naming the offending value, and `0` on clean shutdown.

The service SHALL bind to localhost by default, and SHALL run with no broker, no cloud project, and
no network egress when no ingest source is configured.

#### Scenario: The service starts with only a database path

- **WHEN** the console is started with a database path and no other configuration
- **THEN** it serves the API and the liveness endpoint, and requires no external service

#### Scenario: An unusable configuration exits with a named cause

- **WHEN** the console is started with a malformed ingest URI or an unwritable database path
- **THEN** the process exits `2` and reports which value was rejected and why

### Requirement: The console retains records for a bounded window

The console SHALL prune records older than a configurable retention window, and SHALL report the
current record counts and the effective retention window through the API so an operator can see
what the store holds.

#### Scenario: Records outside the window are pruned

- **WHEN** retention is configured and records older than the window exist
- **THEN** those records are removed and records inside the window are retained

### Requirement: The console package adds no required dependency

Importing `beam_agents.console` SHALL succeed with none of the console's optional dependencies
installed, and `import beam_agents` SHALL be unaffected by the package's existence. Every optional
client SHALL be imported inside the function or constructor that needs it, and constructing a
component whose dependency is missing SHALL raise an error naming the extra to install.

#### Scenario: The core install is unchanged

- **WHEN** `beam_agents` is imported in an environment with no console extras installed
- **THEN** the import succeeds, and importing `beam_agents.console` also succeeds

#### Scenario: A missing extra is reported actionably

- **WHEN** a source whose client library is not installed is constructed
- **THEN** the error names the extra that provides it, rather than surfacing an ImportError from a
  transitive module

### Requirement: The console ships as a runnable container

The repository SHALL provide a container image and a compose stack that start the console together
with a demo pipeline generating the runtime's full event vocabulary, so a single compose command
lands on a populated console. The image SHALL run as a non-root user, expose a healthcheck, and
persist its database in a named volume across restarts.

#### Scenario: One command yields a populated console

- **WHEN** the console compose stack is started from a clean checkout
- **THEN** the service becomes healthy, the demo pipeline's activations, traces, and errors are
  visible through the API, and the UI is served from the same origin

#### Scenario: The database survives a restart

- **WHEN** the stack is restarted without removing its volumes
- **THEN** previously ingested records are still present
