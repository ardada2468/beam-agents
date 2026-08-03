## ADDED Requirements

### Requirement: The UI presents activations as the primary object

The UI SHALL present a filterable, sortable list of activations keyed by `(entity_key, seq)`,
showing status, kind, model, token totals, LLM and tool call counts, error count, and the wall
time between the activation's start and end events. Selecting an activation SHALL open a detail
view showing every trace event it produced, each event's full attribute map, its staged intents,
its attempts, and the errors attributed to it.

A suspend and its resume SHALL be presented as one activation with multiple attempts, matching the
runtime's one-trace-per-`(entity_key, seq)` identity.

#### Scenario: An activation's detail shows every recorded event

- **WHEN** an activation with LLM calls, tool calls, staged intents, and an error is opened
- **THEN** every one of its trace events is listed with its type, step index, timestamps, and
  complete attribute map

#### Scenario: A resumed activation shows both attempts

- **WHEN** an activation that suspended and resumed is opened
- **THEN** both attempts are shown under one activation, with the resume identified as such

### Requirement: The span view encodes sequence, not fabricated duration

Because the runtime's spans are zero-width by design, the UI SHALL render an activation's spans as
an ordered, nested sequence whose visual extent does not encode duration. It SHALL display
durations only where a real measurement exists, and SHALL state that no measurement exists rather
than drawing a proportional bar where none does.

#### Scenario: Zero-width spans are not drawn as durations

- **WHEN** an activation whose spans all satisfy `start_ms == end_ms` is viewed
- **THEN** the spans are shown in order and nesting with no width encoding elapsed time, and no
  per-span duration is claimed

#### Scenario: Real measurements are shown as numbers

- **WHEN** an activation carries recorded token counts, call counts, or a start-to-end wall time
- **THEN** those values are displayed as explicit numbers attributed to their source

### Requirement: Errors are grouped by the runtime's own vocabulary

The UI SHALL group errors by the runtime's closed `reason` vocabulary and by `error.type`, showing
occurrence counts over time and allowing a drill-down to the failing activations. Where an error
carries failure-position attributes, the UI SHALL render them as a dedicated panel naming the step,
last event, staged intent count, and LLM call count at the point of failure.

#### Scenario: Errors are grouped by reason

- **WHEN** errors with several distinct reasons are present
- **THEN** they are grouped by reason with per-group counts, and selecting a group lists exactly
  that group's failing activations

#### Scenario: Failure position is surfaced when recorded

- **WHEN** an error carrying failure-position attributes is opened
- **THEN** the step, last event, staged intent count, and LLM call count at failure are shown as
  labelled fields

### Requirement: The UI reports model, tool, and approval activity

The UI SHALL provide per-model views of token spend, call volume, cache-hit ratio, retry attempts,
and circuit state; per-tool views of call volume and failure rate; and a human-approval view
listing pending intents with their deadlines, escalations, and recorded decisions. Every figure
SHALL be derived from stored record attributes.

#### Scenario: Model usage is broken down per model

- **WHEN** activations using more than one model are stored
- **THEN** token spend, call volume, and cache-hit ratio are reported separately per model

#### Scenario: Pending approvals are listed with their deadlines

- **WHEN** activations are suspended awaiting approval
- **THEN** each pending intent is listed with its deadline and any recorded decision

### Requirement: The UI reflects a running pipeline without a reload

The UI SHALL consume the console's live stream so that activations, errors, and counts update
while a pipeline runs, without the operator reloading the page, and SHALL indicate whether the live
connection is currently established.

#### Scenario: A new activation appears while the page is open

- **WHEN** a pipeline produces a new activation while the UI is open
- **THEN** it appears in the list without a reload

#### Scenario: A lost live connection is visible

- **WHEN** the live connection drops
- **THEN** the UI indicates that it is no longer live, rather than silently showing stale data

### Requirement: The UI is usable, accessible, and works in both themes

The UI SHALL be operable by keyboard with visible focus, SHALL respect the reduced-motion
preference, SHALL render correctly in both light and dark themes, and SHALL remain usable at a
narrow mobile viewport. Empty states SHALL name the action that would populate them rather than
reporting only that there is no data.

#### Scenario: An empty console explains how to send it data

- **WHEN** the UI is opened against a store with no records
- **THEN** each view states what would populate it and how to configure that ingest path

#### Scenario: Both themes and a narrow viewport render correctly

- **WHEN** any page is rendered in light theme, in dark theme, and at a narrow mobile width
- **THEN** content remains legible and no layout overflows the viewport horizontally
