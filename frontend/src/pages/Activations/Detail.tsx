/**
 * One activation, end to end.
 *
 * The page is organised around what the runtime actually records, in the order
 * someone investigating reads it: what it was, what it measured, what happened
 * in sequence, which attempts it took, every event with its attributes, what it
 * staged, what failed, what state it left behind, and how to replay it.
 *
 * Three things this page is careful about.
 *
 * A suspend and its resume are **one** activation with **two** attempts: trace
 * identity is `uuid5(entity_key, seq)`, and a resume runs under the suspended
 * activation's `seq`, so it recomputes the same trace ID with nothing carried on
 * the wire. The attempts table is where that shows.
 *
 * `null` is "not recorded" and `0` is a measured zero, everywhere, and the
 * failure-position panel is the sharpest case: those four scalars are absent on
 * the routes that cannot reach an activation context, so they render as "not
 * available" rather than as a claim that the activation failed at step 0.
 *
 * And when `complete_provenance` is false the page says so at the top. An
 * OTLP-only activation has no `ACTIVATION_START` — the OTLP encoding cannot
 * represent two events on one span — so start-vs-resume is unknown and the
 * record is partial. Presenting it as complete would be the one lie this console
 * exists to avoid.
 */

import { useQuery } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import { Link, useParams } from 'wouter';

import type { Column } from '@/components/ui';
import {
  Button,
  Chip,
  CodeBlock,
  CopyableId,
  DataTable,
  EmptyState,
  KeyValueGrid,
  Skeleton,
  SkeletonRows,
  StatTile,
  StatusChip,
  TileRow,
} from '@/components/ui';
import { ApiError, api, queryKeys } from '@/lib/api';
import type { ActivationDetail, AttemptSummary, IntentSummary } from '@/lib/api-types';
import {
  EM_DASH,
  formatBytes,
  formatCount,
  formatDuration,
  formatEntityKey,
  formatTimestamp,
  humanizeReason,
  shortId,
} from '@/lib/format';

import { EventList } from './EventList';
import { FailurePositionPanel } from './FailurePositionPanel';
import { Waterfall } from './Waterfall';
import type { FailurePosition } from './trace-attrs';
import { ATTR, KIND_LABEL, attrText, failurePositionFromEvent } from './trace-attrs';

import './activations.css';

function Panel({
  title,
  count,
  note,
  children,
}: {
  title: string;
  count?: number | null;
  note?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section className="panel" aria-label={title}>
      <div className="act-panel__head">
        <h2 className="act-panel__title">
          {title}
          {count !== undefined && count !== null ? (
            <span className="act-panel__count">{formatCount(count)}</span>
          ) : null}
        </h2>
        {note ? <p className="act-panel__note muted">{note}</p> : null}
      </div>
      {children}
    </section>
  );
}

/** One failure, from whichever record carried it. */
interface FailureItem {
  key: string;
  reason: string;
  errorType: string | null;
  detail: string | null;
  atMs: number | null;
  position: FailurePosition;
}

function failureItems(detail: ActivationDetail): FailureItem[] {
  const fromRecords = detail.errors.map((error, index) => ({
    key: `record-${index}`,
    reason: error.reason,
    errorType: error.error_type,
    detail: error.detail,
    atMs: error.event_time_ms,
    position: {
      step: error.failure_step,
      lastEvent: error.failure_last_event,
      stagedIntents: error.failure_staged_intents,
      llmCalls: error.failure_llm_calls,
      source: 'the activation error record',
    } satisfies FailurePosition,
  }));

  if (fromRecords.length > 0) return fromRecords;

  // No error record reached the store for this activation, but the trace did.
  // The ERROR event carries the same four scalars, and the panel names which
  // source it read so the two are never confused.
  return detail.events
    .filter((event) => event.event_type === 'ERROR')
    .map((event, index) => ({
      key: `event-${index}`,
      reason: attrText(event.attributes, ATTR.reason) ?? 'activation_error',
      errorType: attrText(event.attributes, ATTR.errorType),
      detail: null,
      atMs: event.start_ms,
      position: failurePositionFromEvent(event),
    }));
}

export default function ActivationDetailPage() {
  const params = useParams<{ entityKey: string; seq: string }>();
  const entityKey = params.entityKey ?? '';
  const seq = Number(params.seq);
  const valid = entityKey !== '' && Number.isInteger(seq);

  const query = useQuery({
    queryKey: queryKeys.activation(entityKey, seq),
    queryFn: () => api.activation(entityKey, seq),
    enabled: valid,
    // An activation the store does not have is an answer, not a hiccup. Retrying
    // a 404 only holds the skeleton on screen for another second.
    retry: (count, error) => !(error instanceof ApiError && error.status < 500) && count < 1,
  });

  const title = `${formatEntityKey(entityKey)} · seq ${valid ? seq : EM_DASH}`;

  if (!valid || query.isError) {
    const notFound = query.error instanceof ApiError && query.error.status === 404;
    return (
      <div className="page">
        <Breadcrumb />
        <div className="page-title">
          <h1>{title}</h1>
        </div>
        <div className="panel">
          <EmptyState
            title={notFound || !valid ? 'No such activation' : 'Could not load this activation'}
            body={
              notFound || !valid ? (
                <p>
                  The store holds no activation for this entity key and sequence number. It may have
                  aged out of the retention window, or the sequence may belong to another key. The
                  activation list shows everything currently stored.
                </p>
              ) : (
                <p>
                  The console API did not answer:{' '}
                  <span className="mono">{(query.error as Error).message}</span>
                </p>
              )
            }
            action={
              <Link href="/activations">
                <Button>Back to activations</Button>
              </Link>
            }
          />
        </div>
      </div>
    );
  }

  if (query.isPending || !query.data) {
    return (
      <div className="page">
        <Breadcrumb />
        <div className="page-title">
          <h1>{title}</h1>
        </div>
        <Skeleton height="88px" />
        <div className="panel">
          <SkeletonRows rows={8} columns={5} />
        </div>
      </div>
    );
  }

  const detail = query.data;
  const { summary, attempts, spans, events, intents, snapshot } = detail;
  const failures = failureItems(detail);
  const resumed = attempts.filter((attempt) => attempt.kind === 'resume').length;

  const attemptColumns: Column<AttemptSummary>[] = [
    {
      key: 'n',
      header: '#',
      numeric: true,
      render: (_row, index) => index + 1,
    },
    {
      key: 'kind',
      header: 'Kind',
      render: (row) =>
        row.kind === 'resume' ? (
          <Chip tone="suspended">Resume</Chip>
        ) : (
          <Chip plain tone={row.kind === 'unknown' ? 'warn' : 'neutral'}>
            {KIND_LABEL[row.kind]}
          </Chip>
        ),
    },
    {
      key: 'entry',
      header: 'Entry step',
      numeric: true,
      render: (row) => formatCount(row.entry_step_index),
    },
    { key: 'status', header: 'Status', render: (row) => <StatusChip status={row.status} /> },
    {
      key: 'start',
      header: 'Started',
      numeric: true,
      render: (row) => <span className="mono">{formatTimestamp(row.start_ms)}</span>,
    },
    {
      key: 'end',
      header: 'Ended',
      numeric: true,
      render: (row) => <span className="mono">{formatTimestamp(row.end_ms)}</span>,
    },
    {
      key: 'span',
      header: 'Span',
      render: (row) => (
        <CopyableId value={row.span_id} display={shortId(row.span_id, 8, 4)} label="span id" />
      ),
    },
  ];

  const intentColumns: Column<IntentSummary>[] = [
    {
      key: 'id',
      header: 'Intent ID',
      render: (row) => (
        <CopyableId
          value={row.intent_id}
          display={shortId(row.intent_id, 10, 4)}
          label="intent id"
        />
      ),
    },
    {
      key: 'tool',
      header: 'Tool',
      render: (row) => (
        <Link href={`/activations?tool=${encodeURIComponent(row.tool_name)}`} className="mono">
          {row.tool_name}
        </Link>
      ),
    },
    {
      key: 'kind',
      header: 'Kind',
      render: (row) => (
        <Chip plain tone={row.intent_kind === 'APPROVAL' ? 'info' : 'neutral'}>
          {row.intent_kind}
        </Chip>
      ),
    },
    { key: 'step', header: 'Step', numeric: true, render: (row) => formatCount(row.step_index) },
    {
      key: 'emitted',
      header: 'Emitted',
      numeric: true,
      render: (row) => <span className="mono">{formatTimestamp(row.emitted_at_ms)}</span>,
    },
    {
      key: 'expires',
      header: 'Expires',
      numeric: true,
      render: (row) => <span className="mono">{formatTimestamp(row.expires_at_ms)}</span>,
    },
  ];

  return (
    <div className="page">
      <Breadcrumb />

      <div className="page-title">
        <h1>{title}</h1>
        <div className="act-title-side">
          <StatusChip status={summary.status} />
          <Chip plain tone={summary.kind === 'unknown' ? 'warn' : 'neutral'}>
            {KIND_LABEL[summary.kind]}
          </Chip>
        </div>
      </div>

      {!summary.complete_provenance ? (
        <div className="act-notice">
          <Chip tone="warn">Partial record</Chip>
          <p>
            This activation was assembled only from{' '}
            <span className="mono">{summary.provenance.join(', ')}</span>, which carries no{' '}
            <span className="mono">ACTIVATION_START</span> — the OTLP encoding cannot represent two
            events on one span. Start-versus-resume is therefore unknown, attempt boundaries are not
            recorded, and anything below is the part that survived that path, not the complete
            record. Export through <span className="mono">console://</span> to get all of it.
          </p>
        </div>
      ) : null}

      <TileRow>
        <StatTile
          label="Wall time"
          value={formatDuration(summary.wall_ms)}
          meta={
            summary.wall_ms === null ? 'no ACTIVATION_END recorded yet' : 'START → END clock delta'
          }
        />
        <StatTile
          label="Total tokens"
          value={formatCount(summary.total_tokens)}
          meta={`${formatCount(summary.prompt_tokens)} prompt · ${formatCount(
            summary.completion_tokens,
          )} completion`}
        />
        <StatTile
          label="LLM calls"
          value={formatCount(summary.llm_calls)}
          meta={`${formatCount(summary.cache_hits)} cache hits`}
        />
        <StatTile
          label="Tool calls"
          value={formatCount(summary.tool_calls)}
          meta={`${formatCount(summary.tools.length)} distinct tools`}
        />
        <StatTile
          label="Intents"
          value={formatCount(summary.intents)}
          meta={`${formatCount(intents.length)} recorded in detail`}
        />
        <StatTile
          label="Errors"
          value={formatCount(summary.errors)}
          meta={
            summary.reasons.length > 0
              ? summary.reasons.map(humanizeReason).join(', ')
              : 'no reason recorded'
          }
        />
      </TileRow>

      <Panel
        title="Identity"
        note={
          <>
            Every figure above is counted from stored records. Only{' '}
            <span className="mono">wall_ms</span> is a duration.
          </>
        }
      >
        <div className="panel-body">
          <KeyValueGrid
            entries={[
              {
                key: 'entity',
                label: 'Entity key',
                value: (
                  <>
                    <CopyableId value={summary.entity_key} label="entity key" />{' '}
                    <span className="muted">({formatEntityKey(summary.entity_key)})</span>
                  </>
                ),
              },
              { key: 'seq', label: 'Sequence', value: <span className="mono">{summary.seq}</span> },
              {
                key: 'trace',
                label: 'Trace ID',
                value: (
                  <>
                    <CopyableId value={summary.trace_id} label="trace id" />{' '}
                    <Link
                      href={`/traces/${encodeURIComponent(summary.trace_id)}`}
                      className="act-nowrap"
                    >
                      Open trace →
                    </Link>
                  </>
                ),
              },
              {
                key: 'attempts',
                label: 'Attempts',
                value: (
                  <>
                    {formatCount(summary.attempts)}
                    <span className="muted">
                      {resumed > 0
                        ? ` — one activation, ${resumed} of them a resume`
                        : ' — no resume recorded'}
                    </span>
                  </>
                ),
              },
              {
                key: 'started',
                label: 'Started',
                value: <span className="mono">{formatTimestamp(summary.started_ms)}</span>,
              },
              {
                key: 'ended',
                label: 'Ended',
                value: <span className="mono">{formatTimestamp(summary.ended_ms)}</span>,
              },
              {
                key: 'model',
                label: 'Model',
                value: summary.model ? (
                  <Link
                    href={`/activations?model=${encodeURIComponent(summary.model)}`}
                    className="mono"
                  >
                    {summary.model}
                  </Link>
                ) : (
                  <span className="faint">{EM_DASH}</span>
                ),
              },
              {
                key: 'tools',
                label: 'Tools',
                value:
                  summary.tools.length > 0 ? (
                    <span className="act-chips">
                      {summary.tools.map((tool) => (
                        <Link key={tool} href={`/activations?tool=${encodeURIComponent(tool)}`}>
                          <Chip plain tone="neutral">
                            {tool}
                          </Chip>
                        </Link>
                      ))}
                    </span>
                  ) : (
                    <span className="faint">{EM_DASH}</span>
                  ),
              },
              {
                key: 'reasons',
                label: 'Error reasons',
                value:
                  summary.reasons.length > 0 ? (
                    <span className="act-chips">
                      {summary.reasons.map((reason) => (
                        <Link key={reason} href={`/errors?reason=${encodeURIComponent(reason)}`}>
                          <Chip tone="error">{humanizeReason(reason)}</Chip>
                        </Link>
                      ))}
                    </span>
                  ) : (
                    <span className="faint">{EM_DASH}</span>
                  ),
              },
              {
                key: 'provenance',
                label: 'Ingested via',
                value: (
                  <span className="act-chips">
                    {summary.provenance.map((source) => (
                      <Chip key={source} plain tone="neutral">
                        {source}
                      </Chip>
                    ))}
                    {summary.complete_provenance ? null : (
                      <Chip tone="warn">start-vs-resume unknown</Chip>
                    )}
                  </span>
                ),
              },
            ]}
          />
        </div>
      </Panel>

      <Panel
        title="Sequence"
        count={spans.length}
        note={
          <>
            Order and nesting are recorded; elapsed time per span is not. Spans are zero-width by
            design — <span className="mono">start_ms == end_ms</span> — so every rule below is the
            same length and nothing here is scaled by time.
          </>
        }
      >
        {spans.length === 0 ? (
          <EmptyState
            title="No spans recorded"
            body="Spans appear once the activation's trace events reach the store. An activation still in flight, or one whose trace export was dropped, has none."
          />
        ) : (
          <Waterfall spans={spans} />
        )}
      </Panel>

      <Panel
        title="Attempts"
        count={attempts.length}
        note={
          <>
            A suspend and its resume are one activation with two attempts: trace identity is{' '}
            <span className="mono">uuid5(entity_key, seq)</span>, so a resume recomputes the same
            trace ID.
          </>
        }
      >
        <DataTable
          caption="Attempts within this activation"
          columns={attemptColumns}
          rows={attempts}
          rowKey={(row, index) => `${row.span_id}-${index}`}
          empty={
            <EmptyState
              title="No attempts recorded"
              body="Attempt boundaries come from ACTIVATION_START events, which only the native ingest path carries. An activation imported over OTLP has none."
            />
          }
        />
      </Panel>

      <Panel
        title="Events"
        count={events.length}
        note="Every recorded event, with its complete attribute map. Expand a row to read it."
      >
        {events.length === 0 ? (
          <EmptyState
            title="No trace events stored"
            body={
              <p>
                Events arrive when a pipeline exports traces to this console. Set{' '}
                <span className="mono">traces_to=&quot;console://…&quot;</span> with{' '}
                <span className="mono">ConsoleSinkResolver</span>, or point an existing OTLP
                exporter at the console&rsquo;s OTLP endpoint.
              </p>
            }
          />
        ) : (
          <EventList events={events} />
        )}
      </Panel>

      <Panel
        title="Staged intents"
        count={intents.length}
        note="Tool calls this activation handed to the effector boundary rather than running inline."
      >
        <DataTable
          caption="Intents staged by this activation"
          columns={intentColumns}
          rows={intents}
          rowKey={(row) => row.intent_id}
          empty={
            <EmptyState
              title="No intents staged"
              body="Intents appear here when the agent stages a tool call for the effector — an approval request, or any tool routed through WriteIntents. This activation staged none."
            />
          }
        />
      </Panel>

      <Panel title="Failures" count={failures.length}>
        {failures.length === 0 ? (
          summary.errors > 0 ? (
            // The rollup counted errors this view did not receive. Records are
            // at-least-once and the rollup is recomputed on every write, so the
            // record may still be in flight — either way, claiming there were no
            // errors would contradict the count on the tile above.
            <EmptyState
              title={`${formatCount(summary.errors)} ${
                summary.errors === 1 ? 'error is' : 'errors are'
              } counted for this activation, but no error record came with it`}
              body={
                <p>
                  The count comes from the activation rollup, which the store derives from the
                  events it holds. The error record itself either has not arrived yet — records are
                  at-least-once — or is filed against this entity key without this sequence number.
                  The Errors page lists everything the store has.
                </p>
              }
              action={
                <Link
                  href={
                    summary.reasons[0]
                      ? `/errors?reason=${encodeURIComponent(summary.reasons[0])}`
                      : '/errors'
                  }
                >
                  <Button>Open Errors</Button>
                </Link>
              }
            />
          ) : (
            <EmptyState
              title="No errors attributed to this activation"
              body="An error record appears here when the runtime dead-letters an activation — a raised exception, an activation timeout, an exhausted budget, an expired approval, or a TTL that wiped a live suspension."
            />
          )
        ) : (
          <div className="act-failures">
            {failures.map((failure) => (
              <article key={failure.key} className="act-failure">
                <div className="act-failure__head">
                  <Chip tone="error">{humanizeReason(failure.reason)}</Chip>
                  {failure.errorType ? (
                    <span className="mono">{failure.errorType}</span>
                  ) : (
                    <span className="faint">no error type recorded</span>
                  )}
                  <span className="muted act-failure__time">{formatTimestamp(failure.atMs)}</span>
                  <Link
                    href={`/errors?reason=${encodeURIComponent(failure.reason)}`}
                    className="act-failure__link"
                  >
                    All {humanizeReason(failure.reason)} errors →
                  </Link>
                </div>
                {failure.detail ? (
                  <p className="act-failure__detail mono">{failure.detail}</p>
                ) : (
                  <p className="act-failure__detail muted">
                    No detail string on this record — the trace event carries the position below,
                    and the full message is on the error record for this entity key.
                  </p>
                )}
                <FailurePositionPanel position={failure.position} />
              </article>
            ))}
          </div>
        )}
      </Panel>

      <Panel
        title="Snapshot"
        note="Counts only. The state image itself stays opaque to the console."
      >
        {snapshot === null ? (
          <EmptyState
            title="No snapshot recorded"
            body="A snapshot is written when an activation suspends and its state is checkpointed. A completed activation that never suspended has none."
          />
        ) : (
          <div className="panel-body">
            <KeyValueGrid
              entries={[
                {
                  key: 'at',
                  label: 'Snapshot at',
                  value: <span className="mono">{formatTimestamp(snapshot.snapshot_at_ms)}</span>,
                },
                {
                  key: 'schema',
                  label: 'State schema version',
                  value: formatCount(snapshot.state_schema_version),
                },
                {
                  key: 'memory',
                  label: 'Memory',
                  value: `${formatCount(snapshot.memory_entries)} entries · ${formatBytes(
                    snapshot.memory_bytes,
                  )}`,
                },
                {
                  key: 'cache',
                  label: 'LLM cache entries',
                  value: formatCount(snapshot.llm_cache_entries),
                },
                {
                  key: 'continuation',
                  label: 'Continuation',
                  value:
                    snapshot.continuation_step_index === null ? (
                      <span className="faint">{EM_DASH} not suspended at a step</span>
                    ) : (
                      <>
                        step {formatCount(snapshot.continuation_step_index)} ·{' '}
                        <span className="mono">
                          {snapshot.continuation_adapter || 'no adapter'}
                        </span>{' '}
                        · deadline{' '}
                        <span className="mono">
                          {formatTimestamp(snapshot.continuation_deadline_ms)}
                        </span>
                      </>
                    ),
                },
                {
                  key: 'pending',
                  label: 'Pending intents',
                  value:
                    snapshot.pending_intent_ids.length === 0 ? (
                      <span className="muted">0</span>
                    ) : (
                      <span className="act-chips">
                        {snapshot.pending_intent_ids.map((id) => (
                          <CopyableId
                            key={id}
                            value={id}
                            display={shortId(id, 10, 4)}
                            label="intent id"
                          />
                        ))}
                      </span>
                    ),
                },
                {
                  key: 'request',
                  label: 'Request ID',
                  value: snapshot.request_id ? (
                    <CopyableId value={snapshot.request_id} label="request id" />
                  ) : (
                    <span className="faint">{EM_DASH}</span>
                  ),
                },
              ]}
            />
          </div>
        )}
      </Panel>

      <Panel title="Replay" note="Reproduces this activation offline from a captured bundle.">
        <div className="panel-body">
          {detail.replay_command ? (
            <CodeBlock code={detail.replay_command} label="replay command" />
          ) : (
            <p className="muted">
              No replay command is available for this activation. It needs a captured snapshot and
              trace bundle; export one with <span className="mono">beam-agents-capture</span>, or
              use the Connect page to import a bundle you already have.
            </p>
          )}
        </div>
      </Panel>
    </div>
  );
}

function Breadcrumb() {
  return (
    <nav className="act-crumbs" aria-label="Breadcrumb">
      <Link href="/activations">Activations</Link>
      <span aria-hidden="true">/</span>
      <span className="muted">Detail</span>
    </nav>
  );
}
