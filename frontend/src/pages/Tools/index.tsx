/**
 * Tools — what the agents called inline, and what they staged for the effector.
 *
 * The page exists mostly to make one runtime invariant legible: a tool reaches
 * the world by exactly one of two routes, and they are not interchangeable.
 *
 * - `TOOL_CALL` is an **inline, read-only** call. It runs inside the activation,
 *   its result feeds the next model call, and a retried bundle runs it again.
 * - `INTENT_EMITTED` is a **staged, side-effecting** intent. The activation does
 *   not execute it; it stages a `ToolIntent` on the outbox, which is committed
 *   only if the activation succeeds and executed afterwards by the effector.
 *
 * So the two counts are never summed into a single "tool calls" figure here.
 * A tool with intents and no inline calls never ran inside the pipeline at all,
 * and a reader who cannot see that difference cannot reason about what a replay
 * or a retry will repeat.
 *
 * `failure_ratio: null` is "nothing recorded an outcome", not "nothing failed" —
 * so it renders as an em dash, like every other unrecorded measurement.
 */

import { useQuery } from '@tanstack/react-query';
import { useMemo, useState } from 'react';

import type { Column } from '@/components/ui';
import {
  Button,
  Chip,
  CopyableId,
  DataTable,
  EmptyState,
  KeyValueGrid,
  Select,
  SkeletonRows,
  StatTile,
  TileRow,
} from '@/components/ui';
import { ApiError, DEFAULT_WINDOW_MS, api, queryKeys } from '@/lib/api';
import type { ToolSummary } from '@/lib/api-types';
import { EM_DASH, formatCount, formatRatio, formatRelative, formatTimestamp } from '@/lib/format';
import { useWindowAnchor } from '@/lib/window-anchor';

/** Lookback options. `0` means "everything the store still holds". */
const WINDOWS = [
  { value: String(3_600_000), label: 'Last hour' },
  { value: String(6 * 3_600_000), label: 'Last 6 hours' },
  { value: String(DEFAULT_WINDOW_MS), label: 'Last 24 hours' },
  { value: String(7 * 24 * 3_600_000), label: 'Last 7 days' },
  { value: '0', label: 'All recorded' },
];

/** A value that is missing rather than zero, with the reason on hover. */
function Missing({ why }: { why: string }) {
  return (
    <span className="faint" title={why}>
      {EM_DASH}
    </span>
  );
}

/**
 * The inline-versus-staged split for one tool, as a two-segment rule.
 *
 * The segments use the same two tones as the legend chips, so the column is
 * read through the labels rather than through the colors alone.
 */
function ActivityMix({ calls, intents }: { calls: number; intents: number }) {
  const total = calls + intents;
  if (total === 0) {
    return <Missing why="No TOOL_CALL or INTENT_EMITTED event recorded for this tool" />;
  }
  const inlineShare = (calls / total) * 100;
  const label = `${formatCount(calls)} inline calls, ${formatCount(intents)} staged intents`;
  return (
    <span
      role="img"
      aria-label={label}
      title={label}
      style={{
        display: 'flex',
        width: '112px',
        height: '6px',
        borderRadius: 'var(--radius-sm)',
        overflow: 'hidden',
        background: 'var(--surface-3)',
      }}
    >
      <span style={{ width: `${inlineShare}%`, background: 'var(--info)' }} />
      <span style={{ width: `${100 - inlineShare}%`, background: 'var(--suspended)' }} />
    </span>
  );
}

const COLUMNS: Column<ToolSummary>[] = [
  {
    key: 'tool',
    header: 'Tool',
    width: '22%',
    render: (row) => <CopyableId value={row.tool_name} label="tool name" />,
  },
  {
    key: 'calls',
    header: 'Inline calls (TOOL_CALL)',
    numeric: true,
    render: (row) => formatCount(row.calls),
  },
  {
    key: 'intents',
    header: 'Staged intents (INTENT_EMITTED)',
    numeric: true,
    render: (row) => formatCount(row.intents),
  },
  {
    key: 'mix',
    header: 'Split',
    width: '124px',
    render: (row) => <ActivityMix calls={row.calls} intents={row.intents} />,
  },
  { key: 'errors', header: 'Errors', numeric: true, render: (row) => formatCount(row.errors) },
  {
    key: 'failure',
    header: 'Failure ratio',
    numeric: true,
    render: (row) =>
      row.failure_ratio === null ? (
        <Missing why="Nothing recorded an outcome for this tool, so no ratio exists" />
      ) : (
        formatRatio(row.failure_ratio)
      ),
  },
  {
    key: 'last_seen',
    header: 'Last seen',
    numeric: true,
    render: (row) =>
      row.last_seen_ms === null ? (
        <Missing why="No event carried a timestamp for this tool" />
      ) : (
        <span title={formatTimestamp(row.last_seen_ms)}>{formatRelative(row.last_seen_ms)}</span>
      ),
  },
];

const DERIVATION = [
  {
    key: 'calls',
    label: 'Inline calls',
    value: (
      <>
        One per <code>TOOL_CALL</code> trace event carrying this <code>beam_agents.tool_name</code>.
        These ran inside the activation, so a retried bundle runs them again — the reason only
        read-only tools belong on this route.
      </>
    ),
  },
  {
    key: 'intents',
    label: 'Staged intents',
    value: (
      <>
        One per <code>INTENT_EMITTED</code> event. The activation staged a <code>ToolIntent</code>{' '}
        and moved on; the effector executed it afterwards, outside the pipeline, and may have
        refused it as expired.
      </>
    ),
  },
  {
    key: 'errors',
    label: 'Errors and failure ratio',
    value: (
      <>
        Errors attributed to this tool over its inline calls. The ratio is null — an em dash — when
        nothing recorded an outcome to divide, which is not the same as a measured zero.
      </>
    ),
  },
  {
    key: 'expiry',
    label: 'Why a staged intent can vanish',
    value: (
      <>
        Every intent carries <code>expires_at_ms</code>. An effector refuses to execute one at or
        after that instant, and a non-positive value reads as already expired — the fail-closed
        reading, never as unbounded.
      </>
    ),
  },
  {
    key: 'source',
    label: 'Not from metrics',
    value: (
      <>
        Beam user metrics carry no labels, so they cannot answer &ldquo;per tool&rdquo;. Every
        figure here comes from stored trace-event attributes.
      </>
    ),
  },
];

export default function Page() {
  const [windowValue, setWindowValue] = useState(String(DEFAULT_WINDOW_MS));

  // Anchored on the newest stored record rather than on the clock — see
  // `useWindowAnchor`. Still one read per mount, so the query key does not move
  // under the page and refetch forever.
  const anchorMs = useWindowAnchor();
  const sinceMs = useMemo(() => {
    const span = Number(windowValue);
    return span > 0 ? anchorMs - span : undefined;
  }, [windowValue, anchorMs]);

  const tools = useQuery({
    queryKey: queryKeys.tools(sinceMs),
    queryFn: () => api.tools(sinceMs),
  });

  const rows = useMemo(() => tools.data ?? [], [tools.data]);

  const totals = useMemo(() => {
    const measured = rows.filter((row) => row.failure_ratio !== null);
    const measuredCalls = measured.reduce((total, row) => total + row.calls, 0);
    return {
      calls: rows.reduce((total, row) => total + row.calls, 0),
      intents: rows.reduce((total, row) => total + row.intents, 0),
      errors: rows.reduce((total, row) => total + row.errors, 0),
      failureRatio:
        measured.length === 0 || measuredCalls === 0
          ? null
          : measured.reduce((total, row) => total + row.errors, 0) / measuredCalls,
    };
  }, [rows]);

  return (
    <div className="page">
      <div className="page-title">
        <h1>Tools</h1>
        <Select
          label="Window"
          options={WINDOWS}
          value={windowValue}
          onChange={(event) => setWindowValue(event.target.value)}
        />
      </div>

      <p className="muted" style={{ maxWidth: '84ch' }}>
        Every tool the stored activations touched, counted separately along the two routes a tool
        can take out of an activation.
      </p>

      <section className="panel">
        <div className="panel-header">
          <h2 style={{ fontSize: 'var(--text-md)' }}>Two routes, counted separately</h2>
        </div>
        <div
          className="panel-body"
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))',
            gap: 'var(--space-5)',
          }}
        >
          <div className="stack" style={{ gap: 'var(--space-2)' }}>
            <div className="row">
              <Chip tone="info">Inline calls</Chip>
              <code>TOOL_CALL</code>
            </div>
            <p className="muted">
              Read-only work, executed inside the activation while the agent is running. Its result
              feeds the next model call, and it is re-executed when a bundle is retried or a run is
              replayed. That repetition is why only read-only tools may take this route.
            </p>
          </div>
          <div className="stack" style={{ gap: 'var(--space-2)' }}>
            <div className="row">
              <Chip tone="suspended">Staged intents</Chip>
              <code>INTENT_EMITTED</code>
            </div>
            <p className="muted">
              Anything with a side effect. The activation never executes it — it stages a{' '}
              <code>ToolIntent</code> on the outbox, which is committed only if the activation
              succeeds and is executed afterwards by the effector, once, outside the pipeline.
            </p>
          </div>
        </div>
      </section>

      {tools.isPending ? (
        <div className="panel">
          <SkeletonRows rows={6} columns={7} />
        </div>
      ) : tools.isError ? (
        <div className="panel">
          <EmptyState
            title="Could not load tool activity"
            body={
              <p>
                {tools.error instanceof ApiError
                  ? `The console API answered ${tools.error.status}.`
                  : 'The console API could not be reached.'}{' '}
                Check that <code>beam-agents-console</code> is running and serving this page.
              </p>
            }
            action={
              <Button onClick={() => void tools.refetch()} disabled={tools.isFetching}>
                {tools.isFetching ? 'Retrying…' : 'Retry'}
              </Button>
            }
          />
        </div>
      ) : rows.length === 0 ? (
        <div className="panel">
          <EmptyState
            title="No tool activity in this window"
            body={
              <>
                <p>
                  A tool appears here once an activation records a <code>TOOL_CALL</code> event for
                  it, or stages an intent that records an <code>INTENT_EMITTED</code> event.
                </p>
                <p style={{ marginTop: 'var(--space-2)' }}>
                  Register a tool on the agent, point the pipeline at the console with{' '}
                  <code>traces_to=&quot;console://localhost:8787&quot;</code>, and run an activation
                  that uses it. Widening the window above also helps if the run was a while ago.
                </p>
              </>
            }
            action={<a href="/connect">Configure an ingest path</a>}
          />
        </div>
      ) : (
        <>
          <TileRow>
            <StatTile
              label="Tools"
              value={formatCount(rows.length)}
              meta="with recorded activity"
            />
            <StatTile
              label="Inline calls"
              value={formatCount(totals.calls)}
              meta="TOOL_CALL, run in the pipeline"
            />
            <StatTile
              label="Staged intents"
              value={formatCount(totals.intents)}
              meta="INTENT_EMITTED, run by the effector"
            />
            <StatTile
              label="Errors"
              value={formatCount(totals.errors)}
              meta="attributed to a tool"
            />
            <StatTile
              label="Failure ratio"
              value={formatRatio(totals.failureRatio)}
              meta="over tools that recorded an outcome"
            />
          </TileRow>

          <section className="panel">
            <div className="panel-header">
              <h2 style={{ fontSize: 'var(--text-md)' }}>Per tool</h2>
              <span className="row" style={{ gap: 'var(--space-2)' }}>
                <Chip tone="info">Inline calls</Chip>
                <Chip tone="suspended">Staged intents</Chip>
              </span>
            </div>
            <DataTable
              columns={COLUMNS}
              rows={rows}
              rowKey={(row) => row.tool_name}
              caption="Per-tool inline call volume, staged intent count, errors, failure ratio, and last seen"
            />
          </section>

          <section className="panel">
            <div className="panel-header">
              <h2 style={{ fontSize: 'var(--text-md)' }}>Where these numbers come from</h2>
            </div>
            <div className="panel-body">
              <KeyValueGrid entries={DERIVATION} />
            </div>
          </section>
        </>
      )}
    </div>
  );
}
