/**
 * Models — what the runtime spent, per model.
 *
 * Every figure on this page comes from a `LLM_CALL` trace event's attributes,
 * never from the Beam metrics surface: user metrics carry no labels and are
 * attempted-rather-than-committed, so a per-model number taken from them would
 * disagree with the traces under retry. The page says so on screen rather than
 * leaving a reader to wonder which of two numbers is the real one.
 *
 * The distinction this page is most careful about is `null` versus `0`. The
 * facade omits usage attributes for a call whose response it never decoded,
 * because a `0` there is indistinguishable from a real zero-token call to
 * anything that sums the column. `cache_hit_ratio: null` likewise means nothing
 * recorded a cache attribute at all — which is a different fact from "never hit
 * the cache", and printing 0% for it would be a lie. Everything therefore goes
 * through `format.ts`, which renders a missing measurement as an em dash.
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
import type { ModelSummary } from '@/lib/api-types';
import { EM_DASH, formatCompact, formatCount, formatRatio } from '@/lib/format';

/** Lookback options. `0` means "everything the store still holds". */
const WINDOWS = [
  { value: String(3_600_000), label: 'Last hour' },
  { value: String(6 * 3_600_000), label: 'Last 6 hours' },
  { value: String(DEFAULT_WINDOW_MS), label: 'Last 24 hours' },
  { value: String(7 * 24 * 3_600_000), label: 'Last 7 days' },
  { value: '0', label: 'All recorded' },
];

/** Sum the values that exist. All-missing stays missing rather than becoming 0. */
function sumRecorded(values: (number | null)[]): number | null {
  const recorded = values.filter((value): value is number => value !== null);
  return recorded.length === 0 ? null : recorded.reduce((total, value) => total + value, 0);
}

/** The largest recorded value, or null when nothing recorded one. */
function maxRecorded(values: (number | null)[]): number | null {
  const recorded = values.filter((value): value is number => value !== null);
  return recorded.length === 0 ? null : Math.max(...recorded);
}

/**
 * Cache-hit ratio across models, over only the models that measured one.
 *
 * A model whose calls recorded no cache attribute contributes neither a hit nor
 * a call here — folding its calls into the denominator would silently dilute the
 * ratio with unmeasured traffic.
 */
function overallCacheRatio(rows: ModelSummary[]): number | null {
  const measured = rows.filter((row) => row.cache_hit_ratio !== null);
  const calls = measured.reduce((total, row) => total + row.calls, 0);
  if (measured.length === 0 || calls === 0) return null;
  return measured.reduce((total, row) => total + row.cache_hits, 0) / calls;
}

/** Known breaker states first, in the order a breaker moves through them. */
const CIRCUIT_ORDER = ['closed', 'half_open', 'open'];

/** Where a state sorts. Anything the runtime grows later lands after the known three. */
function circuitOrder(state: string): number {
  const index = CIRCUIT_ORDER.indexOf(state);
  return index === -1 ? CIRCUIT_ORDER.length : index;
}

const CIRCUIT_TONE: Record<string, 'ok' | 'warn' | 'error' | 'neutral'> = {
  closed: 'ok',
  half_open: 'warn',
  open: 'error',
};

const CIRCUIT_LABEL: Record<string, string> = {
  closed: 'Closed',
  half_open: 'Half-open',
  open: 'Open',
};

/**
 * The per-model breaker-state distribution.
 *
 * States with no calls are dropped once any state has some, because a row of
 * `Open 0` chips on every model is noise. When every recorded state is zero the
 * zeros are shown, since a measured zero is a fact and hiding it would leave the
 * cell looking unrecorded.
 */
function CircuitStates({ states }: { states: Record<string, number> }) {
  const entries = Object.entries(states);
  const positive = entries.filter(([, count]) => count > 0);
  const shown = (positive.length > 0 ? positive : entries).sort(
    (a, b) => circuitOrder(a[0]) - circuitOrder(b[0]),
  );

  if (shown.length === 0) {
    return (
      <span className="faint" title="No call recorded a beam_agents.circuit_state attribute">
        {EM_DASH}
      </span>
    );
  }

  return (
    <span className="row" style={{ gap: 'var(--space-1)' }}>
      {shown.map(([state, count]) => (
        <Chip key={state} tone={CIRCUIT_TONE[state] ?? 'neutral'}>
          {CIRCUIT_LABEL[state] ?? state} {formatCount(count)}
        </Chip>
      ))}
    </span>
  );
}

/** A value that is missing rather than zero, with the reason on hover. */
function Missing({ why }: { why: string }) {
  return (
    <span className="faint" title={why}>
      {EM_DASH}
    </span>
  );
}

const COLUMNS: Column<ModelSummary>[] = [
  {
    key: 'model',
    header: 'Model',
    width: '24%',
    render: (row) => <CopyableId value={row.model} label="model id" />,
  },
  { key: 'calls', header: 'Calls', numeric: true, render: (row) => formatCount(row.calls) },
  {
    key: 'prompt',
    header: 'Prompt',
    numeric: true,
    render: (row) =>
      row.prompt_tokens === null ? (
        <Missing why="No call recorded gen_ai.usage.input_tokens" />
      ) : (
        formatCount(row.prompt_tokens)
      ),
  },
  {
    key: 'completion',
    header: 'Completion',
    numeric: true,
    render: (row) =>
      row.completion_tokens === null ? (
        <Missing why="No call recorded gen_ai.usage.output_tokens" />
      ) : (
        formatCount(row.completion_tokens)
      ),
  },
  {
    key: 'total',
    header: 'Total tokens',
    numeric: true,
    render: (row) =>
      row.total_tokens === null ? (
        <Missing why="No call recorded token usage" />
      ) : (
        formatCount(row.total_tokens)
      ),
  },
  {
    key: 'cache_hits',
    header: 'Cache hits',
    numeric: true,
    render: (row) => formatCount(row.cache_hits),
  },
  {
    key: 'cache_ratio',
    header: 'Cache hit ratio',
    numeric: true,
    render: (row) =>
      row.cache_hit_ratio === null ? (
        <Missing why="No call recorded a beam_agents.cache_hit attribute" />
      ) : (
        formatRatio(row.cache_hit_ratio)
      ),
  },
  { key: 'errors', header: 'Errors', numeric: true, render: (row) => formatCount(row.errors) },
  {
    key: 'attempts',
    header: 'Max attempts',
    numeric: true,
    render: (row) =>
      row.max_attempts === null ? (
        <Missing why="No call recorded a beam_agents.attempts attribute" />
      ) : (
        formatCount(row.max_attempts)
      ),
  },
  {
    key: 'circuit',
    header: 'Circuit state',
    width: '18%',
    render: (row) => <CircuitStates states={row.circuit_states} />,
  },
];

const DERIVATION = [
  {
    key: 'calls',
    label: 'Calls',
    value: (
      <>
        One per <code>LLM_CALL</code> trace event, including calls served from the replay cache.
      </>
    ),
  },
  {
    key: 'tokens',
    label: 'Token spend',
    value: (
      <>
        <code>gen_ai.usage.input_tokens</code> and <code>gen_ai.usage.output_tokens</code>. The
        facade omits them for a call whose response it never decoded rather than writing zero, so an
        em dash here means no call reported usage — not that the calls were free.
      </>
    ),
  },
  {
    key: 'cache',
    label: 'Cache hits and ratio',
    value: (
      <>
        <code>beam_agents.cache_hit</code>. A hit re-uses a stored response and is not billed, so
        its tokens are reported but marked <code>billed=false</code>. The ratio is null when nothing
        recorded the attribute at all.
      </>
    ),
  },
  {
    key: 'attempts',
    label: 'Max attempts',
    value: (
      <>
        The highest <code>beam_agents.attempts</code> any one call reached, bounded by{' '}
        <code>RetryPolicy.max_attempts</code>. A value of 1 means no call was ever retried.
      </>
    ),
  },
  {
    key: 'circuit',
    label: 'Circuit state',
    value: (
      <>
        <code>beam_agents.circuit_state</code> as it stood for each call. The breaker is
        worker-local and per endpoint, so this is a distribution across workers, not one global
        state.
      </>
    ),
  },
  {
    key: 'source',
    label: 'Not from metrics',
    value: (
      <>
        Beam user metrics are unlabelled and attempted-not-committed, so they cannot answer
        &ldquo;per model&rdquo; and disagree with traces under retry. Nothing here reads them.
      </>
    ),
  },
];

export default function Page() {
  const [windowValue, setWindowValue] = useState(String(DEFAULT_WINDOW_MS));

  // Anchored to the window choice rather than recomputed each render: a
  // `Date.now()` in the query key would change on every render and refetch
  // forever.
  const sinceMs = useMemo(() => {
    const span = Number(windowValue);
    return span > 0 ? Date.now() - span : undefined;
  }, [windowValue]);

  const models = useQuery({
    queryKey: queryKeys.models(sinceMs),
    queryFn: () => api.models(sinceMs),
  });

  const rows = useMemo(() => models.data ?? [], [models.data]);

  const totals = useMemo(
    () => ({
      calls: rows.reduce((total, row) => total + row.calls, 0),
      prompt: sumRecorded(rows.map((row) => row.prompt_tokens)),
      completion: sumRecorded(rows.map((row) => row.completion_tokens)),
      tokens: sumRecorded(rows.map((row) => row.total_tokens)),
      cacheRatio: overallCacheRatio(rows),
      errors: rows.reduce((total, row) => total + row.errors, 0),
      attempts: maxRecorded(rows.map((row) => row.max_attempts)),
    }),
    [rows],
  );

  return (
    <div className="page">
      <div className="page-title">
        <h1>Models</h1>
        <Select
          label="Window"
          options={WINDOWS}
          value={windowValue}
          onChange={(event) => setWindowValue(event.target.value)}
        />
      </div>

      <p className="muted" style={{ maxWidth: '84ch' }}>
        Call volume, token spend, cache behaviour, retries, and circuit state for every model the
        stored activations used. Each figure is derived from <code>LLM_CALL</code> trace-event
        attributes.
      </p>

      {models.isPending ? (
        <div className="panel">
          <SkeletonRows rows={4} columns={7} />
        </div>
      ) : models.isError ? (
        <div className="panel">
          <EmptyState
            title="Could not load model usage"
            body={
              <p>
                {models.error instanceof ApiError
                  ? `The console API answered ${models.error.status}.`
                  : 'The console API could not be reached.'}{' '}
                Check that <code>beam-agents-console</code> is running and serving this page.
              </p>
            }
            action={
              <Button onClick={() => void models.refetch()} disabled={models.isFetching}>
                {models.isFetching ? 'Retrying…' : 'Retry'}
              </Button>
            }
          />
        </div>
      ) : rows.length === 0 ? (
        <div className="panel">
          <EmptyState
            title="No model calls in this window"
            body={
              <>
                <p>
                  A model appears here once an activation records an <code>LLM_CALL</code> event
                  through <code>LlmFacade</code>.
                </p>
                <p style={{ marginTop: 'var(--space-2)' }}>
                  Point a pipeline at the console with{' '}
                  <code>traces_to=&quot;console://localhost:8787&quot;</code> and{' '}
                  <code>sink_resolver=ConsoleSinkResolver()</code>, then run an activation that
                  calls a model. Widening the window above also helps if the run was a while ago.
                </p>
              </>
            }
            action={<a href="/connect">Configure an ingest path</a>}
          />
        </div>
      ) : (
        <>
          <TileRow>
            <StatTile label="Models" value={formatCount(rows.length)} meta="with recorded calls" />
            <StatTile
              label="LLM calls"
              value={formatCount(totals.calls)}
              meta="cache hits included"
            />
            <StatTile
              label="Total tokens"
              value={formatCompact(totals.tokens)}
              meta={`${formatCompact(totals.prompt)} prompt · ${formatCompact(totals.completion)} completion`}
            />
            <StatTile
              label="Cache hit ratio"
              value={formatRatio(totals.cacheRatio)}
              meta="over calls that recorded the attribute"
            />
            <StatTile label="Errors" value={formatCount(totals.errors)} meta="calls that failed" />
            <StatTile
              label="Max attempts"
              value={formatCount(totals.attempts)}
              meta="highest retry count reached"
            />
          </TileRow>

          <section className="panel">
            <div className="panel-header">
              <h2 style={{ fontSize: 'var(--text-md)' }}>Per model</h2>
              <span className="muted" style={{ fontSize: 'var(--text-xs)' }}>
                Ordered by call volume
              </span>
            </div>
            <DataTable
              columns={COLUMNS}
              rows={rows}
              rowKey={(row) => row.model}
              caption="Per-model call volume, token spend, cache behaviour, retries, and circuit state"
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
