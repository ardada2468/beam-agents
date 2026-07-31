/**
 * The Overview page: the one screen that answers "is it running, and is it
 * healthy" without a click.
 *
 * Three decisions worth knowing about before editing this file.
 *
 * **Nothing here polls.** The live stream (`lib/live.ts`) invalidates the
 * `overview` query key as records arrive, so this page refreshes because the
 * pipeline moved, not because a timer fired. The freshness line under the title
 * reports when the figures last changed; the connection state itself belongs to
 * the shell's top bar and is deliberately not duplicated here.
 *
 * **`null` is not `0`.** `cache_hit_ratio`, `total_tokens`, and both wall-time
 * percentiles are null when nothing recorded the underlying attribute, and a
 * tile printing "0%" for "no cache attribute was ever seen" would be a lie the
 * operator cannot detect. Every figure goes through `format.ts`, and `StatTile`
 * renders the resulting em dash in a lighter weight.
 *
 * **The empty state distinguishes an empty window from an empty store.** They
 * need different instructions: one says widen the window, the other says
 * configure a sink. Reporting "no data" for both is the failure this page's
 * empty state exists to avoid.
 */

import { useQuery } from '@tanstack/react-query';
import { useEffect, useRef, useState } from 'react';
import { Link } from 'wouter';

import type { SeriesSpec } from '@/components/ui';
import {
  Button,
  Chip,
  CodeBlock,
  EmptyState,
  Select,
  Skeleton,
  SkeletonRows,
  StatTile,
  TileRow,
  TimeSeries,
} from '@/components/ui';
import { api, ApiError, DEFAULT_WINDOW_MS, queryKeys } from '@/lib/api';
import type { Overview } from '@/lib/api-types';
import {
  formatCompact,
  formatCount,
  formatDuration,
  formatRatio,
  formatRelative,
} from '@/lib/format';

import { Measured, RecentErrorsPanel, StorePanel, TopModelsPanel, TopToolsPanel } from './panels';

import './overview.css';

/* -- The window selector --------------------------------------------------- */

const HOUR_MS = 3_600_000;

/**
 * Bucket counts are chosen per window so a bar is a round unit of time — one
 * minute, five minutes, half an hour, two hours — rather than whatever
 * `window / 48` happens to come to.
 */
const WINDOWS = [
  { value: '1h', label: 'Last 1 hour', ms: HOUR_MS, buckets: 60 },
  { value: '6h', label: 'Last 6 hours', ms: 6 * HOUR_MS, buckets: 72 },
  { value: '24h', label: 'Last 24 hours', ms: DEFAULT_WINDOW_MS, buckets: 48 },
  { value: '7d', label: 'Last 7 days', ms: 168 * HOUR_MS, buckets: 84 },
] as const;

type WindowChoice = (typeof WINDOWS)[number];

const DEFAULT_CHOICE: WindowChoice = WINDOWS[2];
const WIDEST_CHOICE: WindowChoice = WINDOWS[3];

const WINDOW_OPTIONS = WINDOWS.map(({ value, label }) => ({ value, label }));

/** The snippet that turns an empty console into a populated one. */
const SINK_SNIPPET = `AgentConfig(
    traces_to="console://localhost:8787",
    errors_to="console://localhost:8787",
    snapshots_to="console://localhost:8787",
    sink_resolver=ConsoleSinkResolver(),
)`;

/* -- Page ------------------------------------------------------------------ */

export default function OverviewPage() {
  const [windowValue, setWindowValue] = useState<string>(DEFAULT_CHOICE.value);
  const choice = WINDOWS.find((option) => option.value === windowValue) ?? DEFAULT_CHOICE;

  const { data, isPending, isError, error, isFetching, dataUpdatedAt, refetch } = useQuery({
    queryKey: queryKeys.overview(choice.ms),
    queryFn: () => api.overview(choice.ms, choice.buckets),
  });

  const arrived = useArrivals(data, choice.ms);

  return (
    <div className="page">
      <header className="ov-head">
        <h1>Overview</h1>
        <Select
          label="Window"
          options={WINDOW_OPTIONS}
          value={windowValue}
          onChange={(event) => setWindowValue(event.target.value)}
        />
      </header>

      <div className="ov-content">
        {isPending ? (
          <LoadingOverview />
        ) : isError ? (
          <FailedOverview error={error} onRetry={() => void refetch()} />
        ) : (
          <>
            <Freshness updatedAt={dataUpdatedAt} fetching={isFetching} arrived={arrived} />
            <Headline overview={data} />
            <Body
              overview={data}
              windowLabel={choice.label}
              onWiden={() => setWindowValue(WIDEST_CHOICE.value)}
            />
            <StorePanel store={data.store} />
          </>
        )}
      </div>
    </div>
  );
}

/* -- Live activity --------------------------------------------------------- */

/**
 * "Updated 4s ago", plus how much arrived while the page was open.
 *
 * Its own component because it re-renders on a one-second tick and the three
 * SVG series below it have no reason to.
 */
function Freshness({
  updatedAt,
  fetching,
  arrived,
}: {
  updatedAt: number;
  fetching: boolean;
  arrived: number;
}) {
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);

  return (
    <p className="ov-freshness">
      <span>
        Figures updated {formatRelative(updatedAt, now)}. They refresh as the live stream reports
        new records; the top bar says whether that stream is connected.
      </span>
      {fetching ? <Chip tone="pending">Refreshing</Chip> : null}
      {arrived > 0 ? (
        <Chip tone="info">{formatCount(arrived)} new since you opened this page</Chip>
      ) : null}
    </p>
  );
}

/**
 * How many activations have arrived since this window was first drawn.
 *
 * The stream carries identity only, so this page never sees "an activation
 * arrived" — it sees the aggregate move. Counting the increases is what turns
 * that into a sentence an operator can read. It resets when the window changes,
 * because "since you opened this page" would otherwise be measured against a
 * different question.
 */
function useArrivals(data: Overview | undefined, windowMs: number): number {
  const seen = useRef<{ windowMs: number; activations: number } | null>(null);
  const [arrived, setArrived] = useState(0);

  useEffect(() => {
    if (!data) return;
    const previous = seen.current;
    if (previous === null || previous.windowMs !== windowMs) {
      seen.current = { windowMs, activations: data.activations };
      setArrived(0);
      return;
    }
    if (data.activations > previous.activations) {
      const delta = data.activations - previous.activations;
      seen.current = { windowMs, activations: data.activations };
      setArrived((count) => count + delta);
    } else if (data.activations < previous.activations) {
      // The window slid past older activations. Not an arrival — re-baseline.
      seen.current = { windowMs, activations: data.activations };
    }
  }, [data, windowMs]);

  return arrived;
}

/* -- Headline figures ------------------------------------------------------ */

function Headline({ overview }: { overview: Overview }) {
  return (
    <TileRow>
      <StatTile
        label="Activations"
        value={formatCompact(overview.activations)}
        meta={`${formatCount(overview.completed)} completed · ${formatCount(
          overview.suspended,
        )} suspended · ${formatCount(overview.in_flight)} in flight`}
      />
      <StatTile
        label="Error rate"
        value={formatRatio(overview.error_ratio)}
        meta={`${formatCount(overview.errors)} error records`}
      />
      <StatTile
        label="Total tokens"
        value={formatCompact(overview.total_tokens)}
        meta="Summed from recorded usage only"
      />
      <StatTile
        label="p95 wall time"
        value={formatDuration(overview.p95_wall_ms)}
        meta={`p50 ${formatDuration(overview.p50_wall_ms)}`}
      />
      <StatTile
        label="Cache hit ratio"
        value={formatRatio(overview.cache_hit_ratio)}
        meta={
          overview.cache_hit_ratio === null
            ? 'No cache attribute recorded'
            : 'Of LLM calls in this window'
        }
      />
      <StatTile
        label="LLM calls"
        value={formatCompact(overview.llm_calls)}
        meta={`${formatCount(overview.tool_calls)} tool calls`}
      />
    </TileRow>
  );
}

/* -- Series and tables ----------------------------------------------------- */

function Body({
  overview,
  windowLabel,
  onWiden,
}: {
  overview: Overview;
  windowLabel: string;
  onWiden: () => void;
}) {
  const storedRows = overview.store
    ? Object.values(overview.store.row_counts).reduce((sum, count) => sum + count, 0)
    : null;

  if (overview.activations === 0 && overview.errors === 0) {
    return (
      <section className="panel">
        {storedRows === 0 ? (
          <NeverIngested />
        ) : (
          <EmptyWindow label={windowLabel} storedRows={storedRows} onWiden={onWiden} />
        )}
      </section>
    );
  }

  return (
    <>
      <div className="ov-charts">
        <SeriesPanel
          title="Activations"
          figure={formatCompact(overview.activations)}
          series={{ key: 'activations', label: 'Activations', points: overview.activation_series }}
        />
        <SeriesPanel
          title="Errors"
          figure={formatCompact(overview.errors)}
          series={{
            key: 'errors',
            label: 'Errors',
            points: overview.error_series,
            tone: 'error',
          }}
        />
        <SeriesPanel
          title="Tokens"
          figure={formatCompact(overview.total_tokens)}
          series={{ key: 'tokens', label: 'Tokens', points: overview.token_series, tone: 'info' }}
        />
      </div>

      <div className="ov-pair">
        <TopModelsPanel models={overview.top_models} />
        <TopToolsPanel tools={overview.top_tools} />
      </div>

      <RecentErrorsPanel errors={overview.recent_errors} />
    </>
  );
}

function SeriesPanel({
  title,
  figure,
  series,
}: {
  title: string;
  figure: string;
  series: SeriesSpec;
}) {
  return (
    <section className="panel">
      <div className="panel-header">
        <h2 className="ov-panel-title">{title}</h2>
        <span className="ov-panel-figure">
          <Measured text={figure} />
        </span>
      </div>
      <div className="panel-body">
        <TimeSeries series={series} ariaLabel={`${title} per time bucket`} />
      </div>
    </section>
  );
}

/* -- Empty, loading, and failed states ------------------------------------- */

function NeverIngested() {
  return (
    <EmptyState
      title="Nothing has reached this console yet"
      body={
        <>
          <p>
            The store is empty. Route a pipeline&rsquo;s traces, errors, and snapshots here by
            resolving its sinks through the console:
          </p>
          <div style={{ marginTop: 'var(--space-3)' }}>
            <CodeBlock code={SINK_SNIPPET} label="console sink configuration" />
          </div>
        </>
      }
      action={
        <Link href="/connect" className="btn">
          See every ingest path
        </Link>
      }
    />
  );
}

/**
 * `storedRows` is null when the console reported no store status: the page then
 * cannot claim there are records outside the window, so it does not say so.
 */
function EmptyWindow({
  label,
  storedRows,
  onWiden,
}: {
  label: string;
  storedRows: number | null;
  onWiden: () => void;
}) {
  return (
    <EmptyState
      title={`Nothing ran in the ${label.replace(/^Last /, 'last ')}`}
      body={
        storedRows === null
          ? 'Widen the window to reach older records, or open Activations for everything the console has kept.'
          : `The store holds ${formatCount(storedRows)} rows outside this window. Widen the window to reach them, or open Activations for everything the console has kept.`
      }
      action={
        <span className="row">
          <Button onClick={onWiden}>Widen to 7 days</Button>
          <Link href="/activations" className="btn">
            Open Activations
          </Link>
        </span>
      }
    />
  );
}

function LoadingOverview() {
  return (
    <>
      <div className="ov-freshness">
        <Skeleton width="240px" />
      </div>
      <TileRow>
        {['activations', 'errors', 'tokens', 'wall', 'cache', 'llm'].map((key) => (
          <StatTile key={key} label={<Skeleton width="70%" />} value={<Skeleton width="50%" />} />
        ))}
      </TileRow>
      <div className="ov-charts">
        {['activations', 'errors', 'tokens'].map((key) => (
          <section className="panel" key={key}>
            <div className="panel-body">
              <Skeleton height="160px" />
            </div>
          </section>
        ))}
      </div>
      <div className="panel">
        <SkeletonRows rows={5} columns={5} />
      </div>
    </>
  );
}

function FailedOverview({ error, onRetry }: { error: unknown; onRetry: () => void }) {
  const status = error instanceof ApiError ? error.status : null;
  return (
    <section className="panel">
      <EmptyState
        title="The console did not answer"
        body={
          <>
            <p>
              {status === null
                ? 'The request for these figures failed before a response arrived.'
                : `The request for these figures came back ${formatCount(status)}.`}{' '}
              Nothing is wrong with the stored records — this page could not read them.
            </p>
            <p style={{ marginTop: 'var(--space-2)' }}>
              Check that the console process is still running and serving on the address this page
              was loaded from.
            </p>
          </>
        }
        action={
          <Button variant="primary" onClick={onRetry}>
            Try again
          </Button>
        }
      />
    </section>
  );
}
