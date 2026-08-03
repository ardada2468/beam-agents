/**
 * The Overview page's tabular panels.
 *
 * Split out of `index.tsx` so the page module reads as a layout — headline
 * figures, series, tables, store — rather than as six column definitions with a
 * page hidden between them.
 *
 * Every nullable figure goes through `Measured`, which renders the em dash that
 * `format.ts` produced in a lighter weight. That is the one distinction this
 * whole UI turns on: a missing measurement must never look like a measured
 * zero, and `0` in a column of dashes is exactly the mistake it prevents.
 */

import { Link } from 'wouter';

import type { Column, KeyValueEntry } from '@/components/ui';
import { Chip, CopyableId, DataTable, EmptyState, KeyValueGrid } from '@/components/ui';
import type { ErrorRecord, ModelSummary, StoreStatus, ToolSummary } from '@/lib/api-types';
import {
  EM_DASH,
  formatBytes,
  formatCompact,
  formatCount,
  formatEntityKey,
  formatRatio,
  formatTime,
  formatTimestamp,
  humanizeReason,
} from '@/lib/format';

/** A formatted figure that may be an em dash, marked as absent when it is. */
export function Measured({ text }: { text: string }) {
  if (text !== EM_DASH) return <>{text}</>;
  return (
    <span className="faint" title="Not recorded">
      {EM_DASH}
    </span>
  );
}

function PanelHeader({ title, children }: { title: string; children?: React.ReactNode }) {
  return (
    <div className="panel-header">
      <h2 className="ov-panel-title">{title}</h2>
      {children}
    </div>
  );
}

/* -- Top models ------------------------------------------------------------ */

const MODEL_COLUMNS: Column<ModelSummary>[] = [
  {
    key: 'model',
    header: 'Model',
    render: (row) => <CopyableId value={row.model} label="model name" />,
  },
  {
    key: 'calls',
    header: 'Calls',
    numeric: true,
    render: (row) => formatCount(row.calls),
  },
  {
    key: 'tokens',
    header: 'Tokens',
    numeric: true,
    render: (row) => <Measured text={formatCompact(row.total_tokens)} />,
  },
  {
    key: 'cache',
    header: 'Cache hit',
    numeric: true,
    render: (row) => <Measured text={formatRatio(row.cache_hit_ratio)} />,
  },
  {
    key: 'errors',
    header: 'Errors',
    numeric: true,
    render: (row) => formatCount(row.errors),
  },
];

export function TopModelsPanel({ models }: { models: ModelSummary[] }) {
  return (
    <section className="panel">
      <PanelHeader title="Top models">
        <Link href="/models" className="ov-panel-link">
          All models
        </Link>
      </PanelHeader>
      <DataTable
        columns={MODEL_COLUMNS}
        rows={models}
        rowKey={(row) => row.model}
        caption="Models by activity in the selected window"
        empty={
          <EmptyState
            title="No model was named in this window"
            body="A model appears here once an activation records an LLM call carrying a gen_ai.request.model attribute. Run an agent with an LLM step, or widen the window."
          />
        }
      />
    </section>
  );
}

/* -- Top tools ------------------------------------------------------------- */

const TOOL_COLUMNS: Column<ToolSummary>[] = [
  {
    key: 'tool',
    header: 'Tool',
    render: (row) => <CopyableId value={row.tool_name} label="tool name" />,
  },
  {
    key: 'calls',
    header: 'Calls',
    numeric: true,
    render: (row) => formatCount(row.calls),
  },
  {
    key: 'intents',
    header: 'Intents',
    numeric: true,
    render: (row) => formatCount(row.intents),
  },
  {
    key: 'failures',
    header: 'Failure rate',
    numeric: true,
    render: (row) => <Measured text={formatRatio(row.failure_ratio)} />,
  },
  {
    key: 'last',
    header: 'Last seen',
    numeric: true,
    render: (row) => <Measured text={formatTime(row.last_seen_ms)} />,
  },
];

export function TopToolsPanel({ tools }: { tools: ToolSummary[] }) {
  return (
    <section className="panel">
      <PanelHeader title="Top tools">
        <Link href="/tools" className="ov-panel-link">
          All tools
        </Link>
      </PanelHeader>
      <DataTable
        columns={TOOL_COLUMNS}
        rows={tools}
        rowKey={(row) => row.tool_name}
        caption="Tools by activity in the selected window"
        empty={
          <EmptyState
            title="No tool was called in this window"
            body="A tool appears here once an activation calls one inline or stages an intent for it. Register a tool on the agent and run it, or widen the window."
          />
        }
      />
    </section>
  );
}

/* -- Recent errors --------------------------------------------------------- */

/**
 * `seq` is nullable on an error: some failure routes never reach an activation
 * context, so there is nothing to link to. Those rows show a dash rather than a
 * link to a sequence number the record does not have.
 */
const ERROR_COLUMNS: Column<ErrorRecord>[] = [
  {
    key: 'time',
    header: 'Time',
    render: (row) => (
      <span className="mono" title={formatTimestamp(row.event_time_ms)}>
        {formatTime(row.event_time_ms)}
      </span>
    ),
  },
  {
    key: 'reason',
    header: 'Reason',
    render: (row) => <Chip tone="error">{humanizeReason(row.reason)}</Chip>,
  },
  {
    key: 'type',
    header: 'Type',
    render: (row) => (
      <span className="mono">
        <Measured text={row.error_type ?? EM_DASH} />
      </span>
    ),
  },
  {
    key: 'entity',
    header: 'Entity key',
    render: (row) => (
      <CopyableId
        value={row.entity_key}
        display={formatEntityKey(row.entity_key)}
        label="entity key"
      />
    ),
  },
  {
    key: 'detail',
    header: 'Detail',
    render: (row) => (
      <span className="ov-detail" title={row.detail}>
        {row.detail}
      </span>
    ),
  },
  {
    key: 'open',
    header: <span className="visually-hidden">Failing activation</span>,
    render: (row) =>
      row.seq === null ? (
        <span className="faint" title="This error is not attributed to a sequence number">
          {EM_DASH}
        </span>
      ) : (
        <Link href={`/activations/${encodeURIComponent(row.entity_key)}/${row.seq}`}>
          seq {row.seq} →
        </Link>
      ),
  },
];

export function RecentErrorsPanel({ errors }: { errors: ErrorRecord[] }) {
  return (
    <section className="panel">
      <PanelHeader title="Recent errors">
        <Link href="/errors" className="ov-panel-link">
          All errors
        </Link>
      </PanelHeader>
      <DataTable
        columns={ERROR_COLUMNS}
        rows={errors}
        rowKey={(row, index) =>
          `${row.entity_key}/${row.seq ?? 'none'}/${row.event_time_ms}/${index}`
        }
        caption="The most recent errors in the selected window"
        empty={
          <EmptyState
            title="No error was recorded in this window"
            body="Errors land here when a pipeline writes to errors_to on this console. Nothing failed in this window — widen it, or open Errors to see everything the store holds."
            action={
              <Link href="/errors" className="btn">
                Open Errors
              </Link>
            }
          />
        }
      />
    </section>
  );
}

/* -- Store ----------------------------------------------------------------- */

/**
 * What the store holds. Deliberately the quietest thing on the page: it is
 * consulted once, to answer "is this console receiving anything at all", and
 * then ignored for the rest of the session.
 */
export function StorePanel({ store }: { store: StoreStatus | null }) {
  return (
    <section className="panel">
      <PanelHeader title="Store" />
      <div className="panel-body ov-store">
        {store === null ? (
          <p>
            This console did not report its store status. It is answering queries, so the store is
            reachable — an older console build simply does not include the field.
          </p>
        ) : (
          <KeyValueGrid compact entries={storeEntries(store)} />
        )}
      </div>
    </section>
  );
}

function storeEntries(store: StoreStatus): KeyValueEntry[] {
  return [
    {
      key: 'rows',
      label: 'Rows',
      value:
        Object.keys(store.row_counts).length === 0 ? (
          <span className="faint">No tables reported</span>
        ) : (
          <span className="ov-counts">
            {Object.entries(store.row_counts).map(([table, count]) => (
              <span key={table}>
                <strong>{formatCount(count)}</strong> {table}
              </span>
            ))}
          </span>
        ),
    },
    {
      key: 'retention',
      label: 'Retention',
      value:
        store.retention_hours === null ? (
          <span className="faint" title="No retention window configured">
            {EM_DASH} nothing is pruned
          </span>
        ) : (
          `${formatCount(store.retention_hours)} hours`
        ),
    },
    {
      key: 'oldest',
      label: 'Oldest record',
      value: <Measured text={formatTimestamp(store.oldest_record_ms)} />,
    },
    {
      key: 'newest',
      label: 'Newest record',
      value: <Measured text={formatTimestamp(store.newest_record_ms)} />,
    },
    {
      key: 'database',
      label: 'Database',
      value: (
        <>
          <span className="mono">{store.database_path}</span> ·{' '}
          <Measured text={formatBytes(store.database_bytes)} />
        </>
      ),
    },
    {
      key: 'schema',
      label: 'Schema',
      value: `version ${formatCount(store.schema_version)}`,
    },
  ];
}
