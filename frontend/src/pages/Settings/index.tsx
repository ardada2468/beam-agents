/**
 * Settings — the console's three preferences, and the store's own numbers.
 *
 * The preferences are deliberately few. This is a local, single-user tool with
 * no account and no server-side profile, so every knob here is a browser
 * preference in `localStorage`; anything that changes what the *console* does —
 * the retention window, the database path, which ingest sources run — is a
 * process flag, and is reported here as a read-only fact rather than offered as
 * an editable field the page could not actually apply.
 *
 * The store panel is that report: row counts, retention, database path and
 * size, schema version, and the oldest and newest record the store holds.
 */

import { useQuery } from '@tanstack/react-query';
import { useMemo, useState } from 'react';
import { Link } from 'wouter';

import type { Column } from '@/components/ui';
import {
  Button,
  Chip,
  CopyableId,
  DataTable,
  EmptyState,
  KeyValueGrid,
  Select,
  Skeleton,
  StatTile,
  TileRow,
} from '@/components/ui';
import { api, queryKeys } from '@/lib/api';
import { formatBytes, formatCount, formatRelative, formatTimestamp } from '@/lib/format';
import { applyTheme, readTheme, type Theme } from '@/lib/theme';
import { DEFAULT_PAGE_SIZE, PAGE_SIZES, useLivePreference, usePageSize } from './preferences';

/** A link inside prose: the base stylesheet underlines on hover only. */
const LINK = { textDecoration: 'underline', textUnderlineOffset: '2px' } as const;

const THEME_OPTIONS = [
  { value: 'system', label: 'Follow the system' },
  { value: 'light', label: 'Light' },
  { value: 'dark', label: 'Dark' },
];

const LIVE_OPTIONS = [
  { value: 'on', label: 'Connected — update as records arrive' },
  { value: 'off', label: 'Paused — hold the view still' },
];

const ROW_COUNT_COLUMNS: Column<[string, number]>[] = [
  { key: 'table', header: 'Table', render: ([name]) => name },
  { key: 'rows', header: 'Rows', numeric: true, render: ([, count]) => formatCount(count) },
];

export default function Page() {
  const [theme, setTheme] = useState<Theme>(() => readTheme());
  const [live, setLive] = useLivePreference();
  const [pageSize, setPageSize] = usePageSize();

  const store = useQuery({ queryKey: queryKeys.store, queryFn: () => api.store() });
  const health = useQuery({ queryKey: queryKeys.health, queryFn: () => api.health() });

  const rowCounts = useMemo(
    () => Object.entries(store.data?.row_counts ?? {}).sort(([a], [b]) => a.localeCompare(b)),
    [store.data?.row_counts],
  );
  const totalRows = rowCounts.reduce((sum, [, count]) => sum + count, 0);

  const changeTheme = (next: Theme) => {
    setTheme(next);
    applyTheme(next);
  };

  return (
    <div className="page">
      <div className="page-title">
        <h1>Settings</h1>
        <p className="muted">Browser preferences, and what the store currently holds.</p>
      </div>

      <section className="panel">
        <div className="panel-header">
          <h2 style={{ fontSize: 'var(--text-md)' }}>Preferences</h2>
          <Button
            size="sm"
            onClick={() => {
              changeTheme('system');
              setLive(true);
              setPageSize(DEFAULT_PAGE_SIZE);
            }}
          >
            Reset to defaults
          </Button>
        </div>
        <div className="panel-body stack" style={{ gap: 'var(--space-5)' }}>
          <div className="stack" style={{ gap: 'var(--space-2)', maxWidth: '60ch' }}>
            <Select
              label="Theme"
              options={THEME_OPTIONS}
              value={theme}
              onChange={(event) => changeTheme(event.target.value as Theme)}
            />
            <p className="muted" style={{ fontSize: 'var(--text-xs)' }}>
              An explicit choice wins over the operating system&apos;s in both directions, including
              choosing light on a machine set to dark. The control in the top bar cycles the same
              preference.
            </p>
          </div>

          <div className="stack" style={{ gap: 'var(--space-2)', maxWidth: '60ch' }}>
            <Select
              label="Live updates"
              options={LIVE_OPTIONS}
              value={live ? 'on' : 'off'}
              onChange={(event) => setLive(event.target.value === 'on')}
            />
            <p className="muted" style={{ fontSize: 'var(--text-xs)' }}>
              The console subscribes to its own event stream and refreshes exactly the views an
              incoming record touches. Pausing holds the connection closed, which is what you want
              while reading a table a running pipeline keeps reordering. Stored in this browser and
              read by the shell when it opens the stream; the top bar always reports the state the
              connection is actually in — a page showing stale data as if it were live is the one
              thing that indicator exists to prevent.
            </p>
          </div>

          <div className="stack" style={{ gap: 'var(--space-2)', maxWidth: '60ch' }}>
            <Select
              label="Rows per page"
              options={PAGE_SIZES.map((size) => ({ value: String(size), label: String(size) }))}
              value={String(pageSize)}
              onChange={(event) => setPageSize(Number(event.target.value))}
            />
            <p className="muted" style={{ fontSize: 'var(--text-xs)' }}>
              How many rows each keyset page requests. Applies to the entity index, entity
              timelines, and search results.
            </p>
          </div>
        </div>
      </section>

      <section className="stack" style={{ gap: 'var(--space-4)' }}>
        <div className="spread">
          <h2 style={{ fontSize: 'var(--text-lg)' }}>Store</h2>
          <Button size="sm" onClick={() => void store.refetch()} disabled={store.isFetching}>
            {store.isFetching ? 'Refreshing…' : 'Refresh'}
          </Button>
        </div>

        {store.isPending ? (
          <div className="panel">
            <div className="panel-body">
              <Skeleton height="6em" />
            </div>
          </div>
        ) : store.isError ? (
          <div className="panel">
            <EmptyState
              title="The store did not answer"
              body={
                <div className="stack" style={{ gap: 'var(--space-2)' }}>
                  <p>{(store.error as Error).message}</p>
                  <p>
                    The API can be up while the database is not readable. Check the{' '}
                    <span className="mono">--db</span> path the console was started with, or the
                    ingest paths on{' '}
                    <Link href="/connect" style={LINK}>
                      Connect
                    </Link>
                    .
                  </p>
                </div>
              }
              action={<Button onClick={() => void store.refetch()}>Retry</Button>}
            />
          </div>
        ) : (
          <>
            <TileRow>
              <StatTile
                label="Rows stored"
                value={formatCount(totalRows)}
                meta={`Across ${rowCounts.length} tables`}
              />
              <StatTile
                label="On disk"
                value={formatBytes(store.data.database_bytes)}
                meta="SQLite file, WAL mode"
              />
              <StatTile
                label="Retention"
                value={
                  store.data.retention_hours === null
                    ? 'Unbounded'
                    : `${formatCount(store.data.retention_hours)}h`
                }
                meta={
                  store.data.retention_hours === null
                    ? 'Nothing is pruned'
                    : 'Older records are pruned'
                }
              />
              <StatTile
                label="Oldest record"
                value={formatRelative(store.data.oldest_record_ms)}
                meta={formatTimestamp(store.data.oldest_record_ms)}
              />
              <StatTile
                label="Newest record"
                value={formatRelative(store.data.newest_record_ms)}
                meta={formatTimestamp(store.data.newest_record_ms)}
              />
            </TileRow>

            <div className="panel">
              <div className="panel-header">
                <h3>Database</h3>
                <Chip tone="info" plain>{`Schema v${store.data.schema_version}`}</Chip>
              </div>
              <div className="panel-body">
                <KeyValueGrid
                  entries={[
                    {
                      key: 'path',
                      label: 'Path',
                      value: <CopyableId value={store.data.database_path} label="database path" />,
                    },
                    {
                      key: 'bytes',
                      label: 'Size',
                      value: formatBytes(store.data.database_bytes),
                    },
                    {
                      key: 'schema',
                      label: 'Schema version',
                      value: formatCount(store.data.schema_version),
                    },
                    {
                      key: 'retention',
                      label: 'Retention window',
                      value:
                        store.data.retention_hours === null
                          ? 'Unbounded — set --retention-hours to prune'
                          : `${formatCount(store.data.retention_hours)} hours`,
                    },
                    {
                      key: 'version',
                      label: 'Console version',
                      value: health.data ? (
                        <span className="mono">{health.data.version}</span>
                      ) : (
                        'Not answering'
                      ),
                    },
                    {
                      key: 'ui',
                      label: 'UI bundle',
                      value: health.data ? (
                        <Chip tone={health.data.ui_bundled ? 'ok' : 'warn'}>
                          {health.data.ui_bundled ? 'Bundled' : 'Not bundled'}
                        </Chip>
                      ) : (
                        'Not answering'
                      ),
                    },
                    {
                      key: 'sources',
                      label: 'Ingest sources',
                      value:
                        health.data && health.data.sources.length > 0 ? (
                          <span className="row" style={{ flexWrap: 'wrap' }}>
                            {health.data.sources.map((source) => (
                              <Chip key={source} tone="neutral" plain>
                                {source}
                              </Chip>
                            ))}
                          </span>
                        ) : (
                          <>
                            None —{' '}
                            <Link href="/connect" style={LINK}>
                              Connect
                            </Link>{' '}
                            a pipeline
                          </>
                        ),
                    },
                  ]}
                />
              </div>
            </div>

            <div className="panel">
              <div className="panel-header">
                <h3>Row counts</h3>
                <span className="muted" style={{ fontSize: 'var(--text-xs)' }}>
                  What the store holds right now
                </span>
              </div>
              <DataTable
                caption="Row counts by table"
                columns={ROW_COUNT_COLUMNS}
                rows={rowCounts}
                rowKey={([name]) => name}
                empty={
                  <EmptyState
                    title="The store is empty"
                    body={
                      <div className="stack" style={{ gap: 'var(--space-2)' }}>
                        <p>
                          The console is running and holds no records. It fills as soon as a
                          pipeline exports to it.
                        </p>
                        <p>
                          <Link href="/connect" style={LINK}>
                            Connect
                          </Link>{' '}
                          has a snippet for each of the five ways in, including one that needs
                          nothing but Docker.
                        </p>
                      </div>
                    }
                  />
                }
              />
            </div>
          </>
        )}
      </section>
    </div>
  );
}
