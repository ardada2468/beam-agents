/**
 * The activation list — the console's primary object.
 *
 * Three decisions worth knowing before editing this file.
 *
 * **Filters live in the URL.** Every filter is a query-string parameter, so a
 * narrowed view is a link: the whole point of grouping by the runtime's closed
 * `reason` vocabulary is that "show me the four activations that hit
 * `budget_exceeded`" is something you can paste into an issue.
 *
 * **Paging is keyset, never offset.** The API returns an opaque `next_cursor`
 * and this asks for the next page with it. Offset paging over a table that is
 * still being written to skips and repeats rows, which in a telemetry viewer
 * means quietly losing the activation you were looking for.
 *
 * **Sorting is client-side and says so.** The list endpoint orders by start time
 * descending and takes no sort parameter, so the column sort reorders the rows
 * already loaded and the footer states that plainly rather than implying the
 * store was asked.
 */

import { useInfiniteQuery, useQuery } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation, useSearch } from 'wouter';

import type { Column } from '@/components/ui';
import {
  Button,
  Chip,
  CopyableId,
  DataTable,
  EmptyState,
  Input,
  Select,
  SkeletonRows,
  StatusChip,
} from '@/components/ui';
import { ApiError, api, queryKeys } from '@/lib/api';
import type {
  ActivationFilters,
  ActivationKind,
  ActivationStatus,
  ActivationSummary,
} from '@/lib/api-types';
import {
  EM_DASH,
  formatCount,
  formatDuration,
  formatEntityKey,
  formatTime,
  formatTimestamp,
  humanizeReason,
} from '@/lib/format';

import { KIND_LABEL } from './trace-attrs';

import './activations.css';

const PAGE_SIZE = 50;

const STATUSES: ActivationStatus[] = ['completed', 'suspended', 'error', 'in_flight'];
const KINDS: ActivationKind[] = ['start', 'resume', 'unknown'];

const STATUS_LABEL: Record<ActivationStatus, string> = {
  completed: 'Completed',
  suspended: 'Suspended',
  error: 'Error',
  in_flight: 'In flight',
};

/** Only the values the API defines get through; anything else in the URL is dropped. */
function asStatus(value: string | null): ActivationStatus | '' {
  return value && (STATUSES as string[]).includes(value) ? (value as ActivationStatus) : '';
}

function asKind(value: string | null): ActivationKind | '' {
  return value && (KINDS as string[]).includes(value) ? (value as ActivationKind) : '';
}

/** Unique across the page even when two activations share `(entity_key, seq)`. */
function rowKeyOf(row: ActivationSummary): string {
  return `${row.entity_key}/${row.seq}/${row.trace_id}`;
}

type SortValue = string | number | null;

function sortValue(row: ActivationSummary, key: string): SortValue {
  switch (key) {
    case 'entity':
      return formatEntityKey(row.entity_key);
    case 'seq':
      return row.seq;
    case 'status':
      return row.status;
    case 'kind':
      return row.kind;
    case 'model':
      return row.model;
    case 'tokens':
      return row.total_tokens;
    case 'llm':
      return row.llm_calls;
    case 'tools':
      return row.tool_calls;
    case 'intents':
      return row.intents;
    case 'errors':
      return row.errors;
    case 'wall':
      return row.wall_ms;
    default:
      return row.started_ms;
  }
}

export default function ActivationsPage() {
  const [location, navigate] = useLocation();
  const search = useSearch();
  const params = useMemo(() => new URLSearchParams(search), [search]);

  const status = asStatus(params.get('status'));
  const kind = asKind(params.get('kind'));
  const model = params.get('model') ?? '';
  const tool = params.get('tool') ?? '';
  const reason = params.get('reason') ?? '';
  const entityKey = params.get('entity_key') ?? '';
  const query = params.get('query') ?? '';

  const setParam = useCallback(
    (key: string, value: string, replace = false) => {
      const next = new URLSearchParams(search);
      if (value) next.set(key, value);
      else next.delete(key);
      const qs = next.toString();
      navigate(qs ? `${location}?${qs}` : location, { replace });
    },
    [search, navigate, location],
  );

  const clearAll = useCallback(() => navigate(location), [navigate, location]);

  /* -- Text filters are debounced so typing does not thrash the URL --------- */

  const [entityDraft, setEntityDraft] = useState(entityKey);
  const [queryDraft, setQueryDraft] = useState(query);

  useEffect(() => setEntityDraft(entityKey), [entityKey]);
  useEffect(() => setQueryDraft(query), [query]);

  useEffect(() => {
    if (entityDraft === entityKey) return;
    const timer = setTimeout(() => setParam('entity_key', entityDraft.trim(), true), 350);
    return () => clearTimeout(timer);
  }, [entityDraft, entityKey, setParam]);

  useEffect(() => {
    if (queryDraft === query) return;
    const timer = setTimeout(() => setParam('query', queryDraft.trim(), true), 350);
    return () => clearTimeout(timer);
  }, [queryDraft, query, setParam]);

  /* -- The list ------------------------------------------------------------- */

  const apiFilters = useMemo<ActivationFilters>(() => {
    const filters: ActivationFilters = {};
    if (status) filters.status = status;
    if (kind) filters.kind = kind;
    if (model) filters.model = model;
    if (tool) filters.tool = tool;
    if (reason) filters.reason = reason;
    if (entityKey) filters.entity_key = entityKey;
    if (query) filters.query = query;
    return filters;
  }, [status, kind, model, tool, reason, entityKey, query]);

  const list = useInfiniteQuery({
    queryKey: queryKeys.activations(apiFilters),
    queryFn: ({ pageParam }) => api.activations(apiFilters, pageParam, PAGE_SIZE),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
    // A 4xx is an answer, not a hiccup: retrying it only delays the message.
    retry: (count, error) => !(error instanceof ApiError && error.status < 500) && count < 1,
  });

  const pages = list.data?.pages;
  const pageCount = pages?.length ?? 0;
  const rows = useMemo(() => pages?.flatMap((page) => page.items) ?? [], [pages]);
  const total = pages?.[0]?.total ?? null;

  /* -- Filter vocabularies, taken from the store rather than invented ------- */

  const models = useQuery({ queryKey: queryKeys.models(), queryFn: () => api.models() });
  const tools = useQuery({ queryKey: queryKeys.tools(), queryFn: () => api.tools() });
  const groups = useQuery({ queryKey: queryKeys.errorGroups(), queryFn: () => api.errorGroups() });

  const modelOptions = useMemo(() => {
    const names = new Set((models.data ?? []).map((entry) => entry.model));
    if (model) names.add(model);
    return Array.from(names)
      .sort()
      .map((value) => ({ value, label: value }));
  }, [models.data, model]);

  const toolOptions = useMemo(() => {
    const names = new Set((tools.data ?? []).map((entry) => entry.tool_name));
    if (tool) names.add(tool);
    return Array.from(names)
      .sort()
      .map((value) => ({ value, label: value }));
  }, [tools.data, tool]);

  const reasonOptions = useMemo(() => {
    const names = new Set((groups.data ?? []).map((entry) => entry.reason));
    if (reason) names.add(reason);
    return Array.from(names)
      .sort()
      .map((value) => ({ value, label: humanizeReason(value) }));
  }, [groups.data, reason]);

  /* -- Client-side sort over the loaded rows -------------------------------- */

  const [sortKey, setSortKey] = useState('started');
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc');

  const sorted = useMemo(() => {
    const copy = [...rows];
    copy.sort((a, b) => {
      const left = sortValue(a, sortKey);
      const right = sortValue(b, sortKey);
      // A missing measurement sorts last in both directions: it is not a small
      // number, and floating it to the top under "ascending" would say it was.
      if (left === null && right === null) return 0;
      if (left === null) return 1;
      if (right === null) return -1;
      const compared =
        typeof left === 'string' && typeof right === 'string'
          ? left.localeCompare(right)
          : Number(left) - Number(right);
      return sortDirection === 'asc' ? compared : -compared;
    });
    return copy;
  }, [rows, sortKey, sortDirection]);

  const onSort = useCallback(
    (key: string) => {
      // Both updates happen outside the updater functions: a state updater that
      // sets other state is not pure, and React may call it twice.
      if (key === sortKey) {
        setSortDirection((direction) => (direction === 'asc' ? 'desc' : 'asc'));
        return;
      }
      setSortKey(key);
      setSortDirection('desc');
    },
    [sortKey],
  );

  /* -- Rows that arrived over the live stream flash once -------------------- */

  const seen = useRef({ filterKey: '', keys: new Set<string>(), pages: 0 });
  const [flash, setFlash] = useState<ReadonlySet<string>>(() => new Set());

  useEffect(() => {
    const filterKey = JSON.stringify(apiFilters);
    const state = seen.current;
    const keys = rows.map(rowKeyOf);
    // Neither a filter change nor a "load more" is an arrival, and the first
    // page of an empty view is not one either.
    const settled =
      state.filterKey === filterKey && pageCount === state.pages && state.keys.size > 0;
    const fresh = settled ? keys.filter((key) => !state.keys.has(key)) : [];
    seen.current = { filterKey, keys: new Set(keys), pages: pageCount };
    if (fresh.length === 0) return;
    setFlash(new Set(fresh));
    const timer = setTimeout(() => setFlash(new Set()), 1200);
    return () => clearTimeout(timer);
  }, [rows, apiFilters, pageCount]);

  /* -- Columns -------------------------------------------------------------- */

  const columns = useMemo<Column<ActivationSummary>[]>(
    () => [
      {
        key: 'entity',
        header: 'Entity key',
        sortable: true,
        render: (row) => (
          <StopRowActivation>
            <CopyableId
              value={row.entity_key}
              display={formatEntityKey(row.entity_key)}
              label="entity key"
            />
          </StopRowActivation>
        ),
      },
      {
        key: 'seq',
        header: 'Seq',
        numeric: true,
        sortable: true,
        render: (row) => <span className="mono">{row.seq}</span>,
      },
      {
        key: 'status',
        header: 'Status',
        sortable: true,
        render: (row) => <StatusChip status={row.status} />,
      },
      {
        key: 'kind',
        header: 'Kind',
        sortable: true,
        render: (row) => (
          <span className="act-kind">
            <Chip plain tone={row.kind === 'unknown' ? 'warn' : 'neutral'}>
              {KIND_LABEL[row.kind]}
            </Chip>
            {row.attempts > 1 ? (
              <span className="muted" title={`${row.attempts} attempts in this activation`}>
                ×{row.attempts}
              </span>
            ) : null}
          </span>
        ),
      },
      {
        key: 'model',
        header: 'Model',
        sortable: true,
        render: (row) =>
          row.model ? (
            <span className="mono act-model" title={row.model}>
              {row.model}
            </span>
          ) : (
            <span className="faint">{EM_DASH}</span>
          ),
      },
      {
        key: 'tokens',
        header: 'Tokens',
        numeric: true,
        sortable: true,
        render: (row) => (
          <span
            title={`prompt ${formatCount(row.prompt_tokens)} · completion ${formatCount(
              row.completion_tokens,
            )}`}
          >
            {formatCount(row.total_tokens)}
          </span>
        ),
      },
      {
        key: 'llm',
        header: 'LLM',
        numeric: true,
        sortable: true,
        render: (row) => formatCount(row.llm_calls),
      },
      {
        key: 'tools',
        header: 'Tools',
        numeric: true,
        sortable: true,
        render: (row) => formatCount(row.tool_calls),
      },
      {
        key: 'intents',
        header: 'Intents',
        numeric: true,
        sortable: true,
        render: (row) => formatCount(row.intents),
      },
      {
        key: 'errors',
        header: 'Errors',
        numeric: true,
        sortable: true,
        render: (row) =>
          row.errors > 0 ? (
            <span className="act-errors" title={row.reasons.map(humanizeReason).join(', ')}>
              {formatCount(row.errors)}
            </span>
          ) : (
            <span className="muted">0</span>
          ),
      },
      {
        key: 'wall',
        header: 'Wall',
        numeric: true,
        sortable: true,
        render: (row) => (
          <span title="ACTIVATION_START to ACTIVATION_END clock delta">
            {formatDuration(row.wall_ms)}
          </span>
        ),
      },
      {
        key: 'started',
        header: 'Started',
        numeric: true,
        sortable: true,
        render: (row) => (
          <span className="mono" title={formatTimestamp(row.started_ms)}>
            {formatTime(row.started_ms)}
          </span>
        ),
      },
    ],
    [],
  );

  /* -- Render --------------------------------------------------------------- */

  const active: { key: string; label: string; value: string }[] = [];
  if (status) active.push({ key: 'status', label: 'Status', value: STATUS_LABEL[status] });
  if (kind) active.push({ key: 'kind', label: 'Kind', value: KIND_LABEL[kind] });
  if (model) active.push({ key: 'model', label: 'Model', value: model });
  if (tool) active.push({ key: 'tool', label: 'Tool', value: tool });
  if (reason) active.push({ key: 'reason', label: 'Reason', value: humanizeReason(reason) });
  if (entityKey) active.push({ key: 'entity_key', label: 'Entity key', value: entityKey });
  if (query) active.push({ key: 'query', label: 'Search', value: query });

  return (
    <div className="page">
      <div className="page-title">
        <h1>Activations</h1>
        <p className="muted">
          One row per <span className="mono">(entity key, seq)</span> — a suspend and its resume are
          one activation.
        </p>
      </div>

      <section className="panel" aria-label="Filters">
        {/*
          One row of controls, and no visible captions on them.

          Every one of these already names itself: the inputs have placeholders
          and the selects sit on "Any status", "Any model", "Any reason". The
          captions above them repeated that word and doubled each control's
          height, which ran seven controls to two rows and pushed the table
          below the fold — so landing here showed the controls for finding an
          activation and not one activation. `aria-label` carries the accessible
          name the visible caption was providing.
        */}
        <div className="act-filters">
          <Input
            aria-label="Search by entity key or trace ID"
            className="act-filters__search"
            placeholder="Search keys or IDs"
            value={queryDraft}
            onChange={(event) => setQueryDraft(event.target.value)}
          />
          <Input
            aria-label="Entity key hex prefix"
            mono
            placeholder="Entity key prefix"
            value={entityDraft}
            onChange={(event) => setEntityDraft(event.target.value)}
          />
          <Select
            aria-label="Status"
            placeholder="Any status"
            value={status}
            onChange={(event) => setParam('status', event.target.value)}
            options={STATUSES.map((value) => ({ value, label: STATUS_LABEL[value] }))}
          />
          <Select
            aria-label="Kind"
            placeholder="Any kind"
            value={kind}
            onChange={(event) => setParam('kind', event.target.value)}
            options={KINDS.map((value) => ({ value, label: KIND_LABEL[value] }))}
          />
          <Select
            aria-label="Model"
            placeholder="Any model"
            value={model}
            onChange={(event) => setParam('model', event.target.value)}
            options={modelOptions}
          />
          <Select
            aria-label="Tool"
            placeholder="Any tool"
            value={tool}
            onChange={(event) => setParam('tool', event.target.value)}
            options={toolOptions}
          />
          <Select
            aria-label="Reason"
            placeholder="Any reason"
            value={reason}
            onChange={(event) => setParam('reason', event.target.value)}
            options={reasonOptions}
          />
        </div>

        {active.length > 0 ? (
          <div className="act-active">
            <span className="eyebrow">Filtering by</span>
            {active.map((entry) => (
              <button
                key={entry.key}
                type="button"
                className="act-pill"
                onClick={() => setParam(entry.key, '')}
                aria-label={`Remove filter ${entry.label} ${entry.value}`}
              >
                <span className="muted">{entry.label}</span>
                <span className="act-pill__value">{entry.value}</span>
                <span aria-hidden="true">✕</span>
              </button>
            ))}
            <Button size="sm" variant="ghost" onClick={clearAll}>
              Clear all
            </Button>
          </div>
        ) : null}
      </section>

      <section className="panel" aria-label="Activations">
        {list.isPending ? (
          <SkeletonRows rows={10} columns={7} />
        ) : list.isError ? (
          <EmptyState
            title="Could not load activations"
            body={
              <>
                <p>
                  The console API did not answer:{' '}
                  <span className="mono">{(list.error as Error).message}</span>
                </p>
                <p>
                  Check that the console is running and serving <span className="mono">/api</span>.
                </p>
              </>
            }
            action={<Button onClick={() => void list.refetch()}>Retry</Button>}
          />
        ) : (
          <>
            <DataTable
              caption="Activations, most recent first"
              columns={columns}
              rows={sorted}
              rowKey={rowKeyOf}
              newKeys={flash}
              sortKey={sortKey}
              sortDirection={sortDirection}
              onSort={onSort}
              onRowClick={(row) =>
                navigate(`/activations/${encodeURIComponent(row.entity_key)}/${row.seq}`)
              }
              empty={
                active.length > 0 ? (
                  <EmptyState
                    title="No activation matches these filters"
                    body="Every filter conjoins with the others, so a narrow combination can exclude everything the store holds. Remove one and try again."
                    action={<Button onClick={clearAll}>Clear {active.length} filters</Button>}
                  />
                ) : (
                  <EmptyState
                    title="No activations recorded yet"
                    body={
                      <>
                        <p>
                          Activations appear here as soon as a pipeline exports to this console.
                          Point one at it with{' '}
                          <span className="mono">
                            AgentConfig(traces_to=&quot;console://localhost:8787&quot;,
                            sink_resolver=ConsoleSinkResolver())
                          </span>
                          , or start the bundled demo with{' '}
                          <span className="mono">
                            docker compose -f docker/compose.console.yaml up
                          </span>
                          .
                        </p>
                        <p>
                          Already exporting to OTLP, Kafka, or BigQuery? The Connect page lists a
                          snippet for each ingest path.
                        </p>
                      </>
                    }
                    action={<Button onClick={() => navigate('/connect')}>Open Connect</Button>}
                  />
                )
              }
            />

            {sorted.length > 0 ? (
              <div className="act-footer">
                <span>
                  {formatCount(sorted.length)} loaded
                  {total !== null ? ` of ${formatCount(total)} matching` : ''} · sorting reorders
                  the loaded rows only
                </span>
                <span className="act-footer__actions">
                  {list.isFetching && !list.isFetchingNextPage ? (
                    <span className="muted">Refreshing…</span>
                  ) : null}
                  {list.hasNextPage ? (
                    <Button
                      onClick={() => void list.fetchNextPage()}
                      disabled={list.isFetchingNextPage}
                    >
                      {list.isFetchingNextPage ? 'Loading…' : `Load ${PAGE_SIZE} more`}
                    </Button>
                  ) : (
                    <span className="muted">End of results</span>
                  )}
                </span>
              </div>
            ) : null}
          </>
        )}
      </section>
    </div>
  );
}

/**
 * Keeps a copy click from also opening the row.
 *
 * `CopyableId` is a button inside a clickable row, so without this the one
 * interaction people use most in a trace UI — copy an ID — would navigate away
 * from the list they were copying it out of.
 */
function StopRowActivation({ children }: { children: ReactNode }) {
  return (
    <span
      className="act-stop"
      onClick={(event) => event.stopPropagation()}
      onKeyDown={(event) => event.stopPropagation()}
    >
      {children}
    </span>
  );
}
