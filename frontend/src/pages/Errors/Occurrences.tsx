/**
 * The individual records inside one group, and the activations they failed.
 *
 * `/api/errors` filters by `reason`, not by `error.type`, so the last narrowing
 * to *exactly* this group is done here on the loaded page. That is deliberate
 * rather than lazy: a group is `reason × error_type`, and drilling into it has
 * to list that group's records and no others, or the count in the table above
 * stops matching the rows below it.
 */

import { Link } from 'wouter';

import { Button, DataTable, EmptyState, SkeletonRows } from '@/components/ui';
import type { Column } from '@/components/ui';
import type { ErrorRecord } from '@/lib/api-types';
import { formatCount, formatEntityKey, formatTimestamp } from '@/lib/format';

import { PositionCell } from './RecordPanel';
import { hasSubReasons, subReasonOf } from './reasons';

export function recordKey(record: ErrorRecord, index: number): string {
  return `${record.entity_key}/${record.seq ?? 'none'}/${record.event_time_ms}/${index}`;
}

export function Occurrences({
  reason,
  records,
  loading,
  selectedKey,
  onSelect,
  hasMore,
  loadingMore,
  onLoadMore,
  total,
}: {
  reason: string;
  records: ErrorRecord[];
  loading: boolean;
  selectedKey: string | undefined;
  onSelect: (record: ErrorRecord, key: string) => void;
  hasMore: boolean;
  loadingMore: boolean;
  onLoadMore: () => void;
  /** Total records the server holds for this reason, or null when it did not say. */
  total: number | null;
}) {
  // Columns that would be entirely "not recorded" for this group are stated
  // once, below the table, instead of once per row: eleven identical
  // "Not available" cells is noise, and the fact is a property of the *route*,
  // not of the individual records.
  const showSubReason =
    hasSubReasons(reason) && records.some((record) => subReasonOf(record.reason, record.detail));
  const showPosition = records.some(
    (record) => record.failure_step !== null || record.failure_last_event !== null,
  );

  const columns: Column<ErrorRecord>[] = [
    {
      key: 'time',
      header: 'Event time',
      width: '170px',
      render: (record) => <span className="mono">{formatTimestamp(record.event_time_ms)}</span>,
    },
    {
      key: 'entity',
      header: 'Entity key',
      render: (record) => (
        <span className="mono" title={record.entity_key}>
          {formatEntityKey(record.entity_key)}
        </span>
      ),
    },
    {
      key: 'activation',
      header: 'Activation',
      width: '120px',
      render: (record) =>
        record.seq === null ? (
          <span
            className="errors-unavailable"
            title="This record is not attributed to a single activation."
          >
            Not attributed
          </span>
        ) : (
          <Link
            href={`/activations/${encodeURIComponent(record.entity_key)}/${record.seq}`}
            className="mono"
            onClick={(event) => event.stopPropagation()}
            title="Open this activation"
          >
            seq {record.seq}
          </Link>
        ),
    },
    ...(showSubReason
      ? [
          {
            key: 'sub',
            header: 'Sub-reason',
            render: (record: ErrorRecord) => {
              const code = subReasonOf(record.reason, record.detail);
              return code ? (
                <span className="mono">{code}</span>
              ) : (
                <span className="errors-unavailable">Not recorded</span>
              );
            },
          } satisfies Column<ErrorRecord>,
        ]
      : []),
    ...(showPosition
      ? [
          {
            key: 'position',
            header: 'Failure position',
            render: (record: ErrorRecord) => <PositionCell record={record} />,
          } satisfies Column<ErrorRecord>,
        ]
      : []),
  ];

  return (
    <section className="panel" aria-labelledby="errors-occurrences-heading">
      <div className="panel-header">
        <h2 id="errors-occurrences-heading" style={{ fontSize: 'var(--text-md)' }}>
          Occurrences
        </h2>
        <span className="muted" style={{ fontSize: 'var(--text-xs)' }}>
          {loading
            ? 'Loading'
            : total === null
              ? `${formatCount(records.length)} shown`
              : `${formatCount(records.length)} shown of ${formatCount(total)} for this reason`}
        </span>
      </div>

      {loading ? (
        <SkeletonRows rows={6} columns={4} />
      ) : (
        <>
          <DataTable
            caption="Individual error records in the selected group"
            columns={columns}
            rows={records}
            rowKey={recordKey}
            selectedKey={selectedKey}
            onRowClick={(record) => {
              const index = records.indexOf(record);
              onSelect(record, recordKey(record, index));
            }}
            empty={
              <EmptyState
                title="No records loaded for this group"
                body="The group's counts come from the aggregate query; its individual records are fetched separately and none arrived. Widen the time window, or reload if the store was pruned between the two calls."
              />
            }
          />
          {records.length > 0 && !showPosition ? (
            <p className="errors-note errors-footnote">
              No record here carries a failure position. This route cannot reach an activation
              context, so the runtime omits those attributes rather than defaulting them to zero.
            </p>
          ) : null}
          {hasMore ? (
            <div className="errors-more">
              <Button onClick={onLoadMore} disabled={loadingMore}>
                {loadingMore ? 'Loading…' : 'Load more'}
              </Button>
            </div>
          ) : null}
        </>
      )}
    </section>
  );
}
