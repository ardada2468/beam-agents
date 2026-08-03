/**
 * What one `reason` × `error.type` group actually means.
 *
 * The counts are the easy part; the part worth building is the sentence that
 * says what the reason *is*. The runtime's vocabulary is closed and documented
 * (`docs/errors.md`), so the console can carry that documentation to the point
 * of use instead of leaving an operator to search for `ttl_wiped_suspension` in
 * a repository they may not have checked out.
 */

import { Chip, KeyValueGrid, TimeSeries } from '@/components/ui';
import type { ErrorGroup, ErrorRecord } from '@/lib/api-types';
import { formatCount, formatRelative, formatTimestamp, humanizeReason } from '@/lib/format';

import { ORPHANED_SUB_REASONS, hasSubReasons, reasonInfo, subReasonOf } from './reasons';

/** One breakdown row: a code, how many records carried it, and what it means. */
interface SubReasonRow {
  code: string;
  count: number;
  meaning: string;
}

/**
 * Count the sub-reasons across the records loaded for this group.
 *
 * For `orphaned_result` the four documented codes are always listed, including
 * the ones at zero — that zero is a real count over the loaded records, and
 * seeing which of the four admission checks did *not* fire is most of the
 * triage. For `intent_dead_letter` the codes are whatever `WriteIntents` put in
 * the JSON, so only observed ones can be listed.
 */
function breakdown(reason: string, records: ErrorRecord[]): SubReasonRow[] {
  const counts = new Map<string, number>();
  let unclassified = 0;
  for (const record of records) {
    const code = subReasonOf(reason, record.detail);
    if (code === null) unclassified += 1;
    else counts.set(code, (counts.get(code) ?? 0) + 1);
  }

  const rows: SubReasonRow[] =
    reason === 'orphaned_result'
      ? ORPHANED_SUB_REASONS.map((entry) => ({
          code: entry.code,
          count: counts.get(entry.code) ?? 0,
          meaning: entry.meaning,
        }))
      : Array.from(counts.entries())
          .sort((a, b) => b[1] - a[1])
          .map(([code, count]) => ({
            code,
            count,
            meaning: 'Reported by WriteIntents when the intent could not be serialized.',
          }));

  if (unclassified > 0) {
    rows.push({
      code: 'not recorded',
      count: unclassified,
      meaning:
        reason === 'orphaned_result'
          ? 'These records’ detail did not begin with one of the four codes above.'
          : 'These records’ detail was not the documented JSON object.',
    });
  }
  return rows;
}

export function GroupPanel({
  group,
  records,
  loadedAll,
}: {
  group: ErrorGroup;
  records: ErrorRecord[];
  /** Whether `records` is the whole group or only the pages fetched so far. */
  loadedAll: boolean;
}) {
  const info = reasonInfo(group.reason);
  const rows = hasSubReasons(group.reason) ? breakdown(group.reason, records) : [];

  return (
    <section className="panel" aria-labelledby="errors-group-heading">
      <div className="panel-header">
        <div className="row" style={{ flexWrap: 'wrap', gap: 'var(--space-2)' }}>
          <h2 id="errors-group-heading" style={{ fontSize: 'var(--text-md)' }}>
            {humanizeReason(group.reason)}
          </h2>
          {group.error_type ? (
            <Chip tone="neutral" plain title="error.type on the ERROR trace event">
              {group.error_type}
            </Chip>
          ) : (
            <Chip tone="neutral" plain title="No error.type was recorded for this group">
              No error type
            </Chip>
          )}
        </div>
        <span className="muted" style={{ fontSize: 'var(--text-xs)' }}>
          {info ? info.source : 'Not in this build’s vocabulary'}
        </span>
      </div>

      <div className="panel-body stack">
        <div className="errors-section">
          <p className="errors-note">
            {info ? (
              info.summary
            ) : (
              <>
                <code>{group.reason}</code> is not one of the reasons this console knows about. The
                runtime&rsquo;s vocabulary is closed, so these records came from a source or a
                version this build does not recognise.
              </>
            )}
          </p>
          {info ? <p className="errors-note">{info.consequence}</p> : null}
          <KeyValueGrid
            compact
            entries={[
              { key: 'count', label: 'Occurrences', value: formatCount(group.count) },
              {
                key: 'entities',
                label: 'Entity keys affected',
                value: formatCount(group.entities),
              },
              {
                key: 'first',
                label: 'First seen',
                value: `${formatTimestamp(group.first_seen_ms)} · ${formatRelative(group.first_seen_ms)}`,
              },
              {
                key: 'last',
                label: 'Last seen',
                value: `${formatTimestamp(group.last_seen_ms)} · ${formatRelative(group.last_seen_ms)}`,
              },
              {
                key: 'detail',
                label: 'Detail carries',
                value: info ? info.detail : 'Unknown — the reason is not documented in this build.',
              },
            ]}
          />
        </div>

        <div className="errors-section">
          <h3 className="errors-h3">Occurrences over the window</h3>
          <TimeSeries
            series={{
              key: `${group.reason}:${group.error_type ?? ''}`,
              label: 'Errors',
              points: group.series,
            }}
            format={formatCount}
            ariaLabel={`${humanizeReason(group.reason)} occurrences over time`}
          />
        </div>

        {rows.length > 0 ? (
          <div className="errors-section">
            <h3 className="errors-h3">Sub-reason, from the detail string</h3>
            <p className="errors-note">
              {group.reason === 'orphaned_result'
                ? 'A resume can fail admission four ways. The runtime writes which one it was onto the record so triage does not have to re-derive it.'
                : 'The dead letter carries a JSON object naming why the intent could not be serialized.'}{' '}
              {loadedAll
                ? 'Counted over every record in this group.'
                : `Counted over the ${formatCount(records.length)} record${records.length === 1 ? '' : 's'} loaded so far.`}
            </p>
            <div className="errors-subreasons">
              {rows.map((row) => (
                <div
                  key={row.code}
                  className={`errors-subreason${row.count === 0 ? ' errors-subreason--zero' : ''}`}
                >
                  <span className="errors-subreason__code">{row.code}</span>
                  <span className="errors-subreason__count">{formatCount(row.count)}</span>
                  <span className="errors-subreason__meaning">{row.meaning}</span>
                </div>
              ))}
            </div>
          </div>
        ) : null}
      </div>
    </section>
  );
}
