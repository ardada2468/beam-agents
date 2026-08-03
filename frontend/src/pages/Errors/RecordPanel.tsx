/**
 * One error record, and the failure-position panel that is the point of it.
 *
 * `failure_step`, `failure_last_event`, `failure_staged_intents` and
 * `failure_llm_calls` are captured only on the routes that can reach an
 * activation context. On the timeout route the coroutine may still be running;
 * on a failure before context construction there is nothing to read; the
 * non-activation routes never had one. The runtime omits the attributes
 * entirely on those routes rather than defaulting them, so this panel renders
 * `null` as **not available** and never as `0` — `0` staged intents is a real
 * measurement and means something quite different.
 */

import { Link } from 'wouter';

import { Chip, CodeBlock, CopyableId, EmptyState, KeyValueGrid } from '@/components/ui';
import type { ErrorRecord } from '@/lib/api-types';
import {
  formatCount,
  formatEntityKey,
  formatRelative,
  formatTimestamp,
  humanizeEventType,
} from '@/lib/format';

import { hasSubReasons, orphanedIntentId, subReasonOf } from './reasons';

/** A measurement the runtime did not take. Never rendered as zero. */
export function Unavailable({ why }: { why: string }) {
  return (
    <span className="errors-unavailable" title={why}>
      Not available
    </span>
  );
}

const NO_CONTEXT =
  'This route could not reach an activation context, so the runtime omitted the attribute rather than defaulting it to zero.';

function positionValue(value: number | null) {
  return value === null ? <Unavailable why={NO_CONTEXT} /> : formatCount(value);
}

export function RecordPanel({ record }: { record: ErrorRecord | null }) {
  if (!record) {
    return (
      <section className="panel">
        <div className="panel-header">
          <h2 style={{ fontSize: 'var(--text-md)' }}>Occurrence</h2>
        </div>
        <EmptyState
          title="No occurrence selected"
          body="Pick a row from the occurrences table to see its detail string and, where the runtime could capture one, the position in the activation at which it failed."
        />
      </section>
    );
  }

  const hasPosition =
    record.failure_step !== null ||
    record.failure_last_event !== null ||
    record.failure_staged_intents !== null ||
    record.failure_llm_calls !== null;

  const subReason = hasSubReasons(record.reason) ? subReasonOf(record.reason, record.detail) : null;
  const intentId =
    record.reason === 'orphaned_result' && subReason ? orphanedIntentId(record.detail) : null;

  return (
    <section className="panel" aria-labelledby="errors-record-heading">
      <div className="panel-header">
        <h2 id="errors-record-heading" style={{ fontSize: 'var(--text-md)' }}>
          Occurrence
        </h2>
        <span className="muted" style={{ fontSize: 'var(--text-xs)' }}>
          {formatRelative(record.event_time_ms)}
        </span>
      </div>

      <div className="panel-body stack">
        <div className="errors-section">
          <KeyValueGrid
            entries={[
              {
                key: 'reason',
                label: 'Reason',
                value: (
                  <span className="mono" title={record.reason}>
                    {record.reason}
                  </span>
                ),
              },
              {
                key: 'type',
                label: 'Error type',
                value: record.error_type ? (
                  <span className="mono">{record.error_type}</span>
                ) : (
                  <Unavailable why="No error.type was recorded — this route has no exception to name." />
                ),
              },
              {
                key: 'time',
                label: 'Event time',
                value: formatTimestamp(record.event_time_ms),
              },
              {
                key: 'entity',
                label: 'Entity key',
                value: (
                  <CopyableId
                    value={record.entity_key}
                    display={formatEntityKey(record.entity_key)}
                    label="entity key"
                  />
                ),
              },
              {
                key: 'activation',
                label: 'Activation',
                value:
                  record.seq === null ? (
                    <Unavailable why="This record is not attributed to a single activation." />
                  ) : (
                    <Link
                      href={`/activations/${encodeURIComponent(record.entity_key)}/${record.seq}`}
                      className="mono"
                    >
                      seq {record.seq}
                    </Link>
                  ),
              },
              ...(hasSubReasons(record.reason)
                ? [
                    {
                      key: 'sub',
                      label: 'Sub-reason',
                      value: subReason ? (
                        <span className="mono">{subReason}</span>
                      ) : (
                        <Unavailable why="The detail string did not carry one of the documented codes." />
                      ),
                    },
                  ]
                : []),
              ...(intentId
                ? [
                    {
                      key: 'intent',
                      label: 'Intent id',
                      value: <CopyableId value={intentId} label="intent id" />,
                    },
                  ]
                : []),
            ]}
          />
        </div>

        <div className="errors-section">
          <h3 className="errors-h3">Failure position</h3>
          <div className="errors-position">
            <KeyValueGrid
              compact
              entries={[
                {
                  key: 'step',
                  label: 'Step at failure',
                  value: positionValue(record.failure_step),
                },
                {
                  key: 'last',
                  label: 'Last event',
                  value: record.failure_last_event ? (
                    <>
                      <span className="mono">{record.failure_last_event}</span>{' '}
                      <span className="muted">
                        ({humanizeEventType(record.failure_last_event)})
                      </span>
                    </>
                  ) : (
                    <Unavailable why={NO_CONTEXT} />
                  ),
                },
                {
                  key: 'intents',
                  label: 'Staged intents',
                  value: positionValue(record.failure_staged_intents),
                },
                {
                  key: 'llm',
                  label: 'LLM calls',
                  value: positionValue(record.failure_llm_calls),
                },
              ]}
            />
          </div>
          <p className="errors-note">
            {hasPosition ? (
              <>
                Where the activation was when it failed, not what it staged. Every field is a pure
                function of the activation&rsquo;s deterministic path, so a replay that fails the
                same way records the same position.
              </>
            ) : (
              <>
                This route records no failure position. The timeout route cannot read one (the
                coroutine may still be running), a failure before context construction has none, and
                the non-activation routes never had a context at all — so the runtime omits these
                attributes rather than writing zeros that would read as measurements.
              </>
            )}
          </p>
        </div>

        <div className="errors-section">
          <h3 className="errors-h3">Detail</h3>
          {record.detail ? (
            <div className="errors-detail">
              <CodeBlock code={record.detail} label="error detail" />
            </div>
          ) : (
            <p className="errors-note">
              <Chip tone="neutral" plain>
                Empty
              </Chip>{' '}
              This reason writes no detail — the record carries nothing further rather than
              inventing something to say.
            </p>
          )}
        </div>
      </div>
    </section>
  );
}

/** The compact form used in the occurrences table's own column. */
export function PositionCell({ record }: { record: ErrorRecord }) {
  const parts: string[] = [];
  if (record.failure_step !== null) parts.push(`step ${formatCount(record.failure_step)}`);
  if (record.failure_last_event) parts.push(`after ${record.failure_last_event}`);
  if (parts.length === 0) return <Unavailable why={NO_CONTEXT} />;
  return <span className="mono">{parts.join(' · ')}</span>;
}
