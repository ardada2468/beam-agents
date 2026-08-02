import { STATUS_DESCRIPTIONS, STATUS_LABELS, type Status } from '@/lib/schema';

/**
 * The maturity badge.
 *
 * It appears in the page header, on every section-index row, and beside every
 * navigation entry, so a reader never has to infer how finished something is
 * from the confidence of the prose. It is a `.chip` — flat, outlined, radius 2 —
 * because this site has no filled pills anywhere, and the label itself carries
 * the meaning, so the status colour is reinforcement rather than the only cue.
 *
 * `sm` drops the outline. Eight boxed chips down a 14rem sidebar reads as noise;
 * the mono caps alone are enough at that density, and keeping `.chip` as the
 * base means both sizes still share one set of metrics.
 */
export function StatusBadge({ status, size = 'md' }: { status: Status; size?: 'sm' | 'md' }) {
  const color = `var(--status-${status})`;
  const compact = size === 'sm';
  return (
    <span
      title={STATUS_DESCRIPTIONS[status]}
      className="chip shrink-0"
      style={
        compact
          ? { color, borderColor: 'transparent', padding: 0, fontSize: '0.625rem' }
          : { color, borderColor: 'var(--rule)' }
      }
    >
      <span
        aria-hidden="true"
        className="inline-block"
        style={{ background: color, width: compact ? 10 : 14, height: 2 }}
      />
      {STATUS_LABELS[status]}
    </span>
  );
}
