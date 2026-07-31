import { STATUS_DESCRIPTIONS, STATUS_LABELS, type Status } from '@/lib/schema';

/**
 * The maturity badge.
 *
 * It appears in the page header and beside every navigation entry, so a reader
 * never has to infer how finished something is from the confidence of the
 * prose. A 3px colour bar plus a mono label — no pill, no fill — so it reads
 * as a data attribute rather than a sticker.
 */
export function StatusBadge({ status, size = 'md' }: { status: Status; size?: 'sm' | 'md' }) {
  const color = `var(--status-${status})`;
  return (
    <span
      title={STATUS_DESCRIPTIONS[status]}
      className={
        size === 'sm'
          ? 'mono inline-flex shrink-0 items-center gap-1.5 text-[9.5px] tracking-[0.1em] uppercase'
          : 'mono inline-flex items-center gap-2 text-[10.5px] tracking-[0.12em] uppercase'
      }
      style={{ color }}
    >
      <span
        aria-hidden="true"
        className="inline-block"
        style={{ background: color, width: size === 'sm' ? 10 : 14, height: 2 }}
      />
      {STATUS_LABELS[status]}
    </span>
  );
}
