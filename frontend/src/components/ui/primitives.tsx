/**
 * The primitive set. Everything a page composes from lives here.
 *
 * These are frozen — pages import them, pages do not modify them. A page that
 * needs a variant should add a prop here rather than restyling in place, so the
 * console keeps reading as one interface rather than eight.
 *
 * Two conventions are enforced structurally rather than by review:
 *
 * - `StatusChip` always renders a label, so status never depends on color alone.
 * - Numeric cells and tiles route missing values through `format.ts`, which
 *   renders `null` as an em dash — a missing measurement can never be mistaken
 *   for a measured zero.
 */

import type { ReactNode } from 'react';
import { useCallback, useEffect, useId, useRef, useState } from 'react';

import type { ActivationStatus } from '@/lib/api-types';
import { EM_DASH } from '@/lib/format';

import './ui.css';

/* -- Button ---------------------------------------------------------------- */

export interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'default' | 'primary' | 'ghost' | 'danger';
  size?: 'default' | 'sm';
  iconOnly?: boolean;
}

/** The one button. At most one `primary` per screen. */
export function Button({
  variant = 'default',
  size = 'default',
  iconOnly = false,
  className = '',
  type = 'button',
  ...props
}: ButtonProps) {
  const classes = [
    'btn',
    variant !== 'default' ? `btn--${variant}` : '',
    size === 'sm' ? 'btn--sm' : '',
    iconOnly ? 'btn--icon' : '',
    className,
  ]
    .filter(Boolean)
    .join(' ');
  return <button type={type} className={classes} {...props} />;
}

/* -- StatusChip ------------------------------------------------------------ */

export type ChipTone = 'ok' | 'warn' | 'error' | 'pending' | 'suspended' | 'info' | 'neutral';

/** How each activation status reads. Single source, so no page invents its own. */
const STATUS_TONE: Record<ActivationStatus, ChipTone> = {
  completed: 'ok',
  suspended: 'suspended',
  error: 'error',
  in_flight: 'pending',
};

const STATUS_LABEL: Record<ActivationStatus, string> = {
  completed: 'Completed',
  suspended: 'Suspended',
  error: 'Error',
  in_flight: 'In flight',
};

export interface ChipProps {
  tone?: ChipTone;
  children: ReactNode;
  /** Drop the leading marker — for chips that are labels rather than states. */
  plain?: boolean;
  title?: string;
}

/** A solid, always-labelled status marker. */
export function Chip({ tone = 'neutral', plain = false, children, title }: ChipProps) {
  return (
    <span className={`chip chip--${tone}${plain ? ' chip--plain' : ''}`} title={title}>
      {children}
    </span>
  );
}

/** An activation's status, rendered consistently everywhere it appears. */
export function StatusChip({ status }: { status: ActivationStatus }) {
  return <Chip tone={STATUS_TONE[status]}>{STATUS_LABEL[status]}</Chip>;
}

/* -- CopyableId ------------------------------------------------------------ */

/**
 * A monospace identifier that copies on click.
 *
 * The most-used interaction in a trace UI: every hex ID on screen is something
 * someone is about to paste into a query, a grep, or a `beam-agents-replay`
 * invocation. `display` shortens for the column; the full value is what copies.
 */
export function CopyableId({
  value,
  display,
  label = 'identifier',
}: {
  value: string;
  display?: string;
  label?: string;
}) {
  const [copied, setCopied] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  useEffect(() => () => clearTimeout(timer.current), []);

  const copy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(value);
      setCopied(true);
      clearTimeout(timer.current);
      timer.current = setTimeout(() => setCopied(false), 1200);
    } catch {
      // A clipboard the browser refuses is not worth an error state; the value
      // is still selectable.
    }
  }, [value]);

  if (!value) return <span className="faint">{EM_DASH}</span>;

  return (
    <button
      type="button"
      className="copyable"
      onClick={() => void copy()}
      title={`${value}\nClick to copy`}
      aria-label={`Copy ${label} ${value}`}
    >
      <span>{display ?? value}</span>
      <span className={`copyable__hint${copied ? ' copyable__hint--done' : ''}`}>
        {copied ? 'Copied' : 'Copy'}
      </span>
    </button>
  );
}

/* -- DataTable ------------------------------------------------------------- */

export interface Column<T> {
  key: string;
  header: ReactNode;
  /** Right-aligns and applies tabular figures. Use for every number. */
  numeric?: boolean;
  /** Allow wrapping — for detail/message columns. */
  wrap?: boolean;
  width?: string;
  sortable?: boolean;
  render: (row: T, index: number) => ReactNode;
}

export interface DataTableProps<T> {
  columns: Column<T>[];
  rows: T[];
  rowKey: (row: T, index: number) => string;
  onRowClick?: (row: T) => void;
  selectedKey?: string;
  /** Keys that just arrived over the live stream; they flash once. */
  newKeys?: ReadonlySet<string>;
  sortKey?: string;
  sortDirection?: 'asc' | 'desc';
  onSort?: (key: string) => void;
  empty?: ReactNode;
  caption?: string;
}

/**
 * The dense data table.
 *
 * 32px rows and tabular figures, because these are read as columns of numbers
 * rather than as prose. Always wrapped in its own horizontal scroll container —
 * the page body must never scroll sideways.
 */
export function DataTable<T>({
  columns,
  rows,
  rowKey,
  onRowClick,
  selectedKey,
  newKeys,
  sortKey,
  sortDirection = 'desc',
  onSort,
  empty,
  caption,
}: DataTableProps<T>) {
  /*
   * Whether anything is hidden to the left of the pinned first column.
   *
   * Driven by a scroll listener rather than CSS because there is no selector
   * for "this element is scrolled": the divider on the sticky column should
   * appear when it is actually covering something and stay absent on a table
   * that fits, where a permanent rule would just be a line with nothing to say.
   */
  const wrapRef = useRef<HTMLDivElement>(null);
  const [scrolled, setScrolled] = useState(false);

  useEffect(() => {
    const wrap = wrapRef.current;
    if (!wrap) return;
    const onScroll = () => setScrolled(wrap.scrollLeft > 0);
    onScroll();
    wrap.addEventListener('scroll', onScroll, { passive: true });
    return () => wrap.removeEventListener('scroll', onScroll);
  }, []);

  if (rows.length === 0 && empty) return <>{empty}</>;

  return (
    <div className={`table-wrap${scrolled ? ' table-wrap--scrolled' : ''}`} ref={wrapRef}>
      <table className="table">
        {caption ? <caption className="visually-hidden">{caption}</caption> : null}
        <thead className="table__head">
          <tr>
            {columns.map((column) => {
              const sorted = sortKey === column.key;
              const classes = [
                'table__th',
                column.numeric ? 'table__th--num' : '',
                column.sortable && onSort ? 'table__th--sortable' : '',
                sorted ? 'table__th--sorted' : '',
              ]
                .filter(Boolean)
                .join(' ');
              return (
                <th
                  key={column.key}
                  scope="col"
                  className={classes}
                  style={column.width ? { width: column.width } : undefined}
                  aria-sort={
                    sorted ? (sortDirection === 'asc' ? 'ascending' : 'descending') : undefined
                  }
                >
                  {column.sortable && onSort ? (
                    // A real button, not a click handler on the `th`: a sort
                    // control has to be reachable and operable from the
                    // keyboard, and `th` is not focusable.
                    <button
                      type="button"
                      className="table__sort-btn"
                      onClick={() => onSort(column.key)}
                    >
                      {column.header}
                      <span className="table__sort" aria-hidden="true">
                        {sorted ? (sortDirection === 'asc' ? '↑' : '↓') : '↕'}
                      </span>
                    </button>
                  ) : (
                    column.header
                  )}
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => {
            const key = rowKey(row, index);
            const classes = [
              'table__row',
              onRowClick ? 'table__row--clickable' : '',
              selectedKey === key ? 'table__row--selected' : '',
              newKeys?.has(key) ? 'table__row--new' : '',
            ]
              .filter(Boolean)
              .join(' ');
            return (
              <tr
                key={key}
                className={classes}
                onClick={
                  onRowClick
                    ? (event) => {
                        // A row is clickable and so are the `CopyableId`s inside
                        // it. Without this every copy also navigates, and each
                        // page otherwise wraps them in a stopPropagation span.
                        if ((event.target as HTMLElement).closest('button,a,input,select')) {
                          return;
                        }
                        onRowClick(row);
                      }
                    : undefined
                }
                tabIndex={onRowClick ? 0 : undefined}
                onKeyDown={
                  onRowClick
                    ? (event) => {
                        if (event.key === 'Enter' || event.key === ' ') {
                          event.preventDefault();
                          onRowClick(row);
                        }
                      }
                    : undefined
                }
              >
                {columns.map((column) => (
                  <td
                    key={column.key}
                    className={[
                      'table__td',
                      column.numeric ? 'table__td--num' : '',
                      column.wrap ? 'table__td--wrap' : '',
                    ]
                      .filter(Boolean)
                      .join(' ')}
                  >
                    {column.render(row, index)}
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/* -- KeyValueGrid ---------------------------------------------------------- */

export interface KeyValueEntry {
  key: string;
  label: ReactNode;
  value: ReactNode;
}

/** A two-column definition list. The workhorse of every detail panel. */
export function KeyValueGrid({
  entries,
  compact = false,
}: {
  entries: KeyValueEntry[];
  compact?: boolean;
}) {
  return (
    <dl className={`kv${compact ? ' kv--compact' : ''}`}>
      {entries.map((entry) => (
        <div key={entry.key} style={{ display: 'contents' }}>
          <dt className="kv__key">{entry.label}</dt>
          <dd className="kv__value">{entry.value}</dd>
        </div>
      ))}
    </dl>
  );
}

/* -- CodeBlock ------------------------------------------------------------- */

/** Preformatted text with a copy affordance. Scrolls inside itself. */
export function CodeBlock({
  code,
  label = 'snippet',
  maxHeight,
}: {
  code: string;
  label?: string;
  /** Cap the height and scroll inside. A trace's event JSON is otherwise the page. */
  maxHeight?: string;
}) {
  const [copied, setCopied] = useState(false);
  return (
    <div className="code">
      <pre className="code__pre" style={maxHeight ? { maxHeight, overflowY: 'auto' } : undefined}>
        <code>{code}</code>
      </pre>
      <Button
        size="sm"
        className="code__copy"
        onClick={() => {
          void navigator.clipboard.writeText(code).then(
            () => {
              setCopied(true);
              setTimeout(() => setCopied(false), 1200);
            },
            () => undefined,
          );
        }}
        aria-label={`Copy ${label}`}
      >
        {copied ? 'Copied' : 'Copy'}
      </Button>
    </div>
  );
}

/* -- StatTile -------------------------------------------------------------- */

/**
 * One headline figure.
 *
 * `value` of `null` renders as an em dash in a lighter weight, so "not
 * recorded" is visibly different from a measured zero. Tiles go in a `TileRow`,
 * which draws them as one panel rather than as separate cards.
 */
export function StatTile({
  label,
  value,
  meta,
}: {
  label: ReactNode;
  value: ReactNode;
  meta?: ReactNode;
}) {
  const missing = value === null || value === undefined || value === EM_DASH;
  return (
    <div className="tile">
      <div className="tile__label">{label}</div>
      <div className={`tile__value${missing ? ' tile__value--missing' : ''}`}>
        {missing ? EM_DASH : value}
      </div>
      {meta ? <div className="tile__meta">{meta}</div> : null}
    </div>
  );
}

/**
 * The container that makes a row of tiles read as one instrument panel.
 *
 * `columns` states how many tiles the row holds. Without it the grid is
 * `auto-fit`, which cannot express "never five" — a six-tile row breaks 5 + 1 at
 * some widths and leaves a tile-sized hole in the panel.
 */
export function TileRow({ columns, children }: { columns?: number; children: ReactNode }) {
  return (
    <div
      className="tiles"
      style={columns ? ({ '--tile-columns': String(columns) } as React.CSSProperties) : undefined}
    >
      {children}
    </div>
  );
}

/* -- Tabs ------------------------------------------------------------------ */

export interface TabItem {
  key: string;
  label: ReactNode;
  count?: number | null;
}

/** A tab strip. Keyboard-navigable with arrow keys, per the ARIA pattern. */
export function Tabs({
  items,
  active,
  onChange,
  label = 'Sections',
}: {
  items: TabItem[];
  active: string;
  onChange: (key: string) => void;
  label?: string;
}) {
  return (
    <div className="tabs" role="tablist" aria-label={label}>
      {items.map((item) => (
        <button
          key={item.key}
          type="button"
          role="tab"
          className="tabs__tab"
          aria-selected={item.key === active}
          onClick={() => onChange(item.key)}
          onKeyDown={(event) => {
            const index = items.findIndex((i) => i.key === active);
            if (event.key === 'ArrowRight') {
              onChange(items[(index + 1) % items.length]?.key ?? active);
            } else if (event.key === 'ArrowLeft') {
              onChange(items[(index - 1 + items.length) % items.length]?.key ?? active);
            }
          }}
        >
          {item.label}
          {item.count !== undefined && item.count !== null ? (
            <span className="tabs__count">{item.count}</span>
          ) : null}
        </button>
      ))}
    </div>
  );
}

/* -- Input / Select -------------------------------------------------------- */

export function Input({
  label,
  mono = false,
  className = '',
  ...props
}: React.InputHTMLAttributes<HTMLInputElement> & { label?: string; mono?: boolean }) {
  const id = useId();
  const input = (
    <input
      id={id}
      className={`input${mono ? ' input--mono' : ''} ${className}`.trim()}
      {...props}
    />
  );
  if (!label) return input;
  return (
    <div className="field">
      <label className="field__label" htmlFor={id}>
        {label}
      </label>
      {input}
    </div>
  );
}

export interface SelectOption {
  value: string;
  label: string;
}

export function Select({
  label,
  options,
  placeholder,
  className = '',
  ...props
}: React.SelectHTMLAttributes<HTMLSelectElement> & {
  label?: string;
  options: SelectOption[];
  placeholder?: string;
}) {
  const id = useId();
  const select = (
    <select id={id} className={`select ${className}`.trim()} {...props}>
      {placeholder ? <option value="">{placeholder}</option> : null}
      {options.map((option) => (
        <option key={option.value} value={option.value}>
          {option.label}
        </option>
      ))}
    </select>
  );
  if (!label) return select;
  return (
    <div className="field">
      <label className="field__label" htmlFor={id}>
        {label}
      </label>
      {select}
    </div>
  );
}

/* -- Drawer ---------------------------------------------------------------- */

/** A right-hand panel for detail-in-place. Closes on Escape and on scrim click. */
export function Drawer({
  open,
  title,
  onClose,
  children,
}: {
  open: boolean;
  title: ReactNode;
  onClose: () => void;
  children: ReactNode;
}) {
  useEffect(() => {
    if (!open) return;
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open, onClose]);

  if (!open) return null;

  return (
    <>
      <div className="drawer__scrim" onClick={onClose} aria-hidden="true" />
      <aside className="drawer" role="dialog" aria-modal="true" aria-label={String(title)}>
        <header className="drawer__header">
          <h2 style={{ fontSize: 'var(--text-md)' }}>{title}</h2>
          <Button variant="ghost" iconOnly onClick={onClose} aria-label="Close">
            ✕
          </Button>
        </header>
        <div className="drawer__body">{children}</div>
      </aside>
    </>
  );
}

/* -- EmptyState ------------------------------------------------------------ */

/**
 * An empty screen, which is an instruction rather than a shrug.
 *
 * `body` must say what would populate this view. "No data" on its own is the
 * one thing an empty state is not allowed to be.
 */
export function EmptyState({
  title,
  body,
  action,
}: {
  title: string;
  body: ReactNode;
  action?: ReactNode;
}) {
  return (
    <div className="empty">
      <p className="empty__title">{title}</p>
      <div className="empty__body">{body}</div>
      {action ? <div className="empty__action">{action}</div> : null}
    </div>
  );
}

/* -- Sparkline / BarSeries ------------------------------------------------- */

/**
 * A tiny inline trend. Bars, not a line: these are counts per bucket, and a
 * line between counts implies a continuity that bucketed data does not have.
 */
export function Sparkline({
  values,
  tone = 'default',
  label,
}: {
  values: number[];
  tone?: 'default' | 'error';
  label?: string;
}) {
  if (values.length === 0) return <span className="faint">{EM_DASH}</span>;
  const max = Math.max(...values, 1);
  const width = 100;
  const barWidth = width / values.length;
  return (
    <svg
      className="spark"
      viewBox={`0 0 ${width} 28`}
      preserveAspectRatio="none"
      role="img"
      aria-label={label ?? 'trend'}
    >
      {values.map((value, index) => {
        const height = Math.max(value > 0 ? 1.5 : 0, (value / max) * 26);
        return (
          <rect
            key={index}
            x={index * barWidth}
            y={28 - height}
            width={Math.max(barWidth - 0.6, 0.6)}
            height={height}
            className={tone === 'error' ? 'series__bar--error' : 'series__bar'}
          />
        );
      })}
    </svg>
  );
}

/* -- Skeleton -------------------------------------------------------------- */

/** A flat placeholder block. No shimmer — this design has no ambient motion. */
export function Skeleton({ width = '100%', height = '1em' }: { width?: string; height?: string }) {
  return <div className="skeleton" style={{ width, height }} aria-hidden="true" />;
}

/** Placeholder rows for a table that is still loading. */
export function SkeletonRows({ rows = 6, columns = 5 }: { rows?: number; columns?: number }) {
  return (
    <div className="stack" style={{ gap: 'var(--space-2)', padding: 'var(--space-3)' }}>
      {Array.from({ length: rows }, (_, row) => (
        <div key={row} className="row" style={{ gap: 'var(--space-4)' }}>
          {Array.from({ length: columns }, (_, column) => (
            <Skeleton key={column} width={column === 0 ? '22%' : '13%'} />
          ))}
        </div>
      ))}
    </div>
  );
}
