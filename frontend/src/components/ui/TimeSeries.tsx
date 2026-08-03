/**
 * The bucketed time series chart.
 *
 * Bars, not lines, and this is not a style preference. The API returns counts
 * per contiguous time bucket; a line drawn between two bucket counts implies a
 * value at every instant between them, which bucketed data does not have. Bars
 * say "this much, in this interval", which is exactly what the data means.
 *
 * Buckets arrive contiguous with explicit zeros, so a quiet period is drawn as
 * a run of zero-height bars — a visible gap — rather than as a line skipping
 * across missing time.
 *
 * Rendered as plain SVG. A charting library for two chart types would add a
 * dependency, a bundle, and a second styling system to keep in sync with the
 * tokens.
 */

import { useId, useMemo, useState } from 'react';

import type { BucketPoint } from '@/lib/api-types';
import { formatCompact, formatTime } from '@/lib/format';

export interface SeriesSpec {
  key: string;
  label: string;
  points: BucketPoint[];
  /** A `--series-*` token name, or `error`. Defaults to the first series color. */
  tone?: 'default' | 'error' | 'info' | 'warn';
}

const TONE_VAR: Record<NonNullable<SeriesSpec['tone']>, string> = {
  default: 'var(--series-1)',
  error: 'var(--error)',
  info: 'var(--series-2)',
  warn: 'var(--series-3)',
};

const PAD_LEFT = 0;
const PAD_BOTTOM = 0;
const PAD_TOP = 8;
const HEIGHT = 160;
const VIEW_WIDTH = 720;

/**
 * A single-series bar chart over time, with a hover readout.
 *
 * `format` controls how values are labelled — counts, tokens, and ratios all
 * read differently and the axis should say which one it is.
 */
export default function TimeSeries({
  series,
  format = formatCompact,
  ariaLabel,
}: {
  series: SeriesSpec;
  format?: (value: number | null) => string;
  ariaLabel?: string;
}) {
  const gradientId = useId();
  const [hover, setHover] = useState<number | null>(null);

  const { points, max } = useMemo(() => {
    const values = series.points.map((p) => p.value);
    return { points: series.points, max: Math.max(...values, 1) };
  }, [series.points]);

  if (points.length === 0) {
    return (
      <p className="muted" style={{ padding: 'var(--space-4)' }}>
        No data in this window.
      </p>
    );
  }

  const plotWidth = VIEW_WIDTH - PAD_LEFT;
  const plotHeight = HEIGHT - PAD_BOTTOM - PAD_TOP;
  const barWidth = plotWidth / points.length;
  const fill = TONE_VAR[series.tone ?? 'default'];
  const active = hover !== null ? points[hover] : undefined;

  // Three gridlines: zero, mid, max. More would be noise on a chart this size.
  const gridValues = [0, max / 2, max];

  return (
    <figure style={{ margin: 0 }}>
      <div className="series-frame">
        <div className="series-ticks" aria-hidden="true">
          {gridValues
            .slice()
            .reverse()
            .map((value) => (
              <span key={value}>{format(value)}</span>
            ))}
        </div>
        <svg
          className="series"
          viewBox={`0 0 ${VIEW_WIDTH} ${HEIGHT}`}
          preserveAspectRatio="none"
          role="img"
          aria-label={ariaLabel ?? `${series.label} over time`}
          onMouseLeave={() => setHover(null)}
        >
          <title>{`${series.label}: peak ${format(max)}`}</title>
          <defs>
            <clipPath id={gradientId}>
              <rect x={PAD_LEFT} y={PAD_TOP} width={plotWidth} height={plotHeight} />
            </clipPath>
          </defs>

          {gridValues.map((value) => {
            const y = PAD_TOP + plotHeight - (value / max) * plotHeight;
            return (
              <line
                key={value}
                x1={PAD_LEFT}
                x2={VIEW_WIDTH}
                y1={y}
                y2={y}
                className="series__grid"
                vectorEffect="non-scaling-stroke"
              />
            );
          })}

          <g clipPath={`url(#${gradientId})`}>
            {points.map((point, index) => {
              const height = point.value > 0 ? Math.max(1.5, (point.value / max) * plotHeight) : 0;
              return (
                <rect
                  key={point.bucket_ms}
                  x={PAD_LEFT + index * barWidth}
                  y={PAD_TOP + plotHeight - height}
                  width={Math.max(barWidth - 1, 0.8)}
                  height={height}
                  fill={fill}
                  opacity={hover === null || hover === index ? 1 : 0.35}
                  onMouseEnter={() => setHover(index)}
                />
              );
            })}
          </g>
        </svg>
        <div className="series-times" aria-hidden="true">
          <span>{formatTime(points[0]?.bucket_ms)}</span>
          <span>{formatTime(points[points.length - 1]?.bucket_ms)}</span>
        </div>
      </div>

      <figcaption
        className="muted"
        style={{ fontSize: 'var(--text-xs)', minHeight: '1.4em', marginTop: 'var(--space-1)' }}
      >
        {active ? (
          <>
            <span className="mono">{formatTime(active.bucket_ms)}</span> · {format(active.value)}{' '}
            {series.label.toLowerCase()}
          </>
        ) : (
          <>
            {series.label} · peak {format(max)}
          </>
        )}
      </figcaption>
    </figure>
  );
}
