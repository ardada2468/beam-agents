import type { ReactNode } from 'react';

/**
 * The shared frame for every hand-drawn diagram on this site.
 *
 * The site had exactly one real diagram — the animated pipeline on the landing
 * page — and everywhere else a diagram was needed, the page drew one out of
 * box-drawing characters inside a code fence. Those read as code, wrap badly on
 * a phone, are invisible to a screen reader, and cannot use the stream colours
 * that mean something everywhere else on the site. This component is what
 * replaces them.
 *
 * Three things it guarantees, so no individual diagram has to remember them:
 *
 * 1. **Server-rendered SVG, no client JavaScript.** Same constraint the rest of
 *    the site holds, and the reason `scripts/check_site_ssr.mjs` passes.
 * 2. **A real accessible name and description.** `title` is the short name;
 *    `desc` is the prose a reader who cannot see the diagram gets instead, and
 *    it has to carry the same information the picture does, not name it.
 * 3. **It scrolls rather than squashing.** A diagram scaled to 360px wide is
 *    unreadable, so wide ones scroll inside their own box and the page never
 *    scrolls sideways. The floor is the `viewBox`'s own width — see
 *    `legibleFloor` — so no caller can author a diagram into being shrunk
 *    below the size its labels were drawn at.
 *
 * Geometry is authored against the `viewBox`, in the same coordinate space as
 * `PipelineDiagram`, and every colour comes from a design token (see
 * `app/diagram.css`) so both themes and the contrast check keep working.
 */

/**
 * Element ids are derived from the title rather than from a render counter.
 *
 * A counter would produce different ids on the server and on a re-render, and
 * `aria-labelledby` pointing at an id that moved is worse than no label at all.
 * The title is already required to be unique on a page — two diagrams called
 * the same thing are a content bug, and `check_a11y.mjs` reports the duplicate
 * id if one ever ships.
 */
function idFor(title: string): string {
  const slug = title
    .toLowerCase()
    .replace(/[^\w\s-]/g, '')
    .trim()
    .replace(/\s+/g, '-');
  return `dg-${slug || 'diagram'}`;
}

export interface DiagramProps {
  /** Short accessible name, e.g. "How an activation commits". */
  title: string;
  /**
   * The diagram in words, for a reader who does not get the picture. Say what
   * the diagram says — not "a diagram of the commit path".
   */
  desc: string;
  /** Printed under the diagram, for everyone. */
  caption?: string;
  /** SVG user-space box the children are drawn in. */
  viewBox: string;
  children: ReactNode;
}

/**
 * The width the drawing may never render below, in CSS pixels.
 *
 * It is the `viewBox`'s own width, so one user-space unit is at worst one CSS
 * pixel and the labels are never smaller than the size they were authored at.
 * This used to be a per-call-site prop, and almost every call site set it
 * *below* its own `viewBox` width — 560 under a 620-wide box, 560 under a
 * 640-wide one. That does not make a diagram fit; it silently rescales it, and
 * the 9.5px qualifier labels were landing at 8.3–8.9px on a phone and, because
 * the reading measure is narrower than these diagrams are wide, on a 1024px
 * tablet too. Deriving the floor from the geometry means a diagram cannot be
 * authored into that state.
 *
 * Above the floor the SVG still scales up with its column; below it the figure
 * scrolls, which is what `.dg-scroll` is for.
 */
function legibleFloor(viewBox: string): number {
  const width = Number(viewBox.trim().split(/[\s,]+/)[2]);
  return Number.isFinite(width) && width > 0 ? width : 620;
}

export function Diagram({ title, desc, caption, viewBox, children }: DiagramProps) {
  const id = idFor(title);
  const titleId = `${id}-title`;
  const descId = `${id}-desc`;

  return (
    <figure className="dg-figure">
      {/*
        A region that scrolls has to be reachable by keyboard, so it takes a
        tabindex and an accessible name — the same treatment `rehypeScrollable
        Tables` gives a wide table. axe's `scrollable-region-focusable` rule
        cannot see this one, because jsdom has no layout and so never observes
        the overflow.
      */}
      <div className="dg-scroll" tabIndex={0} role="region" aria-label={`${title} (scrollable)`}>
        <svg
          viewBox={viewBox}
          role="img"
          aria-labelledby={`${titleId} ${descId}`}
          preserveAspectRatio="xMidYMid meet"
          style={{ minWidth: legibleFloor(viewBox) }}
        >
          <title id={titleId}>{title}</title>
          <desc id={descId}>{desc}</desc>
          <defs>
            <marker
              id={`${id}-arrow`}
              viewBox="0 0 8 8"
              refX="7"
              refY="4"
              markerWidth="7"
              markerHeight="7"
              orient="auto"
            >
              <path d="M0,1 L7,4 L0,7 z" className="dg-arrow" />
            </marker>
          </defs>
          <g style={{ ['--dg-marker' as string]: `url(#${id}-arrow)` }}>{children}</g>
        </svg>
      </div>
      {caption ? <figcaption className="dg-caption">{caption}</figcaption> : null}
    </figure>
  );
}

/**
 * A labelled box. `sub` is the small uppercase second line the pipeline diagram
 * uses to qualify a node ("FLATTEN", "SEPARATE SERVICE") — the qualifier is
 * usually the part that carries the information.
 */
export function DgNode({
  x,
  y,
  w,
  h,
  label,
  sub,
  soft = false,
  stream,
}: {
  x: number;
  y: number;
  w: number;
  h: number;
  label: string;
  sub?: string;
  /** Renders as a secondary, filled block rather than a primary outlined one. */
  soft?: boolean;
  /** Tints the label with one of the four stream colours. */
  stream?: 'output' | 'intents' | 'traces' | 'errors';
}) {
  const cx = x + w / 2;
  const cy = y + h / 2;
  const labelY = sub ? cy - 1 : cy + 4;
  return (
    <g>
      <rect
        x={x}
        y={y}
        width={w}
        height={h}
        rx="2"
        className={soft ? 'dg-node dg-node--soft' : 'dg-node'}
      />
      <text
        x={cx}
        y={labelY}
        textAnchor="middle"
        className={stream ? `dg-label dg-label--${stream}` : 'dg-label'}
      >
        {label}
      </text>
      {sub ? (
        <text x={cx} y={cy + 13} textAnchor="middle" className="dg-label--faint">
          {sub}
        </text>
      ) : null}
    </g>
  );
}

/**
 * A connector. `dashed` marks a path that leaves the pipeline — the same
 * convention the landing-page diagram uses for the intent loop, where a dashed
 * line means "this goes out through the message bus and comes back".
 */
export function DgEdge({
  d,
  dashed = false,
  stream,
  label,
  labelX,
  labelY,
  arrow = true,
}: {
  d: string;
  dashed?: boolean;
  stream?: 'output' | 'intents' | 'traces' | 'errors';
  label?: string;
  labelX?: number;
  labelY?: number;
  arrow?: boolean;
}) {
  const className = [
    'dg-line',
    dashed ? 'dg-line--dashed' : null,
    stream ? `dg-line--${stream}` : null,
    // `marker-end` is set in CSS from `--dg-marker`, because a `markerEnd`
    // attribute cannot reference a custom property and each Diagram mints its
    // own marker id.
    arrow ? null : 'dg-line--open',
  ]
    .filter(Boolean)
    .join(' ');
  return (
    <g>
      <path d={d} className={className} />
      {label !== undefined && labelX !== undefined && labelY !== undefined ? (
        <text x={labelX} y={labelY} textAnchor="middle" className="dg-label--faint">
          {label}
        </text>
      ) : null}
    </g>
  );
}

/** Free-standing text, for rail names and annotations outside a node. */
export function DgText({
  x,
  y,
  children,
  anchor = 'start',
  variant = 'label',
  stream,
}: {
  x: number;
  y: number;
  children: string;
  anchor?: 'start' | 'middle' | 'end';
  variant?: 'label' | 'title' | 'faint';
  stream?: 'output' | 'intents' | 'traces' | 'errors';
}) {
  const base =
    variant === 'title' ? 'dg-label--title' : variant === 'faint' ? 'dg-label--faint' : 'dg-label';
  return (
    <text x={x} y={y} textAnchor={anchor} className={stream ? `${base} dg-label--${stream}` : base}>
      {children}
    </text>
  );
}
