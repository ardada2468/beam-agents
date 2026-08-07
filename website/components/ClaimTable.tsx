import { sourceAnchor } from '@/lib/sources';

/**
 * The comparison table.
 *
 * Two rules are enforced by the component rather than by authorial care:
 *
 * 1. A cell with nothing behind it renders the literal text "Not established".
 *    Empty cells read as "no difference"; blank space in a comparison table is
 *    a claim, and this makes the absence explicit instead.
 * 2. A cell about another project must carry `source` (a URL that also appears
 *    in the page's frontmatter `sources`), and renders a footnote marker
 *    linking to the dated citation. `scripts/verify_docs_claims.py` fails the
 *    build if the frontmatter entry is missing.
 *
 * A cell about beam-agents carries `backing` — a spec or test path — which
 * renders as the cell's own provenance line.
 *
 * The styling is the site's figure frame — a hairline scroll box and a caption
 * under it, the same one `Diagram`, `Figure`, and `Example` use — so a
 * comparison reads as one more piece of evidence rather than as a widget. The
 * `<caption className="sr-only">` and the `scope` attributes are not
 * presentation and must survive any restyle: they are what make the table
 * navigable by row and column to a screen reader, and `scripts/check_a11y.mjs`
 * audits a comparison route for exactly that.
 */

export interface ClaimCell {
  readonly text: string;
  /** Citation URL, for claims about another project. */
  readonly source?: string;
  /** `openspec/specs/...` or `tests/...` path, for claims about beam-agents. */
  readonly backing?: string;
}

export interface ClaimRow {
  readonly aspect: string;
  readonly cells: readonly (ClaimCell | null)[];
}

export function Cell({ children }: { children?: string }) {
  return <>{children ?? 'Not established'}</>;
}

function CellBody({ cell }: { cell: ClaimCell | null }) {
  if (cell === null) {
    return (
      <span style={{ color: 'var(--ink-3)' }} data-not-established="true">
        Not established
      </span>
    );
  }
  return (
    <>
      <span>{cell.text}</span>
      {cell.source ? (
        <sup>
          <a
            href={`#${sourceAnchor(cell.source)}`}
            aria-label="Jump to the citation for this claim"
            className="mono"
            style={{ marginLeft: '0.15em', fontSize: '0.68rem' }}
          >
            [src]
          </a>
        </sup>
      ) : null}
      {cell.backing ? (
        <span className="mono mt-1.5 block text-[0.7rem]" style={{ color: 'var(--ink-3)' }}>
          {cell.backing}
        </span>
      ) : null}
    </>
  );
}

export function ClaimTable({
  columns,
  rows,
  caption,
}: {
  columns: readonly string[];
  rows: readonly ClaimRow[];
  caption?: string;
}) {
  return (
    <figure className="dg-figure">
      {/* Focusable for the same reason `rehypeScrollableTables` makes the
          markdown ones focusable: a comparison this wide always scrolls on a
          phone, and a scrolling region no keyboard can reach hides its own
          right-hand columns. */}
      <div
        className="table-scroll"
        tabIndex={0}
        role="region"
        aria-label={`${caption ?? 'Comparison'} (scrollable table)`}
      >
        <table>
          <caption className="sr-only">{caption ?? 'Comparison'}</caption>
          <thead>
            <tr>
              <th scope="col">Aspect</th>
              {columns.map((column) => (
                <th key={column} scope="col">
                  {column}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.aspect}>
                <th scope="row" className="font-medium">
                  {row.aspect}
                </th>
                {columns.map((column, index) => (
                  <td key={column}>
                    <CellBody cell={row.cells[index] ?? null} />
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {caption ? <figcaption className="dg-caption">{caption}</figcaption> : null}
    </figure>
  );
}
