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
      <span style={{ color: 'var(--fg-faint)' }} data-not-established="true">
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
            style={{ marginLeft: '0.15em' }}
          >
            [src]
          </a>
        </sup>
      ) : null}
      {cell.backing ? (
        <span className="mt-1 block font-mono text-[0.72rem]" style={{ color: 'var(--fg-faint)' }}>
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
    <figure className="my-6">
      <div className="table-scroll">
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
      {caption ? (
        <figcaption className="mt-1.5 text-xs" style={{ color: 'var(--fg-muted)' }}>
          {caption}
        </figcaption>
      ) : null}
    </figure>
  );
}
