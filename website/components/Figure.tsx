import type { ReactNode } from 'react';

/**
 * A captioned block for wide content (diagrams, tables) that scrolls itself.
 *
 * It borrows `Diagram`'s frame verbatim — `.dg-figure`, `.dg-scroll`,
 * `.dg-caption` from `app/diagram.css` — rather than defining a second, nearly
 * identical one. A hand-drawn diagram and a wide table dropped onto the same
 * page are both figures, and they should look like siblings; when the frame
 * lived in two places they drifted apart instead.
 */
export function Figure({ caption, children }: { caption?: string; children: ReactNode }) {
  return (
    <figure className="dg-figure">
      <div className="dg-scroll">{children}</div>
      {caption ? <figcaption className="dg-caption">{caption}</figcaption> : null}
    </figure>
  );
}
