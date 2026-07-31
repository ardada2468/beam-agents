import type { ReactNode } from 'react';

/** A captioned block for wide content (diagrams, tables) that scrolls itself. */
export function Figure({ caption, children }: { caption?: string; children: ReactNode }) {
  return (
    <figure className="my-5">
      <div className="table-scroll">{children}</div>
      {caption ? (
        <figcaption className="mt-1.5 text-xs" style={{ color: 'var(--fg-muted)' }}>
          {caption}
        </figcaption>
      ) : null}
    </figure>
  );
}
