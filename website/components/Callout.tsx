import type { ReactNode } from 'react';

/**
 * A boxed aside. `kind="not-implemented"` is the standard opener every
 * `planned` page must carry — it is not decoration, it is the statement that
 * the code being described does not exist.
 */
export function Callout({
  kind = 'note',
  title,
  children,
}: {
  kind?: 'note' | 'warning' | 'not-implemented';
  title?: string;
  children: ReactNode;
}) {
  const warn = kind !== 'note';
  const heading = title ?? (kind === 'not-implemented' ? 'Not implemented' : undefined);
  return (
    <aside
      role="note"
      className="my-5 rounded-md border-l-4 px-4 py-3 text-[0.95rem]"
      style={{
        background: warn ? 'var(--warn-bg)' : 'var(--note-bg)',
        borderColor: warn ? 'var(--warn-border)' : 'var(--note-border)',
        color: warn ? 'var(--warn-fg)' : 'var(--note-fg)',
      }}
    >
      {heading ? <p className="mb-1 font-semibold">{heading}</p> : null}
      <div className="[&>*+*]:mt-2">{children}</div>
    </aside>
  );
}
