import type { ReactNode } from 'react';

type Kind = 'note' | 'warning' | 'not-implemented';

/**
 * A standing aside.
 *
 * `kind="not-implemented"` is the standard opener every `planned` page must
 * carry — it is not decoration, it is the statement that the code being
 * described does not exist. That is why it is the loudest thing this site
 * draws, and why it may not be softened into an ordinary note.
 *
 * The emphasis comes from the site's own vocabulary rather than from a coloured
 * card: a flat `--paper-2` ground with no radius and no border of its own, a
 * 2px coloured edge, and a mono eyebrow in the same colour. The two colours it
 * can take are the ones the landing page already uses for the same two
 * meanings — `--s-errors` heads its "Not built" column, and `--s-intents` is
 * the amber it reserves for something staged but not yet done — so the palette
 * stays closed and every pair here is one `scripts/check_a11y.mjs` already
 * checks for contrast in both themes.
 *
 * `role="note"` is what makes the aside announce itself as a note rather than
 * as an unnamed complementary landmark; the a11y check depends on it.
 */

const ACCENT: Record<Kind, string> = {
  note: 'var(--rule-2)',
  warning: 'var(--s-intents)',
  'not-implemented': 'var(--s-errors)',
};

export function Callout({
  kind = 'note',
  title,
  children,
}: {
  kind?: Kind;
  title?: string;
  children: ReactNode;
}) {
  const accent = ACCENT[kind];
  // A plain note earns a heading only when the author writes one. The
  // not-implemented opener always gets one, because the heading *is* the claim.
  const heading = title ?? (kind === 'not-implemented' ? 'Not implemented' : undefined);
  return (
    <aside
      role="note"
      className="my-6 py-4 pr-5 pl-4"
      style={{
        background: 'var(--paper-2)',
        borderLeft: `2px solid ${accent}`,
      }}
    >
      {heading ? (
        <p className="eyebrow" style={{ color: kind === 'note' ? 'var(--ink-3)' : accent }}>
          {heading}
        </p>
      ) : null}
      <div
        className={`text-[0.95rem] [&>*+*]:mt-2 ${heading ? 'mt-2.5' : ''}`}
        style={{ color: kind === 'note' ? 'var(--ink-2)' : 'var(--ink)' }}
      >
        {children}
      </div>
    </aside>
  );
}
