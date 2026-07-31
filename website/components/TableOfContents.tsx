import type { Heading } from '@/lib/content';

/**
 * The in-page table of contents.
 *
 * It is a gutter, not a panel: an `.eyebrow` label over quiet token-coloured
 * links, with depth carried by indent alone. Fewer than two headings and it is
 * not a contents list, it is a restatement of the title — so it renders nothing.
 *
 * Plenty of pages on this site are short enough to have no headings at all, so
 * the "renders nothing" case is the common one. `hasTableOfContents` exists so
 * the page can ask before it draws the gutter's hairline: a rule with nothing
 * beside it is worse than no rule.
 */

function shownHeadings(headings: readonly Heading[]): readonly Heading[] {
  return headings.filter((heading) => heading.depth <= 3);
}

export function hasTableOfContents(headings: readonly Heading[]): boolean {
  return shownHeadings(headings).length >= 2;
}

export function TableOfContents({ headings }: { headings: readonly Heading[] }) {
  const shown = shownHeadings(headings);
  if (shown.length < 2) return null;
  return (
    <nav aria-label="On this page" className="text-[0.85rem]">
      <p className="eyebrow">On this page</p>
      <ul className="mt-3.5 space-y-2">
        {shown.map((heading) => (
          <li key={heading.id} style={{ paddingLeft: heading.depth === 3 ? '0.85rem' : 0 }}>
            <a
              href={`#${heading.id}`}
              className="no-underline"
              style={{ color: heading.depth === 3 ? 'var(--ink-3)' : 'var(--ink-2)' }}
            >
              {heading.text}
            </a>
          </li>
        ))}
      </ul>
    </nav>
  );
}
