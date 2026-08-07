import Link from 'next/link';
import type { Heading } from '@/lib/content';
import { pagesInSection } from '@/lib/content';
import { SECTION_BY_SLUG } from '@/lib/sections';
import { Literals } from './Literals';
import { StatusBadge } from './StatusBadge';
import { hasTableOfContents } from './TableOfContents';

/**
 * The two navigation gutters, folded into the article for the widths that have
 * no room for a gutter.
 *
 * A documentation page has three columns on a wide screen: where you are in the
 * section, the article, and where you are in the article. Both gutters were
 * simply dropped below the three-column breakpoint, which left every phone and
 * every tablet reader with a breadcrumb and nothing else — no way to reach the
 * next page in a section, and no way to see or skip to a heading on a page that
 * runs to several screens. That is most of the site's traffic navigating by
 * scrolling.
 *
 * They come back as disclosures, collapsed, so they cost one line each above
 * the article and nothing below it. `<details>` is used rather than a scripted
 * menu for the same reason the pipeline's pause control is a checkbox: the rest
 * of the site works with JavaScript disabled, and `scripts/check_site_ssr.mjs`
 * holds it to that.
 *
 * Each disclosure appears exactly where its gutter is not: the section list
 * below `lg`, where the left gutter is hidden, and the contents below `xl`,
 * where the right one is. On a wide screen both are gone and the gutters are
 * back.
 */
export function CompactNav({
  sectionSlug,
  current,
  headings,
}: {
  sectionSlug: string;
  current: string;
  headings: readonly Heading[];
}) {
  const section = SECTION_BY_SLUG.get(sectionSlug);
  const pages = pagesInSection(sectionSlug);
  const showSection = Boolean(section) && pages.length > 1;
  const showContents = hasTableOfContents(headings);
  if (!showSection && !showContents) return null;

  return (
    <div className="mb-9 grid gap-2.5">
      {showSection ? (
        <details className="disclosure lg:hidden">
          <summary className="disclosure__summary">
            <span className="eyebrow">In {section?.title}</span>
            <span className="disclosure__count mono">{pages.length}</span>
          </summary>
          <ul className="disclosure__body">
            {pages.map((page) => {
              const active = page.slug === current;
              return (
                <li key={page.href}>
                  <Link
                    href={page.href}
                    aria-current={active ? 'page' : undefined}
                    className="disclosure__row no-underline"
                    style={{
                      color: active ? 'var(--ink)' : 'var(--ink-2)',
                      fontWeight: active ? 600 : 400,
                    }}
                  >
                    <span>{page.frontmatter.title}</span>
                    <StatusBadge status={page.frontmatter.status} size="sm" />
                  </Link>
                </li>
              );
            })}
          </ul>
        </details>
      ) : null}

      {showContents ? (
        <details className="disclosure xl:hidden">
          <summary className="disclosure__summary">
            <span className="eyebrow">On this page</span>
          </summary>
          <ul className="disclosure__body">
            {headings
              .filter((heading) => heading.depth <= 3)
              .map((heading) => (
                <li key={heading.id}>
                  <a
                    href={`#${heading.id}`}
                    className="disclosure__row no-underline"
                    style={{
                      color: heading.depth === 3 ? 'var(--ink-3)' : 'var(--ink-2)',
                      paddingLeft: heading.depth === 3 ? '1.9rem' : undefined,
                    }}
                  >
                    <Literals text={heading.text} />
                  </a>
                </li>
              ))}
          </ul>
        </details>
      ) : null}
    </div>
  );
}
