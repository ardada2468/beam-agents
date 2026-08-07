import type { Metadata } from 'next';
import Link from 'next/link';
import { notFound } from 'next/navigation';
import { allPages, findPage, isIndexable } from '@/lib/content';
import { renderMdx } from '@/lib/mdx';
import { absoluteUrl, SITE_NAME } from '@/lib/site';
import { CompactNav } from '@/components/CompactNav';
import { Sidebar } from '@/components/Sidebar';
import { hasTableOfContents, TableOfContents } from '@/components/TableOfContents';
import { StatusBadge } from '@/components/StatusBadge';
import { Provenance } from '@/components/Provenance';
import { STATUS_DESCRIPTIONS } from '@/lib/schema';

interface Params {
  section: string;
  slug: string;
}

/**
 * Every content page is pre-rendered at build time from the content tree, so
 * adding an MDX file is the whole act of adding a page — no route wiring, no
 * registration list that can fall out of sync.
 */
export async function generateStaticParams(): Promise<Params[]> {
  return allPages().map((page) => ({ section: page.section.slug, slug: page.slug }));
}

export async function generateMetadata({ params }: { params: Promise<Params> }): Promise<Metadata> {
  const { section, slug } = await params;
  const page = findPage(section, slug);
  if (!page) return {};
  const url = absoluteUrl(page.href);
  return {
    title: page.frontmatter.title,
    description: page.frontmatter.summary,
    alternates: { canonical: url },
    // Planned pages describe code that does not exist. They stay reachable and
    // stay out of the index.
    robots: isIndexable(page) ? undefined : { index: false, follow: true },
    openGraph: {
      title: `${page.frontmatter.title} — ${SITE_NAME}`,
      description: page.frontmatter.summary,
      url,
      type: 'article',
    },
  };
}

/**
 * A documentation page.
 *
 * The three columns sit inside the same `.shell` the landing page and the site
 * header use, so the reading column lands on the same gutters rather than in a
 * narrower box of its own — walking from `/` into a doc page should not look
 * like walking into a different site. The two gutters are separated from the
 * article by hairlines, which is how this site divides everything; nothing is
 * drawn as a card.
 *
 * The column track stays the same whether or not a page has a table of
 * contents, so the reading measure does not jump between pages; only the
 * gutter's contents and its hairline come and go with it.
 *
 * Three columns need about 1280px to be worth having. Below that they come off
 * one at a time rather than both at once: the contents gutter goes at `xl` and
 * the section gutter at `lg`, which gives the article the whole of a tablet's
 * width instead of leaving it in a laptop-sized reading column with two empty
 * gutters beside it. Whichever gutter is gone reappears as a collapsed
 * disclosure above the article, so no width loses the navigation entirely —
 * see `CompactNav`.
 *
 * The header deliberately carries no bottom rule. The first `.prose h2` already
 * draws one, so adding a second here would stack two rules a few lines apart.
 */
export default async function ContentPage({ params }: { params: Promise<Params> }) {
  const { section, slug } = await params;
  const page = findPage(section, slug);
  if (!page) notFound();

  const body = await renderMdx(page.body);
  const jsonLd = {
    '@context': 'https://schema.org',
    '@type': 'TechArticle',
    headline: page.frontmatter.title,
    description: page.frontmatter.summary,
    url: absoluteUrl(page.href),
    articleSection: page.section.title,
    isPartOf: { '@type': 'WebSite', name: SITE_NAME, url: absoluteUrl('/') },
  };

  return (
    <div className="shell">
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
      />

      <div className="grid gap-x-8 gap-y-10 py-10 sm:py-12 lg:grid-cols-[13rem_minmax(0,1fr)] xl:grid-cols-[14rem_minmax(0,1fr)_12rem] xl:gap-x-7">
        <aside
          className="hidden lg:block lg:border-r lg:pr-8 xl:pr-7"
          style={{ borderColor: 'var(--rule)' }}
        >
          <div className="sticky top-20">
            <Sidebar sectionSlug={section} current={slug} />
          </div>
        </aside>

        <article className="min-w-0">
          {/* `.eyebrow` sits on the nav so its line box matches the two
              gutters' labels; on the link alone the parent strut wins and the
              three headers sit on visibly different baselines. */}
          <nav aria-label="Breadcrumb" className="eyebrow mb-5">
            <Link
              href={`/${page.section.slug}`}
              className="no-underline"
              style={{ color: 'var(--ink-3)' }}
            >
              {page.section.title}
            </Link>
          </nav>

          <header className="mb-8">
            <h1 className="h-page">{page.frontmatter.title}</h1>
            <p className="lede mt-4">{page.frontmatter.summary}</p>
            <p className="mt-5 flex flex-wrap items-center gap-x-3 gap-y-2">
              <StatusBadge status={page.frontmatter.status} />
              <span className="text-[0.85rem]" style={{ color: 'var(--ink-3)' }}>
                {STATUS_DESCRIPTIONS[page.frontmatter.status]}
              </span>
            </p>
          </header>

          <CompactNav sectionSlug={section} current={slug} headings={page.headings} />

          <div className="prose">{body}</div>
          <Provenance verifies={page.frontmatter.verifies} sources={page.frontmatter.sources} />
        </article>

        {hasTableOfContents(page.headings) ? (
          <aside
            className="hidden xl:block xl:border-l xl:pl-7"
            style={{ borderColor: 'var(--rule)' }}
          >
            <div className="sticky top-20">
              <TableOfContents headings={page.headings} />
            </div>
          </aside>
        ) : null}
      </div>
    </div>
  );
}
