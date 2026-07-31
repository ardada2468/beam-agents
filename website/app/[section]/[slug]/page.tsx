import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { allPages, findPage, isIndexable } from '@/lib/content';
import { renderMdx } from '@/lib/mdx';
import { absoluteUrl, SITE_NAME } from '@/lib/site';
import { Sidebar } from '@/components/Sidebar';
import { TableOfContents } from '@/components/TableOfContents';
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
    <div className="mx-auto grid max-w-6xl gap-8 px-5 py-8 lg:grid-cols-[13rem_minmax(0,1fr)_13rem]">
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
      />
      <aside className="hidden lg:block">
        <div className="sticky top-20">
          <Sidebar sectionSlug={section} current={slug} />
        </div>
      </aside>

      <article className="min-w-0">
        <nav aria-label="Breadcrumb" className="mb-2 text-sm">
          <a href={`/${page.section.slug}`} style={{ color: 'var(--fg-muted)' }}>
            {page.section.title}
          </a>
        </nav>
        <header className="mb-6">
          <h1 className="text-[1.9rem] leading-tight font-bold tracking-tight">
            {page.frontmatter.title}
          </h1>
          <p className="mt-2 max-w-[70ch]" style={{ color: 'var(--fg-muted)' }}>
            {page.frontmatter.summary}
          </p>
          <p className="mt-3 flex flex-wrap items-center gap-2 text-xs">
            <StatusBadge status={page.frontmatter.status} />
            <span style={{ color: 'var(--fg-faint)' }}>
              {STATUS_DESCRIPTIONS[page.frontmatter.status]}
            </span>
          </p>
        </header>
        <div className="prose">{body}</div>
        <Provenance verifies={page.frontmatter.verifies} sources={page.frontmatter.sources} />
      </article>

      <aside className="hidden lg:block">
        <div className="sticky top-20">
          <TableOfContents headings={page.headings} />
        </div>
      </aside>
    </div>
  );
}
