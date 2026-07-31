import type { Metadata } from 'next';
import Link from 'next/link';
import { searchServerSide } from '@/lib/search';
import { absoluteUrl } from '@/lib/site';

/**
 * Server-rendered search.
 *
 * This route is the no-JavaScript path: the header's search box is a real GET
 * form, and this page answers it with results already in the HTML. It carries
 * `noindex` because a search-results URL is not content.
 *
 * Presentation follows the landing page: a reading-width `.shell-narrow`, flat
 * `.field` and `.btn` controls, and results as hairline-separated rows rather
 * than a stack of bordered cards.
 */
export const metadata: Metadata = {
  title: 'Search',
  description: 'Search the beam-agents documentation.',
  alternates: { canonical: absoluteUrl('/search') },
  robots: { index: false, follow: true },
};

// Rendered per request, not pre-rendered: the results depend on `?q=`, and a
// statically-generated page would answer every query with an empty list. This
// is the one route on the site that is genuinely dynamic.
export const dynamic = 'force-dynamic';

export default async function SearchPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = await searchParams;
  const raw = params.q;
  const query = (Array.isArray(raw) ? raw[0] : raw)?.trim() ?? '';
  const results = query ? searchServerSide(query) : [];

  return (
    <div className="shell-narrow pt-12 pb-16 sm:pt-16 sm:pb-20">
      <p className="eyebrow">Documentation search</p>

      <h1 className="h-page mt-4">Search</h1>

      <p className="lede mt-5">
        Every indexable page and every public API symbol, matched on this server. The header&rsquo;s
        box does the same search in the browser; this page is the answer without JavaScript.
      </p>

      {/* The row stretches, so the input takes the button's height rather than
          the two controls disagreeing by a few pixels. */}
      <form action="/search" method="get" role="search" className="mt-8 flex gap-2">
        <label htmlFor="q" className="sr-only">
          Search query
        </label>
        <input
          id="q"
          name="q"
          type="search"
          defaultValue={query}
          placeholder="intents, replay cache, HitlPolicy…"
          className="field flex-1"
        />
        <button type="submit" className="btn btn--primary">
          Search
        </button>
      </form>

      {query ? (
        <p className="eyebrow mt-8">
          {results.length} result{results.length === 1 ? '' : 's'} for &ldquo;{query}&rdquo;
        </p>
      ) : null}

      {results.length > 0 ? (
        <ul className="list-rule mt-4">
          {results.map((result) => (
            <li key={result.href}>
              <Link href={result.href} className="font-medium no-underline">
                {result.title}
              </Link>
              <p className="mono mt-1.5 text-[0.72rem]" style={{ color: 'var(--ink-3)' }}>
                {result.section} · {result.status} · {result.href}
              </p>
              {/* An API hit's summary is the symbol's own signature, so it is
                  set in mono and clamped — a constructor signature runs to
                  several hundred characters and would otherwise bury the rows
                  around it. The symbol's page carries it in full. A page's
                  summary is prose, already a sentence, and is left alone. */}
              <p
                className={`mt-2 ${result.section === 'API' ? 'mono line-clamp-2 text-[0.82rem]' : 'text-[0.93rem]'}`}
                style={{ color: 'var(--ink-2)' }}
              >
                {result.summary}
              </p>
            </li>
          ))}
        </ul>
      ) : null}

      {query && results.length === 0 ? (
        <p className="mt-4 max-w-[56ch]" style={{ color: 'var(--ink-2)' }}>
          Nothing matched. Try a single term — the fallback search requires every word to appear.
        </p>
      ) : null}
    </div>
  );
}
