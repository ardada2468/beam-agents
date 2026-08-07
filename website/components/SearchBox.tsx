'use client';

import { useEffect, useId, useRef, useState } from 'react';
import MiniSearch from 'minisearch';
import type { SearchDoc } from '@/lib/search';

/**
 * Search input.
 *
 * The element is a real `<form method="get" action="/search">`, so with
 * JavaScript disabled it submits and the server-rendered `/search` route
 * answers. When JavaScript is available the same input additionally shows
 * instant results from the pre-built index, which is fetched lazily on first
 * focus — no reader pays for the index who never searches.
 *
 * The field and its dropdown are styled like everything else on this site:
 * flat, hairline-bordered, square. The dropdown in particular is separated
 * from the page by a stronger rule (`--rule-2`) and an opaque ground rather
 * than by a drop shadow, because this site has no shadows anywhere.
 */
export function SearchBox() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<SearchDoc[]>([]);
  const [open, setOpen] = useState(false);
  const engine = useRef<MiniSearch<SearchDoc> | null>(null);
  const loading = useRef(false);
  const listId = useId();

  async function ensureIndex() {
    if (engine.current || loading.current) return;
    loading.current = true;
    try {
      const response = await fetch('/search-index.json');
      const docs = (await response.json()) as SearchDoc[];
      const search = new MiniSearch<SearchDoc>({
        fields: ['title', 'summary', 'headings', 'body'],
        storeFields: ['title', 'summary', 'href', 'section', 'status'],
        searchOptions: { prefix: true, fuzzy: 0.2, boost: { title: 3, headings: 2 } },
      });
      search.addAll(docs);
      engine.current = search;
      if (query) setResults(runSearch(search, query));
    } finally {
      loading.current = false;
    }
  }

  useEffect(() => {
    if (!engine.current || !query) {
      setResults([]);
      return;
    }
    setResults(runSearch(engine.current, query));
  }, [query]);

  return (
    <form
      action="/search"
      method="get"
      role="search"
      // Below `sm` the field takes what the header row has left; above it, a
      // fixed measure, so the wordmark and nav do not shift as it grows.
      className="relative min-w-0 flex-1 sm:flex-none"
      onSubmit={() => setOpen(false)}
    >
      <label htmlFor={listId} className="sr-only">
        Search the documentation
      </label>
      <input
        id={listId}
        type="search"
        name="q"
        value={query}
        placeholder="Search"
        autoComplete="off"
        onFocus={() => {
          void ensureIndex();
          setOpen(true);
        }}
        onBlur={() => window.setTimeout(() => setOpen(false), 150)}
        // Escape dismisses the dropdown first and clears the field second, which
        // is the order a reader expects from an overlay. The default action has
        // to be suppressed for the first press: Chrome empties a
        // `type="search"` input on Escape, so without this the query would
        // vanish along with the results the reader was trying to look past.
        onKeyDown={(event) => {
          if (event.key === 'Escape' && open) {
            event.preventDefault();
            setOpen(false);
          }
        }}
        onChange={(event) => setQuery(event.target.value)}
        className="field w-full sm:w-48"
      />
      {open && results.length > 0 ? (
        // Two positionings, because the field sits in a different place at each
        // width. On a wide header there is room to hang a fixed-width panel off
        // the field's right edge. On a narrow one the field is only a few
        // centimetres from the left margin, so a 20rem panel anchored to its
        // right edge starts off-screen and `overflow-x: hidden` on the body
        // silently shears the titles off. There it is pinned to the viewport
        // instead: `top` is left auto, so a fixed box keeps the static vertical
        // position it would have had — directly under the field — while left
        // and right take the viewport's margins.
        <ul
          className="fixed inset-x-3 z-30 mt-1 max-h-80 overflow-y-auto border py-1 sm:absolute sm:inset-x-auto sm:right-0 sm:w-80"
          style={{ borderColor: 'var(--rule-2)', background: 'var(--paper)', borderRadius: 2 }}
        >
          {results.map((result) => (
            <li key={result.href}>
              <a
                href={result.href}
                className="block px-3 py-1.5 text-[0.88rem] no-underline hover:underline"
                style={{ color: 'var(--ink)' }}
              >
                {result.title}
                <span className="mono ml-2 text-[0.7rem]" style={{ color: 'var(--ink-3)' }}>
                  {result.section}
                </span>
              </a>
            </li>
          ))}
        </ul>
      ) : null}
    </form>
  );
}

function runSearch(engine: MiniSearch<SearchDoc>, query: string): SearchDoc[] {
  return engine.search(query).slice(0, 8) as unknown as SearchDoc[];
}
