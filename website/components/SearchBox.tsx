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
      className="relative"
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
        onChange={(event) => setQuery(event.target.value)}
        className="w-36 rounded border px-2 py-1 text-sm sm:w-48"
        style={{ borderColor: 'var(--border)', background: 'var(--bg)', color: 'var(--fg)' }}
      />
      {open && results.length > 0 ? (
        <ul
          className="absolute right-0 z-30 mt-1 max-h-80 w-80 overflow-y-auto rounded border py-1 text-sm shadow-lg"
          style={{ borderColor: 'var(--border)', background: 'var(--bg)' }}
        >
          {results.map((result) => (
            <li key={result.href}>
              <a href={result.href} className="block px-3 py-1.5 no-underline hover:underline">
                <span style={{ color: 'var(--fg)' }}>{result.title}</span>
                <span className="ml-2 text-xs" style={{ color: 'var(--fg-faint)' }}>
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
