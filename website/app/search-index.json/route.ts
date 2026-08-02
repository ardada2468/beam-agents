import { searchDocs } from '@/lib/search';

/**
 * The client-side search index, built at build time and served as a static
 * asset. `force-static` is what makes it a build artifact rather than a
 * per-request computation — a reader searching never triggers a server-side
 * index build, and the site works with no network egress.
 */
export const dynamic = 'force-static';

export function GET(): Response {
  return new Response(JSON.stringify(searchDocs()), {
    headers: { 'content-type': 'application/json' },
  });
}
