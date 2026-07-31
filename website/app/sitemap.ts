import type { MetadataRoute } from 'next';
import { allPages, isIndexable } from '@/lib/content';
import { apiSymbols } from '@/lib/api';
import { SECTIONS } from '@/lib/sections';
import { absoluteUrl } from '@/lib/site';

/**
 * The sitemap lists what may be indexed and nothing else.
 *
 * Excluded on purpose: `/search` (a query URL is not content) and every
 * `planned` page (they describe code that does not exist).
 */
export default function sitemap(): MetadataRoute.Sitemap {
  const routes: MetadataRoute.Sitemap = [
    { url: absoluteUrl('/'), priority: 1 },
    { url: absoluteUrl('/api'), priority: 0.8 },
  ];

  for (const section of SECTIONS) {
    if (!section.inNav) continue;
    routes.push({ url: absoluteUrl(`/${section.slug}`), priority: 0.7 });
  }

  for (const page of allPages()) {
    if (!isIndexable(page)) continue;
    routes.push({ url: absoluteUrl(page.href), priority: 0.6 });
  }

  for (const symbol of apiSymbols()) {
    routes.push({ url: absoluteUrl(`/api/${symbol.name}`), priority: 0.5 });
  }

  return routes;
}
