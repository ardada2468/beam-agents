import type { Source } from './schema';

/**
 * Stable anchor id for a citation.
 *
 * Derived from the URL so a comparison cell can link to its footnote without
 * the table component needing to know the page's source list — the page
 * renders the footnotes, the cell renders the marker, and both agree because
 * both hash the same URL.
 */
export function sourceAnchor(url: string): string {
  let hash = 0;
  for (let i = 0; i < url.length; i += 1) {
    hash = (hash * 31 + url.charCodeAt(i)) | 0;
  }
  return `source-${(hash >>> 0).toString(36)}`;
}

export function sourceIndex(sources: readonly Source[], url: string): number {
  return sources.findIndex((source) => source.url === url) + 1;
}
