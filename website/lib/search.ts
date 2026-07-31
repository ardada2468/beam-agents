import { allPages, isIndexable } from './content';
import { apiSymbols } from './api';

/**
 * The search corpus, built once at build time and served two ways: as a static
 * JSON index for the client, and directly by the server-rendered `/search`
 * route for clients without JavaScript.
 */

export interface SearchDoc {
  readonly id: string;
  readonly title: string;
  readonly summary: string;
  readonly href: string;
  readonly section: string;
  readonly status: string;
  readonly headings: string;
  readonly body: string;
}

/** Strip MDX syntax down to prose so the index holds words, not markup. */
function toPlainText(body: string): string {
  return body
    .replace(/```[\s\S]*?```/g, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/[#*_`|>[\]()-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 6000);
}

export function searchDocs(): SearchDoc[] {
  const docs: SearchDoc[] = [];
  for (const page of allPages()) {
    // Planned pages describe code that does not exist; surfacing one as a
    // search hit is the same misrepresentation as indexing it.
    if (!isIndexable(page)) continue;
    docs.push({
      id: page.href,
      title: page.frontmatter.title,
      summary: page.frontmatter.summary,
      href: page.href,
      section: page.section.title,
      status: page.frontmatter.status,
      headings: page.headings.map((heading) => heading.text).join(' '),
      body: toPlainText(page.body),
    });
  }
  for (const symbol of apiSymbols()) {
    docs.push({
      id: `/api/${symbol.name}`,
      title: symbol.name,
      summary: symbol.signature ?? `${symbol.kind} in the beam_agents public API`,
      href: `/api/${symbol.name}`,
      section: 'API',
      status: 'stable',
      headings: symbol.members.map((member) => member.name).join(' '),
      body: toPlainText(symbol.doc ?? ''),
    });
  }
  return docs;
}

/**
 * Server-side search for the no-JavaScript path.
 *
 * Deliberately a plain scoring pass rather than MiniSearch: the corpus is
 * small, and this keeps the fallback free of any dependency on the client
 * index format.
 */
export function searchServerSide(query: string, limit = 25): SearchDoc[] {
  const terms = query.toLowerCase().split(/\s+/).filter(Boolean);
  if (terms.length === 0) return [];
  const scored: { doc: SearchDoc; score: number }[] = [];
  for (const doc of searchDocs()) {
    const title = doc.title.toLowerCase();
    const headings = doc.headings.toLowerCase();
    const haystack = `${title} ${doc.summary.toLowerCase()} ${headings} ${doc.body.toLowerCase()}`;
    let score = 0;
    for (const term of terms) {
      if (!haystack.includes(term)) {
        score = 0;
        break;
      }
      if (title.includes(term)) score += 8;
      if (headings.includes(term)) score += 3;
      score += 1;
    }
    if (score > 0) scored.push({ doc, score });
  }
  return scored
    .sort((a, b) => b.score - a.score || a.doc.title.localeCompare(b.doc.title))
    .slice(0, limit)
    .map((entry) => entry.doc);
}
