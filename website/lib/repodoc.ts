import { readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { repoFileUrl } from './site';

/**
 * Read repository markdown — the files under `docs/` and the capability specs
 * under `openspec/specs/` — for rendering directly on the site.
 *
 * This is the strongest form of the fidelity rule available: rather than
 * paraphrasing the repository's documentation into MDX (where it would drift
 * the first time someone edits one and not the other), the site renders the
 * repository file itself. There is only one copy, and it is the one in the
 * repo.
 *
 * The only transformation is link rewriting: relative links that make sense
 * inside a checkout have to become links that make sense on a website.
 */

const REPO_ROOT = resolve(process.cwd(), '..');

/** `docs/<name>.md` → the site route that publishes it. */
const DOC_ROUTES: Record<string, string> = {
  'errors.md': '/docs/errors',
  'metrics.md': '/docs/metrics',
  'traces.md': '/docs/traces',
  'effector.md': '/docs/effector',
  'ci.md': '/docs/testing-and-ci',
};

/** `openspec/specs/<capability>/spec.md` → the site route. */
export function specRoute(capability: string): string {
  return `/specs/${capability}`;
}

export function readRepoFile(path: string): string {
  return readFileSync(join(REPO_ROOT, path), 'utf8');
}

/**
 * Rewrite links so the rendered copy works as a web page.
 *
 * - A sibling doc link becomes the site route that publishes it.
 * - A repository-relative path becomes a link into the source at the pinned
 *   ref, so the reader lands on the real file rather than a 404.
 * - Anything already absolute is left alone.
 */
export function rewriteRepoLinks(markdown: string, fromDir: string): string {
  return markdown.replace(/\]\(([^)\s]+)(\s+"[^"]*")?\)/g, (whole, href: string, title = '') => {
    if (/^(https?:|mailto:|#)/.test(href)) return whole;

    const [rawPath, anchor] = href.split('#');
    const path = rawPath ?? '';
    const suffix = anchor ? `#${anchor}` : '';

    const route = DOC_ROUTES[path];
    if (route) return `](${route}${suffix}${title})`;

    const specMatch = /^(?:\.\.\/)*openspec\/specs\/([a-z0-9-]+)\/spec\.md$/.exec(path);
    if (specMatch?.[1]) return `](${specRoute(specMatch[1])}${suffix}${title})`;

    // Resolve against the file's own directory, then point at the repository.
    const normalized = path.startsWith('/')
      ? path.slice(1)
      : normalizeRelative(`${fromDir}/${path}`);
    return `](${repoFileUrl(normalized)}${suffix}${title})`;
  });
}

function normalizeRelative(path: string): string {
  const parts: string[] = [];
  for (const segment of path.split('/')) {
    if (segment === '.' || segment === '') continue;
    if (segment === '..') parts.pop();
    else parts.push(segment);
  }
  return parts.join('/');
}

/** Strip the leading `# Title` — the page already renders its own `<h1>`. */
export function stripTitle(markdown: string): string {
  return markdown.replace(/^#\s+.*\n+/, '');
}
