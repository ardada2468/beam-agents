import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import matter from 'gray-matter';
import { formatFrontmatterError, frontmatterSchema, type Frontmatter } from './schema';
import { SECTION_BY_SLUG, SECTIONS, type SectionDef } from './sections';

/**
 * Filesystem-backed content loader.
 *
 * Content lives at `content/<section>/<slug>.mdx`. The tree is read once per
 * process and memoized: `generateStaticParams`, `generateMetadata`, and the
 * page component each need it, and re-reading the tree for every route turns a
 * linear build into a quadratic one.
 */

export const CONTENT_ROOT = join(process.cwd(), 'content');

export interface Page {
  readonly section: SectionDef;
  readonly slug: string;
  /** Route path, e.g. `/docs/errors`. */
  readonly href: string;
  /** Repository-relative source path, used in error messages. */
  readonly file: string;
  readonly frontmatter: Frontmatter;
  readonly body: string;
  readonly headings: readonly Heading[];
}

export interface Heading {
  readonly depth: number;
  readonly text: string;
  readonly id: string;
}

let cache: Page[] | null = null;

/**
 * GitHub-style heading slug: lowercase, punctuation dropped, spaces to
 * hyphens. Must match `rehype-slug`'s output, or the table of contents links
 * to anchors that do not exist — and the link checker will say so.
 */
export function slugifyHeading(text: string): string {
  return text
    .toLowerCase()
    .replace(/[`*_~]/g, '')
    .replace(/[^\w\s-]/g, '')
    .trim()
    .replace(/\s+/g, '-');
}

/**
 * Collect ATX headings for the in-page table of contents.
 *
 * Fenced code is skipped: a `# comment` on the first column of a shell block
 * is not a heading, and treating it as one produces a table of contents full
 * of nonsense.
 */
function extractHeadings(body: string): Heading[] {
  const headings: Heading[] = [];
  let inFence = false;
  for (const line of body.split('\n')) {
    if (/^\s*```/.test(line)) {
      inFence = !inFence;
      continue;
    }
    if (inFence) continue;
    const match = /^(#{2,4})\s+(.*)$/.exec(line);
    if (!match) continue;
    const hashes = match[1];
    const text = match[2];
    if (hashes === undefined || text === undefined) continue;
    const clean = text.trim();
    headings.push({ depth: hashes.length, text: clean, id: slugifyHeading(clean) });
  }
  return headings;
}

function readSection(section: SectionDef): Page[] {
  const dir = join(CONTENT_ROOT, section.slug);
  let entries: string[];
  try {
    entries = readdirSync(dir);
  } catch {
    return [];
  }
  const pages: Page[] = [];
  for (const entry of entries.sort()) {
    if (!entry.endsWith('.mdx')) continue;
    const path = join(dir, entry);
    if (!statSync(path).isFile()) continue;
    const slug = entry.slice(0, -'.mdx'.length);
    const file = `website/content/${section.slug}/${entry}`;
    const raw = readFileSync(path, 'utf8');
    const parsed = matter(raw);
    const result = frontmatterSchema.safeParse(parsed.data);
    if (!result.success) {
      throw new Error(formatFrontmatterError(file, result.error));
    }
    pages.push({
      section,
      slug,
      href: `/${section.slug}/${slug}`,
      file,
      frontmatter: result.data,
      body: parsed.content,
      headings: extractHeadings(parsed.content),
    });
  }
  return pages;
}

export function allPages(): readonly Page[] {
  if (cache) return cache;
  const pages: Page[] = [];
  for (const section of SECTIONS) {
    pages.push(...readSection(section));
  }
  const seen = new Set<string>();
  for (const page of pages) {
    if (seen.has(page.href)) {
      throw new Error(`duplicate content route ${page.href} (${page.file})`);
    }
    seen.add(page.href);
  }
  cache = pages;
  return cache;
}

/** Pages in one section, ordered by explicit `order` then title. */
export function pagesInSection(sectionSlug: string): readonly Page[] {
  return allPages()
    .filter((page) => page.section.slug === sectionSlug)
    .sort((a, b) => {
      const ao = a.frontmatter.order ?? Number.MAX_SAFE_INTEGER;
      const bo = b.frontmatter.order ?? Number.MAX_SAFE_INTEGER;
      if (ao !== bo) return ao - bo;
      return a.frontmatter.title.localeCompare(b.frontmatter.title);
    });
}

export function findPage(sectionSlug: string, slug: string): Page | undefined {
  return allPages().find((page) => page.section.slug === sectionSlug && page.slug === slug);
}

/**
 * A page is indexable unless it is `planned`.
 *
 * Planned pages describe code that does not exist. Letting a search engine
 * surface one as documentation is the exact misrepresentation the status
 * taxonomy exists to prevent, so they carry `noindex` and stay out of the
 * sitemap.
 */
export function isIndexable(page: Page): boolean {
  return page.frontmatter.status !== 'planned';
}

export function navSections(): { section: SectionDef; pages: readonly Page[] }[] {
  return SECTIONS.filter((section) => section.inNav)
    .map((section) => ({ section, pages: pagesInSection(section.slug) }))
    .filter((entry) => entry.pages.length > 0);
}

export function sectionFor(slug: string): SectionDef | undefined {
  return SECTION_BY_SLUG.get(slug);
}
