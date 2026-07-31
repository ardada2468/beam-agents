/**
 * Link checker.
 *
 * Three classes of link, three ways to be wrong:
 *
 * 1. **Internal routes** must exist in the built route set. A link to
 *    `/docs/typo` is a 404 nobody notices until a reader hits it.
 * 2. **In-page anchors** must match a heading id actually emitted. A table of
 *    contents that scrolls nowhere is worse than none.
 * 3. **Repository links** must resolve to a file that exists at the pinned ref.
 *    This is the one that rots silently: a page cites
 *    `src/beam_agents/memory/stores.py`, the module is never written, and the
 *    citation keeps looking authoritative.
 *
 * External http(s) links are NOT fetched. A network check makes the build
 * depend on other people's uptime, and a flaky gate gets disabled. Their
 * freshness is handled instead by the dated `sources` entries the claim
 * verifier requires.
 */

import { readFileSync, readdirSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const WEBSITE = resolve(HERE, '..');
const REPO = resolve(WEBSITE, '..');
const CONTENT = join(WEBSITE, 'content');
const EXAMPLES = join(WEBSITE, 'examples');

const findings = [];

function fail(file, line, message) {
  findings.push(`${file}:${line}: ${message}`);
}

/* -- build the set of routes the site actually serves ---------------------- */

function contentFiles() {
  const out = [];
  for (const section of readdirSync(CONTENT)) {
    const dir = join(CONTENT, section);
    if (!statSync(dir).isDirectory()) continue;
    for (const entry of readdirSync(dir)) {
      if (entry.endsWith('.mdx'))
        out.push({ section, slug: entry.slice(0, -4), path: join(dir, entry) });
    }
  }
  return out;
}

const pages = contentFiles();
const apiReference = JSON.parse(readFileSync(join(WEBSITE, 'generated', 'api.json'), 'utf8'));

const routes = new Set(['/', '/api', '/search']);
const sections = new Set();
for (const page of pages) {
  routes.add(`/${page.section}/${page.slug}`);
  sections.add(`/${page.section}`);
}
for (const section of sections) routes.add(section);
// Sections with no pages yet still have an index route.
for (const section of [
  'learn',
  'docs',
  'examples',
  'specs',
  'comparison',
  'community',
  'roadmap',
]) {
  routes.add(`/${section}`);
}
for (const symbol of apiReference.symbols) routes.add(`/api/${symbol.name}`);

/* -- heading ids, mirroring rehype-slug ------------------------------------ */

function slugify(text) {
  return text
    .toLowerCase()
    .replace(/[`*_~]/g, '')
    .replace(/[^\w\s-]/g, '')
    .trim()
    .replace(/\s+/g, '-');
}

function headingIds(body) {
  const ids = new Set();
  let inFence = false;
  for (const line of body.split('\n')) {
    if (/^\s*```/.test(line)) {
      inFence = !inFence;
      continue;
    }
    if (inFence) continue;
    const match = /^(#{2,6})\s+(.*)$/.exec(line);
    if (match) ids.add(slugify(match[2].trim()));
  }
  return ids;
}

/* -- the checks ------------------------------------------------------------ */

const LINK = /\[[^\]]*\]\(([^)\s]+)(?:\s+"[^"]*")?\)/g;
const REPO_PATH = /^(src|tests|docs|openspec|scripts|protos|docker|website)\//;

for (const page of pages) {
  const rel = `website/content/${page.section}/${page.slug}.mdx`;
  const raw = readFileSync(page.path, 'utf8');
  const ids = headingIds(raw);

  raw.split('\n').forEach((text, index) => {
    const line = index + 1;
    LINK.lastIndex = 0;
    let match;
    while ((match = LINK.exec(text)) !== null) {
      const href = match[1];
      if (href.startsWith('http://') || href.startsWith('https://') || href.startsWith('mailto:')) {
        continue;
      }
      if (href.startsWith('#')) {
        if (!ids.has(href.slice(1))) {
          fail(rel, line, `in-page anchor "${href}" matches no heading on this page`);
        }
        continue;
      }
      if (href.startsWith('/')) {
        const [path, anchor] = href.split('#');
        if (!routes.has(path)) {
          fail(rel, line, `internal link "${path}" is not a route the site serves`);
        } else if (anchor) {
          const target = pages.find((p) => `/${p.section}/${p.slug}` === path);
          if (target && !headingIds(readFileSync(target.path, 'utf8')).has(anchor)) {
            fail(rel, line, `anchor "#${anchor}" does not exist on ${path}`);
          }
        }
        continue;
      }
      if (REPO_PATH.test(href)) {
        const [path] = href.split('#');
        if (!existsSync(join(REPO, path))) {
          fail(rel, line, `repository path "${path}" does not exist`);
        }
        continue;
      }
      fail(rel, line, `link "${href}" is neither a site route, an anchor, nor a repository path`);
    }
  });

  // <Example file="..." /> must point at a file that exists.
  const EXAMPLE = /<Example\s+file="([^"]+)"(?:\s+region="([^"]+)")?/g;
  let embed;
  while ((embed = EXAMPLE.exec(raw)) !== null) {
    const line = raw.slice(0, embed.index).split('\n').length;
    if (!existsSync(join(EXAMPLES, embed[1]))) {
      fail(rel, line, `<Example file="${embed[1]}" /> does not exist under website/examples/`);
    } else if (embed[2]) {
      const source = readFileSync(join(EXAMPLES, embed[1]), 'utf8');
      if (!new RegExp(`^\\s*#\\s*region:\\s*${embed[2]}\\s*$`, 'm').test(source)) {
        fail(rel, line, `example ${embed[1]} has no region "${embed[2]}"`);
      }
    }
  }
}

/* -- generated source links ------------------------------------------------ */

for (const symbol of apiReference.symbols) {
  const locations = [symbol.source, ...symbol.members.map((m) => m.source)].filter(Boolean);
  for (const location of locations) {
    if (!existsSync(join(REPO, location.path))) {
      fail('website/generated/api.json', 1, `source path "${location.path}" does not exist`);
    }
  }
}

/* -- report ---------------------------------------------------------------- */

if (findings.length > 0) {
  console.error(`link check failed: ${findings.length} finding(s)\n`);
  for (const finding of findings) console.error(finding);
  console.error('\nReproduce locally with:\n    pnpm --dir website check:links');
  process.exit(1);
}

console.log(`link check passed: ${pages.length} page(s), ${routes.size} route(s)`);
