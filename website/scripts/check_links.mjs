/**
 * Link checker.
 *
 * Five classes of link, five ways to be wrong:
 *
 * 1. **Internal routes** must exist in the built route set. A link to
 *    `/docs/typo` is a 404 nobody notices until a reader hits it.
 * 2. **In-page anchors** must match a heading id actually emitted. A table of
 *    contents that scrolls nowhere is worse than none.
 * 3. **Repository paths** must resolve to a file that exists at the pinned ref.
 *    This is the one that rots silently: a page cites
 *    `src/beam_agents/memory/stores.py`, the module is never written, and the
 *    citation keeps looking authoritative. Inside repository markdown they are
 *    also real links, rewritten to the pinned ref when the page is rendered.
 *    Inside MDX they are *not* — nothing rewrites an MDX link, so a bare
 *    `[x](src/foo.py)` ships as a relative href and 404s. Both facts are
 *    checked.
 * 4. **Internal routes written in TypeScript.** Prose is not the only place a
 *    route is typed by hand. The landing page, `Header`, `Footer`, `Sidebar`,
 *    the section index and the API pages all hard-code paths like
 *    `/learn/what-is-beam-agents` as JSX `href` literals, and a typo in one of
 *    those compiles, type-checks, renders, and ships a 404 that no other gate
 *    sees. `app/**` and `components/**` are scanned for static route literals
 *    and validated against the same route set as the prose.
 * 5. **Anchors into repository-rendered bodies.** A page whose body is
 *    `<RepoDoc file="docs/errors.md" />` or `<Spec capability="fake-llm" />`
 *    gets its headings from the *repository* file, not from its own MDX. Those
 *    headings are resolved here too, so `/docs/errors#retry-budgets` is checked
 *    in both directions: the anchor is known to exist, and it is not reported
 *    as missing merely because the heading lives in the repo rather than in the
 *    MDX. The links *inside* that repository markdown are checked as well —
 *    they are rewritten for the web by `lib/repodoc.ts`, and a sibling-doc link
 *    that rewrites to a site route the site does not serve is a 404 that only
 *    exists on the published page.
 *
 * External http(s) links are NOT fetched. A network check makes the build
 * depend on other people's uptime, and a flaky gate gets disabled. Their
 * freshness is handled instead by the dated `sources` entries the claim
 * verifier requires.
 *
 * The same reasoning bounds classes 4 and 5: only *statically decidable*
 * links are checked. A template literal with an interpolation
 * (`` href={`/api/${symbol.name}`} ``) is skipped rather than guessed at,
 * because a checker that cries wolf on correct code is a checker somebody
 * turns off.
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
// Machine-readable endpoints. They are routes like any other, and a footer or
// a `<link>` tag pointing at a mistyped one fails the same way.
for (const endpoint of ['/sitemap.xml', '/robots.txt', '/search-index.json']) routes.add(endpoint);

/* -- heading ids, mirroring rehype-slug ------------------------------------ */

function slugify(text) {
  return text
    .toLowerCase()
    .replace(/[`*_~]/g, '')
    .replace(/[^\w\s-]/g, '')
    .trim()
    .replace(/\s+/g, '-');
}

/**
 * Heading ids for one document.
 *
 * Repeated headings are suffixed `-1`, `-2`, … the way `github-slugger` (which
 * is what `rehype-slug` uses) disambiguates them. The capability specs repeat
 * scenario titles across requirements often enough that ignoring this would
 * report working anchors as broken.
 *
 * Ids are per *document*, not per page: an MDX body and the repository
 * markdown it embeds are compiled by two separate `compileMDX` calls, so each
 * gets its own slugger and its own numbering. Callers union the two sets.
 */
function headingIds(body) {
  const ids = new Set();
  const seen = new Map();
  let inFence = false;
  for (const line of body.split('\n')) {
    if (/^\s*```/.test(line)) {
      inFence = !inFence;
      continue;
    }
    if (inFence) continue;
    const match = /^(#{2,6})\s+(.*)$/.exec(line);
    if (!match) continue;
    const base = slugify(match[2].trim());
    const count = seen.get(base) ?? 0;
    seen.set(base, count + 1);
    ids.add(count === 0 ? base : `${base}-${count}`);
  }
  return ids;
}

/* -- repository markdown rendered in place --------------------------------- */

/**
 * `docs/<name>.md` → the site route that publishes it.
 *
 * Read out of `lib/repodoc.ts` rather than restated here. That table is what
 * the rendered page actually uses to rewrite sibling-doc links; a second copy
 * in this file would agree with it until the day it did not, and the check
 * would then be validating a rewrite the site does not perform.
 */
function docRoutes() {
  const source = readFileSync(join(WEBSITE, 'lib', 'repodoc.ts'), 'utf8');
  const block = /const DOC_ROUTES[^{]*\{([\s\S]*?)\n\};/.exec(source);
  const table = new Map();
  if (block) {
    for (const entry of block[1].matchAll(/'([^']+)':\s*'([^']+)'/g)) {
      table.set(entry[1], entry[2]);
    }
  }
  if (table.size === 0) {
    throw new Error(
      'could not read DOC_ROUTES out of website/lib/repodoc.ts. The link checker mirrors that ' +
        'table to resolve sibling-doc links inside rendered repository markdown; if the table ' +
        'moved or changed shape, update the parser in check_links.mjs rather than dropping the ' +
        'check.',
    );
  }
  return table;
}

const DOC_ROUTES = docRoutes();

/** Mirrors `normalizeRelative` in `lib/repodoc.ts`. */
function normalizeRelative(path) {
  const parts = [];
  for (const segment of path.split('/')) {
    if (segment === '.' || segment === '') continue;
    if (segment === '..') parts.pop();
    else parts.push(segment);
  }
  return parts.join('/');
}

/** Every repository markdown file a page renders in place. */
function repoEmbeds(raw) {
  const embeds = [];
  for (const match of raw.matchAll(/<RepoDoc\s+file="([^"]+)"/g)) {
    embeds.push({ file: match[1], line: raw.slice(0, match.index).split('\n').length });
  }
  for (const match of raw.matchAll(/<Spec\s+capability="([^"]+)"/g)) {
    embeds.push({
      file: `openspec/specs/${match[1]}/spec.md`,
      line: raw.slice(0, match.index).split('\n').length,
    });
  }
  return embeds;
}

/**
 * Classify one link found inside repository markdown, the way
 * `rewriteRepoLinks` in `lib/repodoc.ts` does.
 *
 * Returns `null` for links this checker deliberately leaves alone (absolute
 * http(s) and `mailto:`), matching the external-link policy above.
 */
function classifyRepoLink(href, fromDir) {
  if (/^(https?:|mailto:)/.test(href)) return null;
  if (href.startsWith('#')) return { kind: 'anchor', anchor: href.slice(1) };

  const [rawPath, anchor] = href.split('#');
  const path = rawPath ?? '';

  const route = DOC_ROUTES.get(path);
  if (route) return { kind: 'route', route, anchor };

  const spec = /^(?:\.\.\/)*openspec\/specs\/([a-z0-9-]+)\/spec\.md$/.exec(path);
  if (spec) return { kind: 'route', route: `/specs/${spec[1]}`, anchor };

  const normalized = path.startsWith('/') ? path.slice(1) : normalizeRelative(`${fromDir}/${path}`);
  return { kind: 'repo', path: normalized, anchor };
}

/* -- the checks ------------------------------------------------------------ */

const LINK = /\[[^\]]*\]\(([^)\s]+)(?:\s+"[^"]*")?\)/g;
const REPO_PATH = /^(src|tests|docs|openspec|scripts|protos|docker|website)\//;

/**
 * Every markdown link target on one line.
 *
 * `matchAll` rather than a `while (regex.exec(...))` loop: a `/g` regex carries
 * `lastIndex` between calls, and this one is now used from two nested scans
 * (the MDX body, and the repository markdown that body embeds). Iterating a
 * fresh match list means neither scan can resume the other's position.
 */
function linkTargets(text) {
  return [...text.matchAll(LINK)].map((match) => match[1]);
}

/**
 * Every heading id a page serves — its own MDX headings plus those of any
 * repository markdown it renders in place.
 *
 * Memoized because cross-page anchor resolution asks for the same page's ids
 * once per inbound link, and a spec is a few hundred lines of markdown.
 */
const idCache = new Map();

function idsForPage(page) {
  const key = `/${page.section}/${page.slug}`;
  const cached = idCache.get(key);
  if (cached) return cached;
  const raw = readFileSync(page.path, 'utf8');
  const ids = headingIds(raw);
  for (const embed of repoEmbeds(raw)) {
    const absolute = join(REPO, embed.file);
    if (!existsSync(absolute)) continue; // reported once, where the embed is.
    for (const id of headingIds(readFileSync(absolute, 'utf8'))) ids.add(id);
  }
  idCache.set(key, ids);
  return ids;
}

function checkRoute(file, line, href, describe) {
  const [path, anchor] = href.split('#');
  if (!routes.has(path)) {
    fail(file, line, `${describe} "${path}" is not a route the site serves`);
    return;
  }
  if (!anchor) return;
  const target = pages.find((p) => `/${p.section}/${p.slug}` === path);
  if (target && !idsForPage(target).has(anchor)) {
    fail(file, line, `anchor "#${anchor}" does not exist on ${path}`);
  }
}

for (const page of pages) {
  const rel = `website/content/${page.section}/${page.slug}.mdx`;
  const raw = readFileSync(page.path, 'utf8');
  const ids = idsForPage(page);

  raw.split('\n').forEach((text, index) => {
    const line = index + 1;
    for (const href of linkTargets(text)) {
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
        checkRoute(rel, line, href, 'internal link');
        continue;
      }
      if (REPO_PATH.test(href)) {
        const [path] = href.split('#');
        if (!existsSync(join(REPO, path))) {
          fail(rel, line, `repository path "${path}" does not exist`);
          continue;
        }
        // The file is real, but this is MDX: nothing rewrites the href, so the
        // browser resolves it against the current route and lands on a 404.
        // The rewrite only happens for markdown rendered out of the repository
        // (see `lib/repodoc.ts`), which MDX is not.
        fail(
          rel,
          line,
          `repository path "${path}" is linked from MDX, where nothing rewrites it — the ` +
            'browser would resolve it relative to this page and 404. Cite it as inline code, ' +
            'or link the full https://github.com/… URL at the pinned ref.',
        );
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

  // Repository markdown rendered in place: the file has to exist, and the
  // links inside it have to survive the rewrite that publishes them.
  for (const { file, line } of repoEmbeds(raw)) {
    const absolute = join(REPO, file);
    if (!existsSync(absolute)) {
      fail(rel, line, `renders repository file "${file}", which does not exist`);
      continue;
    }
    const markdown = readFileSync(absolute, 'utf8');
    const own = headingIds(markdown);
    const fromDir = file.split('/').slice(0, -1).join('/');
    const rendered = `/${page.section}/${page.slug}`;

    markdown.split('\n').forEach((text, index) => {
      const at = index + 1;
      for (const href of linkTargets(text)) {
        const link = classifyRepoLink(href, fromDir);
        if (link === null) continue;
        if (link.kind === 'anchor') {
          if (!own.has(link.anchor)) {
            fail(
              file,
              at,
              `in-page anchor "#${link.anchor}" matches no heading (rendered on ${rendered})`,
            );
          }
          continue;
        }
        if (link.kind === 'route') {
          const target = link.anchor ? `${link.route}#${link.anchor}` : link.route;
          checkRoute(file, at, target, 'link rewrites to site route');
          continue;
        }
        if (!existsSync(join(REPO, link.path))) {
          fail(file, at, `repository path "${link.path}" does not exist (rendered on ${rendered})`);
        }
      }
    });
  }
}

/* -- internal routes hard-coded in TypeScript ------------------------------ */

/**
 * `href="/x"`, `href={'/x'}`, `` href={`/x`} ``, and the object-literal form
 * `href: '/x'` that feeds them.
 *
 * Anything else — `href={page.href}`, `href={REPO_URL}` — carries no literal to
 * check, and is skipped rather than guessed at.
 */
const TS_HREF = /\bhref\s*[=:]\s*\{?\s*(['"`])([^'"`\n]*)\1/g;

function tsSources(dir) {
  const out = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) out.push(...tsSources(path));
    else if (/\.tsx?$/.test(entry.name)) out.push(path);
  }
  return out;
}

let tsScanned = 0;
for (const root of ['app', 'components']) {
  for (const path of tsSources(join(WEBSITE, root))) {
    tsScanned += 1;
    const rel = `website/${path.slice(WEBSITE.length + 1)}`;
    readFileSync(path, 'utf8')
      .split('\n')
      .forEach((text, index) => {
        for (const match of text.matchAll(TS_HREF)) {
          const href = match[2];
          // Interpolated: the route is assembled at render time and the
          // literal here is only a fragment of it.
          if (href.includes('${')) continue;
          // Anything that is not a site-absolute path has no target to check
          // here: an external URL is out of scope by policy, and a bare
          // `#anchor` in a component resolves against whichever page the
          // component renders into.
          if (!href.startsWith('/')) continue;
          checkRoute(rel, index + 1, href, 'hard-coded internal route');
        }
      });
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

console.log(
  `link check passed: ${pages.length} page(s), ${tsScanned} TypeScript source(s), ` +
    `${routes.size} route(s)`,
);
