/**
 * Assert that the built site is genuinely server-rendered.
 *
 * "Indexable" is easy to intend and easy to lose — one `'use client'` in the
 * wrong place and a page ships as an empty shell that only fills in after
 * hydration. This check makes it a tested property instead of an intention:
 * start the production server, fetch every URL in the sitemap, and assert on
 * the raw HTML, before any JavaScript runs.
 *
 * Per route: 200, a non-empty <h1>, a <meta name="description"> matching the
 * page's summary, an absolute <link rel="canonical">, and at least 200
 * characters of body text. Plus, across routes, that no two pages share a
 * title or a description — duplicate metadata is how a docs site ends up with
 * one indexed page instead of forty.
 */

import { spawn } from 'node:child_process';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const WEBSITE = resolve(HERE, '..');
const PORT = Number(process.env.SSR_CHECK_PORT ?? 3123);
const ORIGIN = `http://127.0.0.1:${PORT}`;
const MIN_BODY_TEXT = 200;

const findings = [];
const fail = (route, message) => findings.push(`${route}: ${message}`);

function textOf(html) {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&[a-z]+;/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function attr(html, pattern) {
  const match = pattern.exec(html);
  return match ? match[1] : null;
}

async function waitForServer(deadlineMs = 60_000) {
  const started = Date.now();
  while (Date.now() - started < deadlineMs) {
    try {
      const response = await fetch(`${ORIGIN}/`, { redirect: 'manual' });
      if (response.status < 500) return;
    } catch {
      /* not up yet */
    }
    await new Promise((r) => setTimeout(r, 400));
  }
  throw new Error(`server did not become ready on ${ORIGIN} within ${deadlineMs}ms`);
}

// A server left over from an earlier run would answer these fetches with a
// stale build, and the check would report on HTML nobody just produced —
// passing or failing for reasons unrelated to the current tree. Refuse to
// start rather than measure the wrong thing.
try {
  const probe = await fetch(`${ORIGIN}/`, {
    redirect: 'manual',
    signal: AbortSignal.timeout(2000),
  });
  if (probe.status < 500) {
    console.error(
      `SSR check aborted: something is already listening on ${ORIGIN}.\n` +
        'That is probably a server left running by an earlier check, and it would\n' +
        'serve a stale build. Stop it first:\n' +
        `    kill $(lsof -ti :${PORT})`,
    );
    process.exit(1);
  }
} catch {
  /* nothing listening — the expected case */
}

const server = spawn('node_modules/.bin/next', ['start', '--port', String(PORT)], {
  cwd: WEBSITE,
  stdio: ['ignore', 'pipe', 'pipe'],
  env: { ...process.env, NODE_ENV: 'production' },
});
let serverLog = '';
server.stdout.on('data', (chunk) => (serverLog += chunk));
server.stderr.on('data', (chunk) => (serverLog += chunk));

try {
  await waitForServer();

  // The sitemap is the site's own claim about what it publishes; checking
  // exactly that set means a page cannot be advertised and broken at once.
  const sitemapXml = await (await fetch(`${ORIGIN}/sitemap.xml`)).text();
  const routes = [...sitemapXml.matchAll(/<loc>([^<]+)<\/loc>/g)].map((m) =>
    new URL(m[1]).pathname.replace(/\/$/, ''),
  );
  if (routes.length === 0) throw new Error('sitemap.xml listed no routes');

  const summaries = new Map();
  for (const page of listContentPages()) summaries.set(page.href, page.summary);

  const titles = new Map();
  const descriptions = new Map();

  for (const path of routes) {
    const route = path === '' ? '/' : path;
    const response = await fetch(`${ORIGIN}${route}`);
    if (response.status !== 200) {
      fail(route, `expected 200, got ${response.status}`);
      continue;
    }
    const html = await response.text();

    const h1 = /<h1[^>]*>([\s\S]*?)<\/h1>/i.exec(html);
    if (!h1 || textOf(h1[1]).length === 0) fail(route, 'no non-empty <h1> in the server response');

    const description = attr(html, /<meta\s+name="description"\s+content="([^"]*)"/i);
    if (!description) fail(route, 'no <meta name="description">');

    const canonical = attr(html, /<link\s+rel="canonical"\s+href="([^"]*)"/i);
    if (!canonical) fail(route, 'no <link rel="canonical">');
    else if (!/^https?:\/\//.test(canonical))
      fail(route, `canonical "${canonical}" is not absolute`);

    const body = textOf(html);
    if (body.length < MIN_BODY_TEXT) {
      fail(route, `only ${body.length} characters of body text (expected >= ${MIN_BODY_TEXT})`);
    }

    const expected = summaries.get(route);
    if (expected && description && decode(description) !== expected) {
      fail(
        route,
        `<meta name="description"> does not match the page summary.\n    meta:    ${decode(description)}\n    summary: ${expected}`,
      );
    }

    const title = attr(html, /<title>([\s\S]*?)<\/title>/i);
    if (title) {
      if (titles.has(title)) fail(route, `duplicate <title> (also on ${titles.get(title)})`);
      else titles.set(title, route);
    }
    if (description) {
      if (descriptions.has(description)) {
        fail(route, `duplicate description (also on ${descriptions.get(description)})`);
      } else descriptions.set(description, route);
    }
  }

  // /search must answer without JavaScript: the header form is a plain GET.
  const searchHtml = await (await fetch(`${ORIGIN}/search?q=intent`)).text();
  if (!/result/i.test(textOf(searchHtml))) {
    fail('/search?q=intent', 'server-rendered search returned no result markup');
  }
  if (!/<meta\s+name="robots"[^>]*noindex/i.test(searchHtml)) {
    fail('/search', 'search results page is missing noindex');
  }

  if (findings.length > 0) {
    console.error(`SSR check failed: ${findings.length} finding(s)\n`);
    for (const finding of findings) console.error(finding);
    console.error(
      '\nReproduce locally with:\n    pnpm --dir website build && pnpm --dir website check:ssr',
    );
    process.exit(1);
  }

  console.log(`SSR check passed: ${routes.length} route(s) served complete HTML`);
} catch (error) {
  console.error(`SSR check errored: ${error.message}`);
  if (serverLog) console.error(`\nserver output:\n${serverLog.slice(-3000)}`);
  process.exitCode = 1;
} finally {
  server.kill('SIGTERM');
}

function decode(value) {
  return value
    .replace(/&quot;/g, '"')
    .replace(/&#x27;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}

function listContentPages() {
  const CONTENT = join(WEBSITE, 'content');
  const out = [];
  for (const section of readdirSync(CONTENT)) {
    const dir = join(CONTENT, section);
    if (!statSync(dir).isDirectory()) continue;
    for (const entry of readdirSync(dir)) {
      if (!entry.endsWith('.mdx')) continue;
      const raw = readFileSync(join(dir, entry), 'utf8');
      const match = /^summary:\s*(.*)$/m.exec(raw.split('---')[1] ?? '');
      out.push({
        href: `/${section}/${entry.slice(0, -4)}`,
        summary: match ? unquote(match[1].trim()) : null,
      });
    }
  }
  return out;
}

function unquote(value) {
  if (
    (value.startsWith('"') && value.endsWith('"')) ||
    (value.startsWith("'") && value.endsWith("'"))
  ) {
    return value.slice(1, -1);
  }
  return value;
}
