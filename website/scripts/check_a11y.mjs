/**
 * Accessibility check: structural rules in jsdom, contrast at the token level.
 *
 * Two halves, because one tool cannot honestly do both:
 *
 * 1. **Structure** — axe-core runs against the server-rendered HTML in jsdom:
 *    landmarks, heading order, form labels, link names, duplicate ids, image
 *    alt text, ARIA validity. These are real DOM properties and jsdom answers
 *    them correctly.
 *
 * 2. **Contrast** — computed directly from `app/globals.css`'s design tokens
 *    rather than from rendered pixels. jsdom has no layout engine, so axe
 *    reports colour-contrast as "incomplete" and a check that trusted it would
 *    be theatre. Instead every foreground/background token pair the site
 *    actually uses is checked against the WCAG 2.1 AA ratio, in both themes.
 *    That is a narrower claim than "every rendered pixel passes" — and it is
 *    one that is actually true.
 *
 * A representative route from every section is audited, which is what keeps
 * the run fast enough to sit in the default gate.
 */

import { readFileSync, readdirSync, statSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { JSDOM } from 'jsdom';
import axe from 'axe-core';
import { spawn } from 'node:child_process';

const HERE = dirname(fileURLToPath(import.meta.url));
const WEBSITE = resolve(HERE, '..');
const PORT = Number(process.env.A11Y_CHECK_PORT ?? 3124);
const ORIGIN = `http://127.0.0.1:${PORT}`;

const findings = [];
const fail = (where, message) => findings.push(`${where}: ${message}`);

/* -- 1. contrast, from the tokens ------------------------------------------ */

const AA_NORMAL = 4.5;
const AA_LARGE = 3.0;

/**
 * Pairs the site actually renders, as (foreground, background, min ratio).
 *
 * These are the raw tokens, not the compatibility aliases: the aliases are
 * defined once as `var(--paper)` indirections, so only the raw names carry a
 * literal hex value in each theme block for this check to read.
 */
const CONTRAST_PAIRS = [
  ['--ink', '--paper', AA_NORMAL],
  ['--ink', '--paper-2', AA_NORMAL],
  ['--ink', '--paper-3', AA_NORMAL],
  ['--ink-2', '--paper', AA_NORMAL],
  ['--ink-2', '--paper-2', AA_NORMAL],
  ['--ink-3', '--paper', AA_NORMAL],
  ['--ink-3', '--paper-2', AA_NORMAL],
  ['--link', '--paper', AA_NORMAL],
  ['--link', '--paper-2', AA_NORMAL],
  ['--paper', '--ink', AA_NORMAL],
  ['--warn-fg', '--warn-bg', AA_NORMAL],
  ['--note-fg', '--note-bg', AA_NORMAL],
  // Status labels are small mono caps, so the normal ratio applies — not the
  // large-text exemption.
  ['--status-stable', '--paper', AA_NORMAL],
  ['--status-experimental', '--paper', AA_NORMAL],
  ['--status-planned', '--paper', AA_NORMAL],
  ['--status-stable', '--paper-2', AA_NORMAL],
  ['--status-experimental', '--paper-2', AA_NORMAL],
  ['--status-planned', '--paper-2', AA_NORMAL],
  // The stream key. These are read as text (rail labels, `.output` and
  // friends) as well as drawn as strokes, so they are held to the text ratio.
  ['--s-output', '--paper', AA_NORMAL],
  ['--s-intents', '--paper', AA_NORMAL],
  ['--s-traces', '--paper', AA_NORMAL],
  ['--s-errors', '--paper', AA_NORMAL],
  ['--s-intents', '--paper-2', AA_NORMAL],
  ['--s-traces', '--paper-2', AA_NORMAL],
  ['--s-errors', '--paper-2', AA_NORMAL],
  // Non-text UI: hairlines that carry meaning, and the focus ring.
  ['--rule-2', '--paper', AA_LARGE],
  ['--focus', '--paper', AA_LARGE],
];

function parseTokens(css, selector) {
  const start = css.indexOf(selector);
  if (start === -1) throw new Error(`token block ${selector} not found in globals.css`);
  const open = css.indexOf('{', start);
  const close = css.indexOf('}', open);
  const block = css.slice(open + 1, close);
  const tokens = {};
  for (const line of block.split('\n')) {
    const match = /^\s*(--[\w-]+):\s*([^;]+);/.exec(line);
    if (match) tokens[match[1]] = match[2].trim();
  }
  return tokens;
}

function toRgb(hex) {
  const value = hex.replace('#', '');
  const full =
    value.length === 3
      ? value
          .split('')
          .map((c) => c + c)
          .join('')
      : value;
  return [0, 2, 4].map((i) => parseInt(full.slice(i, i + 2), 16));
}

function relativeLuminance(hex) {
  const [r, g, b] = toRgb(hex).map((channel) => {
    const s = channel / 255;
    return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

function contrastRatio(a, b) {
  const la = relativeLuminance(a);
  const lb = relativeLuminance(b);
  const [light, dark] = la > lb ? [la, lb] : [lb, la];
  return (light + 0.05) / (dark + 0.05);
}

const css = readFileSync(join(WEBSITE, 'app', 'globals.css'), 'utf8');
for (const [themeName, selector] of [
  ['light', ":root[data-theme='light']"],
  ['dark', ":root[data-theme='dark']"],
]) {
  const tokens = parseTokens(css, selector);
  for (const [fg, bg, minimum] of CONTRAST_PAIRS) {
    const fgValue = tokens[fg];
    const bgValue = tokens[bg];
    if (!fgValue || !bgValue) {
      fail(`contrast/${themeName}`, `token ${!fgValue ? fg : bg} is not defined`);
      continue;
    }
    const ratio = contrastRatio(fgValue, bgValue);
    if (ratio < minimum) {
      fail(
        `contrast/${themeName}`,
        `${fg} (${fgValue}) on ${bg} (${bgValue}) is ${ratio.toFixed(2)}:1, below the ${minimum}:1 minimum`,
      );
    }
  }
}

/* -- 2. structure, via axe-core in jsdom ----------------------------------- */

function representativeRoutes() {
  const CONTENT = join(WEBSITE, 'content');
  const routes = new Set(['/', '/api', '/search?q=intent']);
  for (const section of readdirSync(CONTENT)) {
    const dir = join(CONTENT, section);
    if (!statSync(dir).isDirectory()) continue;
    routes.add(`/${section}`);
    const first = readdirSync(dir)
      .filter((entry) => entry.endsWith('.mdx'))
      .sort()[0];
    if (first) routes.add(`/${section}/${first.slice(0, -4)}`);
  }
  const api = JSON.parse(readFileSync(join(WEBSITE, 'generated', 'api.json'), 'utf8'));
  if (api.symbols[0]) routes.add(`/api/${api.symbols[0].name}`);
  return [...routes];
}

// Rules jsdom cannot answer honestly (no layout, no painted pixels). Disabled
// explicitly and named, rather than left to report a misleading pass.
const DISABLED_RULES = {
  'color-contrast': 'no layout engine in jsdom — covered by the token pass above',
  'target-size': 'requires layout',
  'scrollable-region-focusable': 'requires layout',
};

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
  throw new Error(`server did not become ready on ${ORIGIN}`);
}

const server = spawn('node_modules/.bin/next', ['start', '--port', String(PORT)], {
  cwd: WEBSITE,
  stdio: ['ignore', 'ignore', 'pipe'],
  env: { ...process.env, NODE_ENV: 'production' },
});

try {
  await waitForServer();

  for (const route of representativeRoutes()) {
    const html = await (await fetch(`${ORIGIN}${route}`)).text();
    // `runScripts: 'outside-only'` gives the window a script context we can
    // inject axe into, without executing the page's own scripts — which is
    // exactly right here: the point is to audit the markup a crawler or a
    // reader without JavaScript receives.
    const dom = new JSDOM(html, {
      url: `${ORIGIN}${route}`,
      pretendToBeVisual: true,
      runScripts: 'outside-only',
    });
    const { window } = dom;

    window.eval(axe.source);
    const results = await window.axe.run(window.document, {
      resultTypes: ['violations'],
      rules: Object.fromEntries(
        Object.keys(DISABLED_RULES).map((rule) => [rule, { enabled: false }]),
      ),
    });

    for (const violation of results.violations) {
      if (violation.impact !== 'serious' && violation.impact !== 'critical') continue;
      const targets = violation.nodes
        .slice(0, 3)
        .map((node) => node.target.join(' '))
        .join(', ');
      fail(route, `[${violation.impact}] ${violation.id}: ${violation.help} (${targets})`);
    }
    window.close();
  }

  if (findings.length > 0) {
    console.error(`accessibility check failed: ${findings.length} finding(s)\n`);
    for (const finding of findings) console.error(finding);
    console.error(
      '\nReproduce locally with:\n    pnpm --dir website build && pnpm --dir website check:a11y',
    );
    process.exit(1);
  }

  console.log(
    `accessibility check passed: ${CONTRAST_PAIRS.length * 2} token pairs, ` +
      `${representativeRoutes().length} routes (rules skipped in jsdom: ${Object.keys(DISABLED_RULES).join(', ')})`,
  );
} catch (error) {
  console.error(`accessibility check errored: ${error.message}`);
  process.exitCode = 1;
} finally {
  server.kill('SIGTERM');
}
