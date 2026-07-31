import { chromium } from 'playwright';
import { mkdirSync } from 'node:fs';

const base = process.env.BASE ?? 'http://127.0.0.1:5173';
const out = process.env.OUT ?? 'screenshots';
mkdirSync(out, { recursive: true });

const routes = (process.env.ROUTES ?? '/').split(',');
const browser = await chromium.launch({
  executablePath: process.env.CHROME ?? '/opt/pw-browsers/chromium-1194/chrome-linux/chrome',
});

for (const theme of ['light', 'dark']) {
  const ctx = await browser.newContext({
    viewport: { width: 1440, height: 950 },
    colorScheme: theme,
  });
  const page = await ctx.newPage();
  for (const route of routes) {
    await page.goto(`${base}${route}`, { waitUntil: 'networkidle' });
    await page.waitForTimeout(400);
    const name = route === '/' ? 'overview' : route.replace(/\//g, '-').slice(1);
    await page.screenshot({ path: `${out}/${name}-${theme}.png`, fullPage: false });
    console.log(`shot ${name}-${theme}`);
  }
  await ctx.close();
}

const mobile = await browser.newContext({
  viewport: { width: 390, height: 844 },
  colorScheme: 'light',
});
const mpage = await mobile.newPage();
await mpage.goto(`${base}${routes[0]}`, { waitUntil: 'networkidle' });
await mpage.waitForTimeout(400);
await mpage.screenshot({ path: `${out}/mobile.png` });
console.log('shot mobile');
await browser.close();
