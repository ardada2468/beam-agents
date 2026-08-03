import { fileURLToPath, URL } from 'node:url';

import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

// The bundle is built straight into the Python package rather than into a
// sibling `dist/`. Hatchling force-includes `console/static/**` when it exists,
// so building here is what makes a wheel ship the UI — and it means the Docker
// build is a plain `npm run build` with no copy step to get out of sync.
const OUT_DIR = fileURLToPath(new URL('../src/beam_agents/console/static', import.meta.url));

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: { '@': fileURLToPath(new URL('./src', import.meta.url)) },
  },
  build: {
    outDir: OUT_DIR,
    emptyOutDir: true,
    // The console is served from a single origin with no CDN, so there is
    // nothing to gain from aggressive chunking and something to lose: a page
    // that stalls on a second request is worse than one that ships 200 KB.
    chunkSizeWarningLimit: 900,
  },
  server: {
    port: 5173,
    proxy: {
      // Dev runs against a real console when one is up; the fixture interceptor
      // in `src/lib/fixtures.ts` takes over when the proxy target is absent, so
      // the UI is buildable and reviewable with no backend at all.
      '/api': { target: 'http://127.0.0.1:8787', changeOrigin: true },
      '/healthz': { target: 'http://127.0.0.1:8787', changeOrigin: true },
    },
  },
});
