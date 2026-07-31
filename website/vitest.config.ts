import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

/**
 * The unit tests need the same `@/…` module alias the site is written against.
 *
 * `@/*` is declared in `tsconfig.json`, but a tsconfig path is a compiler
 * fiction: Next resolves it, plain Node and vite do not. Any test that imports
 * a module which reaches a component — `lib/mdx.tsx` pulls in the whole MDX
 * component scope, for instance — fails to resolve without this. Keep the two
 * declarations in step; the alias has to point at the same directory the
 * tsconfig `baseUrl` implies, which is the `website/` root.
 */
export default defineConfig({
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('.', import.meta.url)).replace(/\/$/, ''),
    },
  },
});
