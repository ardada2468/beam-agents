import type { NextConfig } from 'next';

/**
 * The site is pre-rendered at build time and served by `next start` (Node
 * runtime) rather than exported statically — see design decision D1 in
 * `openspec/changes/add-docs-website/design.md`. Pre-rendering is what makes
 * every route indexable without client-side JavaScript; keeping the Node
 * server preserves redirects, headers, and future ISR.
 */
const nextConfig: NextConfig = {
  pageExtensions: ['ts', 'tsx'],
  reactStrictMode: true,
  // `next build` must fail on a type or lint error: the fidelity checks are
  // worthless if the build that runs them tolerates broken code.
  typescript: { ignoreBuildErrors: false },
  eslint: { ignoreDuringBuilds: false },
  outputFileTracingIncludes: {
    '/**': ['./content/**/*', './examples/**/*', './generated/**/*'],
  },
};

export default nextConfig;
