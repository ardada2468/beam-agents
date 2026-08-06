/**
 * Site-wide constants.
 *
 * Every fact here is checked against the repository by
 * `scripts/verify_docs_claims.py` — the version and the release state in
 * particular, because they are what install instructions depend on.
 */

/**
 * The production origin is `https://beamagent.org` (Cloudflare-managed domain,
 * chosen 2026-08-04). It is the default so canonical URLs, the sitemap origin,
 * and `metadataBase` are right in any production build; local dev and preview
 * deployments override it with `NEXT_PUBLIC_SITE_URL`. The GitHub Pages host
 * (ardada2468.github.io/beam-agents) serves the mkdocs tree, not this site.
 */
export const SITE_URL = (process.env.NEXT_PUBLIC_SITE_URL ?? 'https://beamagent.org').replace(
  /\/$/,
  '',
);

export const SITE_NAME = 'beam-agents';

export const SITE_TAGLINE =
  'A runtime for running AI agents as keyed, stateful, fault-tolerant Apache Beam transforms.';

export const REPO_URL = 'https://github.com/ardada2468/beam-agents';

/** Branch the source links point at. */
export const REPO_REF = 'main';

export const LICENSE = 'Apache-2.0';

/**
 * The declared package version, mirroring `pyproject.toml`.
 * `scripts/verify_docs_claims.py` fails the check when the two drift.
 */
export const PACKAGE_VERSION = '1.0.0';

/**
 * Release state is deliberately NOT derived from the version string: the
 * version bumps in the release PR before the `v1.0.0` tag is pushed, and
 * during that window the package is declared but unpublished. This flips to
 * `true` at tag time, by hand, and `scripts/verify_docs_claims.py` holds it to
 * the actual `git tag -l v{version}` state — so flipping it early, or
 * forgetting to flip it after the tag, fails the build.
 */
export const IS_RELEASED = false;

export function repoFileUrl(path: string, line?: number): string {
  const anchor = line !== undefined ? `#L${line}` : '';
  return `${REPO_URL}/blob/${REPO_REF}/${path}${anchor}`;
}

export function absoluteUrl(path: string): string {
  return `${SITE_URL}${path.startsWith('/') ? path : `/${path}`}`;
}

/**
 * The standing disclaimer. `beam-agents` is Apache-2.0 licensed and built on
 * Apache Beam; it is not an Apache Software Foundation project, and the
 * distinction matters enough to appear on every page rather than in an
 * about-page footnote.
 */
export const ASF_DISCLAIMER =
  'beam-agents is licensed under Apache-2.0 and builds on Apache Beam. It is not ' +
  'an Apache Software Foundation project and is not endorsed by the ASF. Apache, ' +
  'Apache Beam, and Apache Flink are trademarks of the Apache Software Foundation.';
