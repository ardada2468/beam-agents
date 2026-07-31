/**
 * Site-wide constants.
 *
 * Every fact here is checked against the repository by
 * `scripts/verify_docs_claims.py` — the version and the release state in
 * particular, because they are what install instructions depend on.
 */

export const SITE_URL = (process.env.NEXT_PUBLIC_SITE_URL ?? 'http://localhost:3000').replace(
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
 * The declared package version. `0.0.0` means unreleased, and the install
 * page's shape depends on it — see the release-state check in
 * `scripts/verify_docs_claims.py`.
 */
export const PACKAGE_VERSION = '0.0.0';

export const IS_RELEASED = PACKAGE_VERSION !== '0.0.0';

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
