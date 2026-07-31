import type { Metadata } from 'next';
import type { ReactNode } from 'react';
// Fonts are installed as packages and bundled with the build. Nothing is
// fetched from a font CDN at render time, so the site stays hermetic and works
// with no network egress — the same constraint the search index is built under.
import '@fontsource-variable/instrument-sans';
import '@fontsource/ibm-plex-mono/400.css';
import '@fontsource/ibm-plex-mono/500.css';
import './globals.css';
import { Header } from '@/components/Header';
import { Footer } from '@/components/Footer';
import { SITE_NAME, SITE_TAGLINE, SITE_URL } from '@/lib/site';

export const metadata: Metadata = {
  metadataBase: new URL(SITE_URL),
  title: { default: `${SITE_NAME} — agents as Beam transforms`, template: `%s — ${SITE_NAME}` },
  description: SITE_TAGLINE,
  openGraph: { siteName: SITE_NAME, type: 'website' },
  twitter: { card: 'summary' },
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body>
        {/* Keyboard users land here first; it is the only reason the main
            landmark carries an id. */}
        <a
          href="#main"
          className="sr-only focus:not-sr-only focus:absolute focus:top-2 focus:left-2 focus:z-50 focus:rounded focus:px-3 focus:py-2"
          style={{ background: 'var(--accent)', color: 'var(--accent-fg)' }}
        >
          Skip to content
        </a>
        <Header />
        <main id="main">{children}</main>
        <Footer />
      </body>
    </html>
  );
}
