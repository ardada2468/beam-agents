import { ImageResponse } from 'next/og';
import { IS_RELEASED, LICENSE, PACKAGE_VERSION, SITE_NAME } from '@/lib/site';

/**
 * The share card, generated at build time (Next.js `app/opengraph-image`
 * convention) so links do not unfurl bare. It restates the landing hero and
 * nothing else — the headline, the invocation, and the version badge — using
 * the dark theme's tokens, hardcoded because the card renders outside the
 * page's CSS variable scope. The version line stays truthful the same way the
 * hero does: it renders from `PACKAGE_VERSION`, which the claim verifier holds
 * to `pyproject.toml`.
 */

export const size = { width: 1200, height: 630 };
export const contentType = 'image/png';
export const alt = 'beam-agents — an agent is a Beam transform.';

export default function OpenGraphImage() {
  return new ImageResponse(
    <div
      style={{
        width: '100%',
        height: '100%',
        display: 'flex',
        flexDirection: 'column',
        justifyContent: 'space-between',
        padding: 72,
        background: '#0c0d0f',
        color: '#ededea',
      }}
    >
      <div
        style={{
          display: 'flex',
          fontSize: 28,
          fontFamily: 'monospace',
          color: '#e0a458',
        }}
      >
        {SITE_NAME}
      </div>
      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          gap: 36,
        }}
      >
        <div style={{ display: 'flex', fontSize: 84, fontWeight: 700, letterSpacing: '-0.02em' }}>
          An agent is a Beam transform.
        </div>
        <div
          style={{
            display: 'flex',
            alignSelf: 'flex-start',
            fontFamily: 'monospace',
            fontSize: 32,
            padding: '18px 28px',
            border: '1px solid #33363b',
            background: '#121316',
            color: '#ededea',
          }}
        >
          events | RunAgent(my_agent)
        </div>
      </div>
      <div style={{ display: 'flex', fontSize: 26, color: '#9a9c9f' }}>
        v{PACKAGE_VERSION}
        {IS_RELEASED ? '' : ' · pre-release'} · {LICENSE} · Python 3.11–3.12
      </div>
    </div>,
    { ...size },
  );
}
