import { ImageResponse } from 'next/og';

/**
 * The favicon, generated at build time (Next.js `app/icon` convention).
 *
 * The mark is the site's signature reduced to its glyph: the pipe from
 * `events | RunAgent(my_agent)` — set in the mono face's spirit on the dark
 * paper token, with the amber the site reserves for "staged". No logo exists
 * for this project, and inventing one here would outrun the identity; a
 * literal pipe on ink is exactly as much brand as the project has.
 */

export const size = { width: 32, height: 32 };
export const contentType = 'image/png';

export default function Icon() {
  return new ImageResponse(
    <div
      style={{
        width: '100%',
        height: '100%',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        // The dark theme's --paper and --s-intents tokens, hardcoded because
        // a favicon renders outside the page's CSS variable scope.
        background: '#0c0d0f',
        color: '#e0a458',
        fontSize: 24,
        fontWeight: 600,
        fontFamily: 'monospace',
      }}
    >
      |
    </div>,
    { ...size },
  );
}
