# Frontend scripts

## `screenshot.mjs`

Captures every route you name, in light and dark, plus one at a 390px mobile
width. Chromium is pre-installed in this environment — never run
`playwright install`.

```bash
npm run dev -- --host 127.0.0.1 --port 5173 &
ROUTES="/,/activations,/errors" node scripts/screenshot.mjs
# → frontend/screenshots/*.png  (gitignored)
```

`BASE` overrides the origin, `OUT` the output directory, and `CHROME` the
browser binary if the pinned path moves.

A page you have not screenshotted in both themes is not finished: the token file
defines two complete ramps and it is entirely possible to write a component that
is legible in only one of them.
