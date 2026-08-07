#!/usr/bin/env python3
"""Extract the docs' and the site's figures as standalone SVGs for the README.

Both documentation surfaces draw their figures as inline SVG painted from the
one design-token palette: the `docs/` tree writes `var(--rule-2, #8e8e87)`
directly, and `website/` writes class names (`dg-node`, `dg-line--intents`)
that `website/app/diagram.css` resolves against the same tokens. Neither form
survives GitHub's README sanitizer, which strips inline `<svg>` outright.

So the README references *files* instead, and this script writes them: each
figure lifted verbatim from its source, wrapped with the palette resolved to
literal colours, once for each theme. The geometry is never re-authored here —
if a figure changes on either site, this script re-emits it; it cannot drift
into drawing something the sites do not.

Two colour themes per figure, because the README pairs them in a `<picture>`
element and GitHub switches on the reader's own theme:

    docs/assets/diagrams/<slug>-light.svg
    docs/assets/diagrams/<slug>-dark.svg

Website figures are read from the production build's pre-rendered HTML rather
than from the TSX, so what lands here is what the site actually serves.

    make site-build                      # populates website/.next/server/app
    uv run python scripts/gen_readme_diagrams.py
    uv run python scripts/gen_readme_diagrams.py --check   # CI: no drift
"""

from __future__ import annotations

import argparse
import html
import re
import sys
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SITE_BUILD = REPO / "website" / ".next" / "server" / "app"
DIAGRAM_CSS = REPO / "website" / "app" / "diagram.css"
OUT_DIR = REPO / "docs" / "assets" / "diagrams"

# The palette, copied from the one place each site defines it:
# `website/app/globals.css` and `docs/stylesheets/extra.css`, which hold the
# same values. Resolved to literals here because a standalone SVG has no
# cascade to inherit them from.
LIGHT = {
    "paper": "#ffffff",
    "paper-2": "#f6f6f4",
    "paper-3": "#edede9",
    "ink": "#0b0c0e",
    "ink-2": "#4a4e54",
    "ink-3": "#63676d",
    "rule": "#e2e2dd",
    "rule-2": "#8e8e87",
    "s-output": "#0b0c0e",
    "s-intents": "#8a5205",
    "s-traces": "#3c5c78",
    "s-errors": "#9e2a18",
}
DARK = {
    "paper": "#0c0d0f",
    "paper-2": "#141618",
    "paper-3": "#1c1f22",
    "ink": "#ededea",
    "ink-2": "#a7abb0",
    "ink-3": "#8f949a",
    "rule": "#262a2e",
    "rule-2": "#5f666d",
    "s-output": "#ededea",
    "s-intents": "#e0a458",
    "s-traces": "#9bbcdd",
    "s-errors": "#ee9683",
}

# GitHub renders a README image at its intrinsic size, capped by the column.
# The sites author their geometry against a ~600-unit article column, so the
# 9.5px labels would land under 10px at 1:1. Scaling the intrinsic size up
# renders them at roughly the size the sites show them at.
SCALE = 1.3

FONT_SANS = "'Instrument Sans', ui-sans-serif, system-ui, -apple-system, sans-serif"
FONT_MONO = "'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"


@dataclass(frozen=True)
class Figure:
    """One figure to extract: where it lives, and what it is called on disk."""

    slug: str
    #: `docs/`-relative markdown path, or a site route under `website/`.
    source: str
    #: The figure's `<title>` text, which is what identifies it in its source.
    title: str


# Website figures, addressed by route. Order follows the README.
SITE_FIGURES = [
    Figure("activation-shape", "learn/what-is-beam-agents", "The shape of an activation"),
    Figure(
        "runtime-vs-framework",
        "comparison/agent-framework-outside-a-runtime",
        "Two arrangements of the same work",
    ),
    Figure("dataflow-shape", "learn/architecture", "The dataflow shape"),
    Figure("two-paths", "learn/architecture", "Two paths through RunAgent"),
    Figure("state-per-key", "learn/state-and-memory", "What one entity key holds"),
    Figure("staged-write", "learn/state-and-memory", "The life of a staged write"),
    Figure(
        "effector-round-trip",
        "docs/effector",
        "How an intent leaves the pipeline and its result comes back",
    ),
    Figure("approval-round-trip", "docs/human-in-the-loop", "The approval round trip"),
    Figure("fail-closed", "docs/human-in-the-loop", "Failing closed at both layers"),
    Figure("errors-path", "docs/errors", "How an element-level failure reaches .errors"),
    Figure("dead-letter", "docs/errors", "What a dead letter looks like on the errors topic"),
    Figure("activation-commit", "learn/correctness-invariants", "How an activation commits"),
    Figure("intent-id", "learn/correctness-invariants", "Where an intent id comes from"),
    Figure(
        "retry-cost",
        "learn/correctness-invariants",
        "What a bundle retry costs on the cached path",
    ),
    Figure("span-tree", "docs/traces", "One activation's span tree"),
    Figure("metrics-map", "docs/metrics", "Where the runtime metrics are recorded"),
    Figure("runners", "docs/runners", "Where a beam-agents pipeline runs"),
    Figure("adapter-seam", "comparison/adapters", "Where an adapter sits"),
]

# `docs/` figures, addressed by markdown path. These are the ones the mkdocs
# tree draws and the website does not.
DOCS_FIGURES = [
    Figure(
        "four-outputs",
        "design/apache-beam-ml-agents.md",
        "One multi-output transform, four tagged outputs, four destinations",
    ),
    Figure("sharding", "sharding.md", "Where ShardKeys sits in the dataflow shape"),
    Figure("errors-envelope", "errors.md", "One errors-topic value"),
    Figure("continuous-eval", "continuous_eval.md", "The evaluation pipeline that closes the loop"),
    Figure(
        "slack-approval",
        "examples/slack-approval.md",
        "The approval round trip through a Slack surface",
    ),
]


def diagram_rules() -> str:
    """The class rules from `diagram.css` that paint SVG geometry.

    The layout rules around the figure (`.dg-figure`, `.dg-scroll`,
    `.dg-caption`) belong to the page, not the drawing, so they are dropped:
    a standalone file has no page to lay out against.
    """
    css = DIAGRAM_CSS.read_text()
    css = re.sub(r"/\*.*?\*/", "", css, flags=re.S)
    kept = []
    for block in re.findall(r"([^{}]+)\{([^{}]*)\}", css):
        selector, body = block[0].strip(), block[1].strip()
        if selector.startswith((".dg-figure", ".dg-scroll", ".dg-caption")):
            continue
        kept.append(f"{selector}{{{' '.join(body.split())}}}")
    return "".join(kept)


def token_block(palette: dict[str, str]) -> str:
    """The palette, as custom properties on the SVG root."""
    props = "".join(f"--{name}:{value};" for name, value in palette.items())
    return f"svg{{{props}--font-sans:{FONT_SANS};--font-mono:{FONT_MONO};}}"


def extract_site_svg(figure: Figure) -> str:
    """Pull one figure's `<svg>` out of the pre-rendered page it appears on."""
    page = SITE_BUILD / f"{figure.source}.html"
    if not page.is_file():
        raise SystemExit(
            f"{page} is missing — run `make site-build` before regenerating the diagrams"
        )
    return _svg_with_title(page.read_text(errors="replace"), figure)


def extract_docs_svg(figure: Figure) -> str:
    """Pull one figure's `<svg>` out of the markdown page it is authored in."""
    page = REPO / "docs" / figure.source
    return _svg_with_title(page.read_text(), figure)


def _svg_with_title(markup: str, figure: Figure) -> str:
    """The one `<svg>` element in `markup` whose `<title>` matches the figure.

    Titles are unescaped before comparing: the site's pre-rendered HTML writes
    an apostrophe as `&#x27;`, so a title matched against the source literal
    would silently miss exactly the figures whose names read most like prose.
    """
    candidates: list[str] = re.findall(r"<svg[^>]*>.*?</svg>", markup, re.S)
    for candidate in candidates:
        title = re.search(r"<title[^>]*>(.*?)</title>", candidate, re.S)
        if title and html.unescape(title.group(1)).strip().startswith(figure.title):
            return candidate
    raise SystemExit(f"no <svg> titled {figure.title!r} in {figure.source}")


def standalone(svg: str, palette: dict[str, str], *, needs_classes: bool) -> str:
    """Wrap one extracted `<svg>` as a self-contained, themed file.

    The element's own geometry is untouched. What is added is the palette it
    was drawn against, the class rules it was painted by (site figures only),
    an explicit background — a transparent SVG borrows whatever is behind it,
    which on the wrong theme means ink on ink — and the `xmlns` a standalone
    file needs and an inline one does not.
    """
    box = re.search(r'viewBox="0 0 ([\d.]+) ([\d.]+)"', svg)
    if box is None:
        raise SystemExit("extracted <svg> has no parseable viewBox")
    width, height = float(box.group(1)), float(box.group(2))

    open_tag = re.match(r"<svg[^>]*>", svg).group(0)  # type: ignore[union-attr]
    body = svg[len(open_tag) : -len("</svg>")]

    # `min-width` is the site's horizontal-scroll affordance, which belongs to
    # the scroll container the standalone file does not have. The `docs/` tree
    # authors `width="100%"` for the same reason, and a percentage width leaves
    # a standalone file with no intrinsic size for GitHub to lay out against.
    open_tag = re.sub(r'\s*style="min-width:[^"]*"', "", open_tag)
    open_tag = re.sub(r'\s*(?:width|height)="[^"]*"', "", open_tag)
    sized = f'width="{round(width * SCALE)}" height="{round(height * SCALE)}"'
    # The `docs/` figures already declare the namespace; the site's do not,
    # because an inline SVG inherits it from the HTML document it sits in.
    namespace = "" if "xmlns=" in open_tag else 'xmlns="http://www.w3.org/2000/svg" '
    open_tag = open_tag.replace("<svg", f"<svg {namespace}{sized}", 1)

    style = token_block(palette) + (diagram_rules() if needs_classes else "")
    background = f'<rect x="0" y="0" width="{width}" height="{height}" fill="var(--paper)"/>'
    return f"{open_tag}<style>{style}</style>{background}{body}</svg>\n"


def build() -> dict[Path, str]:
    """Every file this script owns, keyed by path."""
    files: dict[Path, str] = {}
    for figure, extract, needs_classes in (
        *((f, extract_site_svg, True) for f in SITE_FIGURES),
        *((f, extract_docs_svg, False) for f in DOCS_FIGURES),
    ):
        svg = extract(figure)
        for theme, palette in (("light", LIGHT), ("dark", DARK)):
            files[OUT_DIR / f"{figure.slug}-{theme}.svg"] = standalone(
                svg, palette, needs_classes=needs_classes
            )
    return files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail instead of writing when a file on disk differs from the source figure",
    )
    args = parser.parse_args()

    files = build()
    if args.check:
        stale = [p for p, text in files.items() if not p.is_file() or p.read_text() != text]
        for path in stale:
            print(f"stale: {path.relative_to(REPO)}", file=sys.stderr)
        if stale:
            print(
                "run `make site-build && uv run python scripts/gen_readme_diagrams.py`",
                file=sys.stderr,
            )
        return 1 if stale else 0

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for path, text in files.items():
        path.write_text(text)
    print(f"wrote {len(files)} files to {OUT_DIR.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
