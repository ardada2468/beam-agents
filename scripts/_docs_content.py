"""Shared loading for the documentation-site fidelity checks.

Both `verify_docs_claims.py` and `check_docs_prose.py` need the same thing: the
site's content files, their frontmatter, and a line-accurate view of the body
so a finding can name the line a reader would have to edit. Keeping that in one
place means the two checks can never disagree about what the content *is*.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
CONTENT_ROOT = REPO_ROOT / "website" / "content"
EXAMPLES_ROOT = REPO_ROOT / "website" / "examples"

_FENCE = "```"


@dataclass(frozen=True)
class Finding:
    """One check failure, addressed to whoever has to fix it."""

    file: str
    line: int
    message: str

    def render(self) -> str:
        return f"{self.file}:{self.line}: {self.message}"


@dataclass
class ContentPage:
    path: Path
    """Repository-relative path, for findings."""
    rel: str
    frontmatter: dict[str, Any]
    body: str
    """1-indexed line number in the file where the body starts."""
    body_offset: int
    errors: list[str] = field(default_factory=list)

    @property
    def section(self) -> str:
        return self.path.parent.name

    @property
    def slug(self) -> str:
        return self.path.stem

    @property
    def href(self) -> str:
        return f"/{self.section}/{self.slug}"

    @property
    def status(self) -> str:
        value = self.frontmatter.get("status")
        return value if isinstance(value, str) else ""

    def body_lines(self) -> list[tuple[int, str]]:
        """Body lines as ``(file line number, text)``."""
        return [
            (self.body_offset + index, line) for index, line in enumerate(self.body.split("\n"))
        ]

    def prose_lines(self) -> list[tuple[int, str]]:
        """Body lines outside fenced code blocks.

        Numbers inside a code fence are configuration, not claims: `100 KiB` in
        a snippet is the API, and flagging it as an unsourced statistic would
        make the prose check useless noise.
        """
        out: list[tuple[int, str]] = []
        in_fence = False
        for number, line in self.body_lines():
            if line.lstrip().startswith(_FENCE):
                in_fence = not in_fence
                continue
            if not in_fence:
                out.append((number, line))
        return out


def _split_frontmatter(text: str, rel: str) -> tuple[dict[str, Any], str, int, list[str]]:
    """Split a `---`-delimited YAML header from the body.

    Returns the parsed header, the body, the body's starting line number, and
    any structural errors. A missing header is an error, not an empty dict: a
    page with no frontmatter has no status, and a page with no status must not
    render.
    """
    lines = text.split("\n")
    if not lines or lines[0].strip() != "---":
        return {}, text, 1, [f"{rel}: missing YAML frontmatter (a `---` block must open the file)"]
    for index in range(1, len(lines)):
        if lines[index].strip() == "---":
            raw = "\n".join(lines[1:index])
            body = "\n".join(lines[index + 1 :])
            try:
                parsed = yaml.safe_load(raw) or {}
            except yaml.YAMLError as exc:
                return {}, body, index + 2, [f"{rel}: frontmatter is not valid YAML: {exc}"]
            if not isinstance(parsed, dict):
                return {}, body, index + 2, [f"{rel}: frontmatter must be a mapping"]
            return parsed, body, index + 2, []
    return {}, text, 1, [f"{rel}: frontmatter block is never closed with `---`"]


def load_pages() -> list[ContentPage]:
    """Every content page, in stable path order."""
    pages: list[ContentPage] = []
    if not CONTENT_ROOT.exists():
        return pages
    for path in sorted(CONTENT_ROOT.rglob("*.mdx")):
        rel = path.relative_to(REPO_ROOT).as_posix()
        text = path.read_text(encoding="utf-8")
        frontmatter, body, offset, errors = _split_frontmatter(text, rel)
        pages.append(
            ContentPage(
                path=path,
                rel=rel,
                frontmatter=frontmatter,
                body=body,
                body_offset=offset,
                errors=errors,
            )
        )
    return pages


def frontmatter_line(page: ContentPage, key: str) -> int:
    """The line a top-level frontmatter key sits on, or 1 if it is absent."""
    for index, line in enumerate(page.path.read_text(encoding="utf-8").split("\n"), start=1):
        if line.startswith(f"{key}:"):
            return index
        if index >= page.body_offset:
            break
    return 1
