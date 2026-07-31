#!/usr/bin/env python
"""Reject documentation prose that claims what this project cannot support.

Open-source project sites lie in a small number of predictable ways, and almost
always in one direction. This check makes the common ones mechanically
impossible:

- **Social proof that does not exist.** No adopters, customers, testimonials, or
  download counts. The project is pre-release; there is nothing truthful to say
  here, so nothing may be said.
- **Unsourced superlatives.** "The fastest", "the only" — unfalsifiable as
  written and unverifiable by anyone.
- **Unsourced performance numbers.** Any `<n>x`, `<n>%`, `<n>ms`, `<n> QPS`
  outside a code fence needs a citation or an in-repo source. The latency
  budget in `openspec/project.md` is a *budget*: no benchmark harness exists in
  this repository, so no measured figure may be published.
- **Implied ASF governance.** This is an Apache-2.0 project built on Apache
  Beam. It is not an Apache Software Foundation project, and no phrasing may
  suggest otherwise.

Every rule has an escape for the cases a regex gets wrong:

    <!-- prose-check: ok the 100 KiB blob cap is an API limit, not a benchmark -->

The escape requires a reason, and `prose-check: ok` is a single greppable token
so a reviewer can audit every use in one command.

Usage:  uv run python scripts/check_docs_prose.py
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _docs_content import ContentPage, Finding, load_pages

ESCAPE_TOKEN = "prose-check: ok"
ESCAPE = re.compile(r"<!--\s*prose-check:\s*ok\s+(?P<reason>\S.*?)\s*-->")
ESCAPE_NO_REASON = re.compile(r"<!--\s*prose-check:\s*ok\s*-->")


@dataclass(frozen=True)
class Rule:
    name: str
    pattern: re.Pattern[str]
    message: str


RULES: tuple[Rule, ...] = (
    Rule(
        "social-proof",
        re.compile(
            r"\b(trusted by|used in production by|our customers|thousands of (?:users|teams)|"
            r"battle[- ]tested|industry[- ]leading|loved by|adopted by)\b",
            re.IGNORECASE,
        ),
        "adopter/testimonial language. This project is pre-release and has no "
        "published adopters; there is nothing truthful to claim here.",
    ),
    Rule(
        "superlative",
        # "the only" is deliberately narrowed to its marketing sense. "ctx.act
        # is the only effect path" is a precise claim backed by a spec; "the
        # only runtime that does X" is a market claim. Matching the bare phrase
        # flagged the former far more often than the latter.
        re.compile(
            r"\b(the fastest|world[- ]class|best[- ]in[- ]class|unmatched|"
            r"blazing[- ]fast|the most (?:reliable|scalable|advanced)|"
            r"the only \w+(?: \w+)? (?:framework|runtime|library|project|solution|platform))\b",
            re.IGNORECASE,
        ),
        "unsourced superlative. State the property and what backs it, or drop it.",
    ),
    Rule(
        "asf-governance",
        re.compile(
            r"\b(an Apache project|Apache Software Foundation project|ASF project|"
            r"incubating at the ASF|Apache incubator|donated to the ASF)\b",
            re.IGNORECASE,
        ),
        "implies Apache Software Foundation governance. beam-agents is "
        "Apache-2.0 licensed and built on Apache Beam; it is not an ASF project.",
    ),
    Rule(
        "download-counts",
        re.compile(
            r"\b\d[\d,.]*\s*(?:k|m|million)?\s*(?:downloads|stars|installs)\b", re.IGNORECASE
        ),
        "download/star counts. Nothing publishes these numbers for this project.",
    ),
)

# Numeric performance claims. Deliberately narrow: it matches a number bound to
# a performance unit, not every number in the prose, because `100 KiB` (a cap)
# and `p99 < 60 ms` (a budget, when labelled as one) are legitimate.
PERFORMANCE_NUMBER = re.compile(
    r"\b\d+(?:\.\d+)?\s*(?:x faster|x speedup|%\s*(?:faster|slower|improvement)|"
    r"(?:QPS|qps|req/s|requests per second|events/s))\b"
)

# A performance figure is allowed when the line also names what it is: a
# budget, a target, or a cited measurement.
PERFORMANCE_CONTEXT = re.compile(
    r"\b(budget|target|design goal|not measured|no published measurement|unmeasured)\b",
    re.IGNORECASE,
)


# A negation immediately before the phrase flips it. The window is short on
# purpose: a "not" three clauses earlier does not negate this one. Markdown
# emphasis is allowed between the two, because the disclaimer this exists to
# permit is written "**Not** an Apache Software Foundation project".
_NEGATED = re.compile(r"\b(not|never|neither|nor|without)\b[\s\w'\u2019*_`-]{0,20}$", re.IGNORECASE)


def _is_negated(text: str, position: int) -> bool:
    return bool(_NEGATED.search(text[max(0, position - 40) : position]))


def check_page(page: ContentPage) -> list[Finding]:
    findings: list[Finding] = []
    for number, text in page.prose_lines():
        if ESCAPE_NO_REASON.search(text):
            findings.append(
                Finding(
                    page.rel,
                    number,
                    f"`{ESCAPE_TOKEN}` escape carries no reason. Write "
                    f"`<!-- {ESCAPE_TOKEN} <why this line is truthful> -->` so the "
                    "exemption can be reviewed.",
                )
            )
            continue
        if ESCAPE.search(text):
            continue
        for rule in RULES:
            match = rule.pattern.search(text)
            if not match:
                continue
            # A denial is the opposite of the claim being guarded against.
            # "It is NOT an Apache Software Foundation project" is the sentence
            # this project is obliged to print; flagging it would force the
            # disclaimer off the site.
            if _is_negated(text, match.start()):
                continue
            findings.append(
                Finding(page.rel, number, f"[{rule.name}] {match.group(0)!r}: {rule.message}")
            )
        match = PERFORMANCE_NUMBER.search(text)
        if match and not PERFORMANCE_CONTEXT.search(text):
            findings.append(
                Finding(
                    page.rel,
                    number,
                    f"[performance-claim] {match.group(0)!r}: no benchmark harness exists in "
                    "this repository, so no measured performance figure may be published. "
                    "Label it as an unmeasured design budget, cite a source, or remove it.",
                )
            )
    return findings


def main() -> int:
    pages = load_pages()
    findings: list[Finding] = []
    for page in pages:
        findings.extend(check_page(page))

    if findings:
        print(f"prose check failed: {len(findings)} finding(s)\n", file=sys.stderr)
        for finding in findings:
            print(finding.render(), file=sys.stderr)
        print(
            "\nReproduce locally with:\n    uv run python scripts/check_docs_prose.py\n"
            f"Audit every exemption with:\n    grep -rn '{ESCAPE_TOKEN}' website/content",
            file=sys.stderr,
        )
        return 1

    print(f"prose check passed: {len(pages)} page(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
