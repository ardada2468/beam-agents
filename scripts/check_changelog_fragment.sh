#!/usr/bin/env bash
# Require a changelog fragment whenever src/ changes.
#
# Same shape (and same escape-hatch discipline) as check_openspec_change.sh:
# the changelog entry is authored by the change author, in user voice, in the
# same reviewed diff as the code. A change with no user-observable effect
# satisfies this with an `internal` fragment, which assembly never renders.
set -euo pipefail

if [[ "${BEAM_AGENTS_ALLOW_NO_FRAGMENT:-}" == "1" ]]; then
  exit 0
fi

# Locally (via `git commit`), check what's staged. In CI there's nothing staged
# in a fresh checkout, so `git diff --cached` would trivially pass on every
# run; compare against the PR base instead so the repo-wide `pre-commit run
# --all-files` step in quality.yml is meaningful too. (Same push-to-main
# limitation as check_openspec_change.sh: branch protection's PR-time run is
# what covers it.)
if [[ -n "${CI:-}" ]]; then
  changed_src_files=$(git diff --name-only origin/main...HEAD -- 'src/*' 2>/dev/null || true)
else
  changed_src_files=$(git diff --cached --name-only --diff-filter=ACMR -- 'src/*')
fi
if [[ -z "$changed_src_files" ]]; then
  exit 0
fi

# `*.*.md` requires two dots, so changelog.d/README.md (the format's own
# documentation) is not mistaken for a fragment.
shopt -s nullglob
fragments=(changelog.d/*.*.md)

if [[ ${#fragments[@]} -eq 0 ]]; then
  echo "error: changes touch src/ but changelog.d/ holds no fragment." >&2
  echo "       add changelog.d/<openspec-change-name>.<type>.md — one" >&2
  echo "       user-facing sentence. Types: breaking, added, changed, fixed," >&2
  echo "       docs, internal (internal is never rendered). See CONTRIBUTING.md" >&2
  echo "       and docs/releasing.md, or set BEAM_AGENTS_ALLOW_NO_FRAGMENT=1" >&2
  echo "       to bypass (discouraged)." >&2
  exit 1
fi

# The registry is closed: a typo'd type would otherwise satisfy this hook and
# then be silently dropped at assembly time.
registered=" breaking added changed fixed docs internal "
unregistered=()
for fragment in "${fragments[@]}"; do
  base="${fragment##*/}"
  stem="${base%.md}"
  type="${stem##*.}"
  if [[ "$registered" != *" $type "* ]]; then
    unregistered+=("$base")
  fi
done

if [[ ${#unregistered[@]} -gt 0 ]]; then
  echo "error: unregistered changelog fragment type(s): ${unregistered[*]}" >&2
  echo "       the registry is breaking, added, changed, fixed, docs, internal" >&2
  echo "       (see changelog.d/README.md and docs/releasing.md)." >&2
  exit 1
fi

exit 0
