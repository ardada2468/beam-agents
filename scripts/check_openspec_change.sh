#!/usr/bin/env bash
set -euo pipefail

if [[ "${BEAM_AGENTS_ALLOW_NO_CHANGE:-}" == "1" ]]; then
  exit 0
fi

staged_src_files=$(git diff --cached --name-only --diff-filter=ACMR -- 'src/*')
if [[ -z "$staged_src_files" ]]; then
  exit 0
fi

if ls openspec/changes/*/proposal.md >/dev/null 2>&1; then
  exit 0
fi

echo "error: staged changes touch src/ but no active OpenSpec change was found." >&2
echo "       create one with the OpenSpec propose workflow, or set" >&2
echo "       BEAM_AGENTS_ALLOW_NO_CHANGE=1 to bypass (discouraged)." >&2
exit 1
