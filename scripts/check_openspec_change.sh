#!/usr/bin/env bash
set -euo pipefail

if [[ "${BEAM_AGENTS_ALLOW_NO_CHANGE:-}" == "1" ]]; then
  exit 0
fi

# Locally (via `git commit`), check what's staged. In CI there's nothing
# staged in a fresh checkout, so `git diff --cached` would trivially pass on
# every run; compare against the PR base instead so the hook is meaningful
# there too. (On a push-to-main event HEAD and origin/main coincide, so this
# still can't catch a direct push -- same limitation quality.yml's mutation
# `core-changed` check has; branch protection's PR-time run is what covers it.)
if [[ -n "${CI:-}" ]]; then
  staged_src_files=$(git diff --name-only origin/main...HEAD -- 'src/*' 2>/dev/null || true)
else
  staged_src_files=$(git diff --cached --name-only --diff-filter=ACMR -- 'src/*')
fi
if [[ -z "$staged_src_files" ]]; then
  exit 0
fi

shopt -s nullglob
change_dirs=(openspec/changes/*/)
if [[ ${#change_dirs[@]} -eq 0 ]]; then
  echo "error: staged changes touch src/ but no OpenSpec change directory exists." >&2
  echo "       create one with the OpenSpec propose workflow, or set" >&2
  echo "       BEAM_AGENTS_ALLOW_NO_CHANGE=1 to bypass (discouraged)." >&2
  exit 1
fi

# A proposal.md existing somewhere is not enough -- it must actually relate to
# what's staged. Heuristic but real: correlate each staged src/ file (and its
# top-level module) against the text of every active change's own files, so an
# unrelated, merged-but-unarchived proposal can no longer wave through any
# src/ commit.
while IFS= read -r src_file; do
  module="${src_file#src/beam_agents/}"
  top_level="${module%%/*}"
  for change_dir in "${change_dirs[@]}"; do
    [[ -f "${change_dir}proposal.md" ]] || continue
    if grep -Rlq -- "$module" "$change_dir" 2>/dev/null \
        || grep -Rlq -- "$top_level" "$change_dir" 2>/dev/null; then
      exit 0
    fi
  done
done <<<"$staged_src_files"

echo "error: staged changes touch src/ but no active OpenSpec change" >&2
echo "       (openspec/changes/*/proposal.md) mentions any of the touched files" >&2
echo "       or their module. Create/update one with the OpenSpec propose" >&2
echo "       workflow, or set BEAM_AGENTS_ALLOW_NO_CHANGE=1 to bypass (discouraged)." >&2
exit 1
