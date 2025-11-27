#!/usr/bin/env bash
# Walk merged pull requests for a base branch and generate changelog entries sequentially.

set -euo pipefail

log() {
    # Log a message with timestamp for transparent progress tracking.
    local level message
    level=${1:-INFO}
    shift || true
    message="$*"
    printf '[%s] [%s] %s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$level" "$message"
}

require_command() {
    # Ensure required commands are available before running the batch.
    local cmd="$1"
    command -v "$cmd" >/dev/null 2>&1 || { log ERROR "Required command missing: $cmd"; exit 1; }
}

usage() {
    cat <<'USAGE'
Usage: changelog_pr_batch.sh <base-branch>

Environment variables:
  PR_LIMIT           Limit number of PRs to process (defaults to 200).
  CHANGELOG_MODEL    Override the model passed to llm_changelog.py.
  CHANGELOG_CONFIG   Path to the prompts config file.
  DRY_RUN            If set, forwards --dry-run to llm_changelog.py.
  PY                 Python interpreter to use (defaults to python3).

Example:
  BASE_BRANCH=develop scripts/automation/changelog_pr_batch.sh develop
USAGE
}

main() {
    if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
        usage
        exit 0
    fi

    require_command gh
    require_command git

    local base_branch
    base_branch=${1:-${BASE_BRANCH:-}}
    if [[ -z "$base_branch" ]]; then
        log ERROR "Base branch is required (pass as arg or BASE_BRANCH env)."
        usage
        exit 1
    fi

    local pr_limit model config_path dry_flag python_bin
    pr_limit=${PR_LIMIT:-200}
    model=${CHANGELOG_MODEL:-llama3.1}
    config_path=${CHANGELOG_CONFIG:-scripts/automation/config/prompts.yaml}
    dry_flag=${DRY_RUN:+--dry-run}
    python_bin=${PY:-python3}

    log INFO "Listing merged PRs based on base branch '$base_branch' (limit: $pr_limit)"
    local pr_lines
    if ! pr_lines=$(gh pr list --state merged --base "$base_branch" --limit "$pr_limit" \
        --json number,title,headRefName,baseRefName,mergedAt \
        --template '{{range .}}{{.number}}{{"\t"}}{{.title}}{{"\t"}}{{.headRefName}}{{"\t"}}{{.baseRefName}}{{"\t"}}{{.mergedAt}}{{"\n"}}{{end}}'); then
        log ERROR "Failed to list merged PRs for base '$base_branch'"
        exit 1
    fi

    if [[ -z "$pr_lines" ]]; then
        log INFO "No merged PRs found for base '$base_branch'"
        exit 0
    fi

    while IFS=$'\t' read -r pr_number pr_title head_ref base_ref merged_at; do
        [[ -z "$pr_number" ]] && continue
        log INFO "Processing PR #$pr_number ($merged_at): $pr_title"

        # Instead of diff, build a file containing commit messages for this PR.
        local context_file
        context_file=$(mktemp "/tmp/changelog_pr_${pr_number}_XXXX.log")

        # Fetch commit messages for this PR via GitHub API.
        # We include headline + body (if present) for richer context.
        if ! gh pr view "$pr_number" --json commits \
            --jq '
                "Commit messages for PR #\(.number):" as $header
                | $header, ( .commits[]
                    | "- " + .messageHeadline
                    + (if (.messageBody != null and .messageBody != "") then "\n  " + (.messageBody | gsub("\n"; "\n  ")) else "" end)
                  )
            ' >"$context_file"; then
            log ERROR "Failed to fetch commit messages for PR #$pr_number"
            rm -f "$context_file"
            continue
        fi

        if [[ ! -s "$context_file" ]]; then
            log WARN "No commit messages produced for PR #$pr_number, skipping"
            rm -f "$context_file"
            continue
        fi

        local release_name
        release_name=${RELEASE_NAME:-$pr_title}

        log INFO "Generating changelog for PR #$pr_number (head: $head_ref, base: $base_ref) using commit messages"
        if ! PYTHONPATH=scripts "$python_bin" scripts/automation/llm_changelog.py \
            --diff-file "$context_file" --branch "$head_ref" --release-name "$release_name" \
            --model "$model" --config "$config_path" $dry_flag; then
            log ERROR "Changelog generation failed for PR #$pr_number"
            rm -f "$context_file"
            continue
        fi

        rm -f "$context_file"
        log INFO "Completed PR #$pr_number"
    done <<<"$pr_lines"
}

main "$@"
