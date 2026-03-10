#!/usr/bin/env bash
# Generate an artifact file containing commits from the current branch's open PR.
# Finds the open PR for the current branch and shows only commits unique to this branch.

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
    # Ensure required commands are available before running.
    local cmd="$1"
    command -v "$cmd" >/dev/null 2>&1 || { log ERROR "Required command missing: $cmd"; exit 1; }
}

usage() {
    cat <<'USAGE'
Usage: branch_artifact_builder.sh [branch-name]

Generates an artifact file containing:
  - All unique commits on the branch (commits not in the PR's base branch)
  - All merged PRs with that branch as head reference

If no branch is provided, uses the current branch.
Automatically detects the base branch from the open PR.

Output artifact is written to:
  output/artifacts/branch_<branch-name>_<timestamp>.log

Environment variables:
  OUTPUT_DIR  Override output directory (defaults to output/artifacts).
  PR_LIMIT    Limit number of PRs to process (defaults to 200).

Example:
  scripts/automation/branch_artifact_builder.sh
  scripts/automation/branch_artifact_builder.sh feature/my-feature
USAGE
}

main() {
    if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
        usage
        exit 0
    fi

    require_command gh
    require_command git

    local branch_name
    branch_name=${1:-}
    
    # If no branch provided, use current branch
    if [[ -z "$branch_name" ]]; then
        branch_name=$(git branch --show-current)
        if [[ -z "$branch_name" ]]; then
            log ERROR "Could not determine current branch (not on a branch?)"
            exit 1
        fi
    fi

    log INFO "Branch: $branch_name"
    
    # Find the open PR for this branch
    log INFO "Finding open PR for branch '$branch_name'..."
    local pr_info
    if ! pr_info=$(gh pr list --state open --head "$branch_name" --limit 1 \
        --json number,baseRefName \
        --jq 'if length > 0 then [.[0].number, .[0].baseRefName] | @tsv else "" end' 2>/dev/null); then
        log ERROR "Failed to query GitHub for PRs"
        exit 1
    fi

    if [[ -z "$pr_info" ]]; then
        log ERROR "No open PR found for branch '$branch_name'"
        exit 1
    fi

    IFS=$'\t' read -r pr_number base_branch <<<"$pr_info"
    log INFO "Found PR #$pr_number with base branch: $base_branch"

    local pr_limit output_dir artifact_file
    pr_limit=${PR_LIMIT:-200}
    output_dir=${OUTPUT_DIR:-output/artifacts}
    mkdir -p "$output_dir"

    local timestamp
    timestamp=$(date -u +"%Y%m%d_%H%M%SZ")
    artifact_file="${output_dir}/branch_${branch_name//\//_}_${timestamp}.log"

    log INFO "Building artifact for branch: $branch_name (vs. $base_branch)"
    log INFO "Output: $artifact_file"

    {
        echo "================================================================================"
        echo "BRANCH ARTIFACT REPORT"
        echo "================================================================================"
        echo "Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
        echo "Current Branch: $branch_name"
        echo "PR Base Branch: $base_branch"
        echo "PR: #$pr_number"
        echo ""

        # Section 1: Commits unique to this branch
        echo "================================================================================"
        echo "UNIQUE COMMITS (on $branch_name, NOT on $base_branch)"
        echo "================================================================================"
        log INFO "Fetching commits on $branch_name that are NOT in $base_branch"
        
        if git log "$base_branch..$branch_name" --pretty=format:"%H%n%an%n%ae%n%ai%n%s%n%b%n---" 2>/dev/null; then
            :
        else
            echo "No commits found or branch comparison failed."
        fi
        echo ""

        # Section 2: Merged PRs with this branch as head
        echo "================================================================================"
        echo "MERGED PRs (HEAD: $branch_name)"
        echo "================================================================================"
        log INFO "Listing merged PRs with head branch: $branch_name (limit: $pr_limit)"
        
        if gh pr list --state merged --head "$branch_name" --limit "$pr_limit" \
            --json number,title,body,commits,mergedAt,baseRefName \
            --template '{{range .}}
PR #{{.number}} - {{.title}}
Merged At: {{.mergedAt}}
Base: {{.baseRefName}}
Description:
{{.body}}
Commits ({{len .commits}}):
{{range .commits}}  - {{.messageHeadline}}
{{end}}
---
{{end}}' 2>/dev/null; then
            :
        else
            echo "No merged PRs found for head branch '$branch_name' or GitHub CLI error."
        fi
        echo ""

        # Footer
        echo "================================================================================"
        echo "END OF REPORT"
        echo "================================================================================"
    } > "$artifact_file"

    log INFO "Artifact written to: $artifact_file"
    log INFO "Artifact size: $(wc -c < "$artifact_file") bytes"
    
    cat "$artifact_file"
}

main "$@"
