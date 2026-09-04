#!/usr/bin/env bash
set -euo pipefail

SUITE="${1:-}"
if [[ -z "$SUITE" ]]; then
  echo "usage: $0 <pr|contracts|runtime-reporting|backend|full|db|core|provider|runtime|botlens|web|cli|reports|docs|integration>" >&2
  exit 2
fi

USE_DOCKER="${CI_USE_DOCKER:-0}"
if [[ "$SUITE" == "db" ]]; then
  USE_DOCKER=1
fi
COMPOSE_FILE="docker/docker-compose.test.yml"
run_pytest_host() {
  local cmd="$1"
  bash -lc "$cmd"
}

run_pytest_docker() (
  local cmd="$1"
  local source_revision
  local source_tree_hash
  local test_token
  local test_project
  local cleanup_status
  local original_status
  local -a compose
  if ! command -v docker >/dev/null 2>&1; then
    echo "ci_runner_prereq_missing: docker CLI is required when CI_USE_DOCKER=1" >&2
    exit 127
  fi
  source_revision="${SOURCE_REVISION:-$(git rev-parse HEAD)}"
  source_tree_hash="${SOURCE_TREE_HASH:-$(python scripts/provenance/source_tree_hash.py --git-revision "$source_revision")}"
  export SOURCE_REVISION="$source_revision"
  export SOURCE_TREE_HASH="$source_tree_hash"
  test_token="$(python -c 'import secrets; print(secrets.token_hex(8))')"
  test_project="qt-test-${test_token}"
  export QT_TEST_POSTGRES_USER="qt_test_${test_token}"
  export QT_TEST_POSTGRES_PASSWORD="$(python -c 'import secrets; print(secrets.token_hex(24))')"
  export QT_TEST_POSTGRES_DB="qt_test_${test_token}"
  compose=(docker compose --project-name "$test_project" -f "$COMPOSE_FILE")

  cleanup_test_stack() {
    original_status=$?
    trap - EXIT
    cleanup_status=0
    "${compose[@]}" down --volumes --remove-orphans --rmi local || cleanup_status=$?
    if [[ "$original_status" -eq 0 && "$cleanup_status" -ne 0 ]]; then
      original_status=$cleanup_status
    fi
    exit "$original_status"
  }
  trap cleanup_test_stack EXIT

  "${compose[@]}" build test
  "${compose[@]}" run --rm \
    -e SOURCE_REVISION="$source_revision" \
    -e SOURCE_TREE_HASH="$source_tree_hash" \
    test bash -lc '
    export PG_DSN="postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB}" &&
    if [ ! -r "/app/scripts/wait-for-db.sh" ]; then
      echo "ci_runner_wait_script_missing_or_unreadable: path=/app/scripts/wait-for-db.sh" >&2
      exit 1
    fi
    bash /app/scripts/wait-for-db.sh bash -lc "$1"
  ' _ "$cmd"
)

run_suite() {
  local cmd="$1"
  if [[ "$USE_DOCKER" == "1" ]]; then
    run_pytest_docker "$cmd"
  else
    run_pytest_host "$cmd"
  fi
}

profile_command() {
  local profile_args
  printf -v profile_args '%q ' "$@"
  echo "for profile in ${profile_args}; do echo \"ci_profile_start profile=\${profile}\"; if [[ \"\${profile}\" == \"docs\" ]]; then python scripts/docs/build_architecture_index.py --check; fi; QT_OMIT_DB_TESTS=1 QT_CI_PROFILE=\"\${profile}\" pytest -q; done"
}

run_profiles() {
  run_suite "$(profile_command "$@")"
}

case "$SUITE" in
  pr)
    run_suite "QT_OMIT_DB_TESTS=1 pytest -q"
    ;;
  contracts)
    run_profiles core provider cli docs
    ;;
  runtime-reporting)
    run_profiles runtime botlens web reports
    ;;
  backend)
    run_suite "QT_OMIT_DB_TESTS=1 pytest -q"
    ;;
  full)
    run_suite "pytest -q"
    ;;
  db)
    # Optional pytest arguments narrow a disposable DB iteration without
    # weakening isolation or relying on a developer's ambient PG_DSN.
    shift
    printf -v db_pytest_args '%q ' "$@"
    if [[ "$#" -eq 0 ]]; then db_pytest_args=""; fi
    run_suite "QT_DB_TEST_ISOLATED=1 RUN_DB_TESTS=1 pytest -q -m db ${db_pytest_args}"
    ;;
  core)
    run_profiles core
    ;;
  provider)
    run_profiles provider
    ;;
  runtime)
    run_profiles runtime
    ;;
  botlens)
    run_profiles botlens
    ;;
  web)
    run_profiles web
    ;;
  cli)
    run_profiles cli
    ;;
  reports)
    run_profiles reports
    ;;
  docs)
    run_profiles docs
    ;;
  integration)
    run_suite "pytest -m 'not db' --ignore=tests/test_reports/test_reports_endpoints.py --cov=src --cov-report=term --cov-report=xml"
    ;;
  *)
    echo "unknown suite: $SUITE" >&2
    exit 2
    ;;
esac
