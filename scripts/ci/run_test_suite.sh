#!/usr/bin/env bash
set -euo pipefail

SUITE="${1:-}"
if [[ -z "$SUITE" ]]; then
  echo "usage: $0 <contracts|runtime-reporting|backend|core|provider|runtime|botlens|web|cli|reports|docs|integration>" >&2
  exit 2
fi

USE_DOCKER="${CI_USE_DOCKER:-0}"
COMPOSE_FILE="docker/docker-compose.test.yml"

run_pytest_host() {
  local cmd="$1"
  python -m pip install --upgrade pip
  bash -lc "$cmd"
}

run_pytest_docker() {
  local cmd="$1"
  if ! command -v docker >/dev/null 2>&1; then
    echo "ci_runner_prereq_missing: docker CLI is required when CI_USE_DOCKER=1" >&2
    exit 127
  fi
  docker compose -f "$COMPOSE_FILE" build test
  docker compose -f "$COMPOSE_FILE" run --rm test bash -lc '
    python -m pip install --upgrade pip &&
    if [ ! -r "/app/scripts/wait-for-db.sh" ]; then
      echo "ci_runner_wait_script_missing_or_unreadable: path=/app/scripts/wait-for-db.sh" >&2
      exit 1
    fi
    bash /app/scripts/wait-for-db.sh bash -lc "$1"
  ' _ "$cmd"
}

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
  echo "for profile in ${profile_args}; do echo \"ci_profile_start profile=\${profile}\"; if [[ \"\${profile}\" == \"docs\" ]]; then python scripts/docs/build_architecture_index.py; fi; QT_CI_PROFILE=\"\${profile}\" pytest -q; done"
}

run_profiles() {
  run_suite "$(profile_command "$@")"
}

case "$SUITE" in
  contracts)
    run_profiles core provider cli docs
    ;;
  runtime-reporting)
    run_profiles runtime botlens web reports
    ;;
  backend)
    run_profiles core provider runtime botlens web cli reports docs
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
    run_suite "pytest -m 'not db' --ignore=tests/test_reports/test_report_exports.py --ignore=tests/test_reports/test_reports_endpoints.py --cov=src --cov-report=term --cov-report=xml"
    ;;
  *)
    echo "unknown suite: $SUITE" >&2
    exit 2
    ;;
esac
