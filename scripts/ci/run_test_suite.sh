#!/usr/bin/env bash
set -euo pipefail

SUITE="${1:-}"
if [[ -z "$SUITE" ]]; then
  echo "usage: $0 <core|provider|web|integration>" >&2
  exit 2
fi

USE_DOCKER="${CI_USE_DOCKER:-0}"
COMPOSE_FILE="docker/docker-compose.test.yml"

run_pytest_host() {
  local cmd="$1"
  python -m pip install --upgrade pip
  eval "$cmd"
}

run_pytest_docker() {
  local cmd="$1"
  if ! command -v docker >/dev/null 2>&1; then
    echo "ci_runner_prereq_missing: docker CLI is required when CI_USE_DOCKER=1" >&2
    exit 127
  fi
  docker compose -f "$COMPOSE_FILE" build test
  docker compose -f "$COMPOSE_FILE" run --rm test bash -lc "
    python -m pip install --upgrade pip &&
    if [ ! -r '/app/scripts/wait-for-db.sh' ]; then
      echo 'ci_runner_wait_script_missing_or_unreadable: path=/app/scripts/wait-for-db.sh' >&2
      exit 1
    fi
    bash /app/scripts/wait-for-db.sh ${cmd}
  "
}

run_suite() {
  local cmd="$1"
  if [[ "$USE_DOCKER" == "1" ]]; then
    run_pytest_docker "$cmd"
  else
    run_pytest_host "$cmd"
  fi
}

case "$SUITE" in
  core)
    run_suite "pytest -q tests/test_signals/test_signal_contract.py tests/test_signals/test_signal_generator_runtime_contract.py tests/test_portal/test_snapshot_signal_evaluator.py tests/smoke/test_import_boundaries.py"
    ;;
  provider)
    run_suite "pytest -q tests/test_data_providers/test_base_provider.py tests/test_data_providers/test_ccxt_provider.py tests/test_data_providers/test_interactive_brokers.py tests/contract/providers/test_provider_factory_routing.py tests/contract/providers/test_registry_provider_inference.py"
    ;;
  web)
    run_suite "pytest -q tests/smoke/test_import_boundaries.py tests/test_portal/test_strategy_service.py"
    ;;
  integration)
    run_suite "pytest -m 'not db' --ignore=tests/test_reports/test_report_exports.py --ignore=tests/test_reports/test_reports_endpoints.py --cov=src --cov-report=term --cov-report=xml"
    ;;
  *)
    echo "unknown suite: $SUITE" >&2
    exit 2
    ;;
esac
