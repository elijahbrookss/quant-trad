#!/usr/bin/env bash
set -euo pipefail

SUITE="${1:-}"
if [[ -z "$SUITE" ]]; then
  echo "usage: $0 <core|provider|web|integration>" >&2
  exit 2
fi

COMPOSE_FILE="docker/docker-compose.test.yml"

run_in_container() {
  local cmd="$1"
  docker compose -f "$COMPOSE_FILE" run --rm test bash -lc "python -m pip install --upgrade pip && /app/scripts/wait-for-db.sh ${cmd}"
}

case "$SUITE" in
  core)
    run_in_container "pytest -q tests/test_signals/test_signal_contract.py tests/test_signals/test_signal_generator_runtime_contract.py tests/test_portal/test_snapshot_signal_evaluator.py tests/test_smoke/test_import_boundaries.py"
    ;;
  provider)
    run_in_container "pytest -q tests/test_data_providers/test_base_provider.py tests/test_data_providers/test_ccxt_provider.py tests/test_data_providers/test_interactive_brokers.py"
    ;;
  web)
    run_in_container "pytest -q tests/test_smoke/test_import_boundaries.py tests/test_portal/test_strategy_service.py"
    ;;
  integration)
    docker compose -f "$COMPOSE_FILE" run --rm test
    ;;
  *)
    echo "unknown suite: $SUITE" >&2
    exit 2
    ;;
esac
