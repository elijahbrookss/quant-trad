#!/usr/bin/env bash
set -euo pipefail

printf 'ci_proof_step Prove encrypted restore through actual QT current and frozen readers\n'
./scripts/ci/run_test_suite.sh incremental-recovery tests/test_market_data/test_incremental_application_db.py -s -o cache_dir=/tmp/qt-incremental-pytest-cache -p scripts.ci.proof_pytest


printf 'ci_proof_step Prove packaged retained raw placement through encrypted restore\n'
./scripts/ci/run_test_suite.sh incremental-recovery tests/test_market_data/test_incremental_raw_placement_db.py -s -o cache_dir=/tmp/qt-incremental-raw-pytest-cache -p scripts.ci.proof_pytest
