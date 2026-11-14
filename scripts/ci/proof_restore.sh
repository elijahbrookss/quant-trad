#!/usr/bin/env bash
set -euo pipefail

printf 'ci_proof_step Prove encrypted restore through actual QT current and frozen readers\n'
./scripts/ci/run_test_suite.sh incremental-recovery tests/test_market_data/test_incremental_application_db.py -s -o cache_dir=/tmp/qt-incremental-pytest-cache -p scripts.ci.proof_pytest


printf 'ci_proof_step Prove packaged retained raw placement through encrypted restore\n'
./scripts/ci/run_test_suite.sh incremental-recovery tests/test_market_data/test_incremental_raw_placement_db.py -s -o cache_dir=/tmp/qt-incremental-raw-pytest-cache -p scripts.ci.proof_pytest


printf 'ci_proof_step Prove exact recovery of the cold layout after acknowledged provider intake\n'
./scripts/ci/run_test_suite.sh incremental-recovery tests/test_market_data/test_canonical_postwrite_recovery_db.py::test_postwrite_pair_restores_exact_canonical_row_and_raw_archive -s -o cache_dir=/tmp/qt-incremental-postwrite-pytest-cache -p scripts.ci.proof_pytest


printf 'ci_proof_step Prove exact pre-switch recovery at a named post-drain WAL point\n'
./scripts/ci/run_test_suite.sh incremental-recovery tests/test_market_data/test_canonical_postwrite_recovery_db.py::test_named_wal_anchor_restores_original_layout_at_fenced_endpoint -s -o cache_dir=/tmp/qt-incremental-named-anchor-pytest-cache -p scripts.ci.proof_pytest
