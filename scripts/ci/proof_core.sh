#!/usr/bin/env bash
set -euo pipefail

printf 'ci_proof_step Recreate actual QT core on disposable storage without provider egress\n'
python3 scripts/ci/test_server_core_recreation.py

printf 'ci_proof_step Prove fixed SSD/HDD runtime and held preserving cutover\n'
python3 scripts/ci/test_server_core_recreation.py --storage-layout
docker pull python:3.12.3-slim
revision="$(git rev-parse HEAD)"
python3 scripts/ci/test_storage_database_preparation.py \
  --runtime-images qt-incremental-runtime-proof \
  "quanttrad-frontend:$revision" "quanttrad-frontend-v2:$revision"
