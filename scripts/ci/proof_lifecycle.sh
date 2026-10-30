#!/usr/bin/env bash
set -euo pipefail

printf 'ci_proof_step Prove production worker encrypted pairing and dependency retirement\n'
docker run --rm --network none --read-only --user 70:70 --cpus 1 --memory 512m --pids-limit 64 \
  --cap-drop ALL --security-opt no-new-privileges --env QT_DISABLE_DOTENV=1 \
  --entrypoint python qt-terminal-runtime-proof -c \
  "from scripts.automation.storage_online_terminal import worker_main; from scripts.db.fact_header_v2_deadline import CONTROLLER_LOCK; print('production terminal imports available')"
docker run --rm --network none --user postgres --cpus 2 --memory 512m --pids-limit 64 \
  --cap-drop ALL --security-opt no-new-privileges \
  --env QT_DISABLE_DOTENV=1 --env PYTHONPATH=/app/src:/app \
  qt-incremental-runtime-proof \
  python -m pytest -q tests/test_market_data/test_incremental_recovery.py \
  -o cache_dir=/tmp/qt-incremental-output-pytest-cache
docker run --rm --network none --user postgres --cpus 2 --memory 2g --pids-limit 128 \
  --cap-drop ALL --security-opt no-new-privileges \
  --env QT_DISABLE_DOTENV=1 --env PYTHONPATH=/app/src:/app \
  qt-incremental-runtime-proof \
  python /app/scripts/ci/rehearse_incremental_runtime.py \
  --pg-bin /usr/lib/postgresql/15/bin --pgbackrest /usr/local/bin/pgbackrest \
  --restic /usr/local/bin/restic


printf 'ci_proof_step Prove online worker reads legacy archives without changing ownership\n'
python3 scripts/ci/rehearse_online_worker.py --image qt-incremental-runtime-proof

printf 'ci_proof_step Prove confined online worker launch and refusal\n'
python3 -m scripts.ci.rehearse_online_launch --image qt-incremental-runtime-proof

printf 'ci_proof_step Prove retired key inspection handoff and bounded preparation recovery\n'
export QT_DISABLE_DOTENV='1'
mkdir -p "$RUNNER_TEMP/qt-forward-keys"
history_root="$(mktemp -d "$RUNNER_TEMP/qt-forward-keys-history.XXXXXX")"
# Separate disposable filesystem; the tiny fixture must exercise placement.
# This is a ceiling, not a reservation or a production capacity claim.
sudo mount -t tmpfs -o "size=6G,nosuid,nodev,mode=0700,uid=$(id -u),gid=$(id -g)" tmpfs "$history_root"
trap 'sudo umount "$history_root"' EXIT
python3 -m venv "$RUNNER_TEMP/qt-storage-operator-python"
"$RUNNER_TEMP/qt-storage-operator-python/bin/python" -m pip install --no-deps -r requirements.lock
"$RUNNER_TEMP/qt-storage-operator-python/bin/python" -m pip check
"$RUNNER_TEMP/qt-storage-operator-python/bin/python" scripts/ci/rehearse_online_prepared.py \
  --image qt-incremental-runtime-proof \
  --database-image quanttrad-postgres:2.14.2-pg15 \
  --output-root "$RUNNER_TEMP/qt-forward-keys" \
  --history-parent "$history_root" --require-distinct-devices \
  --prepare-source --worker-phases --initial-capture \
  --terminal-cancellation --terminal-image qt-terminal-runtime-proof \
  --forward-package-image qt-terminal-runtime-proof \
  --forward-keys --forward-worker --forward-retirement --forward-operation --forward-retirement-recovery


printf 'ci_proof_step Prove interrupted initial storage preparation and source resumption\n'
export QT_STORAGE_TEST_CLIENT_IMAGE='qt-incremental-runtime-proof'
python3 scripts/ci/test_storage_database_preparation.py --online-source
