"""Unmodified collector process maintains disposable storage, stops and resumes."""
from dataclasses import replace
from datetime import timedelta
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from uuid import uuid4

import pytest
from sqlalchemy import select, text

from core.storage_targets import StoragePolicy
from market_data.contracts import DatasetSeriesRequest
from portal.backend.db.market_data_models import MarketCollectorWorkerStateRecord
from portal.backend.db.storage_target_models import StoragePolicyRecord, StorageTargetRecord
from portal.backend.service.storage.header_catalog import read_header_catalog
from portal.backend.service.storage.header_destinations import register_header_tablespaces
from portal.backend.service.storage.header_filesystem import verify_header_filesystem
from portal.backend.service.storage.recovery_copies import _identity
from tests.test_storage_maintenance_runtime import configuration
from tests.test_market_data.test_fact_storage_tiers_db import storage, BASE, _placement
from tests.test_market_data.test_fact_header_copy_placement_db import _configure_placement, _assert_disk

pytestmark = [
    pytest.mark.db,
    pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO") != "1",
                      reason="requires owned two-filesystem runtime image topology"),
]
CONTROL = Path("/usr/lib/postgresql/15/bin/pg_controldata")


def test_real_collector_process_maintains_storage_and_restarts_without_duplicate_copy(storage, tmp_path, monkeypatch):
    storage.open_day = storage.today
    _configure_placement(storage, tmp_path, monkeypatch)
    old_day = storage.today-timedelta(days=45)
    for index, day in enumerate((old_day, storage.today)):
        _placement(monkeypatch, day)
        facts = [replace(storage.fact, observation_key=f"process-{index}-{i}",
                         observation_time=BASE+timedelta(seconds=index*10+i)) for i in range(4)]
        assert storage.repo.ingest_facts(series_id=storage.series_id,
            source_id=storage.source_id, facts=facts).inserted_count == 4
    frozen = storage.repo.freeze_dataset([
        DatasetSeriesRequest(storage.series_id, BASE-timedelta(seconds=1), BASE+timedelta(days=1))])
    before = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id)
    targets = (storage.copy_plan.recent, storage.copy_plan.history)
    policy = StoragePolicy(recent=("ssd",), history=("hdd",), archives=("hdd",), backups=("hdd",),
                           recent_days=30, movement_enabled=True, backup_enabled=True)
    with storage.database.session() as session:
        assert session.scalar(text("SELECT count(*) FROM market.collection_definitions")) == 0
        for target in targets:
            session.add(StorageTargetRecord(id=target.target_id, label=target.label,
                filesystem_uuid=target.filesystem_uuid, root=target.root,
                medium=target.medium, roles=list(target.roles)))
        session.add(StoragePolicyRecord(id=1, revision=1, policy=policy.to_dict()))
    verified = verify_header_filesystem(read_header_catalog(storage.database._engine,
        destination_tablespace_oids=(storage.copy_plan.history_tablespace_oid,)),
        targets, pg_controldata=CONTROL,
        destination_assignments={"hdd": storage.copy_plan.history_tablespace_oid})
    with storage.database.session() as session:
        register_header_tablespaces(session, verified=verified)
        _, namespace = _identity(session)
    archive = Path("/qt-history")/("qt_worker_archive_"+uuid4().hex)
    (archive/"objects").mkdir(parents=True)
    limits_path = tmp_path/"limits.json"
    limits_path.write_text(json.dumps(configuration()))
    env = os.environ.copy()
    env.update(
        PG_DSN=storage.database._engine.url.render_as_string(hide_password=False),
        QT_DISABLE_DOTENV="1", QT_LOGGING_DEBUG="false", QT_LOGGING_LOKI_URL="",
        QT_MARKET_DATA_LIFECYCLE_ENABLED="false", QT_MARKET_DATA_LIFECYCLE_EXECUTION_ENABLED="false",
        QT_MARKET_DATA_LIFECYCLE_INTERVAL_SECONDS="3600",
        QT_STORAGE_MAINTENANCE_LIMITS_PATH=str(limits_path),
        MARKET_STRUCTURE_STORAGE_ROOT=str(archive), QT_MARKET_DATA_EXPECTED_UUID="uuid-copy-hdd",
        QT_WORKERS_COLLECTORS_SHUTDOWN_DRAIN_TIMEOUT_SECONDS="10",
    )
    copies = Path("/qt-history")/"recovery"/namespace
    seen = []

    def run_process(expected_history, expected_backup):
        log = tmp_path/f"worker-{len(seen)}.log"
        with log.open("wb") as output:
            process = subprocess.Popen([sys.executable, "-m", "portal.backend.workers.market_data_collector"],
                                       env=env, stdout=output, stderr=subprocess.STDOUT)
            try:
                worker_id = f"market-data:{socket.gethostname()}:{process.pid}"
                deadline = time.monotonic()+150
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        pytest.fail("collector exited before maintenance completed: "+log.read_text()[-5000:])
                    with storage.database.session() as session:
                        row = session.get(MarketCollectorWorkerStateRecord, worker_id)
                        context = row.context if row is not None else {}
                    phases = context.get("storage_lifecycle", {}).get("maintenance", {})
                    if (phases.get("history_movement", {}).get("state") == expected_history
                            and phases.get("local_recovery", {}).get("state") == expected_backup):
                        seen.append(worker_id)
                        break
                    time.sleep(.2)
                else:
                    pytest.fail("collector maintenance did not finish: "+log.read_text()[-5000:])
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=10)
                        pytest.fail("collector failed bounded graceful shutdown: "+log.read_text()[-5000:])
        assert process.returncode == 0, log.read_text()[-5000:]
        assert "market_data_collector_stopped" in log.read_text()
        with storage.database.session() as session:
            assert session.get(MarketCollectorWorkerStateRecord, seen[-1]).state == "stopped"

    run_process("completed", "completed")
    first_copies = sorted(path.name for path in copies.glob("copy_*"))
    assert len(first_copies) == 1
    assert (copies/first_copies[0]/"database.dump").is_file()
    run_process("idle", "not_due")
    assert seen[0] != seen[1]
    assert sorted(path.name for path in copies.glob("copy_*")) == first_copies
    with storage.database.session() as session:
        _assert_disk(session.connection(), "market.fact_versions_"+old_day.strftime("%Y%m%d"), Path("/qt-history"))
        _assert_disk(session.connection(), "market.fact_versions_"+storage.today.strftime("%Y%m%d"), Path("/qt-source/pgdata"))
        assert all(row.reserved_bytes == row.auxiliary_reserved_bytes == 0
                   for row in session.scalars(select(StorageTargetRecord)))
    assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id) == before
