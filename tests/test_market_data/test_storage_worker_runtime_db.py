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
from core.market_storage_lifecycle import CanonicalFactRetentionPolicy
from market_data.archive import FilesystemRawArchiveObjectStore
from portal.backend.service.market.canonical_retention import CanonicalFactRetentionExecutor
from portal.backend.service.storage.repos.fact_retention import PostgresCanonicalFactRetentionRepository
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from portal.backend.service.storage.repos import market_data as repository_module
from market_data.contracts import DatasetSeriesRequest
from portal.backend.db.market_data_models import MarketCollectorWorkerStateRecord
from portal.backend.db.storage_target_models import StoragePolicyRecord, StorageTargetRecord
from portal.backend.service.storage.header_catalog import read_header_catalog
from portal.backend.service.storage.header_destinations import register_header_tablespaces
from portal.backend.service.storage.header_filesystem import verify_header_filesystem
from portal.backend.service.storage.recovery_copies import _identity
from tests.test_storage_maintenance_runtime import configuration
from tests.test_market_data.storage_server_process_fixture import server_peers
from tests.test_market_data.test_fact_storage_tiers_db import storage, BASE, _placement
from tests.test_market_data.test_fact_header_copy_placement_db import _configure_placement, _assert_disk

pytestmark = [
    pytest.mark.db,
    pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO") != "1",
                      reason="requires owned two-filesystem runtime image topology"),
]
CONTROL = Path("/usr/lib/postgresql/15/bin/pg_controldata")


def test_real_collector_process_maintains_storage_and_restarts_without_duplicate_copy(storage, tmp_path, monkeypatch, server_peers):
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
                           recent_days=60, movement_enabled=True, backup_enabled=True)
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
    working = Path("/qt-source/pgdata")/("qt_worker_live_"+uuid4().hex)
    working.mkdir()
    limits_path = tmp_path/"limits.json"
    limits_path.write_text(json.dumps(configuration()))
    env = os.environ.copy()
    env.update(
        PG_DSN=storage.database._engine.url.render_as_string(hide_password=False),
        QT_CONFIG_PROFILE="prod",
        QT_DISABLE_DOTENV="1", QT_LOGGING_DEBUG="false", QT_LOGGING_LOKI_URL="",
        QT_MARKET_DATA_LIFECYCLE_ENABLED="true", QT_MARKET_DATA_LIFECYCLE_EXECUTION_ENABLED="true",
        QT_MARKET_DATA_LIFECYCLE_ARCHIVE_COMPACTION_ENABLED="false",
        QT_MARKET_DATA_LIFECYCLE_ARCHIVE_EXPIRATION_ENABLED="false",
        QT_MARKET_DATA_LIFECYCLE_CANONICAL_EXECUTION_ENABLED="true",
        QT_MARKET_DATA_LIFECYCLE_CANONICAL_HOT_DAYS="90",
        QT_MARKET_DATA_LIFECYCLE_CANONICAL_HOT_DAYS_BY_FACT_TYPE=json.dumps({"derivatives.funding_rate": 120}),
        QT_MARKET_DATA_LIFECYCLE_CANONICAL_ARCHIVE_MIN_FREE_BYTES="0",
        QT_MARKET_DATA_LIFECYCLE_CANONICAL_MAX_PAGE_LOGICAL_BYTES=str(1024**2),
        QT_MARKET_DATA_LIFECYCLE_CANONICAL_MAX_STEPS_PER_RUN="16",
        QT_MARKET_DATA_LIFECYCLE_CANONICAL_MAX_RUN_SECONDS="120",
        QT_MARKET_DATA_LIFECYCLE_INTERVAL_SECONDS="3600",
        QT_STORAGE_MAINTENANCE_LIMITS_PATH=str(limits_path),
        MARKET_STRUCTURE_STORAGE_ROOT=str(archive), QT_MARKET_DATA_EXPECTED_UUID="uuid-copy-hdd",
        MARKET_STRUCTURE_WORKING_ROOT=str(working), QT_MARKET_DATA_WORKING_EXPECTED_UUID="uuid-copy-ssd",
        QT_WORKERS_COLLECTORS_SHUTDOWN_DRAIN_TIMEOUT_SECONDS="10",
    )
    read_status, peer_frozen_rows, save_policy = server_peers(env)
    frozen_identity = [[r.fact_version_id, r.row_hash, r.market_commit_seq,
                        r.revision, r.fact.known_at.isoformat()] for r in before]
    assert peer_frozen_rows(frozen.dataset_id, storage.series_id) == frozen_identity
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

    def change_policy(**changes):
        nonlocal policy
        policy = replace(policy, **changes)
        save_policy(policy.to_dict())

    # A path outside the assigned HDD cannot become the archival destination.
    from portal.backend.service.storage.history_policy import saved_canonical_policy
    with pytest.raises(ValueError, match="history_archive_root_outside_saved_target"):
        saved_canonical_policy(storage.database,
            policy=CanonicalFactRetentionPolicy(execution_enabled=True),
            storage_root=tmp_path)

    # The saved long window protects history; legacy windows are not authority.
    run_process("idle", "completed")
    first_copies = sorted(path.name for path in copies.glob("copy_*"))
    assert len(first_copies) == 1
    assert (copies/first_copies[0]/"database.dump").is_file()

    # A pause after planning must stop even the first sealing transaction.
    change_policy(recent_days=7)
    executor = CanonicalFactRetentionExecutor(
        repository=PostgresCanonicalFactRetentionRepository(database=storage.database),
        use_saved_history_policy=True)
    step = executor._execute_step
    def pause_after_plan(**kwargs):
        change_policy(movement_enabled=False)
        return step(**kwargs)
    with monkeypatch.context() as fault:
        fault.setattr(executor, "_execute_step", pause_after_plan)
        stopped = executor.run(policy=CanonicalFactRetentionPolicy(execution_enabled=True,
            hot_days=90, hot_days_by_fact_type={"derivatives.funding_rate": 120},
            archive_min_free_bytes=0, max_page_logical_bytes=1024**2, max_steps_per_run=1),
            storage_root=archive, execute=True)
    assert stopped["failure_count"] == 1
    assert "history_policy_changed" in stopped["outcomes"][0]["error"]
    with storage.database.session() as session:
        assert session.scalar(text("SELECT state FROM market.fact_retention_partitions WHERE storage_day=:day"),
                              {"day": old_day}) == "open"
        assert session.scalar(text("SELECT count(*) FROM market.fact_hot_payloads WHERE storage_day=:day"),
                              {"day": old_day}) == 4

    run_process("disabled", "not_due")
    with storage.database.session() as session:
        assert session.scalar(text("SELECT count(*) FROM market.fact_hot_payloads WHERE storage_day=:day"),
                              {"day": old_day}) == 4
        _assert_disk(session.connection(), "market.fact_versions_"+old_day.strftime("%Y%m%d"),
                     Path("/qt-source/pgdata"))

    change_policy(movement_enabled=True)
    run_process("completed", "not_due")
    run_process("idle", "not_due")
    assert len(set(seen)) == len(seen)
    assert sorted(path.name for path in copies.glob("copy_*")) == first_copies
    with storage.database.session() as session:
        _assert_disk(session.connection(), "market.fact_versions_"+old_day.strftime("%Y%m%d"), Path("/qt-history"))
        _assert_disk(session.connection(), "market.fact_versions_"+storage.today.strftime("%Y%m%d"), Path("/qt-source/pgdata"))
        assert session.scalar(text("SELECT state FROM market.fact_retention_partitions WHERE storage_day=:day"),
                              {"day": old_day}) == "reclaimed"
        assert session.scalar(text("SELECT count(*) FROM market.fact_hot_payloads WHERE storage_day=:day"),
                              {"day": old_day}) == 0
        assert session.scalar(text("SELECT count(*) FROM market.fact_hot_payloads WHERE storage_day=:day"),
                              {"day": storage.today}) == 4
        assert all(row.reserved_bytes == row.auxiliary_reserved_bytes == 0
                   for row in session.scalars(select(StorageTargetRecord)))
    reader = FilesystemRawArchiveObjectStore(archive/"objects", writable=False)
    monkeypatch.setattr(repository_module, "canonical_fact_storage_repository",
                        PostgresCanonicalFactStorageRepository(object_store_factory=lambda: reader))
    assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id) == before
    assert peer_frozen_rows(frozen.dataset_id, storage.series_id) == frozen_identity
    status = read_status()
    assert all(row["status"] == "available" for row in status["targets"])
    assert status["policy"]["recent_days"] == 7
    with storage.database.session() as session:
        assert session.scalar(text("SELECT count(*) FROM market.collection_definitions")) == 0
    print("QT_SHARED_OWNER_REPORT="+json.dumps({
        "configuration_profile": "prod",
        "real_backend_supervisor_and_worker_pools_started": True,
        "initializer_installed_local_instruments_without_provider_definitions": True,
        "storage_api_reads_both_drive_identities": True,
        "fresh_process_frozen_reads_before_and_after_movement_agree": True,
        "collector_history_recovery_and_restart_with_api_running": True,
        "reviewed_http_settings_pause_and_resume_control_worker": True,
        "limitation": "shared UID/process namespace fixture, not complete server Compose, Docker socket access or retained ownership migration",
    }, sort_keys=True))
