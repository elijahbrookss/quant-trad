"""Restore the activated cold layout at a real committed provider-write boundary."""

from datetime import timedelta
import hashlib
import os
from pathlib import Path
import secrets
import shutil
from time import monotonic, sleep

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from core.storage_targets import StorageTarget
from market_data.archive import FilesystemRawArchiveObjectStore, read_raw_archive_parquet
from market_data.contracts import DatasetSeriesRequest
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.session import Database
from portal.backend.service.storage.incremental_recovery import (
    EncryptedRecoveryCopies, IncrementalRecoveryConfig, prepare_encrypted_repository,
)
from portal.backend.service.storage.recovery_copies import _identity, _snapshot_layout
from portal.backend.service.storage.repos import market_data, market_lifecycle, market_structure
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from scripts.automation.verify_canonical_recovery_archive import verify_archive
from tests.test_market_data import test_canonical_cold_activate_db as activation_fixture
from tests.test_market_data.test_canonical_recovery_spool_db import (
    _admitted_rows, _define_trade_stream, _native_recover, _seal_trade,
)
from tests.test_market_data.test_fact_storage_tiers_db import storage


pytestmark = [
    pytest.mark.db,
    pytest.mark.skipif(os.getenv("QT_INCREMENTAL_APPLICATION_TEST") != "1",
                       reason="requires owned incremental recovery topology"),
]


def test_postwrite_pair_restores_exact_canonical_row_and_raw_archive(storage, tmp_path, monkeypatch):
    assert os.getuid() == 70 and os.getenv("QT_STORAGE_DEMO") == "1"
    engine = storage.database._engine
    assert engine.url.host == "timescaledb" and engine.url.database.startswith("qt_migration_")
    activation_fixture.test_exact_activation_retires_legacy_and_daily_headers_and_preserves_frozen_reads(
        storage, tmp_path, monkeypatch, owner_path=False,
    )
    monkeypatch.setattr(market_data, "db", storage.database)
    root = tmp_path
    objects = FilesystemRawArchiveObjectStore(root / "objects")
    monkeypatch.setattr(market_structure, "canonical_fact_storage_repository",
                        market_data.canonical_fact_storage_repository)
    structures, claim, series = _define_trade_stream(storage, monkeypatch)
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(root))
    segment, record = _seal_trade(root, claim, trade_id="post-activation-recovery")
    _native_recover(structures, root)
    assert segment.ack_path.is_file() and not segment.sealed_path.exists()
    expected = _admitted_rows(storage.database, series)
    assert len(expected[0]) == len(expected[1]) == 1
    start = record.received_at - timedelta(seconds=10)
    end = record.received_at + timedelta(seconds=10)
    frozen = storage.repo.freeze_dataset([DatasetSeriesRequest(series, start, end)])
    current = storage.repo.read_facts(series_id=series, start=start, end=end)
    expected_frozen = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series)
    assert len(current) == len(expected_frozen) == 1
    with engine.connect() as conn:
        identity = _identity(conn)[0]
        assert _snapshot_layout(conn)["layout_version"] == COLD_HISTORY_LAYOUT_VERSION
        assert conn.scalar(text("SELECT to_regclass('market.fact_versions_legacy') IS NULL"))
        archive_keys = set(conn.execute(text("SELECT object_key FROM market.cold_fact_pages")).scalars())
        assert archive_keys
        older_datasets = conn.execute(text("""
            SELECT dataset_id,series_id FROM market.dataset_series WHERE series_id<>:series
        """), {"series": series}).all()
    assert older_datasets
    older_frozen = {
        (dataset, older_series): storage.repo.read_dataset_fact_revisions(dataset_id=dataset, series_id=older_series)
        for dataset, older_series in older_datasets
    }
    assert all(older_frozen.values())

    hdd = Path("/qt-history")
    udev = tmp_path / "backup-udev"
    udev.mkdir()
    device = hdd.stat().st_dev
    (udev / f"b{os.major(device)}:{os.minor(device)}").write_text("E:ID_FS_UUID=postwrite-backup-fixture\n")
    monkeypatch.setenv("QT_STORAGE_UDEV_ROOT", str(udev))
    secrets_root = Path("/qt-incremental-secrets")
    access = secrets_root / "postwrite-access"
    access.mkdir(mode=0o700)
    for name in ("database-key", "archive-key"):
        key = access / name
        key.touch(mode=0o600, exist_ok=False)
        key.write_text(secrets.token_hex(32))
    config = IncrementalRecoveryConfig(
        Path("/usr/local/bin/pgbackrest"), Path("/usr/local/bin/restic"),
        Path("/qt-source/pgdata"), Path("/var/run/postgresql"),
        access / "database-key", access / "archive-key", max_chain_backups=2,
    )
    limits = dict(
        target=StorageTarget("hdd", "Disposable", "postwrite-backup-fixture", str(hdd), "hdd"),
        database_identity=identity, max_bytes=128*1024**2, reserve_bytes=8*1024**2,
        max_objects=100, timeout_seconds=120,
    )
    archiver = access / "pgbackrest.conf"
    prepare_encrypted_repository(incremental=config, connection_url=engine.url,
                                 archiver_config=archiver, **limits)
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        from psycopg2 import sql
        archive_command = f"pgbackrest --config={archiver} --stanza=qt archive-push %p"
        with conn.connection.driver_connection.cursor() as cursor:
            cursor.execute(sql.SQL("ALTER SYSTEM SET archive_command = {}").format(sql.Literal(archive_command)))
        conn.exec_driver_sql("SELECT pg_reload_conf()")
    worker = EncryptedRecoveryCopies(incremental=config, connection_url=engine.url, **limits)
    with storage.database.locked_snapshot_session(shared_lock_name=market_lifecycle._LIFECYCLE_LOCK_NAME) as snapshot:
        selected = worker.create(snapshot, objects=objects, keep_copies=1)
    assert [point for _, _, point in worker.completed()] == [selected]
    assert selected["storage_layout"]["layout_version"] == COLD_HISTORY_LAYOUT_VERSION
    assert selected["archive_objects"] >= len(archive_keys) + 1
    # This committed write is after the pinned database backup endpoint.
    late = structures.claim_stream(definition_id=claim.definition_id, owner_id="post-point-fixture",
                                   lease_seconds=60, bounded=True)
    structures.append_session_event(late, event_ordinal=0, connection_epoch=0, event_type="session_started",
                                    occurred_at=record.received_at)
    structures.release(late)
    _seal_trade(root, late, trade_id="after-physical-endpoint")
    _native_recover(structures, root)
    assert len(_admitted_rows(storage.database, series)[0]) == 2

    restore_root = Path("/qt-restore")
    assert not list(restore_root.iterdir())
    copied = restore_root / "repository"
    copied.mkdir(mode=0o700)
    shutil.copytree(worker.root / "database", copied / "database")
    shutil.copytree(worker.root / "archives", copied / "archives")
    receipt = copied / "complete.json"
    shutil.copy2(worker.root / selected["name"] / "complete.json", receipt)
    # Hold the original plaintext archive objects out of the runtime reader's path.
    objects.root.rename(root / "original-objects")
    worker._run(worker._br("--repo1-path=" + str(copied / "database"), "--pg1-path=/qt-restore/pgdata",
                "--tablespace-map-all=/qt-restore/history", "--set=" + selected["database_label"],
                "--type=immediate", "--target-action=promote", "--archive-mode=off", "restore"))
    restored_archives = tmp_path / "restored-archives"
    worker._run([str(worker.restic), "--repo", str(copied / "archives"), "--no-cache", "--json",
                 "restore", selected["archive_snapshot"], "--target", str(restored_archives)])
    report = verify_archive(receipt, restored_archives)
    assert report["verified_archive_objects"] == selected["archive_objects"]
    restored_store = FilesystemRawArchiveObjectStore(
        restored_archives / str(objects.root).lstrip("/"), writable=False,
    )
    for key in archive_keys:
        assert restored_store.local_path(key).is_file()
    (restore_root / "start").write_text("committed cold recovery point\n")
    url = engine.url.set(host="restored")
    probe = create_engine(url, connect_args={"connect_timeout": 2})
    deadline = monotonic() + 45
    try:
        while True:
            try:
                with probe.connect() as conn:
                    assert _identity(conn)[0] == identity
                break
            except OperationalError:
                if monotonic() >= deadline:
                    raise
                sleep(0.1)
    finally:
        probe.dispose()

    restored = Database(url.render_as_string(hide_password=False))
    try:
        assert restored.ensure_schema(), str(restored.last_error)
        for module in (market_data, market_structure, market_lifecycle):
            monkeypatch.setattr(module, "db", restored)
        reader = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: restored_store,
                                                         read_cache_factory=lambda: None)
        monkeypatch.setattr(market_data, "canonical_fact_storage_repository", reader)
        monkeypatch.setattr(market_structure, "canonical_fact_storage_repository", reader)
        monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(restored_store.root.parent))
        repository = market_data.PostgresMarketDataRepository()
        assert _admitted_rows(restored, series) == expected
        assert repository.read_facts(series_id=series, start=start, end=end) == current
        assert repository.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series) == expected_frozen
        for (dataset, older_series), records in older_frozen.items():
            assert repository.read_dataset_fact_revisions(dataset_id=dataset, series_id=older_series) == records
        with restored.session() as session:
            assert _snapshot_layout(session) == selected["storage_layout"]
            assert session.scalar(text("SELECT to_regclass('market.fact_versions_legacy') IS NULL"))
        raw_id, _, key, digest = expected[1][0]
        path = restored_store.local_path(key)
        assert raw_id == record.raw_record_id
        assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
        assert read_raw_archive_parquet(path) == [record]
    finally:
        restored._reset_engine()
