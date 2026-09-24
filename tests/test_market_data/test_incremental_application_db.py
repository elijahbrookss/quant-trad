"""Actual QT cold/current/frozen readers after encrypted physical recovery."""
from dataclasses import replace
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
from time import monotonic, sleep

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError

from core.storage_targets import StorageTarget
from market_data.archive import FilesystemRawArchiveObjectStore
from portal.backend.db.session import Database
from portal.backend.service.storage.incremental_recovery import (
    EncryptedRecoveryCopies, IncrementalRecoveryConfig, _PREPARED,
)
from portal.backend.service.storage.recovery_copies import _identity, _json_write, _snapshot_layout
from portal.backend.service.storage.repos import market_data, market_structure, market_lifecycle
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from portal.backend.service.market.market_structure_service import MarketStructureService
from tests.test_market_data.test_fact_storage_tiers_db import storage, BASE, _placement
from tests.test_market_data.test_fact_book_retention_db import (
    test_cold_book_handoff_preserves_frozen_features_checkpoint_and_replay as _cold_book,
)
from tests.test_market_data.test_storage_recovery_restore_db import _preserving_handoff

pytestmark = [
    pytest.mark.db,
    pytest.mark.skipif(os.getenv("QT_INCREMENTAL_APPLICATION_TEST") != "1",
                       reason="requires owned incremental-recovery Compose topology"),
]


def test_encrypted_incremental_restores_qt_cold_current_frozen_and_book_replay(
        storage, tmp_path, monkeypatch):
    assert os.getuid() == 70 and os.getenv("QT_STORAGE_DEMO") == "1"
    source_root = tmp_path/"source-archives"
    source_root.mkdir()
    _cold_book(storage, source_root, monkeypatch, split_sources=False)
    storage.open_day = storage.today+timedelta(days=1)
    _placement(monkeypatch, storage.open_day)
    _preserving_handoff(storage, tmp_path, monkeypatch)
    _placement(monkeypatch, storage.open_day)
    recent = replace(storage.fact, observation_key="incremental-recent",
                     observation_time=BASE+timedelta(days=2))
    assert storage.repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id,
                                    facts=[recent]).inserted_count == 1
    engine = storage.database._engine
    with engine.connect() as conn:
        identity = _identity(conn)[0]
        assert conn.scalar(text("SELECT count(*) FROM qt_fact_header_retained_v1.fact_versions")) > 0
    hdd, secrets_root = Path("/qt-history"), Path("/qt-incremental-secrets")
    device, udev = hdd.stat().st_dev, tmp_path/"udev"
    udev.mkdir()
    (udev/f"b{os.major(device)}:{os.minor(device)}").write_text("E:ID_FS_UUID=uuid-incremental-application\n")
    monkeypatch.setenv("QT_STORAGE_UDEV_ROOT", str(udev))
    target = StorageTarget("hdd", "Disposable", "uuid-incremental-application", str(hdd), "hdd")
    keys = [secrets.token_hex(32), secrets.token_hex(32)]
    for name, key in zip(("database-key", "archive-key"), keys):
        (secrets_root/name).write_text(key)
        (secrets_root/name).chmod(0o600)
    root = hdd/"recovery-incremental"/hashlib.sha256(identity.encode()).hexdigest()[:32]
    root.mkdir(mode=0o700, parents=True)
    root.parent.chmod(0o700)
    for name in ("database", "archives", "locks", "logs"):
        (root/name).mkdir(mode=0o700)
    _json_write(root/"prepared.json", {
        "schema_version":_PREPARED, "database_identity":identity,
        "filesystem_uuid":target.filesystem_uuid,
        "database_key_sha256":hashlib.sha256(keys[0].encode()).hexdigest(),
        "archive_key_sha256":hashlib.sha256(keys[1].encode()).hexdigest(),
    })
    config = IncrementalRecoveryConfig(
        Path("/usr/local/bin/pgbackrest"), Path("/usr/local/bin/restic"),
        Path("/qt-source/pgdata"), Path("/var/run/postgresql"),
        secrets_root/"database-key", secrets_root/"archive-key", max_chain_backups=4)
    url = make_url(storage.dsn)
    def manager():
        return EncryptedRecoveryCopies(incremental=config, connection_url=url, target=target,
            database_identity=identity, max_bytes=128*1024**2, reserve_bytes=8*1024**2,
            timeout_seconds=180, max_objects=1000)
    archiver = secrets_root/"pgbackrest.conf"
    archiver.write_text(
        f"[global]\nrepo1-path={root/'database'}\nrepo1-cipher-type=aes-256-cbc\n"
        f"repo1-cipher-pass={keys[0]}\nlock-path={root/'locks'}\n"
        f"log-path={root/'logs'}\nlog-level-file=off\ncompress-type=zst\n"
        f"[qt]\npg1-path=/qt-source/pgdata\npg1-socket-path=/var/run/postgresql\n"
        f"pg1-user={url.username}\npg1-database={url.database}\n")
    archiver.chmod(0o600)
    initial = manager()
    initial._run(initial._br("stanza-create"))
    initial._run(initial._rs("init"))
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        from psycopg2 import sql
        command = "pgbackrest --config=/qt-incremental-secrets/pgbackrest.conf --stanza=qt archive-push %p"
        with conn.connection.driver_connection.cursor() as cursor:
            cursor.execute(sql.SQL("ALTER SYSTEM SET archive_command = {}").format(sql.Literal(command)))
        conn.exec_driver_sql("SELECT pg_reload_conf()")
    objects = FilesystemRawArchiveObjectStore(source_root/"objects", writable=False)
    with storage.database.locked_snapshot_session(shared_lock_name=market_lifecycle._LIFECYCLE_LOCK_NAME) as snapshot:
        baseline = manager().create(snapshot, objects=objects, keep_copies=2)
    correction = replace(recent, payload={**recent.payload,"rate":"0.3","raw_rate":"0.3"},
                         accepted_at=recent.accepted_at+timedelta(seconds=1),
                         known_at=recent.known_at+timedelta(seconds=1))
    assert storage.repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id,
                                    facts=[correction]).corrected_count == 1
    with engine.connect() as conn:
        ranges = conn.execute(text("SELECT series_id,min(observation_time),max(observation_time) "
                                   "FROM market.fact_versions GROUP BY series_id")).all()
        pairs = conn.execute(text("SELECT dataset_id,series_id FROM market.dataset_series")).all()
        sessions = conn.execute(text("""
            SELECT DISTINCT m.definition_id,m.session_id FROM market.raw_archive_manifests m
            JOIN market.stream_definitions d ON d.id=m.definition_id
            WHERE d.channels @> '["level2"]'::jsonb
        """)).all()
        assert pairs and sessions
    current = {(series,start,end):storage.repo.read_facts(series_id=series,
               start=start-timedelta(seconds=1),end=end+timedelta(seconds=1))
               for series,start,end in ranges}
    frozen = {tuple(pair):storage.repo.read_dataset_fact_revisions(dataset_id=pair[0],series_id=pair[1])
              for pair in pairs}
    replay_service = MarketStructureService(repository=market_structure.PostgresMarketStructureRepository())
    replay = {tuple(pair):replay_service.replay_book_session(definition_id=pair[0],session_id=pair[1],
                                                           storage_root=source_root) for pair in sessions}
    with storage.database.locked_snapshot_session(shared_lock_name=market_lifecycle._LIFECYCLE_LOCK_NAME) as snapshot:
        selected = manager().create(snapshot, objects=objects, keep_copies=2)
    assert baseline["database_type"] == "full" and selected["database_type"] == "incr"
    assert selected["archive_objects"] > 0
    after = replace(recent, observation_key="incremental-after-selected")
    assert storage.repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id,
                                    facts=[after]).inserted_count == 1
    restored_root = Path("/qt-restore")
    assert not list(restored_root.iterdir()), "restore destination must be empty and owned"
    worker = manager()
    worker._run(worker._br("--pg1-path=/qt-restore/pgdata", "--tablespace-map-all=/qt-restore/history",
                "--set="+selected["database_label"], "--type=immediate",
                "--target-action=promote", "--archive-mode=off", "restore"))
    destination = tmp_path/"restored-archives"
    worker._run(worker._rs("restore", selected["archive_snapshot"], "--target", str(destination)))
    inventory = destination/selected["inventory_snapshot_path"].lstrip("/")
    assert hashlib.sha256(inventory.read_bytes()).hexdigest() == selected["inventory_sha256"]
    recovered = destination/str(objects.root).lstrip("/")
    for line in inventory.read_text().splitlines():
        item = json.loads(line)
        assert hashlib.sha256((recovered/item["object_key"]).read_bytes()).hexdigest() == item["sha256"]
    source_root.rename(tmp_path/"retained-original-archives")
    source_root.mkdir()
    shutil.copytree(recovered, source_root/"objects")
    (restored_root/"start").write_text("selected complete recovery point\n")
    restored_url = url.set(host="restored")
    probe = create_engine(restored_url, connect_args={"connect_timeout":2})
    deadline = monotonic()+45
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
    restored = Database(restored_url.render_as_string(hide_password=False))
    try:
        assert restored.ensure_schema(), str(restored.last_error)
        for module in (market_data,market_structure,market_lifecycle):
            monkeypatch.setattr(module, "db", restored)
        reader = FilesystemRawArchiveObjectStore(source_root/"objects", writable=False)
        tiered = PostgresCanonicalFactStorageRepository(object_store_factory=lambda:reader)
        monkeypatch.setattr(market_data, "canonical_fact_storage_repository", tiered)
        monkeypatch.setattr(market_structure, "canonical_fact_storage_repository", tiered)
        repository = market_data.PostgresMarketDataRepository()
        with restored.session() as session:
            assert _snapshot_layout(session) == selected["storage_layout"]
            assert not session.scalar(text("SELECT EXISTS(SELECT 1 FROM market.fact_versions "
                                           "WHERE observation_key='incremental-after-selected')"))
            assert session.scalar(text("SELECT count(*) FROM qt_fact_header_retained_v1.fact_versions")) > 0
        for (series,start,end), expected in current.items():
            assert repository.read_facts(series_id=series,start=start-timedelta(seconds=1),
                                        end=end+timedelta(seconds=1)) == expected
        for (dataset,series), expected in frozen.items():
            assert repository.read_dataset_fact_revisions(dataset_id=dataset,series_id=series) == expected
        service = MarketStructureService(repository=market_structure.PostgresMarketStructureRepository())
        for (definition,session_id), expected in replay.items():
            assert service.replay_book_session(definition_id=definition,session_id=session_id,
                                               storage_root=source_root) == expected
        print(json.dumps({"qt_current_corrections_frozen_and_book_replay_restored":True,
                          "preserved_v1_and_v2_data_restored":True,
                          "later_source_writes_excluded":True,"production_touched":False}))
    finally:
        restored._reset_engine()
