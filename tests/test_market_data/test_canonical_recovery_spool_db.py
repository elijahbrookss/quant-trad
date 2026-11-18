"""Restore a native earlier QT copy and replay a retained provider spool."""

import asyncio
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
from time import monotonic

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from core.storage_targets import StorageTarget
from data_providers.streams.contracts import ProviderRawMessage
from market_data.archive import DurableRawSpoolSegment, FilesystemRawArchiveObjectStore, read_raw_archive_parquet
from market_data.contracts import SourceIdentity
from market_data.structure import RawStreamRecord
from portal.backend.db.session import Database
from portal.backend.service.market.continuous_stream_collector import (
    CoinbaseContinuousTransportAdapter, CoinbaseMarketTradeProjectionAdapter, ContinuousStreamRuntime,
)
from portal.backend.service.storage.recovery_copies import LocalRecoveryCopies
from portal.backend.service.storage.repos import market_data, market_structure
from portal.backend.service.storage.repos.market_lifecycle import PostgresMarketStorageLifecycleRepository
from scripts.automation.verify_canonical_recovery_archive import verify_logical_copy
from tests.test_market_data.migration_test_support import fresh_migration_database
from tests.test_market_data.test_fact_storage_tiers_db import BASE, _placement, storage


pytestmark = pytest.mark.db
DEFINITION = "rollback-spool-fixture"


def _restore_dump(conn, restore_binary, dump, ordered):
    url = conn.engine.url
    env = {"PATH": os.defpath, "LC_ALL": "C", "HOME": "/tmp", "PGHOST": url.host,
           "PGPORT": str(url.port or 5432), "PGDATABASE": url.database, "PGUSER": url.username,
           "PGPASSWORD": url.password or "", "PGCONNECT_TIMEOUT": "5",
           "PGOPTIONS": "-c lock_timeout=5s -c statement_timeout=30s"}
    assert url.host in ("127.0.0.1", "timescaledb") and url.database.startswith("qt_migration_")

    def restore(*args, timeout=30):
        result = subprocess.run([restore_binary, *args, dump], env=env, stdin=subprocess.DEVNULL,
                                text=True, capture_output=True, timeout=timeout)
        assert result.returncode == 0, result.stderr
        return result.stdout

    lines = restore("--list").splitlines()
    registry = [line for line in lines if re.fullmatch(r"\d+; \d+ \d+ TABLE DATA market fact_schemas \S+", line)]
    assert len(registry) == 1
    ordered.write_text("\n".join(registry + [line for line in lines if line not in registry]) + "\n")
    conn.execute(text("SELECT timescaledb_pre_restore()"))
    conn.commit()
    try:
        options = ("--exit-on-error", "--no-owner", "--no-privileges", "--no-tablespaces", "--dbname", url.database)
        restore(*options, "--section=pre-data")
        restore(*options, "--section=data", "--use-list", ordered)
        started = monotonic()
        restore(*options, "--section=post-data", timeout=120)
        print(f"RECOVERY_SPOOL_POST_DATA elapsed_seconds={monotonic() - started:.3f}")
    finally:
        conn.execute(text("SELECT timescaledb_post_restore()"))
        conn.commit()


def _native_recover(repository, root):
    class OfflineTransport:
        def __init__(self, **kwargs):
            pass

        async def connect(self):
            raise AssertionError("recovery must not establish a new provider connection")

        async def close(self):
            pass

    result = asyncio.run(ContinuousStreamRuntime(repository=repository).run(
        definition_id=DEFINITION, owner_id="rollback-native-replay", stop_requested=lambda: True,
        bounded_validation=True, storage_root=root, projection=CoinbaseMarketTradeProjectionAdapter(),
        transport=CoinbaseContinuousTransportAdapter(stream_factory=OfflineTransport),
    ))
    assert result["status"] == "stopped"


def _admitted_rows(database, series):
    with database.session() as session:
        facts = session.execute(text("""
            SELECT id,market_commit_seq,accepted_at,known_at FROM market.fact_versions
            WHERE series_id=:series ORDER BY market_commit_seq
        """), {"series": series}).all()
        archive = session.execute(text("""
            SELECT m.raw_record_id,a.id,a.object_key,a.object_sha256
            FROM market.raw_archive_record_mappings m JOIN market.raw_archive_manifests a ON a.id=m.manifest_id
            WHERE a.definition_id=:definition
        """), {"definition": DEFINITION}).all()
        return facts, archive


def _define_trade_stream(storage, monkeypatch):
    monkeypatch.delenv("MARKET_STRUCTURE_WORKING_ROOT", raising=False)
    monkeypatch.delenv("QT_MARKET_DATA_WORKING_EXPECTED_UUID", raising=False)
    monkeypatch.delenv("QT_MARKET_DATA_EXPECTED_UUID", raising=False)
    monkeypatch.setattr(market_structure, "db", storage.database)
    _placement(monkeypatch, storage.today)
    structures = market_structure.market_structure_repository
    source = SourceIdentity(provider="COINBASE", venue="COINBASE_DIRECT", source_kind="stream",
                            adapter_version="rollback-spool.fixture.v1")
    source_id = storage.repo.register_source(source)
    series = storage.repo.register_series(instrument_id="storage-fixture", fact_type="market.trade",
                                          contract_version="market.trade.v1", timeframe_seconds=None)
    product = "rollback-spool.product.v1"
    structures.register_product_definition(
        definition_version_id=product, source_id=source_id, instrument_id="storage-fixture",
        provider_product_id="BTC-USD", product_type="spot", venue=source.venue, status="fixture",
        base_currency="BTC", quote_currency="USD", provider_size_unit="base", contract_size=None,
        price_increment=None, base_increment=None, effective_at=BASE, received_at=BASE,
        provenance={"fixture": "rollback-spool"},
    )
    structures.upsert_stream_definition(
        definition_id=DEFINITION, source_id=source_id, series_id=series, provider=source.provider,
        venue=source.venue, provider_product_id="BTC-USD", channels=("market_trades", "heartbeats"),
        auth_mode="public", contract_version="market.trade.v1", max_spool_bytes=1024**3,
        max_segment_bytes=128*1024**2, config={"product_definition_version_id": product},
    )
    claim = structures.claim_stream(definition_id=DEFINITION, owner_id="fixture-original", lease_seconds=60,
                                    bounded=True)
    structures.append_session_event(claim, event_ordinal=0, connection_epoch=0, event_type="session_started",
                                    occurred_at=datetime.now(UTC))
    structures.release(claim)
    return structures, claim, series


def _seal_trade(root, claim, *, trade_id):
    timestamp = datetime.now(UTC).isoformat()
    message = ProviderRawMessage.build(
        provider=claim.provider, venue=claim.venue, stream_session_id=claim.session_id, connection_epoch=0,
        receive_ordinal=1, received_at=timestamp, raw_frame=json.dumps({
            "channel": "market_trades", "timestamp": timestamp, "sequence_num": 1,
            "events": [{"type": "update", "trades": [{"product_id": "BTC-USD", "trade_id": trade_id,
                         "price": "100", "size": "0.01", "side": "BUY", "time": timestamp}]}],
        }),
    )
    segment = DurableRawSpoolSegment(root=root / "spool", definition_id=DEFINITION,
                                     session_id=claim.session_id, connection_epoch=0)
    record = RawStreamRecord.from_provider_message(
        message, definition_id=DEFINITION, spool_segment_id=segment.spool_segment_id,
        provider_product_id="BTC-USD", requested_channel="market_trades", observed_channel="market_trades",
    )
    segment.append(record)
    segment.seal()
    return segment, record


def test_logical_copy_restore_replays_retained_acknowledged_provider_spool(storage, tmp_path, monkeypatch):
    if make_url(storage.dsn).host not in ("127.0.0.1", "timescaledb"):
        pytest.skip("requires dedicated isolated PostgreSQL endpoint")
    # Use the installed native clients, not the distribution's PATH wrapper.
    pg_dump, pg_restore = (Path("/usr/lib/postgresql/15/bin") / name
                           for name in ("pg_dump", "pg_restore"))
    assert pg_dump.is_file() and pg_restore.is_file(), "requires native PostgreSQL 15 dump and restore clients"
    target = tmp_path / "copy-filesystem"
    target.mkdir()
    udev = tmp_path / "udev"
    udev.mkdir()
    device = target.stat().st_dev
    (udev / f"b{os.major(device)}:{os.minor(device)}").write_text("E:ID_FS_UUID=recovery-spool-fixture\n")
    monkeypatch.setenv("QT_STORAGE_UDEV_ROOT", str(udev))
    structures, claim, series = _define_trade_stream(storage, monkeypatch)
    source_root = tmp_path / "source"
    store = FilesystemRawArchiveObjectStore(source_root / "objects")
    with PostgresMarketStorageLifecycleRepository.dataset_snapshot_session(database=storage.database) as snapshot:
        identity = snapshot.scalar(text("""
            SELECT c.system_identifier::text||'/'||d.oid::text
            FROM pg_control_system() c CROSS JOIN pg_database d WHERE d.datname=current_database()
        """))
        manager = LocalRecoveryCopies(
            target=StorageTarget("hdd", "Fixture", "recovery-spool-fixture", str(target), "hdd"),
            database_identity=identity, max_bytes=32*1024**2, reserve_bytes=0, timeout_seconds=30,
        )
        receipt = manager.create(snapshot, objects=store, pg_dump=Path(pg_dump), keep_copies=1)
    copied = tmp_path / "copied-logical-generation"
    shutil.copytree(manager.root / receipt["name"], copied)
    check = verify_logical_copy(copied)
    assert check["database_identity"] == identity and check["verified_archive_objects"] == 0

    segment, record = _seal_trade(source_root, claim, trade_id="rollback-trade")
    relative_wal = segment.sealed_path.relative_to(source_root)
    saved_wal = tmp_path / "saved-provider-wal"
    shutil.copy2(segment.sealed_path, saved_wal)
    _native_recover(structures, source_root)
    original_facts, original_archive = _admitted_rows(storage.database, series)
    assert len(original_facts) == len(original_archive) == 1 and not segment.sealed_path.exists()
    assert segment.ack_path.is_file()

    with fresh_migration_database("spool_rollback") as restored_dsn:
        engine = create_engine(restored_dsn)
        try:
            with engine.connect() as conn:
                _restore_dump(conn, pg_restore, copied / "database.dump", tmp_path / "restore-order.list")
        finally:
            engine.dispose()
        restored = Database(restored_dsn)
        try:
            assert restored.ensure_schema(), str(restored.last_error)
            monkeypatch.setattr(market_data, "db", restored)
            monkeypatch.setattr(market_structure, "db", restored)
            assert _admitted_rows(restored, series) == ([], [])
            destination = tmp_path / "restored"
            copied_wal = destination / relative_wal
            copied_wal.parent.mkdir(parents=True)
            shutil.copy2(saved_wal, copied_wal)
            # Even an acknowledgement from the later database is only a projection.
            shutil.copy2(segment.ack_path, copied_wal.parent)
            _native_recover(structures, destination)
            assert not copied_wal.exists()
            new_facts, new_archive = _admitted_rows(restored, series)
            assert new_archive == original_archive
            assert len(new_facts) == 1
            assert new_facts[0].accepted_at > original_facts[0].accepted_at
            assert new_facts[0].known_at > original_facts[0].known_at
            raw_id, _, key, digest = new_archive[0]
            path = destination / "objects" / key
            assert raw_id == record.raw_record_id and hashlib.sha256(path.read_bytes()).hexdigest() == digest
            assert read_raw_archive_parquet(path) == [record]
            # Reintroduce the identical WAL to test idempotency after acknowledgement.
            shutil.copy2(saved_wal, copied_wal)
            _native_recover(structures, destination)
            assert _admitted_rows(restored, series) == (new_facts, new_archive)
            assert not copied_wal.exists() and saved_wal.is_file()
        finally:
            restored._reset_engine()
