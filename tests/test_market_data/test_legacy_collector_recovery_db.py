"""Exact legacy WAL producer and ordinary UID1000 candidate recovery on owned disks.

Opt-in host fixture starts the pinned deployed image when request.json appears.
It supplies only raw WAL bytes; candidate recovery/acknowledgement uses PostgreSQL.
"""
import asyncio
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
from threading import Event
from time import monotonic, sleep

import pytest
from sqlalchemy import text

from data_providers.streams.contracts import ProviderRawMessage
from market_data.contracts import DatasetSeriesRequest, SourceIdentity
from portal.backend.service.market.continuous_stream_collector import (
    CoinbaseContinuousTransportAdapter, CoinbaseMarketTradeProjectionAdapter,
    ContinuousStreamRuntime,
)
from portal.backend.service.storage.repos import market_structure
from tests.test_market_data.test_fact_storage_tiers_db import storage, BASE, _placement

pytestmark = [pytest.mark.db, pytest.mark.skipif(
    os.getenv("QT_LEGACY_WAL_TEST") != "1", reason="requires owned exact-legacy-image producer")]


def test_legacy_private_wal_recovers_and_new_intake_preserves_frozen_reads(storage, monkeypatch):
    assert os.getuid() == 1000 and 70 in os.getgroups()
    working, archive, control = Path("/legacy-working"), Path("/qt-history/archives"), Path("/legacy-control")
    assert working.stat().st_dev != archive.stat().st_dev
    copied_source = os.getenv("QT_LEGACY_SOURCE_COPY") == "1"
    assert (working.stat().st_uid, working.stat().st_gid, working.stat().st_mode & 0o777) == (1000, 1000, 0o700 if copied_source else 0o750)
    monkeypatch.setenv("MARKET_STRUCTURE_WORKING_ROOT", str(working))
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(archive))
    monkeypatch.setattr(market_structure, "db", storage.database)
    _placement(monkeypatch, storage.today)
    structures = market_structure.market_structure_repository
    source = SourceIdentity(provider="COINBASE", venue="COINBASE_DIRECT", source_kind="stream", adapter_version="legacy-recovery.fixture.v1")
    source_id = storage.repo.register_source(source)
    series = storage.repo.register_series(instrument_id="storage-fixture", fact_type="market.trade", contract_version="market.trade.v1", timeframe_seconds=None)
    product = "legacy-recovery.product.v1"
    structures.register_product_definition(definition_version_id=product, source_id=source_id,
        instrument_id="storage-fixture", provider_product_id="BTC-USD", product_type="spot",
        venue=source.venue, status="fixture", base_currency="BTC", quote_currency="USD",
        provider_size_unit="base", contract_size=None, price_increment=None, base_increment=None,
        effective_at=BASE, received_at=BASE, provenance={"fixture": "legacy-recovery"})
    definition = "legacy-recovery"
    structures.upsert_stream_definition(definition_id=definition, source_id=source_id, series_id=series,
        provider=source.provider, venue=source.venue, provider_product_id="BTC-USD",
        channels=("market_trades", "heartbeats"), auth_mode="public", contract_version="market.trade.v1",
        max_spool_bytes=1024**3, max_segment_bytes=128*1024**2,
        config={"product_definition_version_id": product})
    claim = structures.claim_stream(definition_id=definition, owner_id="legacy-fixture", lease_seconds=60, bounded=True)
    structures.append_session_event(claim, event_ordinal=0, connection_epoch=0, event_type="session_started", occurred_at=datetime.now(UTC))
    structures.release(claim)
    storage.repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id, facts=[storage.fact])
    frozen = storage.repo.freeze_dataset([DatasetSeriesRequest(storage.series_id, BASE-timedelta(hours=1), BASE+timedelta(hours=1))])
    frozen_before = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id)
    assert len(frozen_before) == 1
    request = dict(definition_id=definition, session_id=claim.session_id, provider=source.provider, venue=source.venue)
    temporary = control/"request.tmp"
    temporary.write_text(json.dumps(request)); temporary.rename(control/"request.json")
    until = monotonic()+60
    while not (control/"legacy-done.json").exists():
        assert monotonic() < until, "exact legacy producer did not finish within fixture bound"
        sleep(0.1)
    receipt = json.loads((control/"legacy-done.json").read_text())
    assert receipt["source_revision"] == "f673cb62a2c579de40a66d57b2d0c99222923a1b"
    sealed = working/receipt["sealed_relative"]
    assert sealed.stat().st_uid == 1000 and sealed.stat().st_gid == 1000
    assert sealed.stat().st_mode & 0o077 == 0
    assert hashlib.sha256(sealed.read_bytes()).hexdigest() == receipt["sha256"]
    sentinel = working/"retained-private"
    if copied_source:
        assert receipt["original_owner"] == [0, 0]
        assert receipt["preserving_copy"]["source_preserved"] is True
        assert not receipt["preserving_copy"]["runtime_activation_authorized"]
        # The original root-private sentinel stays in the read-only source,
        # which the ordinary app does not mount. The host verifies it separately.
        sentinel.write_bytes(b"candidate-private-sentinel")
        sentinel.chmod(0o600)
    before = (sentinel.read_bytes(), sentinel.stat().st_uid, sentinel.stat().st_gid, sentinel.stat().st_mode)
    stop = Event(); stop.set()
    received = []

    class Stream:
        def __init__(self, **kwargs): self.session_id = kwargs["stream_session_id"]
        async def connect(self): return 0
        async def subscribe(self, subscriptions): assert subscriptions
        async def close(self): pass
        async def raw_messages(self):
            timestamp = datetime.now(UTC).isoformat()
            frame = json.dumps({"channel": "market_trades", "timestamp": timestamp, "sequence_num": 1,
                "events": [{"type": "update", "trades": [{"product_id": "BTC-USD", "trade_id": "fresh-after-legacy",
                "price": "101", "size": "0.01", "side": "BUY", "time": timestamp}]}]})
            received.append(frame)
            yield ProviderRawMessage.build(provider=source.provider, venue=source.venue, stream_session_id=self.session_id,
                connection_epoch=0, receive_ordinal=1, received_at=timestamp, raw_frame=frame)
            stop.set()

    runtime = ContinuousStreamRuntime(repository=structures)
    def collect():
        return asyncio.run(runtime.run(definition_id=definition, owner_id="ordinary-candidate",
            stop_requested=stop.is_set, bounded_validation=True, storage_root=archive,
            projection=CoinbaseMarketTradeProjectionAdapter(), transport=CoinbaseContinuousTransportAdapter(stream_factory=Stream)))
    assert collect()["status"] == "stopped"
    assert not received and not sealed.exists(), "normal acknowledgement must retire recovered WAL before transport"
    assert collect()["status"] == "stopped"  # Normal recovery reentry creates no duplicate facts.
    stop.clear()
    assert collect()["status"] == "stopped" and len(received) == 1
    with storage.database._engine.connect() as conn:
        counts = [conn.scalar(text(sql), {"series": series, "definition": definition}) for sql in (
            "SELECT count(*) FROM market.fact_versions WHERE series_id=:series",
            "SELECT count(*) FROM market.raw_archive_manifests WHERE definition_id=:definition",
            "SELECT count(*) FROM market.raw_archive_record_mappings m JOIN market.raw_archive_manifests a ON a.id=m.manifest_id WHERE a.definition_id=:definition")]
        assert counts == [2, 2, 2]
        manifests = conn.execute(text("SELECT object_key,object_sha256 FROM market.raw_archive_manifests WHERE definition_id=:definition"), {"definition": definition}).all()
    for key, digest in manifests:
        path = archive/"objects"/key
        assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
        assert (path.stat().st_uid, path.stat().st_gid, path.stat().st_mode & 0o777) == (1000, 70, 0o640)
    now = datetime.now(UTC)
    current = storage.repo.read_facts(series_id=series, start=now-timedelta(hours=1), end=now+timedelta(hours=1))
    assert len(current) == 2
    assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id) == frozen_before
    assert (sentinel.read_bytes(), sentinel.stat().st_uid, sentinel.stat().st_gid, sentinel.stat().st_mode) == before
    report = dict(legacy=receipt, facts_manifests_mappings=counts, frozen_preserved=True,
        private_source_preserved=True, normal_wal_retirement=True, fresh_intake=True, current_read_count=len(current),
        host_switch_proven=False, encrypted_pair_proven=False, provider_transport="scripted")
    if os.getenv("QT_LEGACY_RECOVERY_PAIR") == "1":
        report["encrypted_pair"] = _dedicated_pair(storage, control)
        report["encrypted_pair_proven"] = True
        assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id) == frozen_before
    (control/"result.json").write_text(json.dumps(report))
    print("QT_LEGACY_RECOVERY="+json.dumps(report))


def _dedicated_pair(storage, control):
    """Keep this DB alive while the host runs the existing isolated maintenance worker."""
    from core.storage_targets import StoragePolicy, StorageTarget
    from portal.backend.db.storage_target_models import StoragePolicyRecord, StorageTargetRecord
    from portal.backend.db.market_data_models import MarketCollectorWorkerStateRecord
    from sqlalchemy import select
    targets = (StorageTarget("ssd", "Recent", "legacy-pair-ssd", "/qt-source/pgdata", "ssd"),
               StorageTarget("hdd", "History", "legacy-pair-hdd", "/qt-history", "hdd"))
    policy = StoragePolicy(recent=("ssd",), history=("hdd",), archives=("hdd",), backups=("hdd",),
                           backup_enabled=True, movement_enabled=False)
    with storage.database.session() as session:
        for target in targets:
            session.add(StorageTargetRecord(id=target.target_id, label=target.label,
                filesystem_uuid=target.filesystem_uuid, root=target.root, medium=target.medium,
                roles=list(target.roles), state="active"))
        session.add(StoragePolicyRecord(id=1, revision=1, policy=policy.to_dict()))
    # Fixture-only DSN stays in its private control file and is never logged.
    request = control/"pair-request.tmp"
    with request.open("w") as handle:
        os.fchmod(handle.fileno(), 0o600)
        json.dump({"dsn": storage.dsn}, handle)
    request.rename(control/"pair-request.json")
    until = monotonic()+240
    while not (control/"pair-done.json").exists():
        assert monotonic() < until, "dedicated encrypted pair did not finish within fixture bound"
        sleep(0.2)
    receipt = json.loads((control/"pair-done.json").read_text())
    assert receipt["schema_version"] == "qt.encrypted_recovery_pair.v1"
    assert receipt["database_type"] == "full" and receipt["archive_objects"] == 2
    assert receipt["database_label"] and receipt["archive_snapshot"] and receipt["inventory_sha256"]
    with storage.database.session() as session:
        rows = session.scalars(select(MarketCollectorWorkerStateRecord).where(
            MarketCollectorWorkerStateRecord.worker_role == "market_storage_maintenance")).all()
        assert len(rows) == 1 and rows[0].state == "stopped"
        outcome = rows[0].context["storage_lifecycle"]["last_run"]["local_recovery"]
        assert outcome["state"] in {"completed", "not_due"}
        assert outcome["generation"] == receipt["name"]
        assert outcome["policy_hash"] == policy.fingerprint
    return {key: receipt[key] for key in ("name", "database_type", "archive_objects", "inventory_sha256")}
