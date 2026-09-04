from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import text

from market_data.archive import DurableRawSpoolSegment, FilesystemRawArchiveObjectStore, publish_spool_archive
from market_data.canonical_adapters import canonicalize_market_trade
from market_data.contracts import SourceIdentity
from market_data.structure import MarketSide
from portal.backend.service.storage.repos import fact_archival, market_structure
from tests.test_market_data.test_fact_storage_tiers_db import storage, _placement
from tests.test_market_data.test_market_structure_archive import _record
from tests.test_market_data.test_market_state_phase3 import _trade

pytestmark = pytest.mark.db


def test_archive_keeps_each_trade_revisions_exact_raw_delivery(storage, tmp_path, monkeypatch):
    monkeypatch.setattr(market_structure, "db", storage.database)
    structures = market_structure.market_structure_repository
    day = storage.today - timedelta(days=2)
    _placement(monkeypatch, day)
    source = SourceIdentity(provider="COINBASE", venue="COINBASE_DIRECT", source_kind="stream", adapter_version="exact-lineage.fixture.v1")
    source_id = storage.repo.register_source(source, lineage={"fixture": "exact-lineage"})
    series_id = storage.repo.register_series(instrument_id="storage-fixture", fact_type="market.trade",
                                           contract_version="market.trade.v1", timeframe_seconds=None)
    structures.upsert_stream_definition(
        definition_id="exact-lineage", source_id=source_id, series_id=series_id, provider=source.provider,
        venue=source.venue, provider_product_id="BTC-USD", channels=("market_trades",), auth_mode="public",
        contract_version="market.trade.v1", max_spool_bytes=1024**3, max_segment_bytes=128 * 1024**2,
        config={"fixture": "exact-lineage"},
    )
    claim = structures.claim_stream(definition_id="exact-lineage", owner_id="exact-lineage-test", lease_seconds=600, bounded=True)
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    raws, manifests = [], []
    for ordinal in (1, 2):
        segment = DurableRawSpoolSegment(root=tmp_path / "spool", definition_id=claim.definition_id,
                                        session_id=claim.session_id, connection_epoch=0, segment_ordinal=ordinal - 1)
        raw = _record(segment, ordinal)
        segment.append(raw)
        segment.seal()
        encoded, ack, records = publish_spool_archive(segment, object_store=store, temporary_directory=tmp_path / "raw-staging")
        manifests.append(structures.commit_archive(claim, encoded=encoded, acknowledgement=ack, records=records).manifest_id)
        raws.append(raw)
    canonical = []
    for raw in raws:
        trade = replace(_trade("same-trade", offset="0", side=MarketSide.BUY, price="100", receive_ordinal=1),
                        provider_product_id="BTC-USD", provider_event_time=raws[0].received_at - timedelta(seconds=1),
                        provider_message_time=raw.received_at, received_at=raw.received_at,
                        accepted_at=raw.received_at, known_at=raw.received_at, connection_epoch=raw.connection_epoch,
                        receive_ordinal=raw.receive_ordinal, raw_record_id=raw.raw_record_id,
                        coverage_interval_id=None)
        fact = canonicalize_market_trade(trade, source=source)
        canonical.append(fact)
        storage.repo.ingest_facts(series_id=series_id, source_id=source_id, facts=[fact])
    assert canonical[0].material_hash == canonical[1].material_hash
    assert canonical[0].row_hash != canonical[1].row_hash
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=store, temporary_directory=tmp_path / "staging",
        limits=fact_archival.FactArchiveLimits(max_rows=1, row_group_size=1),
    )
    archive.seal_partition(day)
    for expected in manifests:
        page = archive.stage_next_page(day)
        with storage.database.session() as session:
            held = session.execute(text("SELECT target_id FROM market.fact_archive_dependencies WHERE manifest_id=:id"),
                                   {"id": page["manifest_id"]}).scalars().all()
        assert held == [expected], "archival must not replace an older revision's raw evidence with the latest delivery"
        if page["page_ordinal"] == 0:
            with monkeypatch.context() as old_verifier:
                old_verifier.setattr(fact_archival, "FACT_ARCHIVE_VERIFIER_VERSION", "market.canonical_archive_verification.v1")
                assert archive.verify_next_page(day)["page_ordinal"] == 0
            with pytest.raises(RuntimeError, match="verification_missing_or_stale"):
                archive.verify_partition(day)
        assert archive.verify_next_page(day)["status"] == "page_verified"
    assert archive.verify_partition(day)["row_count"] == 2
