"""Real producer deduplication and conservative flow-evidence preservation."""
from dataclasses import replace
from datetime import timedelta
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from data_providers.streams.coinbase import CoinbaseMessageParser
from data_providers.streams.contracts import ProviderRawMessage
from market_data.archive import DurableRawSpoolSegment, FilesystemRawArchiveObjectStore, publish_spool_archive
from market_data.contracts import DatasetSeriesRequest, SourceIdentity
from market_data.canonical_adapters import canonicalize_market_trade, canonicalize_trade_flow
from market_data.structure import (
    ArchiveStatus, ProductContract, RawStreamRecord, TradeCoverageIntervalVersion,
    aggregate_trade_bucket, translate_coinbase_market_trade,
)
from portal.backend.service.storage.repos import fact_archival, market_data, market_structure
from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from portal.backend.service.market.backtest_dataset_service import validate_frozen_dataset_series
from tests.test_market_data.test_fact_derived_retention_db import _archive_day
from tests.test_market_data.test_fact_reclamation_db import _physical
from tests.test_market_data.test_fact_storage_tiers_db import BASE, _placement, storage

pytestmark = pytest.mark.db


def _flow_fixture(storage, tmp_path, monkeypatch, *, include_partial=True):
    monkeypatch.setattr(market_structure, "db", storage.database)
    structures = market_structure.market_structure_repository
    start = BASE.replace(microsecond=0)
    source_day, flow_day = storage.today - timedelta(days=3), storage.today - timedelta(days=2)
    _placement(monkeypatch, source_day)
    source = SourceIdentity(provider="COINBASE", venue="COINBASE_DIRECT", source_kind="stream", adapter_version="flow-retention.fixture.v1")
    source_id = storage.repo.register_source(source)
    trade_series = storage.repo.register_series(instrument_id="storage-fixture", fact_type="market.trade",
        contract_version="market.trade.v1", timeframe_seconds=None)
    flow_series = storage.repo.register_series(instrument_id="storage-fixture", fact_type="market.trade_flow",
        contract_version="market.trade_flow.v1", timeframe_seconds=1)
    contract = ProductContract(provider_product_id="BTC-USD", provider_size_unit="base", base_currency="BTC",
        quote_currency="USD", product_definition_version_id="flow-retention.product.v1")
    structures.register_product_definition(definition_version_id=contract.product_definition_version_id,
        source_id=source_id, instrument_id="storage-fixture", provider_product_id="BTC-USD", product_type="spot",
        venue=source.venue, status="fixture", base_currency="BTC", quote_currency="USD", provider_size_unit="base",
        contract_size=None, price_increment=None, base_increment=None, effective_at=start - timedelta(seconds=1),
        received_at=start - timedelta(seconds=1), provenance={"fixture": "flow-retention"})
    structures.upsert_stream_definition(definition_id="flow-retention", source_id=source_id, series_id=trade_series,
        provider=source.provider, venue=source.venue, provider_product_id="BTC-USD", channels=("market_trades",),
        auth_mode="public", contract_version="market.trade.v1", max_spool_bytes=1024**3, max_segment_bytes=128 * 1024**2,
        config={"product_definition_version_id": contract.product_definition_version_id,
                "aggregate_series_ids": {"1": flow_series}})
    claim = structures.claim_stream(definition_id="flow-retention", owner_id="flow-retention-test", lease_seconds=600, bounded=True)
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    parser = CoinbaseMessageParser(symbol_by_product_id={"BTC-USD": "BTC-USD"})
    raws, manifests, trades = [], [], []
    for ordinal, seconds in ((1, 0), (2, 1.2), (3, 1.4), (4, 4)):
        received = start + timedelta(seconds=seconds)
        timestamp = received.isoformat()
        payload = ({"channel": "heartbeats", "timestamp": timestamp, "sequence_num": ordinal,
            "events": [{"current_time": timestamp, "heartbeat_counter": str(ordinal)}]} if ordinal in (1, 4) else
            {"channel": "market_trades", "timestamp": timestamp, "sequence_num": ordinal - 2,
             "events": [{"type": "snapshot" if ordinal == 2 else "update", "trades": [{
                 "trade_id": "same-provider-trade", "product_id": "BTC-USD", "price": "100", "size": "2",
                 "side": "BUY", "time": (start + timedelta(seconds=1.1)).isoformat()}]}]})
        segment = DurableRawSpoolSegment(root=tmp_path / "spool", definition_id=claim.definition_id,
            session_id=claim.session_id, connection_epoch=0, segment_ordinal=ordinal - 1)
        message = ProviderRawMessage.build(provider=source.provider, venue=source.venue, stream_session_id=claim.session_id,
            connection_epoch=0, receive_ordinal=ordinal, received_at=timestamp, raw_frame=json.dumps(payload))
        raw = RawStreamRecord.from_provider_message(message, definition_id=claim.definition_id,
            spool_segment_id=segment.spool_segment_id, provider_product_id="BTC-USD", requested_channel="market_trades",
            observed_channel=payload["channel"])
        segment.append(raw)
        segment.seal()
        encoded, ack, records = publish_spool_archive(segment, object_store=store, temporary_directory=tmp_path / "raw-staging")
        manifests.append(structures.commit_archive(claim, encoded=encoded, acknowledgement=ack, records=records).manifest_id)
        raws.append(raw)
        for event in parser.parse_raw(raw.raw_frame, received_at=timestamp, raw_ref={"raw_record_id": raw.raw_record_id}):
            if event.event_kind == "market_trade":
                trades.append(translate_coinbase_market_trade(event, contract=contract, raw_record_id=raw.raw_record_id,
                    connection_epoch=0, receive_ordinal=ordinal, accepted_at=start + timedelta(seconds=5, milliseconds=ordinal),
                    coverage_interval_id=None if ordinal == 2 else "flow-retention.coverage"))
    assert len(trades) == 2 and trades[0].material_hash == trades[1].material_hash
    assert structures.ingest_trades(claim, facts=[trades[0]]).inserted_count == 1
    assert structures.ingest_trades(claim, facts=[trades[1]]).noop_count == 1
    request = dict(series_id=trade_series, start=start, end=start + timedelta(seconds=5))
    canonical = storage.repo.read_fact_revisions(**request)
    assert len(canonical) == 1 and canonical[0].fact.provenance["_qt_trade_evidence"]["raw_record_id"] == raws[1].raw_record_id
    opening = structures.append_session_event(claim, event_ordinal=0, connection_epoch=0,
        event_type="trade_coverage_opened", occurred_at=start)
    closing = structures.append_session_event(claim, event_ordinal=1, connection_epoch=0,
        event_type="bounded_stop", occurred_at=start + timedelta(seconds=4))
    coverage = TradeCoverageIntervalVersion(interval_id="flow-retention.coverage", revision=1,
        definition_id=claim.definition_id, session_id=claim.session_id, connection_epoch=0, provider_product_id="BTC-USD",
        channel="market_trades", status="closed_valid", ordering_assurance="provider_sequence_contiguous", archive_status="pending",
        opening_raw_record_id=raws[0].raw_record_id, opening_receive_ordinal=1, opening_effective_at=start,
        last_raw_record_id=raws[-1].raw_record_id, last_receive_ordinal=4, last_effective_at=start + timedelta(seconds=4),
        closing_raw_record_id=raws[-1].raw_record_id, closing_receive_ordinal=4, closing_effective_at=start + timedelta(seconds=4),
        canonicalization_watermark_ordinal=4, archive_complete_through_ordinal=4,
        known_at=start + timedelta(seconds=4, milliseconds=1))
    structures.append_coverage_version(claim, coverage=coverage, opening_session_event_id=opening, closing_session_event_id=closing)
    _placement(monkeypatch, flow_day)
    partial = aggregate_trade_bucket([trades[1]], interval_seconds=1, bucket_start=start + timedelta(seconds=1),
        coverage=coverage, computed_at=start + timedelta(seconds=6))
    if include_partial:
        assert structures.ingest_aggregates(series_id=flow_series, facts=[partial]).inserted_count == 1
    coverage = replace(coverage, revision=2, archive_status=ArchiveStatus.COMPLETE,
        known_at=start + timedelta(seconds=4, milliseconds=2))
    structures.append_coverage_version(claim, coverage=coverage, opening_session_event_id=opening, closing_session_event_id=closing)
    complete = aggregate_trade_bucket([trades[1]], interval_seconds=1, bucket_start=partial.bucket_start,
        coverage=coverage, computed_at=start + timedelta(seconds=7))
    zero = aggregate_trade_bucket([], interval_seconds=1, bucket_start=start + timedelta(seconds=2),
        coverage=coverage, computed_at=start + timedelta(seconds=7))
    assert structures.ingest_aggregates(series_id=flow_series, facts=[complete, zero]).inserted_count == 2
    return SimpleNamespace(start=start, source_day=source_day, flow_day=flow_day, source_id=source_id,
        trade_series=trade_series, flow_series=flow_series, structures=structures, store=store,
        raws=raws, manifests=manifests, canonical=canonical, partial=partial, complete=complete, zero=zero,
        trades=trades, source=source)


def test_flow_archival_keeps_deduplicated_raw_delivery_and_all_causal_evidence(storage, tmp_path, monkeypatch):
    fixture = _flow_fixture(storage, tmp_path, monkeypatch)
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(database=storage.database, object_store=fixture.store,
        temporary_directory=tmp_path / "staging", limits=fact_archival.FactArchiveLimits(max_rows=10, row_group_size=1))
    cold = FilesystemRawArchiveObjectStore(fixture.store.root, writable=False)
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository",
        PostgresCanonicalFactStorageRepository(object_store_factory=lambda: cold))
    archive.seal_partition(fixture.source_day)
    source_page = archive.stage_next_page(fixture.source_day)
    archive.verify_next_page(fixture.source_day)
    archive.verify_partition(fixture.source_day)
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    assert reclaimer.reclaim_partition(fixture.source_day, eligible_before=storage.today, execute=True)["status"] == "partition_reclaimed"
    request = dict(series_id=fixture.flow_series, start=fixture.start, end=fixture.start + timedelta(seconds=5))
    before = storage.repo.read_fact_revisions(**request)
    assert len(before) == 3 and [row.revision for row in before] == [1, 2, 1]
    archive.seal_partition(fixture.flow_day)
    with storage.database.session() as session:
        raw_key = session.execute(text("SELECT object_key FROM market.raw_archive_manifests WHERE id=:id"),
            {"id": fixture.manifests[2]}).scalar_one()
        source_key = session.execute(text("SELECT object_key FROM market.fact_archive_manifests WHERE id=:id"),
            {"id": source_page["manifest_id"]}).scalar_one()
    source_path = fixture.store.local_path(source_key)
    original_source = source_path.read_bytes()
    source_path.write_bytes(b"corrupted cold canonical source")
    with pytest.raises((RuntimeError, ValueError), match="checksum|archive|size"):
        archive.stage_next_page(fixture.flow_day)
    source_path.write_bytes(original_source)
    raw_path = fixture.store.local_path(raw_key)
    original_raw = raw_path.read_bytes()
    raw_path.write_bytes(b"corrupted raw delivery with no canonical row")
    with pytest.raises(RuntimeError, match="archive_verification"):
        for _ in range(8):
            archive.stage_next_page(fixture.flow_day)
    assert archive.inspect_partition(fixture.flow_day)["state"] == "sealed"
    raw_path.write_bytes(original_raw)
    for _ in range(8):
        page = archive.stage_next_page(fixture.flow_day)
        if page["status"] == "page_acknowledged":
            break
        assert page["status"] == "trade_prefix_verified"
    else:
        pytest.fail("flow page did not finish its bounded raw-prefix work")
    assert archive.verify_next_page(fixture.flow_day)["status"] == "page_verified"
    assert archive.verify_partition(fixture.flow_day)["row_count"] == 3
    with storage.database.session() as session:
        dependencies = session.execute(text("SELECT target_id FROM market.fact_archive_dependencies WHERE manifest_id=:id"),
            {"id": page["manifest_id"]}).scalars().all()
        sources = session.execute(text("SELECT fact_version_id FROM market.fact_archive_canonical_dependencies WHERE manifest_id=:id"),
            {"id": page["manifest_id"]}).scalars().all()
        assert set(dependencies) == set(fixture.manifests)
        assert sources == [fixture.canonical[0].fact_version_id]
        assert session.execute(text("SELECT count(*) FROM market.fact_versions WHERE series_id=:series"),
            {"series": fixture.trade_series}).scalar_one() == 1
    assert storage.repo.read_fact_revisions(**request) == before
    assert reclaimer.reclaim_partition(fixture.flow_day, eligible_before=storage.today, execute=True)["status"] == "partition_reclaimed"
    assert _physical(storage, fixture.flow_day)["relation"] is None
    assert storage.repo.read_fact_revisions(**request) == before


@pytest.mark.parametrize("source_cold", [False, True, "legacy"])
def test_trade_and_flow_frozen_histories_survive_physical_reclamation(storage, tmp_path, monkeypatch, source_cold):
    legacy = source_cold == "legacy"
    fixture = _flow_fixture(storage, tmp_path, monkeypatch, include_partial=not legacy)
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    cold = FilesystemRawArchiveObjectStore(fixture.store.root, writable=False)
    tiered = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: cold)
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository", tiered)
    window = dict(start=fixture.start, end=fixture.start + timedelta(seconds=5))
    requests = [DatasetSeriesRequest(series, **window) for series in (fixture.trade_series, fixture.flow_series)]
    with monkeypatch.context() as old:
        old.setattr(market_data, "_preserves_canonical_revision_history", lambda version: False)
        older_dataset = storage.repo.freeze_dataset(requests)
    for request in requests:
        with pytest.raises(RuntimeError, match="revision_history_unpinned"):
            storage.repo.read_dataset_fact_revisions(dataset_id=older_dataset.dataset_id, series_id=request.series_id)
    # Generic historical/import revisions are distinct from the real stream's
    # no-op delivery above. Keep every one, including explicit invalidations.
    _placement(monkeypatch, fixture.source_day)
    second = canonicalize_market_trade(fixture.trades[1], source=fixture.source)
    invalid = replace(second, state="invalidated", accepted_at=fixture.start + timedelta(seconds=8),
        known_at=fixture.start + timedelta(seconds=8), provenance={**second.provenance, "fixture_revision": "invalidated"})
    for fact in (second, invalid):
        storage.repo.ingest_facts(series_id=fixture.trade_series, source_id=fixture.source_id, facts=[fact])
    _placement(monkeypatch, fixture.flow_day)
    flow = canonicalize_trade_flow(fixture.complete, source=fixture.source)
    storage.repo.ingest_facts(series_id=fixture.flow_series, source_id=fixture.source_id,
        facts=[replace(flow, state="invalidated", accepted_at=fixture.start + timedelta(seconds=8),
            known_at=fixture.start + timedelta(seconds=8))])
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_book_prefix_chunks")).scalar_one() == 0
    frozen = storage.repo.freeze_dataset(requests)
    assert frozen.dataset_hash != older_dataset.dataset_hash
    before, bindings, latest, causal = {}, {}, {}, {}
    for entry in frozen.series:
        series = entry["series_id"]
        assert entry["source_summary"]["record_selection"] == "all_canonical_revisions.v1"
        before[series] = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series)
        assert any(record.fact.state.value == "invalidated" for record in before[series])
        bindings[series] = validate_frozen_dataset_series(store=storage.repo, entry={**entry, "dataset_id": frozen.dataset_id})
        latest[series] = storage.repo.read_series_records(series_id=series, **window)
        causal[series] = storage.repo.read_facts(series_id=series, **window, known_at_lte=fixture.complete.known_at)
    assert len(before[fixture.trade_series]) == 3
    assert len(before[fixture.flow_series]) == (3 if legacy else 4)
    if not legacy:
        from market_data.canonical_adapters import decode_trade_flow_record
        assert decode_trade_flow_record(before[fixture.flow_series][0]).fact.archive_complete is False
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_book_prefix_chunks")).scalar_one() == 0, "freeze must not write retention progress"
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(database=storage.database, object_store=fixture.store,
        temporary_directory=tmp_path / "staging")
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    if source_cold:
        _archive_day(archive, fixture.source_day)
        reclaimer.reclaim_partition(fixture.source_day, eligible_before=storage.today, execute=True)
    with monkeypatch.context() as old:
        if legacy:
            from portal.backend.service.storage.repos.fact_flow_admission import collect_trade_history_archive_refs
            old.setattr(fact_archival, "FACT_ARCHIVE_VERIFIER_VERSION", "market.canonical_archive_verification.v8")
            old.setattr(archive, "_source_revisions", lambda session, rows: [])
            old.setattr(archive, "_prepare_book_prefix", lambda *args, **kwargs: None)
            old.setattr(archive, "_flow_references", lambda session, rows, objects, **kwargs:
                collect_trade_history_archive_refs(session, rows=rows, object_store=fixture.store))
        _archive_day(archive, fixture.flow_day)
    if legacy:
        with storage.database.session() as session:
            page_before = dict(session.execute(text("SELECT * FROM market.fact_archive_manifests WHERE storage_day=:day"),
                {"day": fixture.flow_day}).mappings().one())
            receipt_before = dict(session.execute(text("SELECT * FROM market.fact_archive_verifications WHERE manifest_id=:id"),
                {"id": page_before["id"]}).mappings().one())
            assert session.execute(text("SELECT count(*) FROM market.fact_archive_canonical_dependencies WHERE manifest_id=:id"),
                {"id": page_before["id"]}).scalar_one() == 0
        with pytest.raises(RuntimeError, match="verification_missing_or_stale"):
            reclaimer.reclaim_partition(fixture.flow_day, eligible_before=storage.today, execute=True)
        archive.restart_partition_verification(fixture.flow_day)
        for _ in range(16):
            if archive.verify_next_page(fixture.flow_day)["status"] == "no_unverified_pages":
                break
        else:
            pytest.fail("old flow page did not complete bounded reverification")
        archive.verify_partition(fixture.flow_day)
        with storage.database.session() as session:
            assert dict(session.execute(text("SELECT * FROM market.fact_archive_manifests WHERE id=:id"),
                {"id": page_before["id"]}).mappings().one()) == page_before
            assert dict(session.execute(text("SELECT * FROM market.fact_archive_verifications WHERE manifest_id=:id AND verifier_version=:version"),
                {"id": page_before["id"], "version": receipt_before["verifier_version"]}).mappings().one()) == receipt_before
    if not source_cold:
        _archive_day(archive, fixture.source_day)
        reclaimer.reclaim_partition(fixture.source_day, eligible_before=storage.today, execute=True)
    with storage.database.session() as session:
        key = session.execute(text("SELECT object_key FROM market.raw_archive_manifests WHERE id=:id"),
            {"id": fixture.manifests[2]}).scalar_one()
    path = fixture.store.local_path(key)
    original = path.read_bytes()
    path.write_bytes(b"corrupt flow raw evidence after page verification")
    with pytest.raises(RuntimeError, match="archive_verification"):
        reclaimer.reclaim_partition(fixture.flow_day, eligible_before=storage.today, execute=True)
    assert _physical(storage, fixture.flow_day)["relation"] is not None
    path.write_bytes(original)
    physical = _physical(storage, fixture.flow_day)
    removed = reclaimer.reclaim_partition(fixture.flow_day, eligible_before=storage.today, execute=True)
    assert removed["reclaimed_bytes"] == physical["bytes"] > 0
    assert _physical(storage, fixture.flow_day)["relation"] is None
    assert _physical(storage, fixture.source_day)["relation"] is None
    for entry in frozen.series:
        series = entry["series_id"]
        assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series) == before[series]
        assert validate_frozen_dataset_series(store=storage.repo, entry={**entry, "dataset_id": frozen.dataset_id}) == bindings[series]
        assert storage.repo.read_series_records(series_id=series, **window) == latest[series]
        assert storage.repo.read_facts(series_id=series, **window, known_at_lte=fixture.complete.known_at) == causal[series]
    assert storage.repo.freeze_dataset(requests).dataset_hash == frozen.dataset_hash
