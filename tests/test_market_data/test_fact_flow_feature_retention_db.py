"""Physical flow-feature retention with exact hot/cold aggregate witnesses."""
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import text

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.canonical_adapters import DERIVED_MARKET_STATE_SOURCE, canonicalize_trade_flow, canonicalize_trade_flow_feature
from market_data.contracts import DatasetSeriesRequest
from market_data.market_state import derive_trade_flow_feature
from portal.backend.service.market.backtest_dataset_service import validate_frozen_dataset_series
from portal.backend.service.storage.repos import fact_archival, market_data
from portal.backend.service.storage.repos.fact_flow_feature_admission import collect_flow_feature_history_archive_refs
from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from tests.test_market_data.test_fact_derived_retention_db import _archive_day
from tests.test_market_data.test_fact_flow_retention_db import _flow_fixture
from tests.test_market_data.test_fact_reclamation_db import _physical
from tests.test_market_data.test_fact_storage_tiers_db import storage, _placement

pytestmark = pytest.mark.db


@pytest.mark.parametrize("source_cold", [False, True, "legacy"])
def test_flow_feature_history_survives_three_tier_reclamation(storage, tmp_path, monkeypatch, source_cold):
    fixture = _flow_fixture(storage, tmp_path, monkeypatch)
    feature_day = storage.today - timedelta(days=1)
    series_id = storage.repo.register_series(instrument_id="storage-fixture", fact_type="market.trade_flow_feature",
        contract_version="market.trade_flow_feature.v1", timeframe_seconds=1)
    source_id = storage.repo.register_source(DERIVED_MARKET_STATE_SOURCE)
    # All-revision aggregate lookup must keep causal same-material deliveries,
    # but not a future-known revision even if its commit already exists.
    aggregate = canonicalize_trade_flow(fixture.complete, source=fixture.source)
    duplicate = replace(aggregate, known_at=fixture.start + timedelta(seconds=7, milliseconds=100),
        accepted_at=fixture.start + timedelta(seconds=7, milliseconds=100),
        provenance={**aggregate.provenance, "fixture_delivery": "duplicate"})
    future = replace(aggregate, known_at=fixture.start + timedelta(seconds=12),
        accepted_at=fixture.start + timedelta(seconds=12), provenance={**aggregate.provenance, "fixture_delivery": "future"})
    for fact in (duplicate, future):
        storage.repo.ingest_facts(series_id=fixture.flow_series, source_id=fixture.source_id, facts=[fact])
    _placement(monkeypatch, feature_day)
    feature = derive_trade_flow_feature(series_id=series_id, source_trade_flow_series_id=fixture.flow_series,
        aggregate=fixture.complete, trades=[fixture.trades[1]], computed_at=fixture.complete.known_at)
    assert feature is not None
    assert fixture.structures.ingest_market_state_features(flow_facts=[feature]).inserted_count == 1
    canonical = canonicalize_trade_flow_feature(feature)
    invalidated = replace(canonical, state="invalidated", known_at=fixture.start + timedelta(seconds=8),
        accepted_at=fixture.start + timedelta(seconds=8))
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    cold = FilesystemRawArchiveObjectStore(fixture.store.root, writable=False)
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository",
        PostgresCanonicalFactStorageRepository(object_store_factory=lambda: cold))
    window = dict(start=fixture.start, end=fixture.start + timedelta(seconds=5))
    request = DatasetSeriesRequest(series_id, **window)
    with monkeypatch.context() as old:
        old.setattr(market_data, "_preserves_canonical_revision_history", lambda version: False)
        older_dataset = storage.repo.freeze_dataset([request])
    with pytest.raises(RuntimeError, match="revision_history_unpinned"):
        storage.repo.read_dataset_fact_revisions(dataset_id=older_dataset.dataset_id, series_id=series_id)
    storage.repo.ingest_facts(series_id=series_id, source_id=source_id, facts=[invalidated])
    frozen = storage.repo.freeze_dataset([request])
    assert frozen.dataset_hash != older_dataset.dataset_hash
    entry = {**frozen.series[0], "dataset_id": frozen.dataset_id}
    assert entry["source_summary"]["record_selection"] == "all_canonical_revisions.v1"
    history = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series_id)
    assert len(history) == 2 and history[-1].fact.state.value == "invalidated"
    binding = validate_frozen_dataset_series(store=storage.repo, entry=entry)
    latest = storage.repo.read_series_records(series_id=series_id, **window)
    causal = storage.repo.read_facts(series_id=series_id, **window, known_at_lte=feature.known_at)
    assert len(causal) == 1
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_book_prefix_chunks")).scalar_one() == 0
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(database=storage.database, object_store=fixture.store,
        temporary_directory=tmp_path / "staging")
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    if source_cold:
        for day in (fixture.source_day, fixture.flow_day):
            _archive_day(archive, day)
            reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    legacy = source_cold == "legacy"
    with monkeypatch.context() as old:
        if legacy:
            old.setattr(fact_archival, "FACT_ARCHIVE_VERIFIER_VERSION", "market.canonical_archive_verification.v9")
            old.setattr(archive, "_source_revisions", lambda session, rows: [])
            def legacy_dependencies(session, rows, **kwargs):
                refs = collect_flow_feature_history_archive_refs(session, rows=rows, object_store=fixture.store)
                return ([{"target_kind": "raw_manifest", "target_id": identity, "object_key": ref["object_key"],
                          "object_sha256": ref["object_sha256"]} for identity, ref in sorted(refs.items())], [])
            old.setattr(archive, "_dependencies", legacy_dependencies)
        _archive_day(archive, feature_day)
    if legacy:
        with storage.database.session() as session:
            page_before = dict(session.execute(text("SELECT * FROM market.fact_archive_manifests WHERE storage_day=:day"),
                {"day": feature_day}).mappings().one())
            receipt_before = dict(session.execute(text("SELECT * FROM market.fact_archive_verifications WHERE manifest_id=:id"),
                {"id": page_before["id"]}).mappings().one())
        with pytest.raises(RuntimeError, match="verification_missing_or_stale"):
            reclaimer.reclaim_partition(feature_day, eligible_before=storage.today, execute=True)
        archive.restart_partition_verification(feature_day)
        for _ in range(32):
            if archive.verify_next_page(feature_day)["status"] == "no_unverified_pages":
                break
        else:
            pytest.fail("old feature archive did not finish bounded reverification")
        archive.verify_partition(feature_day)
        with storage.database.session() as session:
            assert dict(session.execute(text("SELECT * FROM market.fact_archive_manifests WHERE id=:id"),
                {"id": page_before["id"]}).mappings().one()) == page_before
            assert dict(session.execute(text("SELECT * FROM market.fact_archive_verifications WHERE manifest_id=:id AND verifier_version=:version"),
                {"id": page_before["id"], "version": receipt_before["verifier_version"]}).mappings().one()) == receipt_before
    with storage.database.session() as session:
        edges = session.execute(text("""
            SELECT source.fact_type,source.known_at,source.market_commit_seq FROM market.fact_archive_canonical_dependencies AS edge
            JOIN market.fact_archive_manifests AS page ON page.id=edge.manifest_id
            JOIN market.fact_versions AS source ON source.id=edge.fact_version_id WHERE page.storage_day=:day
        """), {"day": feature_day}).mappings().all()
        assert sorted(row["fact_type"] for row in edges) == ["market.trade", "market.trade_flow", "market.trade_flow"]
        assert all(row["known_at"] <= invalidated.known_at for row in edges)
    if not source_cold:
        for day in (fixture.source_day, fixture.flow_day):
            _archive_day(archive, day)
            reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    # Current source-page corruption must block feature DROP even though its
    # own archive page and receipt are valid and the source is already cold.
    with storage.database.session() as session:
        key = session.execute(text("SELECT object_key FROM market.fact_archive_manifests WHERE storage_day=:day"),
            {"day": fixture.flow_day}).scalar_one()
    path = fixture.store.local_path(key)
    original = path.read_bytes()
    path.write_bytes(b"corrupt exact aggregate page")
    with pytest.raises(RuntimeError, match="archive_verification"):
        reclaimer.reclaim_partition(feature_day, eligible_before=storage.today, execute=True)
    assert _physical(storage, feature_day)["relation"] is not None
    path.write_bytes(original)
    before = _physical(storage, feature_day)
    removed = reclaimer.reclaim_partition(feature_day, eligible_before=storage.today, execute=True)
    assert removed["reclaimed_bytes"] == before["bytes"] > 0
    assert all(_physical(storage, day)["relation"] is None for day in (fixture.source_day, fixture.flow_day, feature_day))
    assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series_id) == history
    assert validate_frozen_dataset_series(store=storage.repo, entry=entry) == binding
    assert storage.repo.read_series_records(series_id=series_id, **window) == latest
    assert storage.repo.read_facts(series_id=series_id, **window, known_at_lte=feature.known_at) == causal
    assert storage.repo.freeze_dataset([request]).dataset_hash == frozen.dataset_hash
