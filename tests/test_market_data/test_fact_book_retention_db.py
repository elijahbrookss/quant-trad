"""Real cold-book admission, dependency guards, and consumer/lifetime proofs."""
from datetime import timedelta

import pytest
from sqlalchemy import text

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.book_archive import publish_book_checkpoint
from market_data.contracts import DatasetSeriesRequest
from market_data.fact_archive import FactArchiveLimits
from market_data.market_state import derive_book_features, MarketStateValuationContract
from portal.backend.service.market.market_structure_service import MarketStructureService
from portal.backend.service.storage.repos import fact_archival, fact_reclamation, market_data, market_structure
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from tests.test_market_data.test_fact_storage_tiers_db import storage, BASE, _placement
from tests.test_market_data.test_fact_raw_lineage_db import _raw_book_fixture, _publish_book_result
from tests.test_market_data.test_fact_reclamation_db import _physical

pytestmark = pytest.mark.db


@pytest.mark.parametrize("split_sources", [False, True, "handoff", "legacy", "paged"])
def test_cold_book_handoff_preserves_frozen_features_checkpoint_and_replay(storage, tmp_path, monkeypatch, split_sources):
    legacy_page = split_sources == "legacy"
    paged = split_sources == "paged"
    if legacy_page or paged:
        split_sources = False
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    fixture = _raw_book_fixture(storage, tmp_path, monkeypatch, trailing_heartbeat=True, replay_features=True)
    for index in range(len(fixture.results)):
        _publish_book_result(fixture, index)
    config = fixture.claim.config
    bbo, depth = derive_book_features((item.state for item in fixture.results),
        contract=MarketStateValuationContract(product_definition_version_id=config["product_definition_version_id"],
            provider_size_unit="base", base_currency="BTC", quote_currency="USD"),
        bbo_series_id=config["bbo_series_id"], depth_series_id=config["depth_series_id"],
        computed_at=BASE + timedelta(minutes=1))
    assert len(bbo) == 2 and len(depth) == 6
    feature_day = fixture.day + timedelta(days=1) if split_sources else fixture.day
    _placement(monkeypatch, feature_day)
    fixture.structures.ingest_market_state_features(bbo_facts=bbo, depth_facts=depth)
    checkpoint = fixture.results[0].checkpoints[0]
    encoded, ack = publish_book_checkpoint(checkpoint, object_store=fixture.store,
                                          temporary_directory=tmp_path / "checkpoints")
    fixture.structures.commit_book_checkpoint(fixture.claim, checkpoint=checkpoint, encoded=encoded,
        acknowledgement=ack, source_manifest_ids=[fixture.manifests[0]])
    reader = FilesystemRawArchiveObjectStore(fixture.store.root, writable=False)
    tiered = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: reader)
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository", tiered)
    monkeypatch.setattr(market_structure, "canonical_fact_storage_repository", tiered)
    request = DatasetSeriesRequest(fixture.series_id, BASE - timedelta(seconds=1), BASE + timedelta(minutes=1))
    # Atomic book events remain replay input, not a newly dataset-eligible
    # family. Retention must not change the existing research contract.
    with pytest.raises(RuntimeError, match="market_dataset_unsupported_fact"):
        storage.repo.freeze_dataset([request])
    history_args = dict(series_id=fixture.series_id, start=request.start, end=request.end)
    before = storage.repo.read_fact_revisions(**history_args)
    assert len(before) == 2
    read_args = dict(series_id=fixture.series_id, start=request.start, end=request.end,
                     known_at_lte=BASE + timedelta(seconds=1))
    known_before = storage.repo.read_facts(**read_args)
    assert len(known_before) == 1
    research_requests = [DatasetSeriesRequest(config[key], request.start, request.end)
                         for key in ("bbo_series_id", "depth_series_id")]
    frozen = storage.repo.freeze_dataset(research_requests)
    frozen_before = {item.series_id: storage.repo.read_dataset_fact_revisions(
        dataset_id=frozen.dataset_id, series_id=item.series_id) for item in research_requests}
    assert sorted(len(records) for records in frozen_before.values()) == [2, 6]
    replay = MarketStructureService(repository=fixture.structures)
    replay_args = dict(definition_id=fixture.claim.definition_id, session_id=fixture.claim.session_id,
                       storage_root=tmp_path)
    replay_before = replay.replay_book_session(**replay_args)
    assert replay_before["checkpoint_count"] == 1
    assert replay_before["checkpoint_delta_equal"] is True
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=fixture.store, temporary_directory=tmp_path / "staging",
        limits=FactArchiveLimits(max_rows=1, row_group_size=1) if paged else FactArchiveLimits())
    checkpoint_path = fixture.store.local_path(ack.object_key)
    checkpoint_bytes = checkpoint_path.read_bytes()
    with monkeypatch.context() as previous_version:
        if legacy_page:
            # Preserve a v5-shaped immutable catalog/receipt. No source or
            # checkpoint edges existed, even though raw bindings were complete.
            original_dependencies = archive._dependencies
            previous_version.setattr(fact_archival, "FACT_ARCHIVE_VERIFIER_VERSION", "market.canonical_archive_verification.v5")
            previous_version.setattr(archive, "_book_sources", lambda *args, **kwargs: [])
            previous_version.setattr(archive, "_dependencies", lambda *args, **kwargs:
                ([item for item in original_dependencies(*args, **kwargs)[0] if item["target_kind"] == "raw_manifest"], []))
        archive.seal_partition(fixture.day)
        checkpoint_path.unlink()
        with pytest.raises(FileNotFoundError):
            for _ in range(10):
                archive.stage_next_page(fixture.day)
        checkpoint_path.write_bytes(checkpoint_bytes)
        for _ in range(32):
            if archive.stage_next_page(fixture.day)["status"] == "source_exhausted":
                break
        else:
            pytest.fail("book archival did not finish within the fixture step budget")
        checkpoint_path.write_bytes(b"corrupt checkpoint")
        with pytest.raises(RuntimeError, match="archive_verification"):
            archive.verify_next_page(fixture.day)
        checkpoint_path.write_bytes(checkpoint_bytes)
        assert archive.verify_next_page(fixture.day)["status"] == "page_verified"
        for _ in range(32):
            if archive.verify_next_page(fixture.day)["status"] == "no_unverified_pages":
                break
        else:
            pytest.fail("book verification did not finish within the fixture step budget")
        assert archive.verify_partition(fixture.day)["row_count"] == (2 if split_sources else 10)
    if legacy_page:
        with storage.database.session() as session:
            old_receipt = dict(session.execute(text("SELECT * FROM market.fact_archive_verifications")).mappings().one())
            old_page = dict(session.execute(text("SELECT * FROM market.fact_archive_manifests")).mappings().one())
            assert session.execute(text("SELECT count(*) FROM market.fact_archive_canonical_dependencies")).scalar_one() == 0
        assert archive.restart_partition_verification(fixture.day)["status"] == "partition_verification_restarted"
        assert archive.verify_next_page(fixture.day)["status"] == "page_verified"
        archive.verify_partition(fixture.day)
        with storage.database.session() as session:
            assert dict(session.execute(text("SELECT * FROM market.fact_archive_manifests")).mappings().one()) == old_page
            assert dict(session.execute(text("SELECT * FROM market.fact_archive_verifications WHERE verifier_version=:version"),
                                        {"version": old_receipt["verifier_version"]}).mappings().one()) == old_receipt
            assert session.execute(text("SELECT count(*) FROM market.fact_archive_verifications")).scalar_one() == 2
    reclaimer = fact_reclamation.PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    checkpoint_path.write_bytes(b"corrupt after verification")
    with pytest.raises(RuntimeError, match="archive_verification"):
        reclaimer.reclaim_partition(fixture.day, eligible_before=storage.today, execute=True)
    assert _physical(storage, fixture.day)["relation"] is not None
    checkpoint_path.write_bytes(checkpoint_bytes)
    physical = _physical(storage, fixture.day)
    if split_sources != "handoff":
        result = reclaimer.reclaim_partition(fixture.day, eligible_before=storage.today, execute=True)
        assert result["status"] == "partition_reclaimed"
        assert result["reclaimed_bytes"] == physical["bytes"] > 0
        assert _physical(storage, fixture.day)["relation"] is None
    if split_sources:
        archive.seal_partition(feature_day)
        for _ in range(10):
            if archive.stage_next_page(feature_day)["status"] == "source_exhausted":
                break
        else:
            pytest.fail("derived book archive did not finish within fixture step budget")
        assert archive.verify_next_page(feature_day)["status"] == "page_verified"
        archive.verify_partition(feature_day)
        if split_sources == "handoff":
            real_monotonic = fact_reclamation.monotonic
            moved = []
            def move_source_between_transactions():
                if not moved:
                    moved.append(True)
                    moved.append(reclaimer.reclaim_partition(fixture.day, eligible_before=storage.today, execute=True))
                return real_monotonic()
            with monkeypatch.context() as concurrent:
                concurrent.setattr(fact_reclamation, "monotonic", move_source_between_transactions)
                with pytest.raises(RuntimeError, match="canonical_reclaim_source_placement_changed"):
                    reclaimer.reclaim_partition(feature_day, eligible_before=storage.today, execute=True)
            assert moved[1]["status"] == "partition_reclaimed"
            assert moved[1]["reclaimed_bytes"] == physical["bytes"]
            assert _physical(storage, fixture.day)["relation"] is None
            assert _physical(storage, feature_day)["relation"] is not None
        with storage.database.session() as session:
            source_page = session.execute(text("SELECT * FROM market.fact_archive_manifests WHERE storage_day=:day"),
                                          {"day": fixture.day}).mappings().one()
        source_path = fixture.store.local_path(source_page["object_key"])
        source_bytes = source_path.read_bytes()
        source_path.write_bytes(b"corrupt canonical source after verification")
        with pytest.raises(RuntimeError, match="archive_verification"):
            reclaimer.reclaim_partition(feature_day, eligible_before=storage.today, execute=True)
        assert _physical(storage, feature_day)["relation"] is not None
        source_path.write_bytes(source_bytes)
        assert reclaimer.reclaim_partition(feature_day, eligible_before=storage.today, execute=True)["status"] == "partition_reclaimed"
    with storage.database.session() as session:
        edges = session.execute(text("""
            SELECT dependency.fact_version_id,dependency.row_hash,source.row_hash AS source_hash
            FROM market.fact_archive_canonical_dependencies AS dependency
            JOIN market.fact_versions AS source ON source.id=dependency.fact_version_id
        """)).mappings().all()
        if paged:
            assert len(edges) > 10, "each separate page binds its own sources, including older checkpoint state"
            assert len({item["fact_version_id"] for item in edges}) == 2
        else:
            assert len(edges) == (3 if split_sources else 2)
        assert all(item["row_hash"] == item["source_hash"] for item in edges)
    assert storage.repo.read_fact_revisions(**history_args) == before
    assert storage.repo.read_facts(**read_args) == known_before
    for series_id, records in frozen_before.items():
        assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series_id) == records
    assert storage.repo.freeze_dataset(research_requests).dataset_hash == frozen.dataset_hash
    assert replay.replay_book_session(**replay_args) == replay_before
    from portal.backend.service.storage.repos.fact_references import lock_canonical_raw_references
    with storage.database.session() as session:
        # One exact delivery fits. Re-scanning the three-position historical
        # prefix would exceed this budget after the final hot holder is gone.
        lock_canonical_raw_references(session, [fixture.facts[-1]], max_mapping_rows=1)
    from portal.backend.service.storage.repos.market_lifecycle import PostgresMarketStorageLifecycleRepository
    with monkeypatch.context() as prior_lifetime:
        prior_lifetime.setattr(PostgresMarketStorageLifecycleRepository, "canonical_book_session_held",
                               staticmethod(lambda *args, **kwargs: False))
        # Checkpoints now have exact permanent edges too. The session anchor
        # additionally protects later control frames and recovery artifacts.
        prior = fixture.structures.archive_retention_status(target_kind="book_checkpoint", target_id=checkpoint.checkpoint_id)
        assert prior["canonical_dependency_count"] > 0 and prior["pinned"] is True
    for kind, identity in [("raw_manifest", item) for item in fixture.manifests] + [("book_checkpoint", checkpoint.checkpoint_id)]:
        status = fixture.structures.archive_retention_status(target_kind=kind, target_id=identity)
        assert status["canonical_backlog_present"] is False
        assert status["canonical_dependency_count"] > 0, (kind, identity, status)
        assert status["pinned"] is True
    from portal.backend.service.storage.repos.market_lifecycle import market_storage_lifecycle_repository
    from portal.backend.service.storage.repos import market_lifecycle
    monkeypatch.setattr(market_lifecycle, "db", storage.database)
    cutoff = BASE + timedelta(days=1)
    candidates = market_storage_lifecycle_repository.list_archive_expiration_candidates(
        raw_trade_cutoff=cutoff, raw_l2_cutoff=cutoff, checkpoint_cutoff=cutoff,
        compacted_source_cutoff=cutoff, limit=20)
    assert len(candidates) == 5
    assert all(row["canonical_dependency_count"] > 0 for row in candidates)
    assert not any(row["canonical_backlog_present"] for row in candidates)
    # The anchor is session-scoped, not a blanket "archives exist" hold.
    from tests.test_market_data.test_fact_raw_lineage_db import _raw_trade_fixture
    unrelated = _raw_trade_fixture(storage, tmp_path, monkeypatch)
    status = unrelated.structures.archive_retention_status(target_kind="raw_manifest", target_id=unrelated.manifests[0])
    assert status["canonical_dependency_count"] == 0 and status["pinned"] is False
