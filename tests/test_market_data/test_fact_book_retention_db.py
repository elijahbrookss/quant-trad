"""Cold-book consumer/lifetime diagnostics, not production L2 admission proof."""
from datetime import timedelta

import pytest

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.book_archive import publish_book_checkpoint
from market_data.contracts import DatasetSeriesRequest
from market_data.market_state import derive_book_features, MarketStateValuationContract
from portal.backend.service.market.market_structure_service import MarketStructureService
from portal.backend.service.storage.repos import fact_archival, fact_reclamation, market_data, market_structure
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from tests.test_market_data.test_fact_storage_tiers_db import storage, BASE
from tests.test_market_data.test_fact_raw_lineage_db import _raw_book_fixture, _publish_book_result
from tests.test_market_data.test_fact_reclamation_db import _physical

pytestmark = pytest.mark.db


def test_cold_book_handoff_preserves_frozen_features_checkpoint_and_replay(storage, tmp_path, monkeypatch):
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
        database=storage.database, object_store=fixture.store, temporary_directory=tmp_path / "staging")
    archive.seal_partition(fixture.day)
    for _ in range(10):
        if archive.stage_next_page(fixture.day)["status"] == "source_exhausted":
            break
    else:
        pytest.fail("book archival did not finish within the fixture step budget")
    assert archive.verify_next_page(fixture.day)["status"] == "page_verified"
    assert archive.verify_partition(fixture.day)["row_count"] == 10
    reclaimer = fact_reclamation.PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    with pytest.raises(RuntimeError, match="canonical_reclaim_dependency_proof_required"):
        reclaimer.reclaim_partition(fixture.day, eligible_before=storage.today, execute=True)
    # Diagnostic only: expose post-DROP consumer defects before admitting L2.
    monkeypatch.setattr(fact_reclamation, "_ADMITTED_FACT_TYPES",
                        fact_reclamation._ADMITTED_FACT_TYPES | {"market.l2_book", "market.bbo", "market.depth_observation"})
    physical = _physical(storage, fixture.day)
    result = reclaimer.reclaim_partition(fixture.day, eligible_before=storage.today, execute=True)
    assert result["status"] == "partition_reclaimed"
    assert result["reclaimed_bytes"] == physical["bytes"] > 0
    assert _physical(storage, fixture.day)["relation"] is None
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
        # Reproduce the previous handoff gap: no exact page/prefix edge and no
        # user/dataset pin protects this checkpoint after hot payloads leave.
        prior = fixture.structures.archive_retention_status(target_kind="book_checkpoint", target_id=checkpoint.checkpoint_id)
        assert prior["canonical_dependency_count"] == 0 and prior["pinned"] is False
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
