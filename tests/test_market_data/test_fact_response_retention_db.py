"""Response archive source closure, before enabling its physical deletion gate."""
from collections import Counter
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import text

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.canonical_adapters import DERIVED_MARKET_STATE_SOURCE, canonicalize_market_trade, canonicalize_response_feature
from market_data.contracts import DatasetSeriesRequest
from market_data.market_state import MarketStateValuationContract, derive_response_features, derive_trade_flow_feature
from portal.backend.service.market.backtest_dataset_service import validate_frozen_dataset_series
from portal.backend.service.storage.repos import fact_archival, market_data
from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from tests.test_market_data.test_fact_derived_retention_db import _archive_day
from tests.test_market_data.test_fact_flow_retention_db import _flow_fixture
from tests.test_market_data.test_fact_raw_lineage_db import _raw_book_fixture, _publish_book_result
from tests.test_market_data.test_fact_storage_tiers_db import storage, _placement

pytestmark = pytest.mark.db


@pytest.mark.parametrize("source_cold", [False, True])
def test_response_archive_keeps_exact_witnesses_and_wider_causal_windows(storage, tmp_path, monkeypatch, source_cold):
    flow = _flow_fixture(storage, tmp_path, monkeypatch)
    book = _raw_book_fixture(storage, tmp_path, monkeypatch, definition_id="response-book", response_window=True)
    for index in range(len(book.results)):
        _publish_book_result(book, index)
    flow_series = storage.repo.register_series(instrument_id="storage-fixture", fact_type="market.trade_flow_feature",
        contract_version="market.trade_flow_feature.v1", timeframe_seconds=1)
    feature = derive_trade_flow_feature(series_id=flow_series, source_trade_flow_series_id=flow.flow_series,
        aggregate=flow.complete, trades=[flow.trades[1]], computed_at=flow.complete.known_at)
    assert feature is not None
    assert flow.structures.ingest_market_state_features(flow_facts=[feature]).inserted_count == 1
    response_series = storage.repo.register_series(instrument_id="storage-fixture", fact_type="market.market_response",
        contract_version="market.market_response.v1", timeframe_seconds=1)
    contract = MarketStateValuationContract(product_definition_version_id=book.results[0].state.product_definition_version_id,
        provider_size_unit="base", base_currency="BTC", quote_currency="USD")
    # The later processing chunk's fifth state is not available at this
    # response decision. All five are already persisted to test known-at reads.
    responses = derive_response_features([item.state for item in book.results[:4]], [flow.trades[0]], [feature],
        contract=contract, series_id=response_series, computed_at=flow.start + timedelta(seconds=8))
    assert len(responses) == 1
    response = responses[0]
    assert response.pre_book_source_position.receive_ordinal == 1
    assert response.trough_book_source_position.receive_ordinal == 3
    assert response.post_book_source_position.receive_ordinal == 4
    response_day = storage.today - timedelta(days=1)
    _placement(monkeypatch, response_day)
    assert flow.structures.ingest_market_state_features(response_facts=responses).inserted_count == 1
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    cold = FilesystemRawArchiveObjectStore(flow.store.root, writable=False)
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository",
        PostgresCanonicalFactStorageRepository(object_store_factory=lambda: cold))
    window = dict(start=flow.start, end=flow.start + timedelta(seconds=5))
    request = DatasetSeriesRequest(response_series, **window)
    with monkeypatch.context() as old:
        old.setattr(market_data, "_preserves_canonical_revision_history", lambda version: False)
        older_dataset = storage.repo.freeze_dataset([request])
    with pytest.raises(RuntimeError, match="revision_history_unpinned"):
        storage.repo.read_dataset_fact_revisions(dataset_id=older_dataset.dataset_id, series_id=response_series)
    # The response's later invalidation keeps its full causal window, including
    # a trade revision newer than the aggregate/feature input clock. Future-known
    # revisions already committed before it must still be excluded.
    _placement(monkeypatch, flow.source_day)
    trade = canonicalize_market_trade(flow.trades[0], source=flow.source)
    for seconds in (7.5, 10):
        storage.repo.ingest_facts(series_id=flow.trade_series, source_id=flow.source_id,
            facts=[replace(trade, state="invalidated", accepted_at=flow.start + timedelta(seconds=seconds),
                           known_at=flow.start + timedelta(seconds=seconds))])
    _placement(monkeypatch, response_day)
    source_id = storage.repo.register_source(DERIVED_MARKET_STATE_SOURCE)
    storage.repo.ingest_facts(series_id=response_series, source_id=source_id,
        facts=[replace(canonicalize_response_feature(response), state="invalidated",
                       accepted_at=flow.start + timedelta(seconds=8), known_at=flow.start + timedelta(seconds=8))])
    frozen = storage.repo.freeze_dataset([request])
    assert frozen.dataset_hash != older_dataset.dataset_hash
    entry = {**frozen.series[0], "dataset_id": frozen.dataset_id}
    assert entry["source_summary"]["record_selection"] == "all_canonical_revisions.v1"
    history = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=response_series)
    assert len(history) == 2 and history[-1].fact.state.value == "invalidated"
    binding = validate_frozen_dataset_series(store=storage.repo, entry=entry)
    causal = storage.repo.read_facts(series_id=response_series, **window, known_at_lte=response.known_at)
    assert len(causal) == 1
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_book_prefix_chunks")).scalar_one() == 0
        frozen_raw_ids = session.execute(text("SELECT raw_archive_manifest_id FROM market.dataset_archive_refs WHERE dataset_id=:id"),
            {"id": frozen.dataset_id}).scalars().all()
        assert set(frozen_raw_ids) == set(flow.manifests + book.manifests[:4])
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(database=storage.database, object_store=flow.store,
        temporary_directory=tmp_path / "canonical-staging")
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    if source_cold:
        for day in (flow.source_day, book.day):
            _archive_day(archive, day)
            reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    archive.seal_partition(response_day)
    with storage.database.session() as session:
        key = session.execute(text("SELECT object_key FROM market.raw_archive_manifests WHERE id=:id"),
            {"id": book.manifests[1]}).scalar_one()
    path = flow.store.local_path(key)
    original = path.read_bytes()
    path.write_bytes(b"corrupt intervening book event, not a named response endpoint")
    with pytest.raises(RuntimeError, match="archive|checksum|sha256|size"):
        storage.repo.freeze_dataset([request])
    with pytest.raises(RuntimeError, match="archive_verification"):
        for _ in range(32):
            archive.stage_next_page(response_day)
    path.write_bytes(original)
    for _ in range(32):
        page = archive.stage_next_page(response_day)
        if page["status"] == "page_acknowledged":
            break
    else:
        pytest.fail("response page did not finish bounded source prefix preparation")
    assert archive.verify_next_page(response_day)["status"] == "page_verified"
    assert archive.verify_partition(response_day)["row_count"] == 2
    with storage.database.session() as session:
        sources = session.execute(text("""
            SELECT source.* FROM market.fact_archive_canonical_dependencies AS edge
            JOIN market.fact_versions AS source ON source.id=edge.fact_version_id WHERE edge.manifest_id=:id
        """), {"id": page["manifest_id"]}).mappings().all()
        assert Counter(row["fact_type"] for row in sources) == {
            "market.l2_book": 4, "market.trade": 2, "market.trade_flow": 1, "market.trade_flow_feature": 1}
        assert any(row["fact_type"] == "market.trade" and row["known_at"] == flow.start + timedelta(seconds=7.5) for row in sources)
        assert all(row["known_at"] <= flow.start + timedelta(seconds=8) for row in sources)
        raw_ids = session.execute(text("SELECT target_id FROM market.fact_archive_dependencies WHERE manifest_id=:id AND target_kind='raw_manifest'"),
            {"id": page["manifest_id"]}).scalars().all()
        assert set(flow.manifests + book.manifests[:4]) <= set(raw_ids) <= set(flow.manifests + book.manifests)
    if not source_cold:
        for day in (flow.source_day, book.day):
            _archive_day(archive, day)
            reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
        archive.verify_partition(response_day)
    assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=response_series) == history
    assert validate_frozen_dataset_series(store=storage.repo, entry=entry) == binding
    assert storage.repo.read_facts(series_id=response_series, **window, known_at_lte=response.known_at) == causal
    # The book archive certifies through fifth/future-known raw position. That
    # larger certificate must not add its wholly later object to this Dataset.
    assert storage.repo.freeze_dataset([request]).dataset_hash == frozen.dataset_hash
    # This source-closure proof is not yet final response-family admission.
    with pytest.raises(RuntimeError, match="dependency_proof_required"):
        reclaimer.reclaim_partition(response_day, eligible_before=storage.today, execute=True)
