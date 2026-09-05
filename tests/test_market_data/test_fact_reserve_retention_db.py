"""Structured source reports retain exact inline evidence after physical cooling."""
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import text

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.canonical import CanonicalFact
from market_data.contracts import DatasetSeriesRequest
from portal.backend.service.market.backtest_dataset_service import validate_frozen_dataset_series
from portal.backend.service.storage.repos import fact_archival, fact_reclamation, market_data
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from tests.test_market_data.test_fact_storage_tiers_db import storage, BASE, _placement
from tests.test_market_data.test_fact_derived_retention_db import _archive_day
from tests.test_market_data.test_fact_reclamation_db import _physical

pytestmark = pytest.mark.db


def test_reserve_report_reclaims_without_losing_response_bundle_or_causal_revisions(storage, tmp_path, monkeypatch):
    day = storage.today - timedelta(days=2)
    _placement(monkeypatch, day)
    series_id = storage.repo.register_series(instrument_id="storage-fixture", fact_type="asset.reserve_state",
        contract_version="asset.reserve_state.v1", timeframe_seconds=None,
        dimensions={"reserve_asset": "BTC"})
    fact = CanonicalFact(fact_type="asset.reserve_state", payload_schema_id="asset.reserve_state.v1",
        observation_key="reserve-report", observation_time=BASE, observation_time_method="source_report_timestamp",
        received_at=BASE, accepted_at=BASE, known_at=BASE, known_at_method="platform_acceptance",
        source=storage.fact.source, transformation_id="reserve-retention.fixture.v1",
        payload={"report_id": "report-1", "reserve_asset": "BTC", "reserve_quantity": "1.234567890123456789", "unit": "BTC"},
        provenance={"schema_version": "market.structured_fact_provenance.v1", "manifest_id": "fixture",
            "manifest_hash": "a" * 64, "binding_id": "fixture-reserve", "response_hash": "b" * 64,
            "provider_observation": {"bundle": "0x0000000000000000000000000000000000000001",
                                     "raw_reserve_quantity": "1234567890123456789", "confirmed_head_block": 123}},
        quality={"classification": "fixture"})
    revised = replace(fact, payload={**fact.payload, "reserve_quantity": "2.234567890123456789"},
        known_at=BASE + timedelta(seconds=1), accepted_at=BASE + timedelta(seconds=1))
    invalidated = replace(revised, state="invalidated", known_at=BASE + timedelta(seconds=2), accepted_at=BASE + timedelta(seconds=2))
    for revision in (fact, revised, invalidated):
        storage.repo.ingest_facts(series_id=series_id, source_id=storage.source_id, facts=[revision])
    request = DatasetSeriesRequest(series_id, BASE - timedelta(seconds=1), BASE + timedelta(minutes=1))
    frozen = storage.repo.freeze_dataset([request])
    before = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series_id)
    assert len(before) == 3 and before[0].fact.provenance["provider_observation"] == fact.provenance["provider_observation"]
    binding_args = dict(store=storage.repo, entry={**frozen.series[0], "dataset_id": frozen.dataset_id})
    binding_before = validate_frozen_dataset_series(**binding_args)
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(database=storage.database, object_store=store,
                                                                 temporary_directory=tmp_path / "staging")
    _archive_day(archive, day)
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_archive_dependencies")).scalar_one() == 0
        page = session.execute(text("SELECT object_key FROM market.fact_archive_manifests WHERE storage_day=:day"),
                               {"day": day}).scalar_one()
    reclaimer = fact_reclamation.PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    path = store.local_path(page)
    original = path.read_bytes()
    path.write_bytes(b"corrupt inline response archive")
    with pytest.raises(RuntimeError, match="archive_verification"):
        reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    assert _physical(storage, day)["relation"] is not None
    path.write_bytes(original)
    physical = _physical(storage, day)
    result = reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    assert result["reclaimed_bytes"] == physical["bytes"] > 0 and _physical(storage, day)["relation"] is None
    reader = FilesystemRawArchiveObjectStore(store.root, writable=False)
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository",
        PostgresCanonicalFactStorageRepository(object_store_factory=lambda: reader))
    assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series_id) == before
    assert storage.repo.freeze_dataset([request]).dataset_hash == frozen.dataset_hash
    assert validate_frozen_dataset_series(**binding_args) == binding_before
    assert len(storage.repo.read_facts(series_id=series_id, start=request.start, end=request.end, known_at_lte=BASE)) == 1
