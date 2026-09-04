"""Physical normalized retention, nested recipes and all-revision freeze parity."""
from collections import Counter
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.canonical_adapters import DERIVED_MARKET_STATE_SOURCE, canonicalize_normalized_feature
from market_data.contracts import DatasetSeriesRequest
from market_data.normalization import NormalizationInput, NormalizationSpec, evaluate_normalization
from portal.backend.service.market.backtest_dataset_service import validate_frozen_dataset_series
from portal.backend.service.storage.repos import fact_archival, market_data, normalization
from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from tests.test_market_data.test_fact_derived_retention_db import _archive_day
from tests.test_market_data.test_fact_reclamation_db import _physical
from tests.test_market_data.test_fact_storage_tiers_db import storage, BASE, _placement

pytestmark = pytest.mark.db


@pytest.mark.parametrize("source_cold", [False, True])
def test_normalized_nested_history_survives_source_and_root_reclamation(storage, tmp_path, monkeypatch, source_cold):
    source_day, inner_day, outer_day = [storage.today - timedelta(days=days) for days in (3, 2, 1)]
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    reader = PostgresCanonicalFactStorageRepository(object_store_factory=lambda:
        FilesystemRawArchiveObjectStore(store.root, writable=False))
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository", reader)
    monkeypatch.setattr(normalization, "canonical_fact_storage_repository", reader)
    monkeypatch.setattr(normalization, "market_data_repo", storage.repo)
    monkeypatch.setattr(normalization, "db", storage.database)
    recipes = normalization.PostgresNormalizationRepository()
    _placement(monkeypatch, source_day)
    facts = [replace(storage.fact, observation_key=f"normalization-input-{index}",
        observation_time=BASE + timedelta(seconds=index), known_at=BASE + timedelta(seconds=index),
        accepted_at=BASE + timedelta(seconds=index),
        payload={**storage.fact.payload, "rate": str(index + 1), "raw_rate": str(index + 1)}) for index in range(5)]
    storage.repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id, facts=facts[:4])
    # Same material, distinct delivery; then a future-known revision committed
    # before the final causal input. Preserve the first two, exclude the third.
    for seconds in (3, 20):
        storage.repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id,
            facts=[replace(facts[0], known_at=BASE + timedelta(seconds=seconds),
                accepted_at=BASE + timedelta(seconds=seconds), provenance={"delivery": seconds})])
    storage.repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id, facts=facts[4:])
    window = dict(start=BASE, end=BASE + timedelta(seconds=10))
    inputs = storage.repo.read_facts(series_id=storage.series_id, **window, known_at_lte=BASE + timedelta(seconds=5))
    assert len(inputs) == 5
    spec = recipes.register_spec(NormalizationSpec(feature_name="retention_zscore", semantic_version="1",
        input_fact_type="derivatives.funding_rate", output_fact_type="market.normalized.retention_zscore",
        formula="causal_zscore", units="zscore", window_seconds=10,
        minimum_observations=1, warmup_observations=1, parameters={"require_full_window": False}))
    inner_series = storage.repo.register_series(instrument_id="storage-fixture", fact_type=spec.output_fact_type,
        contract_version=f"market.normalized_feature.v1/{spec.spec_id}", timeframe_seconds=None)
    normalized = evaluate_normalization(spec, [NormalizationInput(source_series_id=row.series_id,
        effective_at=row.fact.observation_time, known_at=row.fact.known_at, market_commit_seq=row.market_commit_seq,
        material_hash=row.fact.material_hash, value=Decimal(row.fact.payload["rate"])) for row in inputs],
        output_series_id=inner_series)[-1]
    assert normalized.input_count == 5 > len(normalized.source_material_hashes)
    _placement(monkeypatch, inner_day)
    inner_outcome = recipes.ingest([normalized])
    outer_spec = recipes.register_spec(NormalizationSpec(feature_name="retention_nested", semantic_version="1",
        input_fact_type=spec.output_fact_type, output_fact_type="market.normalized.retention_nested",
        formula="basis_points", units="bps", window_seconds=None, minimum_observations=0, warmup_observations=0))
    outer_series = storage.repo.register_series(instrument_id="storage-fixture", fact_type=outer_spec.output_fact_type,
        contract_version=f"market.normalized_feature.v1/{outer_spec.spec_id}", timeframe_seconds=None)
    nested = evaluate_normalization(outer_spec, [NormalizationInput(source_series_id=inner_series,
        effective_at=normalized.effective_at, known_at=normalized.known_at, market_commit_seq=inner_outcome.max_commit_seq,
        material_hash=normalized.material_hash, value=normalized.value)], output_series_id=outer_series)[0]
    _placement(monkeypatch, outer_day)
    recipes.ingest([nested])
    requests = [DatasetSeriesRequest(series_id, **window) for series_id in (storage.series_id, inner_series, outer_series)]
    with monkeypatch.context() as old:
        old.setattr(market_data, "_preserves_canonical_revision_history", lambda version: False)
        older_dataset = storage.repo.freeze_dataset(requests)
    with pytest.raises(RuntimeError, match="revision_history_unpinned"):
        storage.repo.read_dataset_fact_revisions(dataset_id=older_dataset.dataset_id, series_id=outer_series)
    source_id = storage.repo.register_source(DERIVED_MARKET_STATE_SOURCE)
    for day, series_id, fact, recipe in ((inner_day, inner_series, normalized, spec), (outer_day, outer_series, nested, outer_spec)):
        _placement(monkeypatch, day)
        revised = replace(fact, known_at=BASE + timedelta(seconds=6))
        storage.repo.ingest_facts(series_id=series_id, source_id=source_id,
            facts=[replace(canonicalize_normalized_feature(revised, spec=recipe), state="invalidated")])
    frozen = storage.repo.freeze_dataset(requests)
    assert frozen.dataset_hash != older_dataset.dataset_hash
    history, bindings, causal, latest = {}, {}, {}, {}
    for entry in frozen.series:
        series_id = entry["series_id"]
        history[series_id] = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series_id)
        bindings[series_id] = validate_frozen_dataset_series(store=storage.repo, entry={**entry, "dataset_id": frozen.dataset_id})
        causal[series_id] = storage.repo.read_facts(series_id=series_id, **window, known_at_lte=BASE + timedelta(seconds=5))
        latest[series_id] = storage.repo.read_series_records(series_id=series_id, **window)
    assert len(history[inner_series]) == len(history[outer_series]) == 2
    assert history[outer_series][-1].fact.state.value == "invalidated"
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(database=storage.database, object_store=store,
        temporary_directory=tmp_path / "staging")
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    if source_cold:
        for day in (source_day, inner_day):
            _archive_day(archive, day)
            reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    # Recipe rows stay in PostgreSQL and cannot change between deep verification
    # and the final metadata-only handoff. Keep that database guard enabled.
    with pytest.raises(DBAPIError, match="immutable.*normalization_specs"):
        with storage.database.session() as session:
            session.execute(text("UPDATE market.normalization_specs SET units='corrupt' WHERE id=:id"), {"id": spec.spec_id})
    _archive_day(archive, outer_day)
    with storage.database.session() as session:
        edges = session.execute(text("""
            SELECT source.* FROM market.fact_archive_canonical_dependencies AS edge
            JOIN market.fact_versions AS source ON source.id=edge.fact_version_id
            JOIN market.fact_archive_manifests AS manifest ON manifest.id=edge.manifest_id
            WHERE manifest.storage_day=:day
        """), {"day": outer_day}).mappings().all()
        assert Counter(row["fact_type"] for row in edges) == {"derivatives.funding_rate": 6, spec.output_fact_type: 1}
        assert all(row["known_at"] <= BASE + timedelta(seconds=4) for row in edges)
    if not source_cold:
        for day in (source_day, inner_day):
            _archive_day(archive, day)
            reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    # A valid root archive is insufficient if any interior canonical input is corrupt.
    with storage.database.session() as session:
        key = session.execute(text("SELECT object_key FROM market.fact_archive_manifests WHERE storage_day=:day"),
            {"day": source_day}).scalar_one()
    path = store.local_path(key)
    original = path.read_bytes()
    path.write_bytes(b"corrupt full normalization input page")
    with pytest.raises(RuntimeError, match="archive_verification"):
        reclaimer.reclaim_partition(outer_day, eligible_before=storage.today, execute=True)
    assert _physical(storage, outer_day)["relation"] is not None
    path.write_bytes(original)
    physical = _physical(storage, outer_day)
    assert reclaimer.reclaim_partition(outer_day, eligible_before=storage.today)["status"] == "dry_run"
    assert _physical(storage, outer_day) == physical
    removed = reclaimer.reclaim_partition(outer_day, eligible_before=storage.today, execute=True)
    assert removed["reclaimed_bytes"] == physical["bytes"] > 0 and removed["protected_dataset_ranges"] >= 2
    assert all(_physical(storage, day)["relation"] is None for day in (source_day, inner_day, outer_day))
    for entry in frozen.series:
        series_id = entry["series_id"]
        assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series_id) == history[series_id]
        assert validate_frozen_dataset_series(store=storage.repo, entry={**entry, "dataset_id": frozen.dataset_id}) == bindings[series_id]
        assert storage.repo.read_facts(series_id=series_id, **window, known_at_lte=BASE + timedelta(seconds=5)) == causal[series_id]
        assert storage.repo.read_series_records(series_id=series_id, **window) == latest[series_id]
    assert storage.repo.freeze_dataset(requests).dataset_hash == frozen.dataset_hash
    assert reclaimer.reclaim_partition(outer_day, eligible_before=storage.today, execute=True)["status"] == "already_reclaimed"
