"""Derivative-state retention binds exact numeric commits, never latest inputs."""
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import text

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.canonical_adapters import DERIVED_MARKET_STATE_SOURCE, canonicalize_derivative_state_feature
from market_data.contracts import DatasetSeriesRequest
from market_data.market_state import derive_derivative_state_features
from portal.backend.service.market.backtest_dataset_service import validate_frozen_dataset_series
from portal.backend.service.storage.repos import fact_archival, fact_reclamation, market_data, market_structure
from portal.backend.service.storage.repos.fact_derived_admission import resolve_material_source_revisions
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from tests.test_market_data.test_fact_storage_tiers_db import storage, _placement
from tests.test_market_data.test_fact_derived_retention_db import _archive_day
from tests.test_market_data.test_fact_reclamation_db import _physical
from tests.test_market_data.test_market_state_phase3 import BASE, _oi_record, _funding_record

pytestmark = pytest.mark.db


@pytest.mark.parametrize("source_cold", [False, True, "legacy"])
def test_derivative_reclaims_with_exact_previous_commit_and_frozen_history(storage, tmp_path, monkeypatch, source_cold):
    legacy = source_cold == "legacy"
    source_cold = source_cold is True
    source_day = storage.today - timedelta(days=3)
    day = source_day + timedelta(days=1)
    _placement(monkeypatch, source_day)
    monkeypatch.setattr(market_structure, "db", storage.database)
    monkeypatch.setattr(market_structure, "market_data_repo", storage.repo)
    structures = market_structure.PostgresMarketStructureRepository()
    oi_series = storage.repo.register_series(instrument_id="storage-fixture", fact_type="derivatives.open_interest",
        contract_version="derivatives.open_interest.v1", timeframe_seconds=None)
    funding_series = storage.repo.register_series(instrument_id="storage-fixture", fact_type="derivatives.funding_rate",
        contract_version="derivatives.funding_rate.v1", timeframe_seconds=None)
    series_id = storage.repo.register_series(instrument_id="storage-fixture", fact_type="market.derivative_state",
        contract_version="market.derivative_state.v1", timeframe_seconds=None)
    oi_facts = [_oi_record(oi_series, 0, 100, 1).fact, _oi_record(oi_series, 60, 110, 2).fact]
    funding_fact = _funding_record(funding_series, 60, 3).fact
    storage.repo.ingest_open_interest(series_id=oi_series, source_id=storage.source_id, facts=oi_facts)
    storage.repo.ingest_funding_rates(series_id=funding_series, source_id=storage.source_id, facts=[funding_fact])
    window = dict(start=BASE - timedelta(seconds=1), end=BASE + timedelta(minutes=3))
    oi = storage.repo.read_open_interest(series_id=oi_series, **window)
    funding = storage.repo.read_funding_rates(series_id=funding_series, **window)
    expected_commits = {item.market_commit_seq for item in (*oi, *funding)}
    derived = derive_derivative_state_features(instrument_id="storage-fixture", oi_records=oi,
        funding_records=funding, oi_gaps=(), series_id=series_id, expected_oi_interval_seconds=60,
        computed_at=BASE + timedelta(seconds=121))
    assert len(derived) == 1 and derived[0].oi_log_change != 0
    # Both candidates are causal at output time. Only the first previous-OI
    # commit is fingerprint-bound, even though the second has the same value.
    alternative = replace(oi_facts[0], known_at=BASE + timedelta(milliseconds=10),
                          accepted_at=BASE + timedelta(milliseconds=10))
    alternative_outcome = storage.repo.ingest_open_interest(series_id=oi_series, source_id=storage.source_id,
                                                           facts=[alternative])
    assert alternative_outcome.max_commit_seq not in expected_commits
    _placement(monkeypatch, day)
    structures.ingest_market_state_features(derivative_facts=derived)
    source_id = storage.repo.register_source(DERIVED_MARKET_STATE_SOURCE)
    root = canonicalize_derivative_state_feature(derived[0])
    corrected = replace(root, known_at=root.known_at + timedelta(milliseconds=100),
        accepted_at=root.known_at + timedelta(milliseconds=100), provenance={**root.provenance, "fixture_revision": 2})
    invalidated = replace(corrected, state="invalidated", known_at=root.known_at + timedelta(milliseconds=200),
        accepted_at=root.known_at + timedelta(milliseconds=200), provenance={**root.provenance, "fixture_revision": 3})
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    reader = FilesystemRawArchiveObjectStore(store.root, writable=False)
    tiered = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: reader)
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository", tiered)
    monkeypatch.setattr(market_structure, "canonical_fact_storage_repository", tiered)
    request = DatasetSeriesRequest(series_id, **window)
    with monkeypatch.context() as prior_selection:
        prior_selection.setattr(market_data, "_preserves_canonical_revision_history", lambda version: False)
        older_dataset = storage.repo.freeze_dataset([request])
    with pytest.raises(RuntimeError, match="market_dataset_revision_history_unpinned"):
        storage.repo.read_dataset_fact_revisions(dataset_id=older_dataset.dataset_id, series_id=series_id)
    # A historical latest-only freeze must precede invalidation of the sole
    # observation; the old contract rightly refuses an empty latest dataset.
    for revision in (corrected, invalidated):
        storage.repo.ingest_facts(series_id=series_id, source_id=source_id, facts=[revision])
    frozen = storage.repo.freeze_dataset([request])
    assert frozen.dataset_hash != older_dataset.dataset_hash
    before = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series_id)
    assert len(before) == 3 and before[-1].fact.state.value == "invalidated"
    binding_args = dict(store=storage.repo, entry={**frozen.series[0], "dataset_id": frozen.dataset_id})
    binding_before = validate_frozen_dataset_series(**binding_args)
    typed_args = dict(series_id=series_id, **window, known_at=root.known_at)
    typed_before = structures.read_derivative_state_features(**typed_args)
    assert len(typed_before) == 1 and typed_before[0].material_hash == derived[0].material_hash
    known_args = dict(series_id=series_id, **window, known_at_lte=root.known_at)
    known_before = storage.repo.read_facts(**known_args)
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(database=storage.database,
        object_store=store, temporary_directory=tmp_path / "staging")
    reclaimer = fact_reclamation.PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    if source_cold:
        _archive_day(archive, source_day)
        reclaimer.reclaim_partition(source_day, eligible_before=storage.today, execute=True)
        assert _physical(storage, source_day)["relation"] is None
    with monkeypatch.context() as prior:
        if legacy:
            original_dependencies = archive._dependencies
            prior.setattr(fact_archival, "FACT_ARCHIVE_VERIFIER_VERSION", "market.canonical_archive_verification.v7")
            prior.setattr(fact_archival, "legacy_material_alias", lambda row: None)
            prior.setattr(archive, "_dependencies", lambda *args, **kwargs: (original_dependencies(*args, **kwargs)[0], []))
        _archive_day(archive, day)
    if legacy:
        with storage.database.session() as session:
            old_page = dict(session.execute(text("SELECT * FROM market.fact_archive_manifests WHERE storage_day=:day"),
                                           {"day": day}).mappings().one())
            old_receipt = dict(session.execute(text("SELECT * FROM market.fact_archive_verifications WHERE manifest_id=:id"),
                                              {"id": old_page["id"]}).mappings().one())
            for table in ("fact_archive_material_aliases", "fact_archive_canonical_dependencies"):
                assert session.execute(text(f"SELECT count(*) FROM market.{table} WHERE manifest_id=:id"),
                                       {"id": old_page["id"]}).scalar_one() == 0
        with pytest.raises(RuntimeError, match="canonical_archive_verification_missing_or_stale"):
            reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
        assert _physical(storage, day)["relation"] is not None
        archive.restart_partition_verification(day)
        assert archive.verify_next_page(day)["status"] == "page_verified"
        archive.verify_partition(day)
        with storage.database.session() as session:
            assert dict(session.execute(text("SELECT * FROM market.fact_archive_manifests WHERE id=:id"),
                                        {"id": old_page["id"]}).mappings().one()) == old_page
            assert dict(session.execute(text("SELECT * FROM market.fact_archive_verifications "
                "WHERE manifest_id=:id AND verifier_version=:version"),
                {"id": old_page["id"], "version": old_receipt["verifier_version"]}).mappings().one()) == old_receipt
    with storage.database.session() as session:
        edges = session.execute(text("""
            SELECT source.market_commit_seq FROM market.fact_archive_canonical_dependencies AS edge
            JOIN market.fact_archive_manifests AS manifest ON manifest.id=edge.manifest_id
            JOIN market.fact_versions AS source ON source.id=edge.fact_version_id
            WHERE manifest.storage_day=:day
        """), {"day": day}).scalars().all()
        assert len(edges) == 3 and set(edges) == expected_commits
        assert session.execute(text("SELECT count(*) FROM market.fact_archive_material_aliases AS alias "
            "JOIN market.fact_archive_manifests AS manifest ON manifest.id=alias.manifest_id WHERE manifest.storage_day=:day"),
            {"day": day}).scalar_one() == 3
    if not source_cold:
        # Placement may change after verification without rewriting the proof.
        _archive_day(archive, source_day)
        reclaimer.reclaim_partition(source_day, eligible_before=storage.today, execute=True)
    with storage.database.session() as session:
        source_page = session.execute(text("SELECT object_key FROM market.fact_archive_manifests WHERE storage_day=:day"),
                                      {"day": source_day}).scalar_one()
    path = store.local_path(source_page)
    original = path.read_bytes()
    path.write_bytes(b"corrupt derivative numeric input after verification")
    with pytest.raises(RuntimeError, match="archive_verification"):
        reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    assert _physical(storage, day)["relation"] is not None
    path.write_bytes(original)
    physical = _physical(storage, day)
    result = reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    assert result["status"] == "partition_reclaimed" and result["reclaimed_bytes"] == physical["bytes"] > 0
    assert _physical(storage, day)["relation"] is None
    assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=series_id) == before
    assert validate_frozen_dataset_series(**binding_args) == binding_before
    assert storage.repo.freeze_dataset([request]).dataset_hash == frozen.dataset_hash
    assert storage.repo.read_facts(**known_args) == known_before
    assert structures.read_derivative_state_features(**typed_args) == typed_before
    with storage.database.session() as session:
        sources, _ = resolve_material_source_revisions(session, requests=[{
            "root_id": "later-derived-input", "role": "derivative", "series_id": series_id,
            "fact_type": "market.derivative_state", "material_hash": derived[0].material_hash,
            "commit_seq": before[-1].market_commit_seq, "known_at": invalidated.known_at,
        }], reader=tiered, max_rows=10, max_logical_bytes=64 * 1024**2)
        assert {row["id"] for row in sources.values()} == {record.fact_version_id for record in before}
