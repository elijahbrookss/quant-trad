from datetime import timedelta
from decimal import Decimal

import pytest
from sqlalchemy import text

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.contracts import DatasetSeriesRequest
from market_data.normalization import NormalizationInput, NormalizationSpec, evaluate_normalization
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.cold_retirement_schema import prepare_cold_dependency_bindings
from portal.backend.service.storage.repos import market_data, normalization
from portal.backend.service.storage.repos.cold_retirement import prune_oldest_retired_identities
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos.fact_archive_generation import canonical_dependency_projection
from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from tests.test_market_data.test_canonical_cold_activate_db import _configure_fixture_catalog
from tests.test_market_data.test_fact_derived_retention_db import _archive_day
from tests.test_market_data.test_fact_reclamation_db import _physical
from tests.test_market_data.test_fact_storage_tiers_db import BASE, _ingest, _placement, storage

pytestmark = pytest.mark.db


def test_recurring_handoff_requires_an_unchanged_cold_only_dependency(storage, tmp_path, monkeypatch):
    source_day, derived_day = (storage.today - timedelta(days=days) for days in (2, 1))
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    fact_reader = PostgresCanonicalFactStorageRepository(
        object_store_factory=lambda: store, read_cache_factory=lambda: None,
    )
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository", fact_reader)
    monkeypatch.setattr(normalization, "canonical_fact_storage_repository", fact_reader)
    monkeypatch.setattr(normalization, "market_data_repo", storage.repo)
    monkeypatch.setattr(normalization, "db", storage.database)

    _placement(monkeypatch, source_day)
    _ingest(storage)
    start, end = BASE - timedelta(seconds=1), BASE + timedelta(seconds=1)
    source = storage.repo.read_fact_revisions(series_id=storage.series_id, start=start, end=end)[0]
    archive = PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=store, temporary_directory=tmp_path / "staging",
    )
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    _archive_day(archive, source_day)
    with storage.database.session() as session:
        identity = session.scalar(text("""
            SELECT control.system_identifier::text || '/' || db.oid::text
            FROM pg_control_system() AS control CROSS JOIN pg_database AS db
            WHERE db.datname=current_database()
        """))
        _configure_fixture_catalog(session.connection(), identity, store.root)
        prepare_cold_dependency_bindings(session.connection())
        session.execute(text("UPDATE market.fact_storage_state SET layout_version=:layout"),
                        {"layout": COLD_HISTORY_LAYOUT_VERSION})

    def reclaim(day):
        return reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)["status"]

    assert reclaim(source_day) == "cold_page_admitted"
    assert reclaim(source_day) == "partition_reclaimed"
    assert prune_oldest_retired_identities(storage.database, deadline=lambda: None)["day_complete"]
    with storage.database.session() as session:
        assert not session.scalar(text("SELECT EXISTS (SELECT FROM market.fact_identities WHERE id=:id)"),
                                  {"id": source.fact_version_id})
        assert not session.scalar(text("SELECT EXISTS (SELECT FROM market.fact_versions WHERE id=:id)"),
                                  {"id": source.fact_version_id})
        claims = fact_reader.cold_history(session).placement_claims(session, [source.fact_version_id])
        source_location, row_hash = claims[source.fact_version_id]
        assert row_hash == source.row_hash

    recipes = normalization.PostgresNormalizationRepository()
    spec = recipes.register_spec(NormalizationSpec(
        feature_name="cold_source_bps", semantic_version="1", input_fact_type=storage.fact.fact_type,
        output_fact_type="market.normalized.cold_source_bps", formula="basis_points", units="bps",
        window_seconds=None, minimum_observations=0, warmup_observations=0,
    ))
    derived_series = storage.repo.register_series(
        instrument_id="storage-fixture", fact_type=spec.output_fact_type, timeframe_seconds=None,
        contract_version=f"market.normalized_feature.v1/{spec.spec_id}",
    )
    derived = evaluate_normalization(spec, [NormalizationInput(
        source_series_id=source.series_id, effective_at=source.fact.observation_time, known_at=source.fact.known_at,
        market_commit_seq=source.market_commit_seq, material_hash=source.fact.material_hash,
        value=Decimal(source.fact.payload["rate"]),
    )], output_series_id=derived_series)[0]
    _placement(monkeypatch, derived_day)
    recipes.ingest([derived])
    frozen = storage.repo.freeze_dataset([
        DatasetSeriesRequest(series_id, start, end) for series_id in (storage.series_id, derived_series)
    ])
    before = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=derived_series)
    _archive_day(archive, derived_day)
    with storage.database.session() as session:
        edges = session.execute(text(f"""
            SELECT edge.fact_version_id,edge.row_hash FROM {canonical_dependency_projection('edge')}
            JOIN market.fact_archive_manifests AS page ON page.id=edge.manifest_id
            WHERE page.storage_day=:day
        """), {"day": derived_day}).all()
        assert edges == [(source.fact_version_id, source.row_hash)]
    assert reclaim(derived_day) == "cold_page_admitted"

    # Parquet verification in the first phase must read the cold-only source.
    path = store.local_path(source_location.manifest.object_key)
    original = path.read_bytes()
    path.write_bytes(b"x" * len(original))
    try:
        with pytest.raises(RuntimeError, match="checksum_mismatch"):
            reclaim(derived_day)
    finally:
        path.write_bytes(original)
    assert _physical(storage, derived_day)["header_rows"] == 1

    # Make the exact locator stale inside the final transaction after the
    # first proof has captured its visible page. Its rollback restores the
    # injected change as well as retaining the derived header and payload.
    original_timeouts = reclaimer._timeouts
    calls = 0

    def hide_source_in_handoff(session):
        nonlocal calls
        original_timeouts(session)
        calls += 1
        if calls == 2:
            session.execute(text("ALTER TABLE market.cold_fact_pages DISABLE TRIGGER trg_cold_fact_immutable"))
            assert session.execute(text("""
                UPDATE market.cold_fact_pages SET staged_storage_day=:day WHERE id=:page
            """), {"day": storage.today, "page": source_location.manifest.manifest_id}).rowcount == 1
            session.execute(text("ALTER TABLE market.cold_fact_pages ENABLE ALWAYS TRIGGER trg_cold_fact_immutable"))

    with monkeypatch.context() as patch:
        patch.setattr(reclaimer, "_timeouts", hide_source_in_handoff)
        with pytest.raises(RuntimeError, match="canonical_handoff_cold_source_changed"):
            reclaim(derived_day)
    assert calls == 2
    assert _physical(storage, derived_day)["header_rows"] == 1
    assert reclaim(derived_day) == "partition_reclaimed"
    assert _physical(storage, derived_day)["header_rows"] == 0
    assert reclaim(derived_day) == "already_reclaimed"
    assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=derived_series) == before
