from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import text

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.fact_archive import FactArchiveLimits
from portal.backend.service.storage.repos import fact_archival
from tests.test_market_data.test_fact_storage_tiers_db import storage, _placement, _ingest, _read

pytestmark = pytest.mark.db


def test_staging_is_bounded_source_complete_and_resumes_after_unacknowledged_publication(storage, tmp_path, monkeypatch):
    day = storage.today - timedelta(days=2)
    _placement(monkeypatch, day)
    for revision in range(3):
        value = f"0.{revision + 1}"
        _ingest(storage, replace(storage.fact, payload={**storage.fact.payload, "rate": value, "raw_rate": value},
                                 accepted_at=storage.fact.accepted_at + timedelta(seconds=revision),
                                 known_at=storage.fact.known_at + timedelta(seconds=revision)))
    before = _read(storage)
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=store, temporary_directory=tmp_path / "staging",
        limits=FactArchiveLimits(max_rows=2, row_group_size=2),
    )
    first = archive.inspect_partition(day)
    assert first["state"] == "open" and first["page_count"] == 0 and first["archived_rows"] == 0
    assert list(store.root.rglob("*.parquet")) == []
    assert not (tmp_path / "staging").exists()
    assert archive.inspect_partition(day) == first
    with pytest.raises(RuntimeError, match="canonical_archive_active_day"):
        archive.seal_partition(storage.today)
    sealed = archive.seal_partition(day)
    assert sealed["expected_rows"] == 3 and sealed["source_bytes"] > 0
    assert archive.seal_partition(day)["state"] == "sealed"

    tiny = fact_archival.PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=store, temporary_directory=tmp_path / "staging",
        limits=FactArchiveLimits(max_rows=2, row_group_size=2, max_logical_bytes=10),
    )
    with pytest.raises(RuntimeError, match="canonical_archive_source_row_budget_exceeded"):
        tiny.stage_next_page(day)
    assert archive.inspect_partition(day)["page_count"] == 0
    assert list(store.root.rglob("*.parquet")) == []

    def interrupted(*_, **__):
        raise RuntimeError("injected interruption before catalog commit")
    with monkeypatch.context() as patch:
        patch.setattr(fact_archival, "verify_canonical_fact_archive_rows", interrupted)
        with pytest.raises(RuntimeError, match="injected interruption"):
            archive.stage_next_page(day)
    assert archive.inspect_partition(day)["page_count"] == 0
    assert len(list(store.root.rglob("*.parquet"))) == 1

    first_page = archive.stage_next_page(day)
    assert first_page["page_ordinal"] == 0 and first_page["row_count"] == 2
    assert len(list(store.root.rglob("*.parquet"))) == 1  # Identical unacknowledged object reused.
    restarted = fact_archival.PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=store, temporary_directory=tmp_path / "staging",
        limits=FactArchiveLimits(max_rows=2, row_group_size=2),
    )
    second_page = restarted.stage_next_page(day)
    assert second_page["page_ordinal"] == 1 and second_page["row_count"] == 1
    assert restarted.stage_next_page(day)["status"] == "source_exhausted"
    finished = restarted.inspect_partition(day)
    assert finished["page_count"] == 2 and finished["archived_rows"] == 3
    assert finished["state"] == "sealed"  # Staging never grants deletion permission.
    assert _read(storage) == before
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_hot_payloads WHERE storage_day=:day"),
                               {"day": day}).scalar_one() == 3
        assert session.execute(text("SELECT sum(row_count) FROM market.fact_archive_series")).scalar_one() == 3


def test_book_page_checks_raw_bytes_and_commits_permanent_holds_without_dataset_pins(storage, tmp_path, monkeypatch):
    from market_data.contracts import SourceIdentity
    from market_data.fact_registry import get_fact_contract
    from portal.backend.service.storage.repos import market_structure, market_lifecycle
    from tests.test_market_data.test_structured_dataset_revision_identity_db import (
        _commit_book_source_archives, _book_feature_revisions,
    )
    monkeypatch.setattr(market_structure, "db", storage.database)
    monkeypatch.setattr(market_lifecycle, "db", storage.database)
    structures = market_structure.market_structure_repository
    lifecycle = market_lifecycle.market_storage_lifecycle_repository
    day = storage.today - timedelta(days=2)
    _placement(monkeypatch, day)
    source = SourceIdentity(provider="COINBASE", venue="COINBASE_DIRECT", source_kind="stream", adapter_version="archive.holds.fixture.v1")
    source_id = storage.repo.register_source(source, lineage={"fixture": "canonical-archive-holds"})
    series = {}
    for fact_type in ("market.l2_book", "market.bbo", "market.depth_observation"):
        series[fact_type] = storage.repo.register_series(
            instrument_id="storage-fixture", fact_type=fact_type, contract_version=get_fact_contract(fact_type).contract_version,
            timeframe_seconds=None if fact_type == "market.l2_book" else 1,
        )
    structures.upsert_stream_definition(
        definition_id="archive-holds", source_id=source_id, series_id=series["market.l2_book"],
        provider="COINBASE", venue="COINBASE_DIRECT", provider_product_id="BTC-USD", channels=("level2",),
        auth_mode="public", contract_version="market.l2_book.v1", max_spool_bytes=1024**3,
        max_segment_bytes=128 * 1024**2, config={"fixture": "canonical-archive-holds"},
    )
    claim = structures.claim_stream(definition_id="archive-holds", owner_id="archive-holds-test", lease_seconds=600, bounded=True)
    raw_ids = _commit_book_source_archives(tmp_path=tmp_path, claim=claim, receive_ordinals=(1, 2))
    for ordinal in (1, 2):
        bbo, _ = _book_feature_revisions(
            source=source, l2_series_id=series["market.l2_book"], bbo_series_id=series["market.bbo"],
            depth_series_id=series["market.depth_observation"], definition_id=claim.definition_id,
            session_id=claim.session_id, receive_ordinal=ordinal,
        )
        storage.repo.ingest_facts(series_id=series["market.bbo"], source_id=source_id, facts=[bbo])
    for identity in raw_ids:
        assert structures.archive_retention_status(target_kind="raw_manifest", target_id=identity)["pinned"] is False
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=store, temporary_directory=tmp_path / "staging",
    )
    archive.seal_partition(day)
    with storage.database.session() as session:
        key = session.execute(text("SELECT object_key FROM market.raw_archive_manifests WHERE id=:id"),
                              {"id": raw_ids[0]}).scalar_one()
    path = store.local_path(key)
    original = path.read_bytes()
    path.write_bytes(b"corrupted raw archive")
    with pytest.raises(RuntimeError, match="canonical_archive_dependency_corrupt"):
        archive.stage_next_page(day)
    assert archive.inspect_partition(day)["page_count"] == 0
    path.write_bytes(original)
    archive.stage_next_page(day)
    assert archive.inspect_partition(day)["archived_rows"] == 2
    for identity in raw_ids:
        public_status = structures.archive_retention_status(target_kind="raw_manifest", target_id=identity)
        execution_status = lifecycle.archive_target_status(target_kind="raw_manifest", target_id=identity)
        assert public_status["dataset_pin_count"] == execution_status["dataset_pin_count"] == 0
        assert public_status["canonical_dependency_count"] == execution_status["canonical_dependency_count"] == 1
        assert public_status["pinned"] is execution_status["pinned"] is True
        assert public_status["ordinary_retention_eligible"] is False
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_archive_material_aliases")).scalar_one() == 2
        assert session.execute(text("SELECT count(*) FROM market.fact_archive_dependencies")).scalar_one() == 2
