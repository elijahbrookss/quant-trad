from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.fact_archive import FactArchiveLimits
from market_data.archive_verification import ArchiveVerificationLimits
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

    with pytest.raises(RuntimeError, match="verification_missing_or_stale"):
        restarted.verify_partition(day)
    assert restarted.verify_next_page(day)["page_ordinal"] == 0

    def fail_receipt_commit(_conn, _cursor, statement, *_):
        if statement.startswith("INSERT INTO market.fact_archive_verifications"):
            raise RuntimeError("injected interruption after receipt insert")
    event.listen(storage.database._engine, "after_cursor_execute", fail_receipt_commit)
    try:
        with pytest.raises(RuntimeError, match="injected interruption after receipt insert"):
            restarted.verify_next_page(day)
    finally:
        event.remove(storage.database._engine, "after_cursor_execute", fail_receipt_commit)
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_archive_verifications")).scalar_one() == 1
    # A fresh repository resumes the same failed page, not the first page.
    assert archive.verify_next_page(day)["page_ordinal"] == 1
    assert archive.verify_next_page(day)["status"] == "no_unverified_pages"
    with pytest.raises(DBAPIError, match="immutable market-data relation"):
        with storage.database.session() as session:
            session.execute(text("DELETE FROM market.fact_archive_verifications"))
    with pytest.raises(RuntimeError, match="partition_page_budget_exceeded"):
        archive.verify_partition(day, limits=ArchiveVerificationLimits(max_pages=1))
    with pytest.raises(RuntimeError, match="byte_budget_exceeded"):
        archive.verify_partition(day, limits=ArchiveVerificationLimits(max_bytes=1))
    with storage.database.session() as session:
        key = session.execute(text("SELECT object_key FROM market.fact_archive_manifests WHERE page_ordinal=0")).scalar_one()
    path = store.local_path(key)
    data = path.read_bytes()
    path.write_bytes(b"x" * len(data))
    with pytest.raises(RuntimeError, match="checksum_mismatch"):
        archive.verify_partition(day)
    assert archive.inspect_partition(day)["state"] == "sealed"
    path.write_bytes(data)
    path.unlink()
    with pytest.raises(FileNotFoundError):
        archive.verify_partition(day)
    path.write_bytes(data)

    # Resumed whole-partition verification rehashes bytes without decoding all
    # canonical rows again; the immutable page receipts carry that deep proof.
    with monkeypatch.context() as patch:
        patch.setattr(fact_archival, "read_canonical_fact_archive", lambda *_, **__: pytest.fail("unexpected page re-decode"))
        verified = archive.verify_partition(day)
    assert verified["row_count"] == 3 and verified["page_count"] == verified["verified_objects"] == 2
    assert archive.verify_partition(day) == verified
    assert archive.inspect_partition(day)["state"] == "verified"
    assert _read(storage) == before
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_hot_payloads WHERE storage_day=:day"),
                               {"day": day}).scalar_one() == 3


def test_book_page_checks_raw_bytes_and_commits_permanent_holds_without_dataset_pins(storage, tmp_path, monkeypatch):
    from market_data.market_state import derive_book_features, MarketStateValuationContract
    from tests.test_market_data.test_fact_storage_tiers_db import BASE
    from portal.backend.service.storage.repos import market_structure, market_lifecycle
    from tests.test_market_data.test_fact_raw_lineage_db import _raw_book_fixture, _publish_book_result
    monkeypatch.setattr(market_structure, "db", storage.database)
    monkeypatch.setattr(market_lifecycle, "db", storage.database)
    structures = market_structure.market_structure_repository
    lifecycle = market_lifecycle.market_storage_lifecycle_repository
    # Use real retained L2 source revisions and operational metadata. The old
    # fixture fabricated orphan BBO states, which cannot satisfy source proof.
    fixture = _raw_book_fixture(storage, tmp_path, monkeypatch, replay_features=True)
    day = fixture.day
    for index in range(len(fixture.results)):
        _publish_book_result(fixture, index)
    config = fixture.claim.config
    bbo, _ = derive_book_features((item.state for item in fixture.results),
        contract=MarketStateValuationContract(product_definition_version_id=config["product_definition_version_id"],
            provider_size_unit="base", base_currency="BTC", quote_currency="USD"),
        bbo_series_id=config["bbo_series_id"], depth_series_id=config["depth_series_id"],
        computed_at=BASE + timedelta(minutes=1))
    structures.ingest_market_state_features(bbo_facts=bbo, depth_facts=())
    raw_ids = fixture.manifests
    for identity in raw_ids:
        status = structures.archive_retention_status(target_kind="raw_manifest", target_id=identity)
        assert status["pinned"] is True
        assert status["canonical_backlog_present"] is True
        assert status["canonical_dependency_count"] == 0
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
    with pytest.raises(RuntimeError, match="archive_verification_size_mismatch"):
        archive.stage_next_page(day)
    assert archive.inspect_partition(day)["page_count"] == 0
    path.write_bytes(b"x" * len(original))
    with pytest.raises(RuntimeError, match="archive_verification_checksum_mismatch"):
        archive.stage_next_page(day)
    assert archive.inspect_partition(day)["page_count"] == 0
    path.write_bytes(original)
    for _ in range(10):
        if archive.stage_next_page(day)["status"] == "source_exhausted":
            break
    assert archive.inspect_partition(day)["archived_rows"] == 4
    for identity in raw_ids:
        public_status = structures.archive_retention_status(target_kind="raw_manifest", target_id=identity)
        execution_status = lifecycle.archive_target_status(target_kind="raw_manifest", target_id=identity)
        assert public_status["dataset_pin_count"] == execution_status["dataset_pin_count"] == 0
        # One page edge and one shared-prefix edge independently hold each raw object.
        assert public_status["canonical_dependency_count"] == execution_status["canonical_dependency_count"] == 2
        assert public_status["pinned"] is execution_status["pinned"] is True
        assert public_status["ordinary_retention_eligible"] is False
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_archive_material_aliases")).scalar_one() == 2
        assert session.execute(text("SELECT count(*) FROM market.fact_archive_dependencies")).scalar_one() == 3
        assert session.execute(text("SELECT count(*) FROM market.fact_archive_canonical_dependencies")).scalar_one() == 2
    assert archive.verify_next_page(day)["status"] == "page_verified"
    path.write_bytes(b"x" * len(original))
    with pytest.raises(RuntimeError, match="checksum_mismatch"):
        archive.verify_partition(day)
    assert archive.inspect_partition(day)["state"] == "sealed"
    path.write_bytes(original)
    assert archive.verify_partition(day)["verified_objects"] == 4


def test_page_receipts_do_not_cover_unstaged_rows_or_changed_catalogs(storage, tmp_path, monkeypatch):
    from portal.backend.db import MarketFactArchiveMaterialAliasRecord
    day = storage.today - timedelta(days=2)
    _placement(monkeypatch, day)
    for revision in range(2):
        _ingest(storage, replace(storage.fact, observation_key=f"receipt-coverage-{revision}"))
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=store, temporary_directory=tmp_path / "staging",
        limits=FactArchiveLimits(max_rows=1, row_group_size=1),
    )
    archive.seal_partition(day)
    first = archive.stage_next_page(day)
    actual_catalogs = archive._page_catalogs
    def missing_series(*args):
        return {**actual_catalogs(*args), "series": []}
    with monkeypatch.context() as patch:
        patch.setattr(archive, "_page_catalogs", missing_series)
        with pytest.raises(RuntimeError, match="catalog_incomplete.*catalog=series"):
            archive.verify_next_page(day)
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_archive_verifications")).scalar_one() == 0
    archive.verify_next_page(day)
    with pytest.raises(RuntimeError, match="source_coverage_mismatch"):
        archive.verify_partition(day)
    archive.stage_next_page(day)
    with pytest.raises(RuntimeError, match="verification_missing_or_stale"):
        archive.verify_partition(day)
    archive.verify_next_page(day)
    # Immutability prevents changing existing entries, but inserting a new
    # false alias after a receipt must invalidate its exact catalog binding.
    with storage.database.session() as session:
        fact_id = session.execute(text("SELECT first_id FROM market.fact_archive_manifests WHERE id=:id"),
                                  {"id": first["manifest_id"]}).scalar_one()
        session.add(MarketFactArchiveMaterialAliasRecord(
            manifest_id=first["manifest_id"], fact_version_id=fact_id, series_id=storage.series_id,
            evidence_key="forged-test-alias", material_hash="a" * 64,
        ))
    with pytest.raises(RuntimeError, match="verification_missing_or_stale"):
        archive.verify_partition(day)
    assert archive.inspect_partition(day)["state"] == "sealed"


def test_page_verification_fences_expiry_and_other_workers_but_allows_collection(storage, tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
    day = storage.today - timedelta(days=2)
    _placement(monkeypatch, day)
    _ingest(storage)
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=FilesystemRawArchiveObjectStore(tmp_path / "objects"),
        temporary_directory=tmp_path / "staging",
    )
    archive.seal_partition(day)
    archive.stage_next_page(day)
    started, release = Event(), Event()
    real_read = fact_archival.read_canonical_fact_archive
    def paused_read(*args, **kwargs):
        started.set()
        assert release.wait(30), "test did not release the verification worker"
        return real_read(*args, **kwargs)
    monkeypatch.setattr(fact_archival, "read_canonical_fact_archive", paused_read)
    _placement(monkeypatch, storage.today)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(archive.verify_next_page, day)
        try:
            assert started.wait(30), "verification worker did not reach archive read"
            with storage.database.session() as session:
                assert session.execute(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"),
                                       {"name": _LIFECYCLE_LOCK_NAME}).scalar_one() is False
                assert session.execute(text("SELECT pg_try_advisory_xact_lock_shared(hashtextextended(:name,0))"),
                                       {"name": _LIFECYCLE_LOCK_NAME}).scalar_one() is True
            with pytest.raises(RuntimeError, match="canonical_archive_partition_busy"):
                archive.verify_next_page(day)
            fresh = _ingest(storage, replace(storage.fact, observation_key="collected-during-verification"))
            assert fresh.inserted_count == 1
        finally:
            release.set()
        assert future.result(timeout=30)["status"] == "page_verified"
    with storage.database.session() as session:
        assert session.execute(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"),
                               {"name": _LIFECYCLE_LOCK_NAME}).scalar_one() is True
