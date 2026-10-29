from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.fact_archive import FactArchiveLimits
from market_data.archive_verification import ArchiveVerificationLimits
from portal.backend.service.storage.repos import fact_archival
from portal.backend.service.storage.repos.fact_archive_generation import (
    canonical_dependency_projection, material_alias_projection,
)
from tests.test_market_data.test_fact_storage_tiers_db import storage, _placement, _ingest, _read

pytestmark = pytest.mark.db


def test_stage_database_snapshot_reads_postgresql_counters(storage):
    with storage.database.session() as session:
        snapshot = fact_archival._stage_database_snapshot(session)

    assert snapshot is not None
    assert snapshot["backend_pid"] > 0
    assert snapshot["server_version_num"] >= 150000
    assert isinstance(snapshot["track_io_timing"], bool)
    assert snapshot["wal_stats_reset"] is not None
    assert snapshot["writer_stats_reset"] is not None
    for name in fact_archival._ARCHIVE_STAGE_DATABASE_COUNTERS:
        assert snapshot[name] is not None
        assert snapshot[name] >= 0


@pytest.mark.parametrize("text_value", ["ascii-" * 60, "\u007f\u00e9\u20ac\U0001f680\\\"\n" * 16],
                         ids=["ascii", "escaped-unicode"])
def test_page_packing_keeps_codec_byte_bound_and_resumes_without_skipping(
        storage, tmp_path, monkeypatch, text_value):
    from market_data.fact_archive import _json_bytes, read_canonical_fact_archive
    from portal.backend.service.storage.repos.fact_storage import CANONICAL_ROW_COLUMNS, CANONICAL_ROW_FROM

    day = storage.today - timedelta(days=2)
    _placement(monkeypatch, day)
    for index in range(3):
        _ingest(storage, replace(storage.fact, observation_key=f"packing-{index}-{text_value}"))
    with storage.database.session() as session:
        source = session.execute(text(f"""
            SELECT {CANONICAL_ROW_COLUMNS} {CANONICAL_ROW_FROM}
            WHERE versions.storage_day=:day ORDER BY versions.market_commit_seq,versions.id
        """), {"day": day}).mappings().all()
    expected = [{key: value for key, value in row.items() if key != "storage_day"} for row in source]
    # Enough for two ASCII rows and SQL's whitespace/clock allowance, but not
    # three. The former six-times-every-byte estimate could not admit even one.
    byte_limit = sum(len(_json_bytes(row)) for row in expected[:2]) + 512
    assert sum(len(_json_bytes(row)) for row in expected) > byte_limit
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=store, temporary_directory=tmp_path / "staging",
        limits=FactArchiveLimits(max_rows=3, row_group_size=3, max_logical_bytes=byte_limit),
    )
    archive.seal_partition(day)
    pages = []
    for _ in range(4):
        result = archive.stage_next_page(day)
        if result["status"] == "source_exhausted":
            break
        pages.append(result)
    assert result["status"] == "source_exhausted"
    if text_value.isascii() and "\u007f" not in text_value:
        assert [page["row_count"] for page in pages] == [2, 1]
    with storage.database.session() as session:
        manifests = session.execute(text(
            "SELECT * FROM market.fact_archive_manifests WHERE storage_day=:day ORDER BY page_ordinal"
        ), {"day": day}).mappings().all()
    restored = []
    for row in manifests:
        manifest = fact_archival._catalog_manifest(row)
        assert manifest.logical_byte_count <= byte_limit
        restored.extend(read_canonical_fact_archive(
            store.local_path(manifest.object_key), expected=manifest, limits=archive.limits))
    assert restored == expected
    assert archive.inspect_partition(day)["state"] == "sealed"


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

    with pytest.raises(RuntimeError, match="verification_generation_changed"):
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
        assert session.execute(text("SELECT count(*) FROM market.fact_archive_material_aliases")).scalar_one() == 0
        assert session.execute(text(f"SELECT count(*) FROM {material_alias_projection()}")).scalar_one() == 2
        assert session.execute(text("SELECT count(*) FROM market.fact_archive_dependencies")).scalar_one() == 3
        assert session.execute(text("SELECT count(*) FROM market.fact_archive_canonical_dependencies")).scalar_one() == 0
        assert session.execute(text(f"SELECT count(*) FROM {canonical_dependency_projection()}")).scalar_one() == 2
    assert archive.verify_next_page(day)["status"] == "page_verified"
    path.write_bytes(b"x" * len(original))
    with pytest.raises(RuntimeError, match="checksum_mismatch"):
        archive.verify_partition(day)
    assert archive.inspect_partition(day)["state"] == "sealed"
    path.write_bytes(original)
    assert archive.verify_partition(day)["verified_objects"] == 4
    # Final verification and reclamation must both recheck expiration even
    # after a successful page/partition receipt, without touching hot data.
    lifecycle.append_event(operation_id="late-expiration", action="archive_expire",
        event_type="completed", target_kind="raw_manifest", target_id=raw_ids[-1], evidence={})
    with pytest.raises(RuntimeError, match=f"canonical_archive_dependency_expired: target_id={raw_ids[-1]}"):
        archive.verify_partition(day)
    from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    with pytest.raises(RuntimeError, match="canonical_archive_dependency_expired"):
        reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_hot_payloads WHERE storage_day=:day"),
                               {"day": day}).scalar_one() == 4
    assert path.read_bytes() == original


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
    with storage.database.session() as session:
        first_generation = session.execute(text(
            "SELECT generation FROM market.fact_archive_active_generations WHERE manifest_id=:id"
        ), {"id": first["manifest_id"]}).scalar_one()
    actual_catalogs = archive._page_catalogs
    def missing_series(*args):
        return {**actual_catalogs(*args), "series": []}
    with monkeypatch.context() as patch:
        patch.setattr(archive, "_page_catalogs", missing_series)
        assert archive.verify_next_page(day)["status"] == "page_verified"
    with storage.database.session() as session:
        assert session.execute(text(
            "SELECT generation FROM market.fact_archive_active_generations WHERE manifest_id=:id"
        ), {"id": first["manifest_id"]}).scalar_one() == first_generation + 1
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_archive_verifications")).scalar_one() == 1
    assert archive.verify_next_page(day)["status"] == "no_unverified_pages"
    with pytest.raises(RuntimeError, match="source_coverage_mismatch"):
        archive.verify_partition(day)
    archive.stage_next_page(day)
    with pytest.raises(RuntimeError, match="canonical_archive_verification_generation_changed"):
        archive.verify_partition(day)
    archive.verify_next_page(day)
    # Legacy rows remain insert-only for historical compatibility, but an
    # active generation excludes them from its authoritative catalog projection.
    with storage.database.session() as session:
        fact_id = session.execute(text("SELECT first_id FROM market.fact_archive_manifests WHERE id=:id"),
                                  {"id": first["manifest_id"]}).scalar_one()
        session.add(MarketFactArchiveMaterialAliasRecord(
            manifest_id=first["manifest_id"], fact_version_id=fact_id, series_id=storage.series_id,
            evidence_key="forged-test-alias", material_hash="a" * 64,
        ))
    # A legacy row is not authoritative once this manifest has an active
    # prepared generation. It cannot extend or invalidate that generation's
    # verification receipt.
    assert archive.verify_partition(day)["row_count"] == 2
    assert archive.inspect_partition(day)["state"] == "verified"


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


@pytest.mark.parametrize("interruption", [None, "guard", "cancel", "commit", "cursor", "limits"])
def test_prepared_page_reuse_rechecks_admission_and_durable_cursor(
        storage, tmp_path, monkeypatch, interruption):
    from portal.backend.service.market.canonical_retention import CanonicalRetentionStopRequested

    day = storage.today - timedelta(days=2)
    _placement(monkeypatch, day)
    for index in range(2):
        _ingest(storage, replace(storage.fact, observation_key=f"prepared-{index}"))
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    kwargs = dict(database=storage.database, object_store=store,
                  temporary_directory=tmp_path / "staging",
                  limits=FactArchiveLimits(max_rows=1, row_group_size=1))
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(**kwargs)
    archive.seal_partition(day)
    source_reads, prefix_calls = [], []

    def observe_sql(_conn, _cursor, statement, *_):
        if "WITH candidates AS MATERIALIZED" in statement:
            source_reads.append(statement)

    def first_prefix(*args, **kw):
        prefix_calls.append(True)
        if len(prefix_calls) == 1:
            return {"status": "book_prefix_verified", "storage_day": day.isoformat()}
        return None

    # Funding is self-contained. Force the same early return used by a committed
    # book prefix, while exercising real page SQL, locks, publication and reads.
    monkeypatch.setattr(archive, "_prepare_book_prefix", first_prefix)
    event.listen(storage.database._engine, "after_cursor_execute", observe_sql)
    try:
        assert archive.stage_next_page(day)["status"] == "book_prefix_verified"
        assert len(source_reads) == 1
        if interruption in {"guard", "cancel"}:
            error = CanonicalRetentionStopRequested if interruption == "cancel" else RuntimeError

            def refuse(*_):
                raise error("injected current admission refusal")

            archive.partition_guard = refuse
            with pytest.raises(error, match="current admission refusal"):
                archive.stage_next_page(day)
            assert len(source_reads) == 1  # Refuse before using cached input.
            archive.partition_guard = None
        elif interruption == "commit":
            def fail_commit(session):
                raise RuntimeError("injected prefix commit failure")

            with monkeypatch.context() as patch:
                patch.setattr(archive, "_prepare_book_prefix", lambda *_, **__: {"status": "book_prefix_verified"})
                event.listen(storage.database._session_factory, "before_commit", fail_commit)
                try:
                    with pytest.raises(RuntimeError, match="prefix commit failure"):
                        archive.stage_next_page(day)
                finally:
                    event.remove(storage.database._session_factory, "before_commit", fail_commit)
        elif interruption == "cursor":
            other = fact_archival.PostgresCanonicalFactArchiveRepository(**kwargs)
            assert other.stage_next_page(day)["page_ordinal"] == 0
        elif interruption == "limits":
            archive.limits = replace(archive.limits, max_logical_bytes=10)
            with pytest.raises(RuntimeError, match="source_row_budget_exceeded"):
                archive.stage_next_page(day)
            archive.limits = kwargs["limits"]

        result = archive.stage_next_page(day)
        assert result["status"] == "page_acknowledged"
        assert result["page_ordinal"] == (1 if interruption == "cursor" else 0)
        assert len(source_reads) == (1 if interruption is None else 3 if interruption in {"cursor", "limits"} else 2)
        if interruption != "cursor":
            assert archive.stage_next_page(day)["page_ordinal"] == 1
        assert archive.stage_next_page(day)["status"] == "source_exhausted"
        with storage.database.session() as session:
            rows = session.execute(text(
                "SELECT * FROM market.fact_archive_manifests WHERE storage_day=:day ORDER BY page_ordinal"
            ), {"day": day}).mappings().all()
        assert len(rows) == 2
        restored = []
        for row in rows:
            manifest = fact_archival._catalog_manifest(row)
            restored.extend(fact_archival.read_canonical_fact_archive(
                store.local_path(manifest.object_key), expected=manifest, limits=archive.limits))
        assert len(restored) == 2 and len({row["id"] for row in restored}) == 2
    finally:
        event.remove(storage.database._engine, "after_cursor_execute", observe_sql)



def _alias_catalog_page(storage, tmp_path, monkeypatch, *, count):
    """Real BBO correction history with one shared, proven book dependency."""
    from types import SimpleNamespace
    from market_data.canonical_adapters import DERIVED_MARKET_STATE_SOURCE, canonicalize_bbo_feature
    from market_data.contracts import DatasetSeriesRequest
    from market_data.market_state import MarketStateValuationContract, derive_book_features
    from tests.test_market_data.test_fact_raw_lineage_db import _raw_book_fixture, _publish_book_result
    from tests.test_market_data.test_fact_storage_tiers_db import BASE
    from portal.backend.service.storage.repos import market_data, market_structure
    from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository

    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    fixture = _raw_book_fixture(storage, tmp_path, monkeypatch, replay_features=True)
    reader = FilesystemRawArchiveObjectStore(fixture.store.root, writable=False)
    tiered = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: reader)
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository", tiered)
    monkeypatch.setattr(market_structure, "canonical_fact_storage_repository", tiered)
    for index in range(len(fixture.results)):
        _publish_book_result(fixture, index)
    config = fixture.claim.config
    bbo, _ = derive_book_features((item.state for item in fixture.results),
        contract=MarketStateValuationContract(
            product_definition_version_id=config["product_definition_version_id"],
            provider_size_unit="base", base_currency="BTC", quote_currency="USD"),
        bbo_series_id=config["bbo_series_id"], depth_series_id=config["depth_series_id"],
        computed_at=BASE + timedelta(minutes=1))
    root = canonicalize_bbo_feature(bbo[0], source=DERIVED_MARKET_STATE_SOURCE)
    day = fixture.day + timedelta(days=1)
    _placement(monkeypatch, day)
    source_id = storage.repo.register_source(DERIVED_MARKET_STATE_SOURCE)
    for index in range(count):
        # One delivery per commit: ingestion rejects duplicate observation keys
        # within a single batch, and every correction must retain its history.
        storage.repo.ingest_facts(series_id=bbo[0].series_id, source_id=source_id,
            facts=[replace(root, provenance={**root.provenance, "fixture_delivery": index})])
    frozen = storage.repo.freeze_dataset([DatasetSeriesRequest(
        bbo[0].series_id, BASE - timedelta(seconds=1), BASE + timedelta(minutes=2))])
    read = lambda: storage.repo.read_dataset_fact_revisions(
        dataset_id=frozen.dataset_id, series_id=bbo[0].series_id)
    before = read()
    assert len(before) == count
    archive = fact_archival.PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=fixture.store,
        temporary_directory=tmp_path / "staging",
        limits=FactArchiveLimits(max_rows=count, row_group_size=min(128, count)))
    assert archive.seal_partition(day)["expected_rows"] == count
    return SimpleNamespace(archive=archive, day=day, store=fixture.store,
                           read=read, before=before, count=count)


def _acknowledge_alias_page(fixture):
    for _ in range(8):
        result = fixture.archive.stage_next_page(fixture.day)
        if result["status"] == "page_acknowledged":
            return result
        assert result["status"] in {"book_prefix_verified", "trade_prefix_verified"}
    pytest.fail("fixture exceeded its bounded prefix preparation count")


def test_catalogue_writes_finish_under_per_statement_limit_with_slow_rows(
        storage, tmp_path, monkeypatch):
    fixture = _alias_catalog_page(storage, tmp_path, monkeypatch, count=320)
    # Model a cold per-row write cost on a disposable DB. This is a timeout
    # regression, not an HDD benchmark or a production throughput estimate.
    with storage.database.session() as session:
        session.execute(text("""
            CREATE FUNCTION market.test_slow_alias_insert() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.01); RETURN NEW; END $$
        """))
        session.execute(text("""
            CREATE TRIGGER test_slow_alias_insert BEFORE INSERT
            ON market.fact_archive_prepared_material_aliases FOR EACH ROW
            EXECUTE FUNCTION market.test_slow_alias_insert()
        """))
    # Apply the artificial deadline to hidden catalog transactions only.
    # Shared-CI source planning retains the ordinary statement policy.
    fixture.archive._generation.lock_day = lambda session, day: fixture.archive._lock(
        session, day, statement_timeout_ms=2000,
    )
    # A coarse batch is the control under the same row cost and deadline. It
    # must fail at the prepared alias INSERT. Its one transaction rolls back,
    # while the fenced preparation remains resumable and stays invisible.
    with monkeypatch.context() as coarse:
        coarse.setattr(fact_archival, "_ALIAS_CATALOG_WRITE_BATCH_ROWS", 1000)
        with pytest.raises(DBAPIError) as failed:
            _acknowledge_alias_page(fixture)
    assert failed.value.orig.pgcode == "57014"
    assert "INSERT INTO market.fact_archive_prepared_material_aliases" in failed.value.statement
    assert fixture.archive.inspect_partition(fixture.day)["page_count"] == 0
    with storage.database.session() as session:
        preparation = session.execute(text(
            "SELECT id,state FROM market.fact_archive_preparations WHERE storage_day=:day"
        ), {"day": fixture.day}).mappings().one()
        assert preparation["state"] == "building"
        assert session.execute(text(
            "SELECT count(*) FROM market.fact_archive_prepared_material_aliases "
            "WHERE preparation_id=:id"
        ), {"id": preparation["id"]}).scalar_one() == 0
    result = _acknowledge_alias_page(fixture)
    assert result["page_ordinal"] == 0 and result["row_count"] == fixture.count
    with storage.database.session() as session:
        assert session.execute(text(
            "SELECT count(*) FROM market.fact_archive_material_aliases WHERE manifest_id=:id"
        ), {"id": result["manifest_id"]}).scalar_one() == 0
        active = session.execute(text(
            "SELECT preparation_id FROM market.fact_archive_active_generations WHERE manifest_id=:id"
        ), {"id": result["manifest_id"]}).scalar_one()
        assert session.execute(text(
            "SELECT count(*) FROM market.fact_archive_prepared_material_aliases "
            "WHERE preparation_id=:id"
        ), {"id": active}).scalar_one() == fixture.count
    assert fixture.archive.verify_next_page(fixture.day)["status"] == "page_verified"
    assert fixture.read() == fixture.before


def test_canonical_dependency_writes_finish_under_per_statement_limit_with_slow_rows(
        storage, tmp_path, monkeypatch):
    fixture = _alias_catalog_page(storage, tmp_path, monkeypatch, count=320)
    acknowledged = _acknowledge_alias_page(fixture)
    manifest_id = acknowledged["manifest_id"]
    rows = [
        {"fact_version_id": row.fact_version_id, "row_hash": "a" * 64}
        for row in fixture.before
    ]
    with storage.database.session() as session:
        initial_count = session.execute(text(
            "SELECT count(*) FROM market.fact_archive_canonical_dependencies "
            "WHERE manifest_id=:id"
        ), {"id": manifest_id}).scalar_one()
        existing_ids = set(session.execute(text(
            "SELECT fact_version_id FROM market.fact_archive_canonical_dependencies "
            "WHERE manifest_id=:id"
        ), {"id": manifest_id}).scalars())
        session.execute(text("""
            CREATE FUNCTION market.test_slow_canonical_dependency_insert() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.01); RETURN NEW; END $$
        """))
        session.execute(text("""
            CREATE TRIGGER test_slow_canonical_dependency_insert BEFORE INSERT
            ON market.fact_archive_canonical_dependencies FOR EACH ROW
            EXECUTE FUNCTION market.test_slow_canonical_dependency_insert()
        """))
    rows = [row for row in rows if row["fact_version_id"] not in existing_ids]
    assert len(rows) >= 300
    stage_dependencies = fixture.archive._stage_canonical_dependency_catalog_rows

    def write_with_dependency_deadline(batch_rows):
        with monkeypatch.context() as batch:
            batch.setattr(fact_archival, "_CATALOG_WRITE_BATCH_ROWS", batch_rows)
            with storage.database.session() as session:
                previous_timeout = session.execute(text("SHOW statement_timeout")).scalar_one()
                session.execute(text("SELECT set_config('statement_timeout', '2000', true)"))
                result = stage_dependencies(session, manifest_id, rows)
                session.execute(text("SELECT set_config('statement_timeout', :timeout, true)"),
                                {"timeout": previous_timeout})
                return result

    # The exact coarse typed-array control exceeds the fixed statement timeout.
    # Its aborted transaction must leave the existing committed page untouched.
    with pytest.raises(DBAPIError) as failed:
        write_with_dependency_deadline(1000)
    assert failed.value.orig.pgcode == "57014"
    assert failed.value.statement.startswith(
        "INSERT INTO market.fact_archive_canonical_dependencies"
    )
    with storage.database.session() as session:
        assert session.execute(text(
            "SELECT count(*) FROM market.fact_archive_canonical_dependencies "
            "WHERE manifest_id=:id"
        ), {"id": manifest_id}).scalar_one() == initial_count

    bounded = write_with_dependency_deadline(128)
    assert bounded["row_count"] == len(rows)
    assert bounded["batch_count"] == (len(rows) + 127) // 128
    with storage.database.session() as session:
        assert session.execute(text(
            "SELECT count(*) FROM market.fact_archive_canonical_dependencies "
            "WHERE manifest_id=:id"
        ), {"id": manifest_id}).scalar_one() == initial_count + len(rows)
    assert fixture.archive.inspect_partition(fixture.day)["page_count"] == 1
    assert fixture.read() == fixture.before

@pytest.mark.parametrize("stop", ["cancel", "budget"])
def test_catalogue_mid_write_stop_rolls_back_page_and_reuses_immutable_object(
        storage, tmp_path, monkeypatch, stop):
    from portal.backend.service.market.canonical_retention import (
        CanonicalRetentionBudgetExceeded, CanonicalRetentionStopRequested,
    )
    fixture = _alias_catalog_page(storage, tmp_path, monkeypatch, count=129)
    wrote_aliases = False

    def inserted(_conn, _cursor, statement, *_):
        nonlocal wrote_aliases
        if "INSERT INTO market.fact_archive_prepared_material_aliases" in statement:
            wrote_aliases = True

    error = CanonicalRetentionStopRequested if stop == "cancel" else CanonicalRetentionBudgetExceeded

    def check_budget():
        if wrote_aliases:
            raise error("injected stop after an alias write")

    fixture.archive.check_budget = check_budget
    # The generation coordinator captures the production budget callback when
    # the archive repository is constructed.  This test replaces that callback
    # after construction, so replace the coordinator's captured callback too;
    # otherwise the injected stop is first observed only after the whole group
    # commits and does not exercise the group rollback contract.
    fixture.archive._generation.check_budget = check_budget
    event.listen(storage.database._engine, "after_cursor_execute", inserted)
    try:
        with pytest.raises(error, match="injected stop after an alias write"):
            _acknowledge_alias_page(fixture)
    finally:
        event.remove(storage.database._engine, "after_cursor_execute", inserted)
        fixture.archive.check_budget = None
        fixture.archive._generation.check_budget = None
    assert wrote_aliases
    assert fixture.archive.inspect_partition(fixture.day)["page_count"] == 0
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_archive_material_aliases")).scalar_one() == 0
        assert session.execute(text("SELECT count(*) FROM market.fact_archive_series")).scalar_one() == 0
        assert session.execute(text("SELECT count(*) FROM market.fact_archive_canonical_dependencies")).scalar_one() == 0
        preparation = session.execute(text(
            "SELECT id,state FROM market.fact_archive_preparations WHERE storage_day=:day"
        ), {"day": fixture.day}).mappings().one()
        assert preparation["state"] == "building"
        assert session.execute(text(
            "SELECT count(*) FROM market.fact_archive_prepared_material_aliases "
            "WHERE preparation_id=:id"
        ), {"id": preparation["id"]}).scalar_one() == 0
        assert session.execute(text(
            "SELECT count(*) FROM market.fact_hot_payloads WHERE storage_day=:day"
        ), {"day": fixture.day}).scalar_one() == fixture.count
    objects = set(fixture.store.root.rglob("*.parquet"))
    result = _acknowledge_alias_page(fixture)
    assert result["page_ordinal"] == 0 and result["row_count"] == fixture.count
    assert set(fixture.store.root.rglob("*.parquet")) == objects
    assert fixture.archive.verify_next_page(fixture.day)["status"] == "page_verified"
    assert fixture.read() == fixture.before


def test_canonical_dependency_batch_stop_rolls_back_every_page_catalog(
        storage, tmp_path, monkeypatch):
    from portal.backend.service.market.canonical_retention import CanonicalRetentionStopRequested

    fixture = _alias_catalog_page(storage, tmp_path, monkeypatch, count=513)
    wrote_canonical = False

    def inserted(_conn, _cursor, statement, *_):
        nonlocal wrote_canonical
        if "INSERT INTO market.fact_archive_prepared_canonical_dependencies" in statement:
            wrote_canonical = True

    def check_budget():
        if wrote_canonical:
            raise CanonicalRetentionStopRequested("injected stop after canonical dependency batch")

    fixture.archive.check_budget = check_budget
    event.listen(storage.database._engine, "after_cursor_execute", inserted)
    try:
        with pytest.raises(CanonicalRetentionStopRequested, match="injected stop"):
            _acknowledge_alias_page(fixture)
    finally:
        event.remove(storage.database._engine, "after_cursor_execute", inserted)
        fixture.archive.check_budget = None
    assert wrote_canonical
    assert fixture.archive.inspect_partition(fixture.day)["page_count"] == 0
    with storage.database.session() as session:
        for table in (
            "fact_archive_manifests", "fact_archive_series", "fact_archive_material_aliases",
            "fact_archive_dependencies", "fact_archive_canonical_dependencies",
        ):
            assert session.execute(text(f"SELECT count(*) FROM market.{table}")).scalar_one() == 0
        preparation = session.execute(text(
            "SELECT id,state,canonical_dependency_count FROM market.fact_archive_preparations "
            "WHERE storage_day=:day"
        ), {"day": fixture.day}).mappings().one()
        assert preparation["state"] == "building"
        persisted = session.execute(text(
            "SELECT count(*) FROM market.fact_archive_prepared_canonical_dependencies "
            "WHERE preparation_id=:id"
        ), {"id": preparation["id"]}).scalar_one()
        assert 0 < persisted <= fact_archival._CATALOG_WRITE_BATCH_ROWS
        assert persisted == preparation["canonical_dependency_count"]
    objects = set(fixture.store.root.rglob("*.parquet"))
    result = _acknowledge_alias_page(fixture)
    assert result["page_ordinal"] == 0 and result["row_count"] == fixture.count
    assert set(fixture.store.root.rglob("*.parquet")) == objects
    assert fixture.read() == fixture.before


def test_verifier_publishes_changed_catalog_as_atomic_generation(
        storage, tmp_path, monkeypatch):
    fixture = _alias_catalog_page(storage, tmp_path, monkeypatch, count=12)
    original_alias = fact_archival.legacy_material_alias
    monkeypatch.setattr(fact_archival, "legacy_material_alias", lambda _row: None)
    acknowledged = _acknowledge_alias_page(fixture)
    with storage.database.session() as session:
        old = session.execute(text(
            "SELECT preparation_id,generation FROM market.fact_archive_active_generations "
            "WHERE manifest_id=:id"
        ), {"id": acknowledged["manifest_id"]}).mappings().one()
        assert old["generation"] == 1
        assert session.execute(text(
            "SELECT count(*) FROM market.fact_archive_prepared_material_aliases "
            "WHERE preparation_id=:id"
        ), {"id": old["preparation_id"]}).scalar_one() == 0

    monkeypatch.setattr(fact_archival, "legacy_material_alias", original_alias)
    verified = fixture.archive.verify_next_page(fixture.day)
    assert verified["status"] == "page_verified"
    with storage.database.session() as session:
        active = session.execute(text(
            "SELECT preparation_id,generation,catalog_commitment "
            "FROM market.fact_archive_active_generations WHERE manifest_id=:id"
        ), {"id": acknowledged["manifest_id"]}).mappings().one()
        assert active["generation"] == 2
        assert active["preparation_id"] != old["preparation_id"]
        states = dict(session.execute(text(
            "SELECT id,state FROM market.fact_archive_preparations "
            "WHERE manifest_id=:id ORDER BY generation"
        ), {"id": acknowledged["manifest_id"]}).all())
        assert states == {old["preparation_id"]: "superseded",
                          active["preparation_id"]: "active"}
        binding = session.execute(text(
            "SELECT preparation_id,catalog_commitment "
            "FROM market.fact_archive_verification_generations "
            "WHERE manifest_id=:id AND verifier_version=:version"
        ), {"id": acknowledged["manifest_id"],
            "version": fact_archival.FACT_ARCHIVE_VERIFIER_VERSION}).mappings().one()
        assert dict(binding) == {
            "preparation_id": active["preparation_id"],
            "catalog_commitment": active["catalog_commitment"],
        }
        catalogs = fixture.archive._page_catalogs(session, acknowledged["manifest_id"])
        assert len(catalogs["aliases"]) == fixture.count
    assert fixture.read() == fixture.before


def test_failed_generation_supersession_keeps_old_authority_and_resumes(
        storage, tmp_path, monkeypatch):
    fixture = _alias_catalog_page(storage, tmp_path, monkeypatch, count=12)
    original_alias = fact_archival.legacy_material_alias
    monkeypatch.setattr(fact_archival, "legacy_material_alias", lambda _row: None)
    acknowledged = _acknowledge_alias_page(fixture)
    monkeypatch.setattr(fact_archival, "legacy_material_alias", original_alias)
    with storage.database.session() as session:
        old = dict(session.execute(text(
            "SELECT preparation_id,generation,catalog_commitment "
            "FROM market.fact_archive_active_generations WHERE manifest_id=:id"
        ), {"id": acknowledged["manifest_id"]}).mappings().one())

    original_dependencies = fixture.archive._dependencies
    late_hold = {
        "target_kind": "book_checkpoint", "target_id": "fixture-late-hold",
        "object_key": "fixture/late-hold", "object_sha256": "e" * 64,
    }

    def dependencies_with_late_hold(session, rows, **kwargs):
        dependencies, source_rows = original_dependencies(session, rows, **kwargs)
        return [*dependencies, late_hold], source_rows

    monkeypatch.setattr(fixture.archive, "_dependencies", dependencies_with_late_hold)
    with storage.database.session() as session:
        session.execute(text("""
            CREATE FUNCTION market.test_fail_generation_binding() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN
                RAISE EXCEPTION 'fixture_generation_binding_failure';
            END $$
        """))
        session.execute(text("""
            CREATE TRIGGER test_fail_generation_binding BEFORE INSERT
            ON market.fact_archive_verification_generations FOR EACH ROW
            EXECUTE FUNCTION market.test_fail_generation_binding()
        """))

    # This fault occurs after the transaction inserts new permanent holds,
    # changes the active pointer and generation states, and inserts the receipt.
    # PostgreSQL must roll every one of those writes back together.
    with pytest.raises(DBAPIError, match="fixture_generation_binding_failure"):
        fixture.archive.verify_next_page(fixture.day)
    with storage.database.session() as session:
        assert dict(session.execute(text(
            "SELECT preparation_id,generation,catalog_commitment "
            "FROM market.fact_archive_active_generations WHERE manifest_id=:id"
        ), {"id": acknowledged["manifest_id"]}).mappings().one()) == old
        states = session.execute(text(
            "SELECT generation,state,lease_expires_at <= clock_timestamp() AS released "
            "FROM market.fact_archive_preparations "
            "WHERE manifest_id=:id ORDER BY generation"
        ), {"id": acknowledged["manifest_id"]}).all()
        assert [(row.generation, row.state) for row in states] == [
            (1, "active"), (2, "ready"),
        ]
        assert states[1].released is True
        assert session.execute(text(
            "SELECT count(*) FROM market.fact_archive_verifications WHERE manifest_id=:id"
        ), {"id": acknowledged["manifest_id"]}).scalar_one() == 0
        assert session.execute(text(
            "SELECT count(*) FROM market.fact_archive_dependencies "
            "WHERE manifest_id=:id AND target_kind=:kind AND target_id=:target"
        ), {"id": acknowledged["manifest_id"], "kind": late_hold["target_kind"],
            "target": late_hold["target_id"]}).scalar_one() == 0
        assert fixture.archive._page_catalogs(
            session, acknowledged["manifest_id"]
        )["aliases"] == []

    with storage.database.session() as session:
        session.execute(text(
            "DROP TRIGGER test_fail_generation_binding "
            "ON market.fact_archive_verification_generations"
        ))
        session.execute(text("DROP FUNCTION market.test_fail_generation_binding()"))

    assert fixture.archive.verify_next_page(fixture.day)["status"] == "page_verified"
    with storage.database.session() as session:
        active = session.execute(text(
            "SELECT preparation_id,generation FROM market.fact_archive_active_generations "
            "WHERE manifest_id=:id"
        ), {"id": acknowledged["manifest_id"]}).mappings().one()
        assert active["generation"] == 2
        assert session.execute(text(
            "SELECT count(*) FROM market.fact_archive_preparations "
            "WHERE manifest_id=:id"
        ), {"id": acknowledged["manifest_id"]}).scalar_one() == 2
        assert session.execute(text(
            "SELECT count(*) FROM market.fact_archive_dependencies "
            "WHERE manifest_id=:id AND target_kind=:kind AND target_id=:target"
        ), {"id": acknowledged["manifest_id"], "kind": late_hold["target_kind"],
            "target": late_hold["target_id"]}).scalar_one() == 1
    assert fixture.read() == fixture.before


@pytest.mark.parametrize("path", ["final_verification", "publication"])
def test_expiration_query_batches_preserve_exact_keys_tail_and_cancellation(storage, tmp_path, monkeypatch, path):
    from portal.backend.service.storage.repos import market_lifecycle
    from portal.backend.service.market.canonical_retention import (
        CanonicalRetentionBudgetExceeded, CanonicalRetentionStopRequested,
    )
    monkeypatch.setattr(market_lifecycle, "db", storage.database)
    lifecycle = market_lifecycle.market_storage_lifecycle_repository
    dependencies = [{"target_kind": "raw_manifest", "target_id": f"expiration-{index:03d}"}
                    for index in range(129)]
    # Same ID in another target kind, a different action, a failed operation,
    # and an unrelated ID must not count as completed expiry for these inputs.
    for operation, kind, identity, action, state in (
        ("other-kind", "book_checkpoint", "expiration-000", "archive_expire", "completed"),
        ("other-action", "raw_manifest", "expiration-001", "archive_compact", "completed"),
        ("failed-expiry", "raw_manifest", "expiration-002", "archive_expire", "failed"),
        ("other-id", "raw_manifest", "not-in-request", "archive_expire", "completed"),
    ):
        lifecycle.append_event(operation_id=operation, action=action, event_type=state,
                               target_kind=kind, target_id=identity, evidence={})
    archive = None
    if path == "publication":
        import hashlib
        from types import SimpleNamespace
        from tests.test_market_data.test_canonical_fact_archive import _row
        data = b"shared immutable archive"
        raw_path = tmp_path / "raw.parquet"
        raw_path.write_bytes(data)
        reference = {"object_key": "raw.parquet", "object_sha256": hashlib.sha256(data).hexdigest()}
        refs = {item["target_id"]: reference for item in dependencies}
        archive = fact_archival.PostgresCanonicalFactArchiveRepository(
            database=storage.database, object_store=SimpleNamespace(local_path=lambda _: raw_path),
            temporary_directory=tmp_path / "staging")
        monkeypatch.setattr(archive, "_flow_references", lambda *args, **kwargs: refs)
    def check(session, check_budget=None):
        if archive is None:
            fact_archival.PostgresCanonicalFactArchiveRepository._assert_dependencies_unexpired(
                session, dependencies, check_budget=check_budget)
        else:
            archive.check_budget = check_budget
            admitted, sources = archive._dependencies(session, [{**_row(), "fact_type": "market.market_response"}])
            assert [{key: row[key] for key in ("target_kind", "target_id")} for row in admitted] == dependencies
            assert sources == []
    batches = []
    def observe(_conn, _cursor, statement, parameters, *_):
        if "WITH ORDINALITY AS requested" in statement:
            batches.append(len(parameters["ids"]))
    event.listen(storage.database._engine, "after_cursor_execute", observe)
    try:
        with storage.database.session() as session:
            lifecycle.acquire_dataset_pin_lock(session)
            check(session)
        # Fixed workload: at most two SQL round trips for 129 exact key pairs.
        assert len(batches) <= 2 and sum(batches) == len(dependencies)
        for error in (CanonicalRetentionBudgetExceeded, CanonicalRetentionStopRequested):
            batches.clear()
            def cancelled():
                if batches:
                    raise error("stop between expiration batches")
            with pytest.raises(error, match="stop between expiration batches"):
                with storage.database.session() as session:
                    lifecycle.acquire_dataset_pin_lock(session)
                    check(session, check_budget=cancelled)
            assert len(batches) == 1 and sum(batches) < len(dependencies)
        lifecycle.append_event(operation_id="tail-expiry", action="archive_expire", event_type="completed",
            target_kind="raw_manifest", target_id=dependencies[-1]["target_id"], evidence={})
        with pytest.raises(RuntimeError, match="canonical_archive_dependency_expired: target_id=expiration-128"):
            with storage.database.session() as session:
                lifecycle.acquire_dataset_pin_lock(session)
                check(session)
    finally:
        event.remove(storage.database._engine, "after_cursor_execute", observe)
