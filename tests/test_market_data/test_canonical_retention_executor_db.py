from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import text

from core.market_storage_lifecycle import CanonicalFactRetentionPolicy, MarketStorageLifecyclePolicy
from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.contracts import DatasetSeriesRequest
from portal.backend.service.market.market_storage_lifecycle import MarketStorageLifecycleService, MarketStorageLifecycleSupervisor
from portal.backend.service.storage.repos import market_data, market_lifecycle
from portal.backend.service.storage.repos.fact_retention import PostgresCanonicalFactRetentionRepository
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from tests.test_market_data.test_fact_reclamation_db import _physical
from tests.test_market_data.test_fact_retention_plan_db import _state
from tests.test_market_data.test_fact_storage_tiers_db import storage, BASE, _placement, _ingest, _read

pytestmark = pytest.mark.db


def test_lifecycle_executor_resumes_pages_verifies_and_reclaims_without_changing_frozen_history(storage, tmp_path, monkeypatch):
    monkeypatch.setattr(market_lifecycle, "db", storage.database)
    day = storage.today - timedelta(days=31)
    _placement(monkeypatch, day)
    for revision in range(3):
        _ingest(storage, replace(storage.fact, payload={**storage.fact.payload, "rate": f"0.{revision}", "raw_rate": f"0.{revision}"},
                                known_at=BASE + timedelta(seconds=revision), accepted_at=BASE + timedelta(seconds=revision)))
    request = DatasetSeriesRequest(storage.series_id, BASE - timedelta(days=1), BASE + timedelta(days=1))
    frozen = storage.repo.freeze_dataset([request])
    before = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id)
    known_before = _read(storage, known_at_lte=BASE)
    physical = _physical(storage, day)
    policy = MarketStorageLifecyclePolicy(execution_enabled=True, archive_compaction_enabled=False,
        archive_expiration_enabled=False, canonical_retention=CanonicalFactRetentionPolicy(
            execution_enabled=True, max_steps_per_run=4, max_page_rows=1, archive_min_free_bytes=0))
    repository = PostgresCanonicalFactRetentionRepository(database=storage.database)

    def restarted_worker():
        service = MarketStorageLifecycleService(canonical_repository=repository)
        return MarketStorageLifecycleSupervisor(policy=policy, service=service, storage_root=tmp_path)

    worker = restarted_worker()
    initial = _state(storage)
    files = sorted(str(path) for path in tmp_path.rglob("*"))
    dry = worker.service.run(policy=policy, storage_root=tmp_path)
    assert dry["status"] == "dry_run" and _state(storage) == initial
    assert sorted(str(path) for path in tmp_path.rglob("*")) == files
    first = worker.run_once()
    assert first["failure_count"] == 0
    assert [row["action"] for row in first["outcomes"]] == ["seal_partition", "stage_page", "stage_page", "stage_page"]
    assert _physical(storage, day)["hot_rows"] == 3
    assert _state(storage)[0]["state"] == "sealed"
    second = restarted_worker().run_once()
    assert second["failure_count"] == 0
    assert [row["action"] for row in second["outcomes"]] == ["verify_page", "verify_page", "verify_page", "verify_partition"]
    assert _state(storage)[0]["state"] == "verified" and _physical(storage, day)["hot_rows"] == 3

    # Even a committed verification receipt cannot authorize deletion after
    # bytes change. A new worker must rehash current files before the DROP.
    with storage.database.session() as session:
        key = session.execute(text("SELECT object_key FROM market.fact_archive_manifests WHERE storage_day=:day ORDER BY page_ordinal LIMIT 1"),
                              {"day": day}).scalar_one()
    path = tmp_path / "objects" / key
    original = path.read_bytes()
    path.write_bytes(b"x" * len(original))
    failed = restarted_worker().run_once()
    assert failed["status"] == "degraded" and failed["failure_count"] == 1
    assert "checksum_mismatch" in failed["outcomes"][0]["error"]
    assert _state(storage)[0]["state"] == "verified" and _physical(storage, day)["hot_rows"] == 3
    path.write_bytes(original)

    reader = FilesystemRawArchiveObjectStore(tmp_path / "objects", writable=False)
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository",
                        PostgresCanonicalFactStorageRepository(object_store_factory=lambda: reader))
    result = restarted_worker().run_once()
    assert result["failure_count"] == 0
    reclaimed = result["outcomes"][0]
    assert reclaimed["status"] == "partition_reclaimed" and reclaimed["reclaimed_bytes"] == physical["bytes"]
    assert reclaimed["protected_dataset_ranges"] == 1
    after = _physical(storage, day)
    assert after["relation"] is None and after["hot_rows"] == 0 and after["header_rows"] == 3
    assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id) == before
    assert _read(storage, known_at_lte=BASE) == known_before
    assert storage.repo.freeze_dataset([request]).dataset_hash == frozen.dataset_hash
    assert restarted_worker().run_once()["outcomes"] == []


def test_expiration_query_limits_cancel_work_and_reset_after_transaction(storage, monkeypatch):
    from contextlib import contextmanager
    from types import SimpleNamespace
    from portal.backend.service.storage.repos import market_lifecycle
    monkeypatch.setattr(market_lifecycle, "db", storage.database)
    repository = market_lifecycle.PostgresMarketStorageLifecycleRepository()
    names = ["statement_timeout", "temp_file_limit", "max_parallel_workers_per_gather"]

    def settings(session):
        return dict(session.execute(text("SELECT name,setting::bigint FROM pg_settings WHERE name=ANY(:names)"),
                                    {"names": names}).all())

    with storage.database.session() as session:
        baseline = settings(session)
    with repository._archive_witness_session(operation="inspect-limits") as session:
        current = settings(session)
        assert 0 < current["statement_timeout"] <= 5000
        assert 0 <= current["temp_file_limit"] <= 64 * 1024
        assert current["max_parallel_workers_per_gather"] == 0
    with storage.database.session() as session:
        assert settings(session) == baseline
        # Exercise stricter operator settings on the same actual SQL session.
        session.execute(text("SET LOCAL statement_timeout='1000ms'"))
        session.execute(text("SET LOCAL temp_file_limit='8kB'"))
        @contextmanager
        def existing_session():
            yield session
        with monkeypatch.context() as patch:
            patch.setattr(market_lifecycle, "db", SimpleNamespace(session=existing_session))
            with repository._archive_witness_session(operation="stricter-parent") as owned:
                current = settings(owned)
                assert current["statement_timeout"] == 1000 and current["temp_file_limit"] == 8

    monkeypatch.setattr(market_lifecycle, "_ARCHIVE_WITNESS_STATEMENT_MS", 25)
    with pytest.raises(market_lifecycle.MarketArchiveWitnessLimitExceeded, match="sqlstate=57014"):
        with repository._archive_witness_session(operation="timeout") as session:
            session.execute(text("SELECT pg_sleep(0.1)"))
    with storage.database.session() as session:
        assert settings(session) == baseline
        assert session.scalar(text("SELECT 1")) == 1

    monkeypatch.setattr(market_lifecycle, "_ARCHIVE_WITNESS_STATEMENT_MS", 5000)
    monkeypatch.setattr(market_lifecycle, "_ARCHIVE_WITNESS_TEMP_KIB", 16)
    with pytest.raises(market_lifecycle.MarketArchiveWitnessLimitExceeded, match="sqlstate=53400"):
        with repository._archive_witness_session(operation="temp-space") as session:
            session.execute(text("SET LOCAL work_mem='64kB'"))
            session.execute(text("""SELECT repeat(md5(i::text),32) AS payload
                                    FROM generate_series(1,256) AS i ORDER BY payload""")).all()
    with storage.database.session() as session:
        assert settings(session) == baseline
        assert session.scalar(text("SELECT 1")) == 1


@pytest.mark.parametrize("deadline_elapsed", [True, False])
def test_statement_cancel_rolls_back_page_and_preserves_budget_resume(
    storage, tmp_path, monkeypatch, deadline_elapsed,
):
    from sqlalchemy.exc import DBAPIError
    from portal.backend.service.market import canonical_retention as module
    from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository

    day = storage.today - timedelta(days=31)
    _placement(monkeypatch, day)
    _ingest(storage)
    request = DatasetSeriesRequest(storage.series_id, BASE - timedelta(days=1), BASE + timedelta(days=1))
    frozen = storage.repo.freeze_dataset([request])
    before = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id)
    repository = PostgresCanonicalFactRetentionRepository(database=storage.database)
    executor = module.CanonicalFactRetentionExecutor(repository=repository)
    policy = CanonicalFactRetentionPolicy(execution_enabled=True, max_steps_per_run=1,
                                         max_run_seconds=60, archive_min_free_bytes=0)
    assert executor.run(policy=policy, storage_root=tmp_path, execute=True)["outcomes"][0]["status"] == "partition_sealed"
    physical = _physical(storage, day)
    clock = [0.0]
    monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    original = PostgresCanonicalFactArchiveRepository._stage_catalog_rows
    cancellations = []

    def cancel_after_catalog_write(archive, session, record_type, manifest_id, rows):
        original(archive, session, record_type, manifest_id, rows)
        # Apply the artificial timeout only to the intended statement. Source
        # selection and catalog setup retain the ordinary SQL policy.
        session.execute(text("SET LOCAL statement_timeout='25ms'"))
        try:
            session.execute(text("SELECT pg_sleep(0.2)"))
        except DBAPIError as exc:
            cancellations.append(exc.orig.pgcode)
            clock[0] = 60.0 if deadline_elapsed else 1.0
            raise
        pytest.fail("The disposable PostgreSQL statement must time out")

    with monkeypatch.context() as patch:
        patch.setattr(PostgresCanonicalFactArchiveRepository, "_stage_catalog_rows", cancel_after_catalog_write)
        failed = executor.run(policy=policy, storage_root=tmp_path, execute=True)
    assert cancellations == ["57014"]
    assert failed["outcomes"][0]["status"] == ("deferred" if deadline_elapsed else "failed")
    assert failed["failure_count"] == (0 if deadline_elapsed else 1)
    assert failed["next_after_storage_day"] == (day - timedelta(days=1) if deadline_elapsed else day).isoformat()
    if deadline_elapsed:
        assert failed["stop_reason"] == "time_budget"
    with storage.database.session() as session:
        assert session.scalar(text("SELECT count(*) FROM market.fact_archive_manifests WHERE storage_day=:day"),
                              {"day": day}) == 0
    assert _state(storage)[0]["state"] == "sealed"
    after = _physical(storage, day)
    # Rolled-back catalog inserts can retain allocated database pages. The
    # protected hot relation and rows must be unchanged; global file size is
    # neither transaction rollback nor reclamation evidence.
    assert {key: value for key, value in after.items() if key != "database_bytes"} == {
        key: value for key, value in physical.items() if key != "database_bytes"
    }
    assert executor._archive is None

    # A later bounded attempt uses only committed progress. The uncertain object
    # from the rolled-back publication cannot serve as catalog acknowledgement.
    retry = executor.run(policy=policy, storage_root=tmp_path, execute=True,
                         after_storage_day=None if deadline_elapsed else day - timedelta(days=1))
    assert retry["failure_count"] == 0
    assert retry["outcomes"][0]["status"] == "page_acknowledged"
    assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id) == before
    with storage.database.session() as session:
        assert session.scalar(text("SELECT count(*) FROM market.fact_archive_manifests WHERE storage_day=:day"),
                              {"day": day}) == 1
