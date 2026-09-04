from dataclasses import replace
from datetime import timedelta
import hashlib

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.contracts import DatasetSeriesRequest
from market_data.fact_archive import FactArchiveLimits
from portal.backend.db.fact_storage_schema import fact_partition_name
from portal.backend.service.storage.repos import market_data, fact_reclamation
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from tests.test_market_data.test_fact_storage_tiers_db import storage, BASE, _placement, _ingest, _read

pytestmark = pytest.mark.db


def _prepare(storage, tmp_path, monkeypatch, *, rows=2, padding=False):
    day = storage.today - timedelta(days=2)
    _placement(monkeypatch, day)
    ballast = "".join(hashlib.sha256(str(i).encode()).hexdigest() for i in range(1024)) if padding else ""
    for ordinal in range(rows):
        _ingest(storage, replace(storage.fact, observation_key=f"reclaim-{ordinal}",
                                provenance={**storage.fact.provenance, "fixture_padding": ballast}))
    # Preserve two distinct revisions at identical observation time.
    _ingest(storage, replace(storage.fact, observation_key="reclaim-0",
                            payload={**storage.fact.payload, "rate": "0.2", "raw_rate": "0.2"},
                            known_at=BASE + timedelta(seconds=1), accepted_at=BASE + timedelta(seconds=1)))
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    archive = PostgresCanonicalFactArchiveRepository(
        database=storage.database, object_store=store, temporary_directory=tmp_path / "staging",
        limits=FactArchiveLimits(max_rows=16, row_group_size=16),
    )
    archive.seal_partition(day)
    while archive.stage_next_page(day)["status"] != "source_exhausted":
        pass
    while archive.verify_next_page(day)["status"] != "no_unverified_pages":
        pass
    archive.verify_partition(day)
    reader = FilesystemRawArchiveObjectStore(store.root, writable=False)
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository",
                        PostgresCanonicalFactStorageRepository(object_store_factory=lambda: reader))
    return day, archive, PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)


def _physical(storage, day):
    with storage.database.session() as session:
        return dict(session.execute(text("""
            SELECT to_regclass(:relation)::text AS relation,
                   pg_total_relation_size(to_regclass(:relation)) AS bytes,
                   pg_database_size(current_database()) AS database_bytes,
                   (SELECT count(*) FROM market.fact_hot_payloads WHERE storage_day=:day) AS hot_rows,
                   (SELECT count(*) FROM market.fact_versions WHERE storage_day=:day) AS header_rows
        """), {"day": day, "relation": "market." + fact_partition_name(day)}).mappings().one())


def test_actual_reclamation_keeps_pinned_history_and_returns_space(storage, tmp_path, monkeypatch):
    day, archive, reclaimer = _prepare(storage, tmp_path, monkeypatch, rows=32, padding=True)
    request = DatasetSeriesRequest(storage.series_id, BASE - timedelta(days=1), BASE + timedelta(days=1))
    frozen = storage.repo.freeze_dataset([request])
    before = storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id)
    known_before = _read(storage, known_at_lte=BASE)
    state = archive.inspect_partition(day)
    physical = _physical(storage, day)
    files = sorted(str(path) for path in tmp_path.rglob("*"))
    disabled = PostgresCanonicalFactReclamationRepository(archive_repository=archive)
    plan = disabled.reclaim_partition(day, eligible_before=storage.today)
    assert plan["status"] == "dry_run" and plan["enabled"] is False
    assert plan["protected_dataset_ranges"] == 1
    assert plan["reclaimable_bytes"] == physical["bytes"]
    assert archive.inspect_partition(day) == state
    after_dry_run = _physical(storage, day)
    for field in ("relation", "hot_rows", "header_rows"):
        assert after_dry_run[field] == physical[field]
    assert sorted(str(path) for path in tmp_path.rglob("*")) == files
    assert storage.repo.read_dataset_fact_revisions(
        dataset_id=frozen.dataset_id, series_id=storage.series_id
    ) == before
    assert _read(storage, known_at_lte=BASE) == known_before
    with pytest.raises(RuntimeError, match="canonical_reclaim_disabled"):
        disabled.reclaim_partition(day, eligible_before=storage.today, execute=True)

    before_reclaim = _physical(storage, day)
    result = reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    assert result["status"] == "partition_reclaimed" and result["protected_dataset_ranges"] == 1
    assert result["reclaimed_bytes"] > 1024**2
    after = _physical(storage, day)
    assert after["relation"] is None and after["bytes"] is None and after["hot_rows"] == 0
    assert after["header_rows"] == physical["header_rows"] == 33
    # Actual isolated database allocation drops; this is not DELETE plus a
    # promise that VACUUM might eventually make its pages reusable.
    assert before_reclaim["database_bytes"] - after["database_bytes"] >= result["reclaimed_bytes"] - 256 * 1024
    assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id) == before
    assert _read(storage, known_at_lte=BASE) == known_before
    assert storage.repo.freeze_dataset([request]).dataset_hash == frozen.dataset_hash
    restarted = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    assert restarted.reclaim_partition(day, eligible_before=storage.today, execute=True)["status"] == "already_reclaimed"
    # A current-day correction cannot mutate a frozen, now-cold revision set.
    _placement(monkeypatch, storage.today)
    _ingest(storage, replace(storage.fact, observation_key="reclaim-0", known_at=BASE + timedelta(seconds=2),
                            accepted_at=BASE + timedelta(seconds=2), payload={**storage.fact.payload, "rate": "0.3", "raw_rate": "0.3"}))
    assert storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id, series_id=storage.series_id) == before
    assert any(record.revision == 3 for record in _read(storage))


def test_reclamation_failures_leave_hot_partition_and_progress_intact(storage, tmp_path, monkeypatch):
    day, archive, reclaimer = _prepare(storage, tmp_path, monkeypatch)
    initial = archive.inspect_partition(day)
    physical = _physical(storage, day)
    with storage.database.session() as session:
        key = session.execute(text("SELECT object_key FROM market.fact_archive_manifests WHERE storage_day=:day"), {"day": day}).scalar_one()
    path = archive.object_store.local_path(key)
    original = path.read_bytes()
    for missing in (False, True):
        if missing:
            path.unlink()
        else:
            path.write_bytes(b"x" * len(original))
        with pytest.raises(FileNotFoundError if missing else RuntimeError, match=None if missing else "checksum_mismatch"):
            reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
        assert archive.inspect_partition(day) == initial
        assert _physical(storage, day)["bytes"] == physical["bytes"]
        path.write_bytes(original)
    for cutoff in (day, storage.today + timedelta(days=1)):
        with pytest.raises(RuntimeError, match="outside_cutoff"):
            reclaimer.reclaim_partition(day, eligible_before=cutoff, execute=True)

    # Budget exhaustion during the exclusive handoff is not partial success.
    with monkeypatch.context() as patch:
        ticks = iter((0, 20))
        patch.setattr(fact_reclamation, "monotonic", lambda: next(ticks))
        with pytest.raises(RuntimeError, match="handoff_budget_exceeded"):
            reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    assert archive.inspect_partition(day) == initial

    # A catalog insertion between shared byte verification and the exclusive
    # handoff must invalidate the old receipt. The failed handoff transaction
    # rolls this injected test row back as well.
    actual_timeouts = reclaimer._timeouts
    calls = 0
    def changed_catalog(session):
        nonlocal calls
        actual_timeouts(session)
        calls += 1
        if calls == 2:
            session.execute(text("""
                INSERT INTO market.fact_archive_material_aliases
                    (manifest_id, fact_version_id, series_id, evidence_key, material_hash)
                SELECT id, first_id, :series, 'injected-alias', :hash
                FROM market.fact_archive_manifests WHERE storage_day=:day
            """), {"day": day, "series": storage.series_id, "hash": "a" * 64})
    with monkeypatch.context() as patch:
        patch.setattr(reclaimer, "_timeouts", changed_catalog)
        with pytest.raises(RuntimeError, match="verification_missing_or_stale"):
            reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    assert calls == 2
    assert archive.inspect_partition(day) == initial

    def interrupted(_conn, _cursor, statement, *_):
        if statement.startswith('DROP TABLE market."fact_hot_payloads_'):
            raise RuntimeError("injected interruption after DROP before commit")
    event.listen(storage.database._engine, "after_cursor_execute", interrupted)
    try:
        with pytest.raises(RuntimeError, match="injected interruption after DROP"):
            reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    finally:
        event.remove(storage.database._engine, "after_cursor_execute", interrupted)
    assert archive.inspect_partition(day) == initial
    assert _physical(storage, day)["bytes"] == physical["bytes"]
    assert _physical(storage, day)["hot_rows"] == 3

    # Recheck even AFTER transactional DROP. A failed final file stamp must
    # roll back the DDL as well as lifecycle state.
    def changed_during_drop(_conn, _cursor, statement, *_):
        if statement.startswith('DROP TABLE market."fact_hot_payloads_'):
            path.write_bytes(b"x" * len(original))
    event.listen(storage.database._engine, "after_cursor_execute", changed_during_drop)
    try:
        with pytest.raises(RuntimeError, match="object_changed"):
            reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    finally:
        event.remove(storage.database._engine, "after_cursor_execute", changed_during_drop)
        path.write_bytes(original)
    assert archive.inspect_partition(day) == initial
    assert _physical(storage, day)["hot_rows"] == 3

    # Read-only remounts may leave every checksum/stamp unchanged. Unlike a
    # research read, the destructive handoff still must refuse that mount.
    from core.storage_mounts import StorageMountError
    real_mount = fact_reclamation.require_configured_archive_mount
    def read_only_mount(**_):
        raise StorageMountError("injected storage_mount_read_only after DROP")
    def remounted_during_drop(_conn, _cursor, statement, *_):
        if statement.startswith('DROP TABLE market."fact_hot_payloads_'):
            monkeypatch.setattr(fact_reclamation, "require_configured_archive_mount", read_only_mount)
    event.listen(storage.database._engine, "after_cursor_execute", remounted_during_drop)
    try:
        with pytest.raises(StorageMountError, match="storage_mount_read_only"):
            reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    finally:
        event.remove(storage.database._engine, "after_cursor_execute", remounted_during_drop)
        monkeypatch.setattr(fact_reclamation, "require_configured_archive_mount", real_mount)
    assert archive.inspect_partition(day) == initial
    assert _physical(storage, day)["hot_rows"] == 3
    assert reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)["status"] == "partition_reclaimed"


def test_reclamation_handoff_fences_readers_and_never_waits_for_parent_read_locks(storage, tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME, MarketStorageLifecycleBusyError
    day, archive, reclaimer = _prepare(storage, tmp_path, monkeypatch)
    with storage.database.session() as reader:
        reader.execute(text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:name,0))"), {"name": _LIFECYCLE_LOCK_NAME})
        with pytest.raises(MarketStorageLifecycleBusyError, match="canonical_reclaim_lifecycle_busy"):
            reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    assert archive.inspect_partition(day)["state"] == "verified"
    with storage.database.session() as reader:
        reader.execute(text("SELECT count(*) FROM market.fact_hot_payloads"))
        with pytest.raises(DBAPIError, match="could not obtain lock"):
            reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    assert _physical(storage, day)["hot_rows"] == 3
    # Slow archive verification owns a shared fence, not a parent table lock.
    # Collection into today's partition must still finish while it is paused.
    started, release = Event(), Event()
    original_verify = fact_reclamation.ArchiveVerificationBatch.verify
    def paused_verify(*args, **kwargs):
        started.set()
        assert release.wait(30), "test did not release archive verification"
        return original_verify(*args, **kwargs)
    monkeypatch.setattr(fact_reclamation.ArchiveVerificationBatch, "verify", paused_verify)
    _placement(monkeypatch, storage.today)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(reclaimer.reclaim_partition, day, eligible_before=storage.today, execute=True)
        try:
            assert started.wait(30), "reclaimer did not reach byte verification"
            assert _ingest(storage, replace(storage.fact, observation_key="concurrent-today")).inserted_count == 1
            with storage.database.session() as session:
                assert session.execute(text("SELECT count(*) FROM market.fact_hot_payloads WHERE storage_day=:day"),
                                       {"day": storage.today}).scalar_one() == 1
        finally:
            release.set()
        assert future.result(timeout=30)["status"] == "partition_reclaimed"
