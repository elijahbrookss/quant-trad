"""Real transaction contention in the disposable storage-behavior database only."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Event, current_thread
from time import monotonic

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError

from portal.backend.service.storage.repos.market_lifecycle import MarketStorageLifecycleBusyError
from tests.test_market_data.test_fact_reclamation_db import _prepare, _physical
from tests.test_market_data.test_fact_storage_tiers_db import storage, _ingest, _placement, _read

pytestmark = pytest.mark.db
LOCK_NAME = "quant-trad:market-storage-lifecycle:v1"


def _capture_waiter(storage):
    reached = Event()
    identity = {}

    def capture(connection, cursor, statement, parameters, context, executemany):
        if (current_thread().name.startswith("publication-writer")
                and "pg_advisory_xact_lock_shared" in statement):
            identity["pid"] = connection.connection.driver_connection.get_backend_pid()
            reached.set()

    event.listen(storage.database._engine, "before_cursor_execute", capture)
    return reached, identity, capture


def _wait_without_hot_locks(storage, reached, identity, future):
    assert reached.wait(5), "writer did not reach the lifecycle fence"
    deadline = monotonic() + 5
    while monotonic() < deadline:
        if future.done():
            future.result()  # Preserve a real worker exception instead of reporting a wait timeout.
            pytest.fail("writer crossed an exclusively held lifecycle fence")
        with storage.database.session() as session:
            row = session.execute(text("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_locks WHERE pid=:pid
                    AND locktype='advisory' AND NOT granted
                ) AS waiting,
                (SELECT count(*) FROM pg_locks
                 WHERE pid=:pid AND granted AND locktype='relation'
                   AND relation IN ('market.fact_hot_payloads'::regclass,
                                    'market.fact_versions'::regclass)) AS hot_locks
            """), identity).mappings().one()
        if row["waiting"]:
            assert row["hot_locks"] == 0, "waiting publisher retained a hot-parent relation lock"
            return
        Event().wait(.01)
    pytest.fail("publisher never appeared as a lifecycle advisory-lock waiter")


def test_final_handoff_admitted_first_blocks_publisher_before_hot_parents_and_reclaims(
    storage, tmp_path, monkeypatch,
):
    day, archive, reclaimer = _prepare(storage, tmp_path, monkeypatch)
    before = _read(storage)
    old = _physical(storage, day)
    _placement(monkeypatch, storage.today)
    incoming = replace(storage.fact, observation_key="after-final-admission")
    reached, identity, capture = _capture_waiter(storage)
    original = archive._partition_evidence
    futures = []
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="publication-writer") as executor:
        def verify(session, partition, **kwargs):
            if kwargs.get("handoff") is not None and "objects" not in kwargs:
                assert not futures
                future = executor.submit(_ingest, storage, incoming)
                futures.append(future)
                _wait_without_hot_locks(storage, reached, identity, future)
            return original(session, partition, **kwargs)

        monkeypatch.setattr(archive, "_partition_evidence", verify)
        try:
            result = reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
        finally:
            event.remove(storage.database._engine, "before_cursor_execute", capture)
        assert result["status"] == "partition_reclaimed"
        assert futures[0].result(timeout=10).inserted_count == 1
    after = _physical(storage, day)
    assert after["relation"] is None and after["hot_rows"] == 0
    assert after["header_rows"] == old["header_rows"]
    assert all(row in _read(storage) for row in before)
    assert any(row.observation_key == incoming.observation_key for row in _read(storage))


def test_publisher_admitted_first_defers_cleanup_without_changing_verified_partition(
    storage, tmp_path, monkeypatch,
):
    day, archive, reclaimer = _prepare(storage, tmp_path, monkeypatch)
    before = _physical(storage, day)
    state = archive.inspect_partition(day)
    _placement(monkeypatch, storage.today)
    incoming = replace(storage.fact, observation_key="publisher-owned-first")
    class RollbackPublisher(Exception):
        pass
    with pytest.raises(RollbackPublisher):
        with storage.database.session() as writer:
            storage.repo.ingest_facts_in_session(
                writer, series_id=storage.series_id, source_id=storage.source_id, facts=[incoming],
            )
            with pytest.raises(MarketStorageLifecycleBusyError, match="canonical_reclaim_lifecycle_busy"):
                reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
            raise RollbackPublisher
    assert archive.inspect_partition(day) == state
    after = _physical(storage, day)
    for field in ("relation", "hot_rows", "header_rows"):
        assert after[field] == before[field]
    assert not any(row.observation_key == incoming.observation_key for row in _read(storage))


@pytest.mark.parametrize("interruption", ["timeout", "cancel"])
def test_waiting_publication_interruption_rolls_back_entire_transaction(
    storage, monkeypatch, interruption,
):
    _placement(monkeypatch, storage.today)
    incoming = replace(storage.fact, observation_key="interrupted-" + interruption)
    with storage.database.session() as session:
        runs_before = session.execute(text("SELECT count(*) FROM market.ingestion_runs")).scalar_one()
    reached, identity, capture = _capture_waiter(storage)

    def publish():
        with storage.database.session() as writer:
            writer.execute(text("SET LOCAL lock_timeout='2000ms'"))
            return storage.repo.ingest_facts_in_session(
                writer, series_id=storage.series_id, source_id=storage.source_id, facts=[incoming],
            )

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="publication-writer") as executor:
        try:
            with storage.database.session() as holder:
                holder.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:name,0))"), {"name": LOCK_NAME})
                future = executor.submit(publish)
                _wait_without_hot_locks(storage, reached, identity, future)
                if interruption == "cancel":
                    with storage.database.session() as observer:
                        assert observer.execute(text("SELECT pg_cancel_backend(:pid)"), identity).scalar_one()
                with pytest.raises(DBAPIError) as failure:
                    future.result(timeout=5)
                assert failure.value.orig.pgcode == ("57014" if interruption == "cancel" else "55P03")
        finally:
            event.remove(storage.database._engine, "before_cursor_execute", capture)
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.ingestion_runs")).scalar_one() == runs_before
        assert not session.execute(text("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=:pid AND locktype='advisory')"), identity).scalar_one()
    assert not any(row.observation_key == incoming.observation_key for row in _read(storage))


def test_waiting_trade_does_not_block_heartbeat_and_rechecks_expired_lease_after_admission(
    storage, tmp_path, monkeypatch,
):
    from portal.backend.service.storage.repos.market_structure import MarketStructureOwnershipError
    from tests.test_market_data.test_fact_raw_lineage_db import _raw_trade_fixture
    from tests.test_market_data.test_market_structure_phase1 import _fact

    fixture = _raw_trade_fixture(storage, tmp_path, monkeypatch, ordinals=(1,))
    fact, _raw = _fact()
    reached, identity, capture = _capture_waiter(storage)
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="publication-writer") as executor:
        try:
            with storage.database.session() as holder:
                holder.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:name,0))"), {"name": LOCK_NAME})
                publication = executor.submit(fixture.structures.ingest_trades, fixture.claim, facts=[fact])
                _wait_without_hot_locks(storage, reached, identity, publication)
                # Same claim/lease row: this would block if the waiting publisher
                # had taken FOR UPDATE before lifecycle admission.
                heartbeat = executor.submit(fixture.structures.heartbeat, fixture.claim, lease_seconds=60)
                assert heartbeat.result(timeout=3) is not None
                with storage.database.session() as observer:
                    observer.execute(text("UPDATE market.stream_lease_state SET expires_at=clock_timestamp()-interval '1 second' WHERE definition_id=:id"),
                                     {"id": fixture.claim.definition_id})
            with pytest.raises(MarketStructureOwnershipError, match="stale worker mutation rejected"):
                publication.result(timeout=5)
        finally:
            event.remove(storage.database._engine, "before_cursor_execute", capture)
    with storage.database.session() as observer:
        assert observer.execute(text("SELECT count(*) FROM market.fact_versions WHERE series_id=:id"),
                                {"id": fixture.series_id}).scalar_one() == 0
