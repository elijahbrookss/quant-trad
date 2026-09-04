from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from queue import Queue
from time import monotonic, sleep

import pytest
from sqlalchemy import text

from core.market_storage_lifecycle import MarketStorageLifecyclePolicy
from portal.backend.service.market.market_storage_lifecycle import MarketStorageLifecycleService
from portal.backend.service.storage.repos import market_lifecycle
from portal.backend.service.storage.repos.fact_references import lock_canonical_raw_references
from tests.test_market_data.test_fact_raw_lineage_db import _raw_trade_fixture, _canonical_trade, _raw_book_fixture, _publish_book_result
from tests.test_market_data.test_fact_storage_tiers_db import storage

pytestmark = pytest.mark.db


def _expiration_item(storage, manifest_id):
    with storage.database.session() as session:
        row = session.execute(text("SELECT object_key,object_sha256,last_received_at FROM market.raw_archive_manifests WHERE id=:id"),
                              {"id": manifest_id}).mappings().one()
    return {"operation_id": "lifetime-" + manifest_id, "action": "archive_expire",
            "target_kind": "raw_manifest", "target_id": manifest_id,
            "cutoff_at": (row["last_received_at"] + timedelta(days=1)).isoformat(),
            "reason": "disposable reference lifetime proof", "replacement_manifest_id": None,
            "object_key": row["object_key"], "object_sha256": row["object_sha256"]}


def test_hot_backlog_and_new_reference_expiry_races_preserve_evidence(storage, tmp_path, monkeypatch):
    fixture = _raw_trade_fixture(storage, tmp_path, monkeypatch)
    monkeypatch.setattr(market_lifecycle, "db", storage.database)
    lifecycle = market_lifecycle.market_storage_lifecycle_repository
    service = MarketStorageLifecycleService(lifecycle_repository=lifecycle, market_repository=fixture.structures)
    first, second = [_canonical_trade(fixture, raw) for raw in fixture.raws]
    first_item, second_item = [_expiration_item(storage, identity) for identity in fixture.manifests]
    assert lifecycle.archive_target_status(target_kind="raw_manifest", target_id=fixture.manifests[0])["pinned"] is False

    # No hot row is visible yet. Its writer's narrow KEY SHARE must still stop
    # a stale expiration plan; expiry defers immediately, without waiting.
    with storage.database.session() as writer:
        lock_canonical_raw_references(writer, [first])
        deferred = service._execute_archive_expiration(item=first_item, store=fixture.store)
        assert deferred["reason"] == "canonical_reference_busy" and deferred["status"] == "skipped"
        assert fixture.store.local_path(first_item["object_key"]).exists()
        storage.repo.ingest_facts_in_session(writer, series_id=fixture.series_id,
                                            source_id=fixture.source_id, facts=[first])
    status = lifecycle.archive_target_status(target_kind="raw_manifest", target_id=fixture.manifests[0])
    assert status["pinned"] is True and status["canonical_backlog_present"] is True
    assert status["canonical_dependency_count"] == status["dataset_pin_count"] == 0
    held = service._execute_archive_expiration(item=first_item, store=fixture.store)
    assert held["status"] == "skipped" and held["reason"] == "pinned"
    plan = service._plan_archive_expirations(policy=MarketStorageLifecyclePolicy(),
                                            now=first.known_at + timedelta(days=500))
    planned = next(row for row in plan if row["target_id"] == fixture.manifests[0])
    assert planned["blockers"] == ["canonical_hot_backlog"]

    # Expiry wins the second race. Observe the new writer actually waiting on
    # its row lock, then perform the real unlink/completion under that lock.
    # The fresh post-wait check must reject publication of the new reference.
    pid_queue = Queue()

    def late_writer():
        with storage.database.session() as session:
            pid_queue.put(session.execute(text("SELECT pg_backend_pid()")).scalar_one())
            return storage.repo.ingest_facts_in_session(session, series_id=fixture.series_id,
                                                        source_id=fixture.source_id, facts=[second])

    with ThreadPoolExecutor(max_workers=1) as executor:
        with lifecycle.archive_expiration_lock(target_kind="raw_manifest", target_id=fixture.manifests[1]):
            # An unrelated raw reference remains writable while this object is
            # locked. No global ingestion pause was introduced.
            unrelated = _canonical_trade(fixture, fixture.raws[0], trade_id="unrelated")
            assert storage.repo.ingest_facts(series_id=fixture.series_id, source_id=fixture.source_id,
                                            facts=[unrelated]).inserted_count == 1
            future = executor.submit(late_writer)
            pid = pid_queue.get(timeout=10)
            deadline = monotonic() + 10
            waiting = False
            while monotonic() < deadline:
                with storage.database.session() as observer:
                    waiting = observer.execute(text("SELECT wait_event_type='Lock' FROM pg_stat_activity WHERE pid=:pid"),
                                               {"pid": pid}).scalar_one()
                if waiting:
                    break
                sleep(0.02)
            assert waiting and not future.done(), "the reference writer must be observed waiting, not inferred from timing"
            outcome = service._execute_locked_archive_expiration(item=second_item, store=fixture.store)
            assert outcome["status"] == "completed"
        with pytest.raises(RuntimeError, match="canonical_raw_reference_expired"):
            future.result(timeout=10)
    assert fixture.store.local_path(first_item["object_key"]).exists()
    assert not fixture.store.local_path(second_item["object_key"]).exists()
    with storage.database.session() as session:
        rows = session.execute(text("SELECT observation_key,revision FROM market.fact_versions WHERE series_id=:id ORDER BY observation_key"),
                               {"id": fixture.series_id}).all()
    assert len(rows) == 2 and all(row.revision == 1 for row in rows)

    # Existing identical canonical state is still a no-op, not a new raw pin.
    assert storage.repo.ingest_facts(series_id=fixture.series_id, source_id=fixture.source_id,
                                    facts=[first]).noop_count == 1


def test_interrupted_unlink_blocks_new_references_until_expiration_resumes(storage, tmp_path, monkeypatch):
    fixture = _raw_trade_fixture(storage, tmp_path, monkeypatch)
    monkeypatch.setattr(market_lifecycle, "db", storage.database)
    lifecycle = market_lifecycle.market_storage_lifecycle_repository
    service = MarketStorageLifecycleService(lifecycle_repository=lifecycle, market_repository=fixture.structures)
    item = _expiration_item(storage, fixture.manifests[0])
    fact = _canonical_trade(fixture, fixture.raws[0])
    append_event = lifecycle.append_event

    def fail_completion(**event):
        if event["action"] == "archive_expire" and event["event_type"] == "completed":
            raise RuntimeError("injected completion write failure after unlink")
        return append_event(**event)

    with monkeypatch.context() as interrupted:
        interrupted.setattr(lifecycle, "append_event", fail_completion)
        assert service._execute_archive_expiration(item=item, store=fixture.store)["status"] == "failed"
    assert not fixture.store.local_path(item["object_key"]).exists()
    assert lifecycle.archive_target_status(target_kind="raw_manifest", target_id=fixture.manifests[0])["expired"] is False
    with pytest.raises(RuntimeError, match="canonical_raw_reference_expiration_pending"):
        storage.repo.ingest_facts(series_id=fixture.series_id, source_id=fixture.source_id, facts=[fact])
    resumed = service._execute_archive_expiration(item=item, store=fixture.store)
    assert resumed["status"] == "completed" and resumed["recovered_after_prior_plan"] is True
    with pytest.raises(RuntimeError, match="canonical_raw_reference_expired"):
        storage.repo.ingest_facts(series_id=fixture.series_id, source_id=fixture.source_id, facts=[fact])
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_versions WHERE series_id=:id"),
                               {"id": fixture.series_id}).scalar_one() == 0


def test_late_book_prefix_admission_and_control_frame_expiry_exclude_each_other(storage, tmp_path, monkeypatch):
    fixture = _raw_book_fixture(storage, tmp_path, monkeypatch)
    monkeypatch.setattr(market_lifecycle, "db", storage.database)
    lifecycle = market_lifecycle.market_storage_lifecycle_repository
    service = MarketStorageLifecycleService(lifecycle_repository=lifecycle, market_repository=fixture.structures)
    control = _expiration_item(storage, fixture.manifests[1])
    tail = fixture.facts[-1]
    with storage.database.session() as writer:
        lock_canonical_raw_references(writer, [tail])
        deferred = service._execute_archive_expiration(item=control, store=fixture.store)
        assert deferred["reason"] == "canonical_reference_busy" and deferred["status"] == "skipped"
        assert fixture.store.local_path(control["object_key"]).exists()
    # No new Fact was published. Expiry can now win; having the final frame
    # still available must not allow a late import to hide the missing prefix.
    assert service._execute_archive_expiration(item=control, store=fixture.store)["status"] == "completed"
    with pytest.raises(RuntimeError, match="canonical_raw_reference_expired.*prefix"):
        _publish_book_result(fixture, 1)
    with storage.database.session() as session:
        assert session.execute(text("SELECT count(*) FROM market.fact_versions WHERE series_id=:id"),
                               {"id": fixture.series_id}).scalar_one() == 0
