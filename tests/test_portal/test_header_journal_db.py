"""Journal tests use disposable PostgreSQL and synthetic verified disk evidence.

These qualify transactions and capacity accounting, not physical DDL or mounts.
"""
from dataclasses import replace
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import create_engine, select, text, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from core.storage_header_placement import (
    HeaderPartitionPlacement, HeaderPlacementSnapshot, RelationPlacement,
)
from core.storage_mounts import FilesystemEvidence
from core.storage_targets import StoragePolicy, StorageTarget
from portal.backend.db.storage_target_models import (
    StorageTargetRecord, StoragePolicyRecord, StoragePlanRecord,
    StorageHeaderBatchRecord, StorageHeaderMoveRecord, StorageHeaderTablespaceRecord,
)
from portal.backend.service.storage.header_filesystem import VerifiedHeaderPlacement, VerifiedTablespaceDestination
from portal.backend.service.storage.header_destinations import register_header_tablespaces, review_header_moves
from portal.backend.service.storage.header_journal import (
    reserve_header_batch, cancel_unstarted_header_batch,
)
from portal.backend.service.storage_management import StorageConflict
from tests.test_market_data.migration_test_support import fresh_migration_database

pytestmark = pytest.mark.db


@pytest.fixture
def journal():
    with fresh_migration_database("header_journal", install_extensions=False) as dsn:
        engine = create_engine(dsn)
        try:
            for model in (StorageTargetRecord, StoragePolicyRecord, StoragePlanRecord,
                          StorageHeaderTablespaceRecord, StorageHeaderBatchRecord, StorageHeaderMoveRecord):
                model.__table__.create(engine)
            targets = (
                StorageTarget("ssd", "SSD", "uuid-ssd", "/qt/ssd", "ssd"),
                StorageTarget("hdd", "HDD", "uuid-hdd", "/qt/hdd", "hdd"),
            )
            policy = StoragePolicy(recent=("ssd",), history=("hdd",),
                archives=("hdd",), backups=("hdd",), movement_enabled=True)
            with Session(engine) as session, session.begin():
                for target in targets:
                    session.add(StorageTargetRecord(id=target.target_id, label=target.label,
                        filesystem_uuid=target.filesystem_uuid, root=target.root,
                        medium=target.medium, roles=list(target.roles), reserved_bytes=0))
                session.add(StoragePolicyRecord(id=1, revision=3, policy=policy.to_dict()))
                for plan_id in ("plan-a", "plan-b"):
                    session.add(StoragePlanRecord(id=plan_id, request_id=plan_id,
                        base_revision=3, policy=policy.to_dict(), policy_hash=policy.fingerprint,
                        state="queued", impact={}, progress={}))
            with engine.connect() as conn:
                now = conn.scalar(text("SELECT clock_timestamp()"))
                identity = conn.scalar(text("""
                    SELECT c.system_identifier::text || '/' || d.oid::text
                    FROM pg_control_system() c CROSS JOIN pg_database d
                    WHERE d.datname=current_database()
                """))
            groups = tuple(HeaderPartitionPlacement(
                now.date() - timedelta(days=40+i),
                RelationPlacement(100+i*10, 100+i*10, "market", f"heap_{i}", "ssd", 1000),
                (RelationPlacement(101+i*10, 101+i*10, "market", f"index_{i}", "ssd", 200),),
                True, True,
            ) for i in range(2))
            snapshot = HeaderPlacementSnapshot(identity, now.date(), now, groups, True)
            capacity = {t.target_id: FilesystemEvidence(path=t.root, filesystem_uuid=t.filesystem_uuid,
                device_id=f"8:{i}", used_bytes=0, total_bytes=100000,
                available_bytes=100000, read_only=False) for i, t in enumerate(targets)}
            destination = VerifiedTablespaceDestination(identity, "hdd", "uuid-hdd", "8:1",
                9000, "qt_history", "/qt/hdd/tablespace",
                "/qt/hdd/tablespace/PG_15_202209061", "/qt/hdd/tablespace/PG_15_202209061",
                10001, 202209061, "/qt/hdd")
            verified = VerifiedHeaderPlacement(snapshot, capacity, (), now, (destination,))
            with Session(engine) as session, session.begin():
                register_header_tablespaces(session, verified=verified)
            yield engine, targets, policy, verified
        finally:
            engine.dispose()


def proposal(journal, verified=None, reservations=None):
    _, targets, policy, original = journal
    return review_header_moves(verified=verified or original,
        policy=policy, targets=targets,
        reserved_bytes=reservations or {"ssd": 0, "hdd": 0})


def reserve(journal, *, plan_id="plan-a", verified=None, review_hash=None):
    engine, _, _, original = journal
    with Session(engine) as session, session.begin():
        return reserve_header_batch(session, plan_id=plan_id,
            verified=verified or original,
            review_hash=review_hash or proposal(journal, verified)["plan_hash"])


def state(journal):
    with Session(journal[0]) as session:
        return (session.get(StorageTargetRecord, "hdd").reserved_bytes,
                session.scalar(select(func.count()).select_from(StorageHeaderBatchRecord)),
                session.scalar(select(func.count()).select_from(StorageHeaderMoveRecord)))


def test_committed_reservation_records_complete_group_and_replay_never_double_reserves(journal):
    receipt = reserve(journal)
    assert state(journal) == (2400, 1, 2)
    assert not receipt["execution_available"] and not receipt["activation_ready"]
    expired = replace(journal[3], verified_at=journal[3].verified_at - timedelta(hours=1))
    retry = reserve(journal, verified=expired)
    assert retry["reused"] and retry["moves"] == receipt["moves"]
    assert state(journal) == (2400, 1, 2)
    with Session(journal[0]) as session:
        group = session.scalar(select(StorageHeaderMoveRecord))
        assert group.filesystem_uuid == "uuid-hdd"
        assert group.source_group["heap"]["target_id"] == "ssd"
        assert len(group.source_group["indexes"]) == 1
        assert group.source_group["source_space_credited_bytes"] == 0
        assert group.destination_binding["tablespace_oid"] == 9000
        assert group.destination_binding["directory_inode"] == 10001


def test_failure_before_commit_rolls_back_journal_and_reservations_together(journal):
    with pytest.raises(RuntimeError, match="lost connection"):
        with Session(journal[0]) as session, session.begin():
            reserve_header_batch(session, plan_id="plan-a", verified=journal[3],
                                 review_hash=proposal(journal)["plan_hash"])
            raise RuntimeError("lost connection before commit")
    assert state(journal) == (0, 0, 0)
    assert not reserve(journal)["reused"]
    assert state(journal) == (2400, 1, 2)


def test_changed_review_cannot_reuse_batch(journal):
    reserve(journal)
    with pytest.raises(StorageConflict, match="review_conflict"):
        reserve(journal, review_hash="a"*64)
    assert state(journal) == (2400, 1, 2)


def test_changed_reservation_invalidates_review_without_partial_writes(journal):
    with Session(journal[0]) as session, session.begin():
        session.get(StorageTargetRecord, "hdd").reserved_bytes = 300
    with pytest.raises(StorageConflict, match="review_changed"):
        reserve(journal)
    assert state(journal) == (300, 0, 0)


@pytest.mark.parametrize("change, expected", [
    ("expired", "evidence_expired"),
    ("future", "evidence_expired"),
    ("clock_order", "clock_mismatch"),
    ("database", "database_mismatch"),
    ("capacity", "placement_blocked"),
    ("mount", "review_invalid"),
])
def test_invalid_evidence_never_acquires_capacity(journal, change, expected):
    verified = journal[3]
    if change == "expired":
        verified = replace(verified, verified_at=verified.verified_at-timedelta(minutes=2))
    elif change == "future":
        verified = replace(verified, verified_at=verified.verified_at+timedelta(minutes=2))
    elif change == "clock_order":
        verified = replace(verified, verified_at=verified.verified_at-timedelta(seconds=1))
    elif change == "database":
        verified = replace(verified, snapshot=replace(verified.snapshot, database_identity="other/99"))
    else:
        capacity = dict(verified.capacity)
        capacity["hdd"] = replace(capacity["hdd"],
            **({"available_bytes": 1} if change == "capacity" else {"filesystem_uuid": "wrong"}))
        verified = replace(verified, capacity=capacity)
    with pytest.raises(StorageConflict, match=expected):
        reserve(journal, verified=verified, review_hash=proposal(journal)["plan_hash"])
    assert state(journal) == (0, 0, 0)


@pytest.mark.parametrize("change, expected", [
    ("revision", "policy_changed"), ("planned", "plan_not_queued"),
    ("disabled", "policy_not_admitted"), ("hash", "policy_not_admitted"),
])
def test_policy_and_queue_admission(journal, change, expected):
    with Session(journal[0]) as session, session.begin():
        plan = session.get(StoragePlanRecord, "plan-a")
        if change == "revision":
            session.get(StoragePolicyRecord, 1).revision += 1
        elif change == "planned":
            plan.state = "planned"
        elif change == "hash":
            plan.policy_hash = "b"*64
        else:
            plan.policy = {**plan.policy, "movement_enabled": False}
            plan.policy_hash = StoragePolicy.from_dict(plan.policy).fingerprint
    with pytest.raises(StorageConflict, match=expected):
        reserve(journal)
    assert state(journal) == (0, 0, 0)


def test_second_plan_cannot_own_same_heap_even_with_fresh_capacity_review(journal):
    reserve(journal)
    new_hash = proposal(journal, reservations={"ssd": 0, "hdd": 2400})["plan_hash"]
    with pytest.raises(StorageConflict, match="group_already_owned"):
        reserve(journal, plan_id="plan-b", review_hash=new_hash)
    assert state(journal) == (2400, 1, 2)


def test_storage_lock_contention_fails_promptly_then_retry_is_idempotent(journal):
    with Session(journal[0]) as session, session.begin():
        first = reserve_header_batch(session, plan_id="plan-a", verified=journal[3],
                                     review_hash=proposal(journal)["plan_hash"])
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(reserve, journal)
            with pytest.raises(StorageConflict, match="journal_busy"):
                future.result(timeout=5)
    assert reserve(journal)["moves"] == first["moves"]
    assert state(journal) == (2400, 1, 2)


def test_cancellation_preserves_other_capacity_and_replay_cannot_reactivate(journal):
    reserve(journal)
    with Session(journal[0]) as session, session.begin():
        session.get(StorageTargetRecord, "hdd").reserved_bytes += 500
    review_hash = proposal(journal)["plan_hash"]
    for repeated in (False, True):
        with Session(journal[0]) as session, session.begin():
            result = cancel_unstarted_header_batch(session, plan_id="plan-a", review_hash=review_hash)
            assert result["reused"] is repeated
            assert result["cancelled"]
            assert all(row["reserved_bytes"] == 0 for row in result["moves"])
    assert state(journal) == (500, 1, 2)
    assert reserve(journal)["cancelled"]


@pytest.mark.parametrize("state_name", ["running", "blocked", "completed"])
def test_started_or_uncertain_movement_cannot_release_reservation(journal, state_name):
    reserve(journal)
    with Session(journal[0]) as session, session.begin():
        session.scalar(select(StorageHeaderMoveRecord)).state = state_name
    with pytest.raises(StorageConflict, match="reconciliation_required"):
        with Session(journal[0]) as session, session.begin():
            cancel_unstarted_header_batch(session, plan_id="plan-a",
                                           review_hash=proposal(journal)["plan_hash"])
    assert state(journal) == (2400, 1, 2)


def test_cancellation_failure_rolls_back_release_and_states(journal):
    reserve(journal)
    with pytest.raises(RuntimeError):
        with Session(journal[0]) as session, session.begin():
            cancel_unstarted_header_batch(session, plan_id="plan-a",
                                           review_hash=proposal(journal)["plan_hash"])
            raise RuntimeError("rollback")
    assert state(journal) == (2400, 1, 2)
    assert all(row["state"] == "reserved" for row in reserve(journal)["moves"])


def test_empty_batch_has_durable_cancellation_and_retry(journal):
    verified = replace(journal[3], snapshot=replace(journal[3].snapshot, partitions=()))
    receipt = reserve(journal, verified=verified)
    assert receipt["group_count"] == 0 and state(journal) == (0, 1, 0)
    with Session(journal[0]) as session, session.begin():
        assert cancel_unstarted_header_batch(session, plan_id="plan-a",
            review_hash=receipt["review_hash"])["cancelled"]
    assert reserve(journal, verified=verified)["cancelled"]


def test_incomplete_ledger_or_capacity_fails_closed(journal):
    reserve(journal)
    with Session(journal[0]) as session, session.begin():
        session.get(StorageTargetRecord, "hdd").reserved_bytes = 1
    with pytest.raises(StorageConflict, match="reservation_inconsistent"):
        with Session(journal[0]) as session, session.begin():
            cancel_unstarted_header_batch(session, plan_id="plan-a",
                                           review_hash=proposal(journal)["plan_hash"])
    with Session(journal[0]) as session, session.begin():
        session.delete(session.scalar(select(StorageHeaderMoveRecord)))
    with pytest.raises(StorageConflict, match="journal_incomplete"):
        reserve(journal)


def test_database_enforces_active_heap_uniqueness_and_evidence_bounds(journal):
    reserve(journal)
    with Session(journal[0]) as session:
        row = session.scalar(select(StorageHeaderMoveRecord))
        original = dict(row.source_group)
        row_id = row.id
    for evidence in ({}, {**original, "indexes": [{}]*65}):
        with pytest.raises(IntegrityError):
            with Session(journal[0]) as session, session.begin():
                session.get(StorageHeaderMoveRecord, row_id).source_group = evidence
    with pytest.raises(IntegrityError):
        with Session(journal[0]) as session, session.begin():
            rows = list(session.scalars(select(StorageHeaderMoveRecord)))
            rows[1].heap_oid = rows[0].heap_oid


def test_lost_commit_receipt_reconciles_after_connection_pool_restart(journal):
    # The transaction has committed, but its caller never received the receipt.
    reserve(journal)
    journal[0].dispose()
    retry = reserve(journal)
    assert retry["reused"]
    assert state(journal) == (2400, 1, 2)


def test_repeatable_read_cannot_reserve_or_cancel_from_an_old_snapshot(journal):
    reserve(journal)
    for action in ("reserve", "cancel"):
        with journal[0].connect().execution_options(isolation_level="REPEATABLE READ") as conn:
            with Session(conn) as session, session.begin():
                with pytest.raises(StorageConflict, match="requires_read_committed"):
                    if action == "reserve":
                        reserve_header_batch(session, plan_id="plan-a", verified=journal[3],
                                             review_hash=proposal(journal)["plan_hash"])
                    else:
                        cancel_unstarted_header_batch(session, plan_id="plan-a",
                                                      review_hash=proposal(journal)["plan_hash"])
    assert state(journal) == (2400, 1, 2)


def test_cached_orm_target_is_refreshed_before_cancellation(journal):
    reserve(journal)
    with Session(journal[0], expire_on_commit=False) as stale:
        target = stale.get(StorageTargetRecord, "hdd")
        stale.commit()
        with Session(journal[0]) as other, other.begin():
            other.get(StorageTargetRecord, "hdd").reserved_bytes += 500
        assert target.reserved_bytes == 2400
        with stale.begin():
            cancel_unstarted_header_batch(stale, plan_id="plan-a",
                                           review_hash=proposal(journal)["plan_hash"])
    assert state(journal) == (500, 1, 2)


def test_journal_uses_same_lock_as_storage_policy_management(journal):
    from portal.backend.service.storage_management import _lock as management_lock
    with Session(journal[0]) as session, session.begin():
        management_lock(session)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(reserve, journal)
            with pytest.raises(StorageConflict, match="journal_busy"):
                future.result(timeout=5)
    assert state(journal) == (0, 0, 0)
    reserve(journal)
    assert state(journal) == (2400, 1, 2)


def test_registration_reuses_stable_identity_but_new_inode_and_device_change_review(journal):
    engine, _, _, original = journal
    destination = replace(original.destinations[0], device_id="8:9", directory_inode=20002)
    capacity = {**original.capacity, "hdd": replace(original.capacity["hdd"], device_id="8:9")}
    observed = replace(original, destinations=(destination,), capacity=capacity)
    with Session(engine) as session, session.begin():
        assert register_header_tablespaces(session, verified=observed) == [
            {"target_id": "hdd", "tablespace_oid": 9000, "reused": True}]
        binding = session.get(StorageHeaderTablespaceRecord,
                              (original.snapshot.database_identity, "hdd")).binding
        assert "device_id" not in binding and "directory_inode" not in binding
    assert proposal(journal, observed)["plan_hash"] != proposal(journal)["plan_hash"]


@pytest.mark.parametrize("change", ["oid", "name", "location"])
def test_registered_destination_cannot_be_repointed(journal, change):
    original = journal[3]
    destination = original.destinations[0]
    if change == "oid":
        destination = replace(destination, tablespace_oid=9001)
    elif change == "name":
        destination = replace(destination, tablespace_name="renamed")
    else:
        destination = replace(destination, tablespace_location="/qt/hdd/other",
            directory="/qt/hdd/other/PG_15_202209061", server_directory="/qt/hdd/other/PG_15_202209061")
    with pytest.raises(StorageConflict, match="identity_changed"):
        with Session(journal[0]) as session, session.begin():
            register_header_tablespaces(session, verified=replace(original, destinations=(destination,)))
    assert state(journal) == (0, 0, 0)


def test_one_tablespace_cannot_be_registered_to_two_targets(journal):
    original = journal[3]
    destination = replace(original.destinations[0], target_id="ssd", filesystem_uuid="uuid-ssd",
        device_id="8:0", target_root="/qt/ssd", tablespace_location="/qt/ssd/tablespace",
        directory="/qt/ssd/tablespace/PG_15_202209061", server_directory="/qt/ssd/tablespace/PG_15_202209061")
    with pytest.raises(StorageConflict, match="already_registered"):
        with Session(journal[0]) as session, session.begin():
            register_header_tablespaces(session, verified=replace(original, destinations=(destination,)))


def test_registration_rollback_leaves_no_destination_or_reservation(journal):
    key = (journal[3].snapshot.database_identity, "hdd")
    with Session(journal[0]) as session, session.begin():
        session.delete(session.get(StorageHeaderTablespaceRecord, key))
    with pytest.raises(RuntimeError, match="rollback"):
        with Session(journal[0]) as session, session.begin():
            register_header_tablespaces(session, verified=journal[3])
            raise RuntimeError("rollback")
    with Session(journal[0]) as session:
        assert session.get(StorageHeaderTablespaceRecord, key) is None
    with pytest.raises(StorageConflict, match="destination_not_registered"):
        reserve(journal)
    assert state(journal) == (0, 0, 0)


def test_missing_verified_destination_blocks_the_entire_batch(journal):
    observed = replace(journal[3], destinations=())
    with pytest.raises(StorageConflict, match="placement_blocked"):
        reserve(journal, verified=observed)
    assert state(journal) == (0, 0, 0)


def test_changed_destination_inode_requires_new_review_before_reserving(journal):
    original = journal[3]
    observed = replace(original, destinations=(replace(original.destinations[0], directory_inode=22222),))
    with pytest.raises(StorageConflict, match="review_changed"):
        reserve(journal, verified=observed, review_hash=proposal(journal)["plan_hash"])
    assert state(journal) == (0, 0, 0)
    reserve(journal, verified=observed)
    with Session(journal[0]) as session:
        assert session.scalar(select(StorageHeaderMoveRecord)).destination_binding["directory_inode"] == 22222


def test_new_review_cannot_override_immutable_registration(journal):
    original = journal[3]
    observed = replace(original, destinations=(replace(original.destinations[0], tablespace_name="changed"),))
    with pytest.raises(StorageConflict, match="destination_not_registered"):
        reserve(journal, verified=observed)
    assert state(journal) == (0, 0, 0)


def test_reserved_moves_keep_their_registered_tablespace_from_deletion(journal):
    reserve(journal)
    with pytest.raises(IntegrityError):
        with Session(journal[0]) as session, session.begin():
            session.delete(session.get(StorageHeaderTablespaceRecord,
                                       (journal[3].snapshot.database_identity, "hdd")))
    assert state(journal) == (2400, 1, 2)


def test_registration_rechecks_database_and_observation_age(journal):
    original = journal[3]
    for observed, expected in [
        (replace(original, verified_at=original.verified_at-timedelta(minutes=2)), "evidence_expired"),
        (replace(original, snapshot=replace(original.snapshot, database_identity="other/42")), "database_mismatch"),
    ]:
        with pytest.raises(StorageConflict, match=expected):
            with Session(journal[0]) as session, session.begin():
                register_header_tablespaces(session, verified=observed)


def test_changed_registered_root_cannot_reuse_old_verified_observation(journal):
    with Session(journal[0]) as session, session.begin():
        session.get(StorageTargetRecord, "hdd").root = "/qt/repointed"
    with pytest.raises(ValueError, match="identity_mismatch"):
        with Session(journal[0]) as session, session.begin():
            register_header_tablespaces(session, verified=journal[3])
    with pytest.raises(StorageConflict, match="review_invalid"):
        reserve(journal)
    assert state(journal) == (0, 0, 0)
