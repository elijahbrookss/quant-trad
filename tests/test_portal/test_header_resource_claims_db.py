"""Durable resource claims on disposable PostgreSQL with synthetic path adapters."""
from copy import deepcopy
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from portal.backend.db.storage_target_models import StorageHeaderMoveRecord, StorageTargetRecord
from portal.backend.service.storage.header_resource_claims import reserve_header_resources
from portal.backend.service.storage.header_journal import cancel_unstarted_header_batch
from portal.backend.service.storage_management import StorageConflict
from tests.test_portal.test_header_resource_inspection_db import resource_inspection, limits, invoke
from tests.test_portal.test_header_inspection_db import inspection
from tests.test_portal.test_header_journal_db import journal, state

pytestmark = pytest.mark.db


def acquire(session, fixture, **overrides):
    source = fixture[0]
    return reserve_header_resources(session, move_id=source[1], review_hash=source[2],
        pg_controldata="/disposable/pg_controldata", **{**limits(), **overrides})


def counters(fixture):
    with Session(fixture[0][0][0]) as session:
        return {row.id: row.auxiliary_reserved_bytes
                for row in session.scalars(select(StorageTargetRecord))}


def test_claim_commit_and_replay_preserve_copy_ledger_and_bound_other_work(resource_inspection):
    source, holder = resource_inspection
    engine = source[0][0]
    with Session(engine) as session, session.begin():
        session.execute(text("SET LOCAL statement_timeout='5s'"))
        first = acquire(session, resource_inspection)
        assert session.scalar(text("SHOW statement_timeout")) == "5s"
    held = counters(resource_inspection)
    assert held == first["auxiliary_reserved_bytes"]
    assert held["ssd"] > 1150 and held["hdd"] > 50
    assert state(source[0]) == (2400, 1, 2)
    observed = holder["calls"]
    with Session(engine) as session, session.begin():
        retry = acquire(session, resource_inspection)
        inspection = invoke(session, resource_inspection)
    assert retry["reused"] and retry["auxiliary_reserved_bytes"] == held
    assert holder["calls"] == observed + 1  # Explicit fresh preview, not replay.
    assert counters(resource_inspection) == held
    assert not retry["execution_available"]
    rows = {row["target_id"]: row for row in inspection.budget["filesystems"]}
    assert rows["ssd"]["other_reserved_auxiliary_bytes"] == 0
    assert rows["hdd"]["other_reserved_auxiliary_bytes"] == 0


def test_caller_rollback_does_not_leak_auxiliary_reservation(resource_inspection):
    engine = resource_inspection[0][0][0]
    with pytest.raises(RuntimeError, match="abort"):
        with Session(engine) as session, session.begin():
            acquire(session, resource_inspection)
            raise RuntimeError("abort")
    assert counters(resource_inspection) == {"ssd": 0, "hdd": 0}
    with Session(engine) as session:
        assert session.get(StorageHeaderMoveRecord, resource_inspection[0][1]).resource_claim is None


def test_changed_retry_limits_refuse_without_reserving_again(resource_inspection):
    engine = resource_inspection[0][0][0]
    with Session(engine) as session, session.begin():
        acquire(session, resource_inspection)
    held = counters(resource_inspection)
    with pytest.raises(StorageConflict, match="request_conflict"):
        with Session(engine) as session, session.begin():
            acquire(session, resource_inspection, wal_bytes=1001)
    assert counters(resource_inspection) == held


def test_wholly_unstarted_cancellation_releases_auxiliary_once(resource_inspection):
    source = resource_inspection[0]
    engine = source[0][0]
    with Session(engine) as session, session.begin():
        acquire(session, resource_inspection)
    for _ in range(2):
        with Session(engine) as session, session.begin():
            cancel_unstarted_header_batch(session, plan_id="plan-a", review_hash=source[2])
        assert counters(resource_inspection) == {"ssd": 0, "hdd": 0}
    with Session(engine) as session, session.begin():
        replay = acquire(session, resource_inspection)
    assert replay["reused"] and replay["state"] == "cancelled"
    assert replay["auxiliary_reserved_bytes"] == {"ssd": 0, "hdd": 0}


def test_competing_auxiliary_claim_blocks_source_before_any_write(resource_inspection):
    source = resource_inspection[0]
    engine = source[0][0]
    with Session(engine) as session, session.begin():
        session.get(StorageTargetRecord, "ssd").auxiliary_reserved_bytes = 79000
    with pytest.raises(StorageConflict, match="capacity_blocked"):
        with Session(engine) as session, session.begin():
            acquire(session, resource_inspection)
    assert counters(resource_inspection) == {"ssd": 79000, "hdd": 0}
    with Session(engine) as session:
        assert session.get(StorageHeaderMoveRecord, source[1]).resource_claim is None


@pytest.mark.parametrize("corruption", ["amount", "uuid", "hash", "counter"])
def test_inconsistent_claim_cannot_be_released_or_reused(resource_inspection, corruption):
    source = resource_inspection[0]
    engine = source[0][0]
    with Session(engine) as session, session.begin():
        acquire(session, resource_inspection)
    with Session(engine) as session, session.begin():
        move = session.get(StorageHeaderMoveRecord, source[1])
        claim = deepcopy(move.resource_claim)
        if corruption == "amount":
            claim["allocations"]["ssd"]["bytes"] += 1
        elif corruption == "uuid":
            claim["allocations"]["ssd"]["filesystem_uuid"] = "wrong"
        elif corruption == "hash":
            claim["limits_hash"] = "0"*64
        else:
            session.get(StorageTargetRecord, "ssd").auxiliary_reserved_bytes = 0
        move.resource_claim = claim
    before = counters(resource_inspection)
    with pytest.raises(StorageConflict, match="claim_"):
        with Session(engine) as session, session.begin():
            cancel_unstarted_header_batch(session, plan_id="plan-a", review_hash=source[2])
    assert counters(resource_inspection) == before
    assert state(source[0]) == (2400, 1, 2)


def test_competing_connection_cannot_reserve_during_owned_transaction(resource_inspection):
    source = resource_inspection[0]
    engine = source[0][0]
    def contender():
        with Session(engine) as session, session.begin():
            acquire(session, resource_inspection)
    with Session(engine) as session, session.begin():
        acquire(session, resource_inspection)
        with ThreadPoolExecutor(max_workers=1) as pool:
            with pytest.raises(StorageConflict, match="journal_busy"):
                pool.submit(contender).result(timeout=5)
    assert counters(resource_inspection)["ssd"] > 0


def test_database_rejects_negative_auxiliary_counter(resource_inspection):
    engine = resource_inspection[0][0][0]
    with pytest.raises(IntegrityError):
        with Session(engine) as session, session.begin():
            session.get(StorageTargetRecord, "ssd").auxiliary_reserved_bytes = -1
    assert counters(resource_inspection) == {"ssd": 0, "hdd": 0}
