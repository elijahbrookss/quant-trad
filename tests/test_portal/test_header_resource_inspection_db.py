"""Resource composition uses a disposable DB and synthetic physical adapters."""
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from portal.backend.db.storage_target_models import StoragePlanRecord
from portal.backend.service.storage import header_inspection as module
from portal.backend.service.storage.header_resources import VerifiedHeaderResources
from portal.backend.service.storage_management import StorageConflict
from tests.test_market_data.test_header_resource_inspection import make_bindings
from tests.test_portal.test_header_inspection_db import inspection
from tests.test_portal.test_header_journal_db import journal, state

pytestmark = pytest.mark.db


@pytest.fixture
def resource_inspection(inspection, monkeypatch):
    holder = {"change": lambda value: value, "calls": 0, "connection": None}
    def observe(conn, targets, **kwargs):
        holder["calls"] += 1
        holder["connection"] = conn
        context = conn.execute(text("SELECT clock_timestamp() AS now,pg_backend_pid() AS pid")).mappings().one()
        verified = inspection[3]["verified"]
        result = VerifiedHeaderResources(verified.snapshot.database_identity, context["pid"],
            context["now"], context["now"], "", dict(verified.capacity), make_bindings(verified.capacity))
        return holder["change"](result)
    monkeypatch.setattr(module, "observe_header_resources", observe)
    return inspection, holder


def limits():
    return dict(wal_bytes=1000, temporary_bytes={"ssd": 100, "hdd": 0},
        growth_bytes_per_second={"ssd": 10, "hdd": 20}, maintenance_bytes={"ssd": 50, "hdd": 50},
        movement_timeout_seconds=10, cancellation_grace_seconds=2)


def invoke(session, fixture, **overrides):
    inspection, _ = fixture
    return module.inspect_reserved_header_move_resources(session, move_id=inspection[1],
        review_hash=inspection[2], pg_controldata=Path("/disposable/pg_controldata"),
        **{**limits(), **overrides})


def test_composition_preserves_caller_work_timeout_and_reservations(resource_inspection):
    inspection, holder = resource_inspection
    source = inspection[0]
    before = state(source)
    with Session(source[0]) as session:
        transaction = session.begin()
        session.execute(text("SET LOCAL statement_timeout='5s'"))
        session.get(StoragePlanRecord, "plan-a").progress = {"caller": "pending"}
        session.flush()
        result = invoke(session, resource_inspection)
        assert holder["connection"] is session.connection()
        assert session.scalar(text("SHOW statement_timeout")) == "5s"
        assert session.in_transaction()
        assert result.policy_revision == 3 and result.review_hash == inspection[2]
        assert result.temporary_target_ids == ("ssd",)
        assert not result.execution_available and not result.budget["execution_available"]
        assert result.budget["capacity_sufficient_for_declared_limits"]
        rows = {row["target_id"]: row for row in result.budget["filesystems"]}
        assert rows["ssd"]["additional_wal_bytes"] == 1000
        assert rows["hdd"]["copy_bytes"] == rows["hdd"]["other_reserved_copy_bytes"] == 1200
        transaction.rollback()
    assert state(source) == before
    with Session(source[0]) as session:
        assert session.get(StoragePlanRecord, "plan-a").progress == {}


@pytest.mark.parametrize("change", ["database", "backend", "expired", "future", "order"])
def test_resource_identity_and_freshness_are_bound_to_current_inspection(resource_inspection, change):
    inspection, holder = resource_inspection
    def alter(value):
        if change == "database":
            return replace(value, database_identity="999/999")
        if change == "backend":
            return replace(value, backend_pid=value.backend_pid + 1)
        if change in ("expired", "future"):
            delta = timedelta(seconds=-31 if change == "expired" else 31)
            return replace(value, observed_at=value.observed_at + delta, verified_at=value.verified_at + delta)
        return replace(value, observed_at=value.observed_at-timedelta(seconds=1),
                       verified_at=value.verified_at-timedelta(seconds=2))
    holder["change"] = alter
    with pytest.raises(StorageConflict, match="resource_"):
        with Session(inspection[0][0]) as session, session.begin():
            invoke(session, resource_inspection)
    assert state(inspection[0]) == (2400, 1, 2)


def test_source_drive_shortage_returns_conditional_blocker_without_moving(resource_inspection):
    inspection, holder = resource_inspection
    def alter(value):
        capacity = dict(value.capacity)
        capacity["ssd"] = replace(capacity["ssd"], used_bytes=99999, available_bytes=1)
        return replace(value, capacity=capacity)
    holder["change"] = alter
    with Session(inspection[0][0]) as session, session.begin():
        result = invoke(session, resource_inspection)
    assert not result.budget["capacity_sufficient_for_declared_limits"]
    assert [item["target_id"] for item in result.budget["blockers"]] == ["ssd"]
    assert state(inspection[0]) == (2400, 1, 2)


def test_limits_are_copied_before_resource_observation(resource_inspection):
    inspection, holder = resource_inspection
    values = limits()
    def alter(value):
        values["growth_bytes_per_second"]["ssd"] = 1000000
        return value
    holder["change"] = alter
    with Session(inspection[0][0]) as session, session.begin():
        result = invoke(session, resource_inspection, **values)
    row = next(item for item in result.budget["filesystems"] if item["target_id"] == "ssd")
    assert row["ingestion_and_other_growth_bytes"] == 10 * result.budget["growth_window_seconds"]


def test_elapsed_overall_deadline_cannot_return_positive_resource_evidence(resource_inspection, monkeypatch):
    inspection, holder = resource_inspection
    clock = {"now": 0.0}
    monkeypatch.setattr(module, "monotonic", lambda: clock["now"])
    def alter(value):
        clock["now"] = 2.0
        return value
    holder["change"] = alter
    with pytest.raises(RuntimeError, match="resource_time_budget_exceeded"):
        with Session(inspection[0][0]) as session, session.begin():
            invoke(session, resource_inspection, timeout_seconds=1)
    assert state(inspection[0]) == (2400, 1, 2)


def test_tighter_caller_timeout_bounds_resource_queries(resource_inspection):
    inspection, holder = resource_inspection
    def delay(value):
        holder["connection"].exec_driver_sql("SELECT pg_sleep(0.2)")
        return value
    holder["change"] = delay
    with pytest.raises((DBAPIError, RuntimeError)) as raised:
        with Session(inspection[0][0]) as session, session.begin():
            session.execute(text("SET LOCAL statement_timeout='50ms'"))
            invoke(session, resource_inspection)
    if isinstance(raised.value, DBAPIError):
        original = raised.value.orig
        assert (getattr(original, "sqlstate", None) or getattr(original, "pgcode", None)) == "57014"
    else:
        assert str(raised.value) == "storage_move_resource_time_budget_exceeded"
    assert state(inspection[0]) == (2400, 1, 2)


def test_invalid_review_refuses_before_resource_observation(resource_inspection):
    inspection, holder = resource_inspection
    changed = (inspection[0], inspection[1], "0"*64, inspection[3])
    with pytest.raises(StorageConflict, match="review_conflict"):
        with Session(inspection[0][0]) as session, session.begin():
            invoke(session, (changed, holder))
    assert holder["calls"] == 0


def test_resource_inspection_requires_an_owned_transaction(resource_inspection):
    with Session(resource_inspection[0][0][0]) as session:
        with pytest.raises(StorageConflict, match="caller_transaction_required"):
            invoke(session, resource_inspection)
