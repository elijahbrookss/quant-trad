"""Transactional inspection admission; physical adapters are synthetic here.

The private namespace fixture separately proves real adapter composition.
"""
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from core.storage_mounts import StorageMountError
from portal.backend.db.storage_target_models import StorageHeaderMoveRecord, StoragePolicyRecord, StorageTargetRecord
from portal.backend.service.storage import header_inspection as module
from portal.backend.service.storage_management import StorageConflict
from tests.test_portal.test_header_journal_db import journal, reserve, state

pytestmark = pytest.mark.db


@pytest.fixture
def inspection(journal, monkeypatch):
    receipt = reserve(journal)
    observed = replace(journal[3], snapshot=replace(journal[3].snapshot,
        partitions=(journal[3].snapshot.partitions[0],), inventory_complete=False))
    holder = {"verified": observed}
    def catalog(conn, **kwargs):
        assert conn.in_transaction()
        assert kwargs["heap_oid"] == 100
        return object()
    monkeypatch.setattr(module, "read_locked_header_group", catalog)
    monkeypatch.setattr(module, "verify_header_filesystem", lambda *args, **kwargs: holder["verified"])
    selected = next(item for item in receipt["moves"]
                    if item["storage_day"] == observed.snapshot.partitions[0].storage_day.isoformat())
    return journal, selected["id"], receipt["review_hash"], holder


def inspect(inspection):
    journal, move_id, review_hash, _ = inspection
    with Session(journal[0]) as session, session.begin():
        return module.inspect_reserved_header_move(session, move_id=move_id,
            review_hash=review_hash, pg_controldata=Path("/disposable/pg_controldata"))


def test_current_reserved_move_inspection_preserves_state_and_capacity(inspection):
    journal, _, _, _ = inspection
    before = state(journal)
    result = inspect(inspection)
    assert result.copy_bytes == result.reserved_copy_bytes == 1200
    assert [item.oid for item in result.moving_relations] == [100, 101]
    assert not result.execution_available
    assert "wal_temporary_and_growth_headroom" in result.uncovered
    assert state(journal) == before == (2400, 1, 2)
    assert inspect(inspection).copy_bytes == 1200
    assert state(journal) == before


@pytest.mark.parametrize("move_state", ["running", "blocked", "completed", "cancelled"])
def test_started_or_terminal_moves_require_reconciliation(inspection, move_state):
    journal, move_id, _, _ = inspection
    with Session(journal[0]) as session, session.begin():
        row = session.get(StorageHeaderMoveRecord, move_id)
        row.state = move_state
        if move_state == "completed":
            # Synthetic completion used only to test reserved-only admission.
            row.completion_evidence = {"schema_version": "qt.header_move_completion.v1", "physical": {}}
    with pytest.raises(StorageConflict, match="reconciliation_required"):
        inspect(inspection)


def test_changed_policy_revision_blocks_inspection(inspection):
    with Session(inspection[0][0]) as session, session.begin():
        session.get(StoragePolicyRecord, 1).revision += 1
    with pytest.raises(StorageConflict, match="policy_changed"):
        inspect(inspection)


def test_insufficient_aggregate_for_the_known_batch_is_not_accepted(inspection):
    with Session(inspection[0][0]) as session, session.begin():
        session.get(StorageTargetRecord, "hdd").reserved_bytes = 1500
    with pytest.raises(StorageConflict, match="reservation_inconsistent"):
        inspect(inspection)


@pytest.mark.parametrize("change,expected", [
    ("destination_inode", "destination_changed"),
    ("source_node", "source_identity_changed"),
    ("growth", "copy_reservation_exceeded"),
    ("expired", "evidence_expired"),
])
def test_current_physical_evidence_must_still_match_review(inspection, change, expected):
    holder = inspection[3]
    verified = holder["verified"]
    if change == "destination_inode":
        verified = replace(verified, destinations=(replace(verified.destinations[0], directory_inode=50000),))
    elif change == "expired":
        verified = replace(verified, verified_at=verified.verified_at - timedelta(seconds=61))
    else:
        group = verified.snapshot.partitions[0]
        delta = {"relfilenode": 999} if change == "source_node" else {"byte_count": 1300}
        group = replace(group, heap=replace(group.heap, **delta))
        verified = replace(verified, snapshot=replace(verified.snapshot, partitions=(group,)))
    holder["verified"] = verified
    with pytest.raises(StorageConflict, match=expected):
        inspect(inspection)


def test_disk_capacity_includes_other_claims_without_double_counting_this_copy(inspection):
    holder = inspection[3]
    verified = holder["verified"]
    capacity = dict(verified.capacity)
    capacity["hdd"] = replace(capacity["hdd"], used_bytes=78900, available_bytes=21100)
    holder["verified"] = replace(verified, capacity=capacity)
    with pytest.raises(StorageMountError, match="storage_capacity_blocked"):
        inspect(inspection)


def test_wrong_review_cannot_inspect_another_saved_intent(inspection):
    journal, move_id, _, holder = inspection
    with pytest.raises(StorageConflict, match="review_conflict"):
        inspect((journal, move_id, "0" * 64, holder))
