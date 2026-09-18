"""Pure stored-intent comparisons; no filesystem, PostgreSQL or credentials."""
from dataclasses import asdict, replace
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from core.storage_header_placement import RelationPlacement, HeaderPartitionPlacement, HeaderPlacementSnapshot
from portal.backend.service.storage.header_filesystem import VerifiedHeaderPlacement
from portal.backend.service.storage.header_inspection import _compare_reserved_group
from portal.backend.service.storage_management import StorageConflict


@pytest.fixture
def intent():
    heap = RelationPlacement(10, 11, "market", "history", "ssd", 100)
    index = RelationPlacement(20, 21, "market", "history_index", "hdd", 50)
    day = date(2026, 8, 1)
    group = HeaderPartitionPlacement(day, heap, (index,), True, True)
    clock = datetime(2026, 9, 17, tzinfo=UTC)
    snapshot = HeaderPlacementSnapshot("123/45", clock.date(), clock, (group,), False)
    verified = VerifiedHeaderPlacement(snapshot, {}, (), clock)
    source = dict(storage_day=day.isoformat(), destination_target_id="hdd",
        destination_filesystem_uuid="uuid-hdd", heap=asdict(heap), indexes=[asdict(index)],
        copy_bytes=100, reserve_copy_bytes=100, source_space_credited_bytes=0,
        requires_atomic_table_and_index_move=True,
        requires_catalog_mount_capacity_and_policy_recheck=True)
    move = SimpleNamespace(storage_day=day, heap_oid=10, target_id="hdd",
                           filesystem_uuid="uuid-hdd", source_group=source, reserved_bytes=100)
    return move, verified


def test_members_already_on_selected_target_are_not_copied_again(intent):
    move, verified = intent
    moving, byte_count = _compare_reserved_group(move, verified)
    assert [item.oid for item in moving] == [10]
    assert byte_count == 100
    assert verified.snapshot.partitions[0].indexes[0].oid == 20


def test_smaller_current_copy_preserves_identity_and_never_inflates_reservation(intent):
    move, verified = intent
    group = verified.snapshot.partitions[0]
    reduced = replace(group, heap=replace(group.heap, byte_count=40))
    observed = replace(verified, snapshot=replace(verified.snapshot, partitions=(reduced,)))
    moving, byte_count = _compare_reserved_group(move, observed)
    assert byte_count == 40 and move.reserved_bytes == 100
    assert moving[0].relfilenode == group.heap.relfilenode


@pytest.mark.parametrize("field,value", [
    ("relfilenode", 12), ("name", "renamed"), ("target_id", "other"), ("schema", "elsewhere"),
])
def test_changed_source_identity_requires_new_review(intent, field, value):
    move, verified = intent
    group = verified.snapshot.partitions[0]
    changed = replace(group, heap=replace(group.heap, **{field: value}))
    with pytest.raises(StorageConflict, match="source_identity_changed"):
        _compare_reserved_group(move, replace(verified,
            snapshot=replace(verified.snapshot, partitions=(changed,))))


@pytest.mark.parametrize("change", ["added", "missing", "growth", "global", "toast"])
def test_incomplete_changed_or_overgrown_groups_are_refused(intent, change):
    move, verified = intent
    group = verified.snapshot.partitions[0]
    if change == "added":
        group = replace(group, indexes=(*group.indexes, RelationPlacement(30, 31, "market", "extra", "ssd", 5)))
    elif change == "missing":
        group = replace(group, indexes=())
    elif change == "growth":
        group = replace(group, heap=replace(group.heap, byte_count=101))
    elif change == "toast":
        group = replace(group, toast_colocated=False)
    with pytest.raises(StorageConflict):
        _compare_reserved_group(move, replace(verified,
            snapshot=replace(verified.snapshot, partitions=(group,), inventory_complete=change == "global")))


@pytest.mark.parametrize("field,value", [
    ("copy_bytes", 1), ("reserve_copy_bytes", 101), ("source_space_credited_bytes", 100),
    ("requires_atomic_table_and_index_move", False), ("storage_day", "2026-08-02"),
    ("destination_filesystem_uuid", "uuid-other"), ("heap", {}),
    ("indexes", None), ("requires_catalog_mount_capacity_and_policy_recheck", False),
])
def test_malformed_or_internally_inconsistent_durable_intent_is_refused(intent, field, value):
    move, verified = intent
    move.source_group[field] = value
    with pytest.raises(StorageConflict, match="intent_invalid"):
        _compare_reserved_group(move, verified)
