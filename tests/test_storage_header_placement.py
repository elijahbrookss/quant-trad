"""Placement policy tests use synthetic catalog and capacity evidence only."""
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from unittest.mock import Mock

import pytest

from core.storage_header_placement import (
    HeaderPartitionPlacement, HeaderPlacementSnapshot, RelationPlacement,
    plan_header_placement,
)
from core.storage_mounts import FilesystemEvidence
from core.storage_targets import StoragePolicy, StorageTarget


TODAY = date(2026, 9, 17)
CUTOFF = TODAY - timedelta(days=30)


def target(name):
    return StorageTarget(name, name, "uuid-" + name.replace("_", "-"), "/storage/" + name,
                         "ssd" if name == "ssd" else "hdd")


def evidence(item, available=800):
    return FilesystemEvidence(item.root, "device-" + item.target_id,
                              item.filesystem_uuid, 1000, 1000 - available,
                              available, False)


def partition(day=CUTOFF - timedelta(days=1), oid=10, location="ssd",
              heap_bytes=100, index_bytes=50):
    heap = RelationPlacement(oid, oid + 1000, "market", f"headers_{oid}", location, heap_bytes)
    index = RelationPlacement(oid + 1, oid + 1001, "market", f"index_{oid}", location, index_bytes)
    return HeaderPartitionPlacement(day, heap, (index,), True, True)


def snapshot(*partitions):
    return HeaderPlacementSnapshot("disposable-cluster/database-oid", TODAY,
                                   datetime(2026, 9, 17, 12, tzinfo=UTC),
                                   tuple(partitions), True)


def inputs(*partitions):
    targets = [target("ssd"), target("hdd_one"), target("hdd_two")]
    return dict(snapshot=snapshot(*partitions),
                policy=StoragePolicy(recent=("ssd",), history=("hdd_one", "hdd_two"),
                                     archives=("hdd_one",), backups=("hdd_two",)),
                targets=targets,
                capacity={item.target_id: evidence(item) for item in targets})


def test_adding_drive_keeps_existing_history_and_allocates_new_history_without_io(monkeypatch):
    old = partition(location="hdd_one")
    incoming = partition(day=CUTOFF - timedelta(days=2), oid=20)
    args = inputs(old, incoming)
    args["capacity"]["hdd_one"] = evidence(target("hdd_one"), available=300)
    original = repr(args)
    inspect = Mock(side_effect=AssertionError("planner must not inspect disks"))
    monkeypatch.setattr(StorageTarget, "inspect", inspect)
    plan = plan_header_placement(**args)
    assert plan["planning_complete"]
    assert plan["retained"] == [dict(storage_day=old.storage_day.isoformat(),
                                     target_id="hdd_one", role="history")]
    assert len(plan["moves"]) == 1
    assert plan["moves"][0]["destination_target_id"] == "hdd_two"
    assert plan["additional_copy_reservations"] == {"hdd_two": 150}
    assert repr(args) == original
    inspect.assert_not_called()


def test_cutoff_is_strict_and_recent_groups_stay_on_ssd():
    plan = plan_header_placement(**inputs(partition(day=CUTOFF),
                                          partition(day=CUTOFF - timedelta(days=1), oid=20)))
    assert plan["retained"][0]["role"] == "recent"
    assert plan["moves"][0]["storage_day"] == (CUTOFF - timedelta(days=1)).isoformat()


def test_moves_complete_group_and_only_reserves_members_that_change_target():
    group = partition(location="hdd_one")
    group = replace(group, indexes=(replace(group.indexes[0], target_id="ssd"),))
    args = inputs(group)
    args["capacity"]["hdd_one"] = evidence(target("hdd_one"), available=260)
    plan = plan_header_placement(**args)
    move = plan["moves"][0]
    assert move["destination_target_id"] == "hdd_one"  # existing heap before more empty HDD
    assert move["heap"]["oid"] == group.heap.oid
    assert [item["oid"] for item in move["indexes"]] == [group.indexes[0].oid]
    assert move["copy_bytes"] == move["reserve_copy_bytes"] == 50
    assert move["requires_atomic_table_and_index_move"]
    assert move["requires_catalog_mount_capacity_and_policy_recheck"]
    assert move["source_space_credited_bytes"] == 0


def test_aggregate_reservations_exhaust_one_disk_then_allocate_to_next():
    groups = [partition(day=CUTOFF - timedelta(days=n), oid=n * 10,
                        heap_bytes=200, index_bytes=100) for n in (1, 2, 3)]
    args = inputs(*groups)
    args["reserved_bytes"] = {"hdd_one": 100, "hdd_two": 100}
    result = plan_header_placement(**args)
    assert not result["planning_complete"]  # each HDD has only 500 usable; third won't fit
    assert result["moves"] == []
    assert result["additional_copy_reservations"] == {}
    assert result["blockers"][-1]["code"] == "header_destination_capacity_unavailable"
    args["reserved_bytes"] = {}
    result = plan_header_placement(**args)
    assert result["planning_complete"]
    assert result["additional_copy_reservations"] == {"hdd_one": 600, "hdd_two": 300}


def test_source_space_is_not_credited_to_a_later_destination():
    # A's old header moves to B, but A's apparent free bytes must not rise
    # until that move commits. A subsequent larger SSD group cannot fit.
    first = partition(day=CUTOFF - timedelta(days=2), location="hdd_one",
                      heap_bytes=100, index_bytes=100)
    first = replace(first, indexes=(replace(first.indexes[0], target_id="ssd"),))
    second = partition(oid=20, heap_bytes=200, index_bytes=100)
    args = inputs(first, second)
    args["capacity"]["hdd_one"] = evidence(target("hdd_one"), available=250)
    args["capacity"]["hdd_two"] = evidence(target("hdd_two"), available=600)
    result = plan_header_placement(**args)
    assert not result["planning_complete"]
    assert result["moves"] == []
    assert result["additional_copy_reservations"] == {}


def test_no_fallback_to_unassigned_ssd():
    args = inputs(partition(heap_bytes=700, index_bytes=50))
    args["capacity"]["ssd"] = evidence(target("ssd"), available=1000)
    result = plan_header_placement(**args)
    assert result["blockers"][0]["code"] == "header_destination_capacity_unavailable"


@pytest.mark.parametrize("change,code", [
    ({"index_inventory_complete": False}, "header_group_inventory_unproven"),
    ({"toast_colocated": False}, "header_group_inventory_unproven"),
    ({"storage_day": TODAY}, "recent_header_placement_requires_separate_cutover"),
])
def test_incomplete_group_or_recent_cutover_blocks_entire_preview(change, code):
    args = inputs(replace(partition(location="hdd_one"), **change))
    result = plan_header_placement(**args)
    assert result["blockers"][0]["code"] == code
    assert not result["moves"]


@pytest.mark.parametrize("failure", ["missing", "uuid", "readonly", "unregistered"])
def test_source_identity_and_writability_must_be_proven(failure):
    group = partition()
    args = inputs(group)
    if failure == "missing":
        del args["capacity"]["ssd"]
    elif failure == "uuid":
        args["capacity"]["ssd"] = replace(args["capacity"]["ssd"], filesystem_uuid="wrong")
    elif failure == "readonly":
        args["capacity"]["ssd"] = replace(args["capacity"]["ssd"], read_only=True)
    else:
        args["snapshot"] = snapshot(replace(group, heap=replace(group.heap, target_id=None)))
    result = plan_header_placement(**args)
    assert result["blockers"][0]["code"] == "header_source_unverified"
    assert result["moves"] == []


def test_incomplete_catalog_is_not_treated_as_empty_success():
    args = inputs()
    args["snapshot"] = replace(args["snapshot"], inventory_complete=False)
    result = plan_header_placement(**args)
    assert result["blockers"] == [{"code": "header_inventory_incomplete"}]


@pytest.mark.parametrize("duplicate", ["day", "oid", "device"])
def test_duplicate_inventory_cannot_double_count(duplicate):
    first, second = partition(), partition(oid=20, day=CUTOFF - timedelta(days=2))
    args = inputs(first, second)
    if duplicate == "day":
        args["snapshot"] = snapshot(first, replace(second, storage_day=first.storage_day))
    elif duplicate == "oid":
        args["snapshot"] = snapshot(first, replace(second, heap=first.heap))
    else:
        args["capacity"]["hdd_two"] = replace(args["capacity"]["hdd_two"],
                                               device_id=args["capacity"]["hdd_one"].device_id)
    with pytest.raises(ValueError, match="duplicate"):
        plan_header_placement(**args)


def test_planner_budget_refuses_before_target_or_capacity_processing():
    args = inputs(partition(), partition(day=CUTOFF, oid=20))
    args["targets"] = []  # would fail policy checks if processed
    with pytest.raises(ValueError, match="inventory_budget_exceeded"):
        plan_header_placement(**args, max_partitions=1)
    with pytest.raises(ValueError, match="snapshot inventory"):
        snapshot(*([partition()] * 4097))


def test_total_registered_target_inventory_is_bounded():
    args = inputs()
    args["targets"] += [target(f"extra_{n}") for n in range(30)]
    with pytest.raises(ValueError, match="target inventory budget"):
        plan_header_placement(**args)


def test_hash_stable_under_inventory_order_and_sensitive_to_physical_changes():
    group = partition()
    group = replace(group, indexes=(*group.indexes, replace(group.indexes[0], oid=12, name="other")))
    args = inputs(group, partition(day=CUTOFF, oid=20))
    baseline = plan_header_placement(**args)
    reordered = dict(args, targets=list(reversed(args["targets"])),
                     capacity=dict(reversed(list(args["capacity"].items()))),
                     snapshot=snapshot(partition(day=CUTOFF, oid=20),
                                       replace(group, indexes=tuple(reversed(group.indexes)))))
    assert plan_header_placement(**reordered) == baseline
    args["snapshot"] = snapshot(replace(group, heap=replace(group.heap, relfilenode=999)),
                                partition(day=CUTOFF, oid=20))
    assert plan_header_placement(**args)["plan_hash"] != baseline["plan_hash"]
    args["policy"] = replace(args["policy"], recent_days=31)
    assert plan_header_placement(**args)["plan_hash"] != baseline["plan_hash"]


def test_enabled_policy_cannot_turn_preview_into_activation():
    args = inputs(partition())
    args["policy"] = replace(args["policy"], movement_enabled=True, backup_enabled=True)
    result = plan_header_placement(**args)
    assert result["planning_complete"]
    assert not result["activation_ready"]
    assert not result["execution_available"]
    assert "growing_identity_and_raw_mapping_placement" in result["uncovered"]
    assert "wal_temporary_and_growth_headroom" in result["uncovered"]


@pytest.mark.parametrize("value", [True, -1, 2**63])
def test_byte_counts_are_exact_nonnegative_bounded_integers(value):
    with pytest.raises(ValueError, match="relation bytes"):
        replace(partition().heap, byte_count=value)


@pytest.mark.parametrize("change", [{"total_bytes": 0}, {"available_bytes": True},
                                     {"used_bytes": 900}, {"read_only": 0}])
def test_invalid_capacity_is_rejected(change):
    args = inputs(partition())
    args["capacity"]["hdd_one"] = replace(args["capacity"]["hdd_one"], **change)
    with pytest.raises(ValueError, match="header_placement_invalid"):
        plan_header_placement(**args)


def test_snapshot_clock_must_match_utc_database_day():
    with pytest.raises(ValueError, match="database clock"):
        replace(snapshot(), captured_at=datetime(2026, 9, 16, 23, tzinfo=UTC))
