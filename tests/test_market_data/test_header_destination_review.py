"""Pure destination-bound reviews; synthetic observations, no disk/database calls."""
from dataclasses import replace
from datetime import UTC, date, datetime

import pytest

from core.storage_header_placement import HeaderPartitionPlacement, HeaderPlacementSnapshot, RelationPlacement
from core.storage_mounts import FilesystemEvidence
from core.storage_targets import StoragePolicy, StorageTarget
from portal.backend.service.storage.header_destinations import review_header_moves, stable_header_destination
from portal.backend.service.storage.header_filesystem import VerifiedHeaderPlacement, VerifiedTablespaceDestination


@pytest.fixture
def review():
    targets = (StorageTarget("ssd", "SSD", "uuid-ssd", "/qt/ssd", "ssd"),
               StorageTarget("hdd", "HDD", "uuid-hdd", "/qt/hdd", "hdd"))
    captured = datetime(2026, 9, 17, tzinfo=UTC)
    group = HeaderPartitionPlacement(date(2026, 8, 1),
        RelationPlacement(100, 101, "market", "old", "ssd", 100),
        (RelationPlacement(200, 201, "market", "old_index", "ssd", 50),), True, True)
    snapshot = HeaderPlacementSnapshot("12345/42", captured.date(), captured, (group,), True)
    capacity = {t.target_id: FilesystemEvidence(t.root, f"8:{i}", t.filesystem_uuid,
                1000, 0, 1000, False) for i, t in enumerate(targets)}
    destination = VerifiedTablespaceDestination("12345/42", "hdd", "uuid-hdd", "8:1",
        9000, "qt_history", "/qt/hdd/tablespace", "/qt/hdd/tablespace/PG_15_202209061",
        "/qt/hdd/tablespace/PG_15_202209061", 10001, 202209061, "/qt/hdd")
    return dict(verified=VerifiedHeaderPlacement(snapshot, capacity, (), captured, (destination,)),
        policy=StoragePolicy(recent=("ssd",), history=("hdd",), archives=("hdd",), backups=("hdd",)),
        targets=targets, reserved_bytes={"ssd": 0, "hdd": 0})


def test_review_hash_binds_copy_plan_and_exact_destination_without_io(review, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("review must not probe a disk")
    monkeypatch.setattr(StorageTarget, "inspect", forbidden)
    original = repr(review)
    result = review_header_moves(**review)
    assert result["schema_version"] == "qt.header_move_review.v1"
    assert result["planning_complete"]
    assert result["additional_copy_reservations"] == {"hdd": 150}
    assert result["destination_evidence"]["hdd"]["tablespace_oid"] == 9000
    assert result["destination_evidence"]["hdd"]["directory_inode"] == 10001
    assert result["plan_hash"] != result["placement_plan_hash"]
    assert not result["execution_available"] and not result["activation_ready"]
    assert repr(review) == original
    assert review_header_moves(**review) == result


def test_missing_destination_blocks_whole_review_without_partial_reservations(review):
    review["verified"] = replace(review["verified"], destinations=())
    result = review_header_moves(**review)
    assert not result["planning_complete"]
    assert result["moves"] == [] and result["additional_copy_reservations"] == {}
    assert result["blockers"] == [{"code": "header_destination_unverified", "target_id": "hdd"}]


@pytest.mark.parametrize("change", [
    {"tablespace_oid": 9001}, {"tablespace_name": "renamed"}, {"directory_inode": 123456},
])
def test_new_destination_evidence_changes_review_hash(review, change):
    before = review_header_moves(**review)
    observed = review["verified"]
    review["verified"] = replace(observed, destinations=(replace(observed.destinations[0], **change),))
    after = review_header_moves(**review)
    assert before["plan_hash"] != after["plan_hash"]
    assert before["placement_plan_hash"] == after["placement_plan_hash"]


@pytest.mark.parametrize("change", [
    {"target_root": "/qt/other"}, {"filesystem_uuid": "wrong"}, {"device_id": "8:99"}, {"database_identity": "other/42"},
    {"target_id": "unknown"}, {"tablespace_oid": 1664}, {"tablespace_oid": True},
    {"directory_inode": 0}, {"catalog_version": 0}, {"tablespace_name": ""},
    {"directory": "/outside"}, {"server_directory": "/wrong"},
    {"tablespace_location": "../wrong"},
])
def test_inconsistent_typed_destination_is_refused(review, change):
    observed = review["verified"]
    review["verified"] = replace(observed, destinations=(replace(observed.destinations[0], **change),))
    with pytest.raises(ValueError, match="header_destination_"):
        review_header_moves(**review)


def test_duplicate_destinations_are_not_collapsed_or_counted_twice(review):
    observed = review["verified"]
    review["verified"] = replace(observed, destinations=observed.destinations*2)
    with pytest.raises(ValueError, match="identity_invalid"):
        review_header_moves(**review)


def test_stable_registration_excludes_only_volatile_device_and_inode(review):
    observed = review["verified"]
    proof = review_header_moves(**review)["destination_evidence"]["hdd"]
    stable = stable_header_destination(proof, review["targets"][1])
    assert "directory_inode" not in stable and "device_id" not in stable
    assert stable["target_root"] == "/qt/hdd"
    assert stable["tablespace_oid"] == 9000
    assert stable["filesystem_uuid"] == "uuid-hdd"
    changed = {**proof, "device_id": "8:9", "directory_inode": 22222}
    assert stable_header_destination(changed, review["targets"][1]) == stable


def test_no_moves_does_not_require_an_unused_destination(review):
    review["verified"] = replace(review["verified"],
        snapshot=replace(review["verified"].snapshot, partitions=()), destinations=())
    result = review_header_moves(**review)
    assert result["planning_complete"] and result["moves"] == []
    assert result["destination_evidence"] == {}


def test_partial_group_observation_never_authorizes_global_reservations(review):
    observed = review["verified"]
    review["verified"] = replace(observed, snapshot=replace(observed.snapshot, inventory_complete=False))
    result = review_header_moves(**review)
    assert not result["planning_complete"]
    assert result["moves"] == []
    assert result["additional_copy_reservations"] == {}
    assert {item["code"] for item in result["blockers"]} == {"header_inventory_incomplete"}
