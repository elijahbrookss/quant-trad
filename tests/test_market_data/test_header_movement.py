"""Pure physical transition checks; no database or real filesystem."""
from dataclasses import replace
from datetime import UTC, date, datetime

import pytest

from core.storage_header_placement import RelationPlacement, HeaderPartitionPlacement, HeaderPlacementSnapshot
from portal.backend.service.storage.header_filesystem import VerifiedHeaderPlacement
from portal.backend.service.storage.header_movement import _physical_identity, _verify_transition
from portal.backend.service.storage_management import StorageConflict


@pytest.fixture
def transition():
    clock = datetime(2026, 9, 17, tzinfo=UTC)
    heap = RelationPlacement(10, 11, "market", "old", "ssd", 100)
    index = RelationPlacement(20, 21, "market", "existing_history_index", "hdd", 50)
    group = HeaderPartitionPlacement(date(2026, 8, 1), heap, (index,), True, True)
    def binding(relation, inode, tablespace):
        return dict(relation_oid=relation.oid, target_id=relation.target_id,
            filesystem_uuid="uuid-" + relation.target_id, device_id="1:" + relation.target_id,
            inode=inode, tablespace_oid=tablespace, server_path="/private/" + str(relation.relfilenode))
    before = VerifiedHeaderPlacement(HeaderPlacementSnapshot("123/45", clock.date(), clock, (group,), False),
        {}, (binding(heap, 1001, 1663), binding(index, 2001, 9000)), clock)
    copied = replace(heap, relfilenode=12, target_id="hdd")
    after = replace(before, snapshot=replace(before.snapshot,
        partitions=(replace(group, heap=copied),)),
        bindings=(binding(copied, 1002, 9000), before.bindings[1]))
    return before, after


def test_copy_can_change_physical_file_but_must_preserve_logical_members_and_retained_files(transition):
    before, after = transition
    physical = _verify_transition(before, after, {10}, 9000, "hdd")
    assert physical["heap_oid"] == 10
    assert [item["oid"] for item in physical["members"]] == [10, 20]
    assert all(item["target_id"] == "hdd" for item in physical["members"])
    assert _physical_identity(before)["members"][1] == physical["members"][1]


def test_later_byte_growth_does_not_falsely_invalidate_completed_file_identity(transition):
    _, after = transition
    group = after.snapshot.partitions[0]
    grown = replace(after, snapshot=replace(after.snapshot,
        partitions=(replace(group, heap=replace(group.heap, byte_count=100000)),)))
    assert _physical_identity(grown) == _physical_identity(after)


@pytest.mark.parametrize("mutation,expected", [
    ("destination", "destination_changed"), ("retained_inode", "retained_member_changed"),
    ("missing_index", "members"), ("name", "identity_changed"), ("toast", "group_unproven"),
])
def test_post_copy_drift_is_refused(transition, mutation, expected):
    before, after = transition
    group = after.snapshot.partitions[0]
    bindings = list(after.bindings)
    if mutation == "destination":
        bindings[0] = {**bindings[0], "tablespace_oid": 9001}
    elif mutation == "retained_inode":
        bindings[1] = {**bindings[1], "inode": 3000}
    elif mutation == "missing_index":
        group = replace(group, indexes=())
        bindings = bindings[:1]
    elif mutation == "name":
        group = replace(group, heap=replace(group.heap, name="replacement"))
    else:
        group = replace(group, toast_colocated=False)
    after = replace(after, snapshot=replace(after.snapshot, partitions=(group,)), bindings=tuple(bindings))
    with pytest.raises(StorageConflict, match=expected):
        _verify_transition(before, after, {10}, 9000, "hdd")


def test_file_identity_hash_binds_path_without_storing_unbounded_path_arrays(transition):
    _, after = transition
    changed = replace(after, bindings=({**after.bindings[0], "server_path": "/other/path"}, after.bindings[1]))
    assert _physical_identity(changed) != _physical_identity(after)
    assert "server_path" not in _physical_identity(after)["members"][0]


def test_stalled_watcher_invalidates_backend_before_it_can_return_to_pool():
    import threading
    from time import monotonic
    from types import SimpleNamespace
    from portal.backend.service.storage.header_movement import _MoveWatch
    entered,release=threading.Event(),threading.Event()
    evidence=SimpleNamespace(filesystem_uuid="same",device_id="1:2",path="/owned",available_bytes=100)
    class Target:
        target_id="hdd"
        def inspect(self,**kwargs):
            if threading.current_thread().name=="qt-history-move-watch":
                entered.set()
                assert release.wait(5)
            return evidence
    class Driver:
        def cancel(self):
            raise AssertionError("no cancellation requested")
    class Connection:
        invalidated=False
        def invalidate(self):
            self.invalidated=True
    connection=Connection()
    watch=_MoveWatch(driver=Driver(),targets=(Target(),),capacity={"hdd":evidence},
        floors={"hdd":50},deadline=monotonic()+10,cancelled=None,grace=.01)
    try:
        watch.start()
        assert entered.wait(2)
        with pytest.raises(RuntimeError,match="watcher_did_not_stop"):
            watch.stop(connection)
        assert connection.invalidated
    finally:
        release.set()
        watch._thread.join(2)
        assert not watch._thread.is_alive()
