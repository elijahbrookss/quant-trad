from dataclasses import replace

import pytest

from core.storage_mounts import FilesystemEvidence, StorageMountError
from core.storage_targets import StorageLocation, StoragePolicy, StorageTarget, allocate_target


def target(name="hdd_one", uuid="uuid-one", **kwargs):
    return StorageTarget(name, name, uuid, "/storage/" + name, "hdd", **kwargs)


def policy(**kwargs):
    return StoragePolicy(recent=("ssd",), history=("hdd_one",), archives=("hdd_one",), backups=("hdd_one",), **kwargs)


def capacity(item, *, available=800, read_only=False):
    return FilesystemEvidence(item.root, "8:1", item.filesystem_uuid, 1000, 1000-available, available, read_only)


def test_new_drive_receives_new_work_without_changing_old_location():
    first, second = target(), target("hdd_two", "uuid-two")
    ssd = replace(target("ssd", "uuid-ssd"), medium="ssd")
    old = StorageLocation(first.target_id, "canonical/page.parquet")
    updated = replace(policy(), history=("hdd_one", "hdd_two"))
    chosen = allocate_target(policy=updated, role="history", targets=[ssd, first, second],
                             capacity={first.target_id: capacity(first, available=300),
                                       second.target_id: capacity(second)}, required_bytes=200)
    assert chosen == second
    assert old.target_id == first.target_id


def test_capacity_reserve_includes_inflight_reservations():
    ssd, hdd = target("ssd", "uuid-ssd"), target()
    with pytest.raises(StorageMountError, match="storage_capacity_blocked"):
        allocate_target(policy=policy(), role="history", targets=[ssd, hdd],
                        capacity={hdd.target_id: capacity(hdd, available=500)},
                        reserved_bytes={hdd.target_id: 250}, required_bytes=51)


@pytest.mark.parametrize("evidence_change", [
    {"read_only": True}, {"filesystem_uuid": "wrong-uuid"},
])
def test_missing_or_changed_disk_never_allocates_to_an_unassigned_fallback(evidence_change):
    ssd, hdd = target("ssd", "uuid-ssd"), target()
    evidence = replace(capacity(hdd), **evidence_change)
    with pytest.raises(StorageMountError):
        allocate_target(policy=policy(), role="archives", targets=[ssd, hdd],
                        capacity={"ssd": capacity(ssd), hdd.target_id: evidence}, required_bytes=1)


def test_duplicate_filesystem_cannot_double_count_capacity():
    first, second = target(), target("hdd_two", "uuid-one")
    with pytest.raises(ValueError, match="counted as two"):
        policy().validate_targets([target("ssd", "uuid-ssd"), first, second])


@pytest.mark.parametrize("key", ["../x", "/x", "a//b", "a/./b", "a/../b", "a\\b", "", "a\x00b"])
def test_location_rejects_unsafe_keys(key):
    with pytest.raises(ValueError, match="storage_location_invalid"):
        StorageLocation("hdd_one", key)


def test_location_checks_recorded_target_identity_before_filesystem_access():
    with pytest.raises(StorageMountError, match="target_mismatch"):
        StorageLocation("hdd_one", "data/x").resolve(target("hdd_two", "uuid-two"))


def test_symlink_cannot_escape_registered_disk(tmp_path, monkeypatch):
    root = tmp_path / "disk"
    root.mkdir()
    (root / "escape").symlink_to(tmp_path, target_is_directory=True)
    item = replace(target(), root=str(root))
    monkeypatch.setattr(StorageTarget, "inspect", lambda self, **kw: capacity(self))
    with pytest.raises(StorageMountError, match="outside_target"):
        StorageLocation(item.target_id, "escape/outside").resolve(item)


def test_policy_roundtrip_preserves_fingerprint_and_rejects_unknown_settings():
    original = policy()
    assert StoragePolicy.from_dict(original.to_dict()).fingerprint == original.fingerprint
    bad = {**original.to_dict(), "delete_everything": True}
    with pytest.raises(ValueError, match="unknown fields"):
        StoragePolicy.from_dict(bad)


@pytest.mark.parametrize("change", [{"reserve_percent": True}, {"movement_enabled": "false"},
                                    {"recent_days": 0}, {"backup_copies": 0}])
def test_policy_rejects_coercions_and_unbounded_values(change):
    with pytest.raises(ValueError):
        policy(**change)


def test_target_identity_uses_deployment_host_metadata_mount(monkeypatch):
    from pathlib import Path
    from unittest.mock import Mock
    import core.storage_targets as module
    inspect = Mock()
    monkeypatch.setattr(module, "inspect_filesystem", inspect)
    monkeypatch.setenv("QT_STORAGE_UDEV_ROOT", "/run/qt-host-udev/data")
    target = StorageTarget("hdd", "History", "uuid-hdd", "/qt/hdd", "hdd")
    target.inspect(require_writable=True)
    assert inspect.call_args.kwargs["udev_root"] == Path("/run/qt-host-udev/data")
    assert inspect.call_args.kwargs["expected_uuid"] == "uuid-hdd"
