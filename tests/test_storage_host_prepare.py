import pytest

from scripts.automation.storage_host_prepare import admit_device, fstab_update, validate_plan

PLAN = {"device": "/dev/disk/by-id/ata-example", "expected_serial": "disk-01",
        "expected_size_bytes": 16000900661248, "filesystem_uuid": "df88b80e-1b89-49dc-87a9-ac5b48801b79",
        "mountpoint": "/srv/quanttrad/storage/hdd-01", "owner": "qtadmin"}


def evidence(**changes):
    device = {"type": "disk", "serial": "disk-01", "size": PLAN["expected_size_bytes"],
              "fstype": None, "uuid": None, "mountpoints": []}
    device.update(changes)
    return {"topology": {"blockdevices": [device]}, "signatures": {"signatures": []}}


def test_blank_disk_needs_explicit_initialization():
    assert admit_device(PLAN, evidence()) is True


def test_resume_accepts_only_the_exact_filesystem_uuid():
    assert admit_device(PLAN, evidence(fstype="ext4", uuid=PLAN["filesystem_uuid"])) is False
    with pytest.raises(ValueError, match="existing"):
        admit_device(PLAN, evidence(fstype="ext4", uuid="another-filesystem"))


@pytest.mark.parametrize("change", [
    {"serial": "wrong"}, {"size": 1000}, {"children": [{"name": "/dev/sda1"}]},
    {"mountpoints": ["/"]}, {"fstype": "xfs"},
])
def test_admission_refuses_wrong_or_in_use_disk(change):
    with pytest.raises(ValueError):
        admit_device(PLAN, evidence(**change))


def test_signature_without_reported_filesystem_still_blocks_format():
    value = evidence()
    value["signatures"]["signatures"] = [{"type": "gpt"}]
    with pytest.raises(ValueError, match="existing"):
        admit_device(PLAN, value)


def test_fstab_is_idempotent_and_does_not_change_existing_entries():
    original = "# original\nUUID=system / ext4 defaults 0 1\n"
    updated = fstab_update(original, PLAN)
    assert updated.startswith(original)
    assert fstab_update(updated, PLAN) == updated
    with pytest.raises(ValueError, match="conflicting"):
        fstab_update(original + f"UUID=other {PLAN['mountpoint']} ext4 defaults 0 2\n", PLAN)


def test_mount_path_cannot_be_root_or_traverse_outside_storage():
    for path in ("/", "/srv/quanttrad/storage/../app", "/srv/quanttrad/storage"):
        with pytest.raises(ValueError):
            validate_plan({**PLAN, "mountpoint": path})


@pytest.mark.parametrize("device", [
    evidence(),
    evidence(fstype="ext4",uuid=PLAN["filesystem_uuid"]),
    evidence(fstype="ext4",uuid="foreign",mountpoints=[PLAN["mountpoint"]]),
])
def test_runtime_directories_require_existing_verified_mount(monkeypatch,device):
    from types import SimpleNamespace
    from scripts.automation import storage_host_prepare as preparation
    monkeypatch.setattr(preparation.os,"geteuid",lambda:0)
    monkeypatch.setattr(preparation.pwd,"getpwnam",lambda value:SimpleNamespace(pw_gid=1000))
    monkeypatch.setattr(preparation,"audit",lambda *args:device)
    def mutation_forbidden(*args,**kwargs):
        pytest.fail("directory or mount command reached for unprepared disk")
    monkeypatch.setattr(preparation,"run",mutation_forbidden)
    monkeypatch.setattr(preparation,"_runtime_directory",mutation_forbidden)
    with pytest.raises(ValueError,match="existing_verified_mount|existing data"):
        preparation.prepare_runtime_directories(PLAN)
