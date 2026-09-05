from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from core.storage_mounts import (
    StorageMountError,
    inspect_filesystem,
    require_configured_archive_mount,
)
from market_data.archive import DurableRawSpoolSegment, FilesystemRawArchiveObjectStore


@pytest.fixture
def mounted_archive(tmp_path, monkeypatch):
    root = tmp_path / "archive"
    root.mkdir()
    udev = tmp_path / "udev"
    udev.mkdir()
    device = root.stat().st_dev
    record = udev / f"b{os.major(device)}:{os.minor(device)}"
    record.write_text("E:ID_FS_UUID=test-archive-uuid\n", encoding="utf-8")
    monkeypatch.setenv("QT_MARKET_DATA_EXPECTED_UUID", "test-archive-uuid")
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(root))
    monkeypatch.setenv("QT_STORAGE_UDEV_ROOT", str(udev))
    return root, udev, record


def test_expected_uuid_uses_actual_path_device_and_does_not_write(mounted_archive):
    root, udev, record = mounted_archive
    before = record.read_bytes()
    evidence = inspect_filesystem(root, expected_uuid="test-archive-uuid", udev_root=udev)
    assert evidence.filesystem_uuid == "test-archive-uuid"
    assert evidence.total_bytes > 0
    assert evidence.available_bytes >= 0
    assert evidence.read_only is False
    assert list(root.iterdir()) == []
    assert record.read_bytes() == before


@pytest.mark.parametrize("properties", ["E:ID_FS_UUID=nvme-uuid\n", "", "E:ID_FS_UUID=test-archive-uuid\nE:ID_FS_UUID=test-archive-uuid\n"])
def test_wrong_or_ambiguous_device_identity_fails_closed(mounted_archive, properties):
    root, _, record = mounted_archive
    record.write_text(properties)
    # A copied marker cannot substitute for the filesystem actually mounted.
    (root / ".qt-storage-identity").write_text("test-archive-uuid")
    with pytest.raises(StorageMountError, match="identity_mismatch"):
        require_configured_archive_mount(root / "objects")
    assert not (root / "objects").exists()


def test_missing_device_metadata_is_not_treated_as_unconfigured(mounted_archive):
    root, _, record = mounted_archive
    record.unlink()
    with pytest.raises(StorageMountError, match="storage_mount_unavailable"):
        FilesystemRawArchiveObjectStore(root / "objects")
    assert not (root / "objects").exists()


def test_missing_mount_root_is_never_created(mounted_archive):
    root, _, _ = mounted_archive
    root.rmdir()
    with pytest.raises(StorageMountError, match="storage_mount_unavailable"):
        FilesystemRawArchiveObjectStore(root / "objects")
    assert not root.exists()


def test_read_only_remount_blocks_writes_but_allows_explicit_read_probe(
    mounted_archive, monkeypatch
):
    root, _, _ = mounted_archive
    original = os.statvfs(root)
    read_only = SimpleNamespace(
        **{name: getattr(original, name) for name in dir(original) if name.startswith("f_")}
    )
    read_only.f_flag |= os.ST_RDONLY
    monkeypatch.setattr(os, "statvfs", lambda _path: read_only)
    with pytest.raises(StorageMountError, match="storage_mount_read_only"):
        require_configured_archive_mount()
    assert require_configured_archive_mount(require_writable=False).read_only
    assert list(root.iterdir()) == []


def test_permission_denied_is_not_a_healthy_mount(mounted_archive, monkeypatch):
    monkeypatch.setattr(os, "access", lambda *_args: False)
    with pytest.raises(StorageMountError, match="storage_mount_read_only"):
        require_configured_archive_mount()


def test_subtree_symlink_cannot_escape_to_nvme(mounted_archive, tmp_path):
    root, _, _ = mounted_archive
    outside = tmp_path / "nvme"
    outside.mkdir()
    (root / "objects").symlink_to(outside, target_is_directory=True)
    with pytest.raises(StorageMountError, match="storage_path_outside_archive"):
        FilesystemRawArchiveObjectStore(root / "objects")
    assert list(outside.iterdir()) == []


def test_new_nested_paths_are_admitted_without_being_created(mounted_archive):
    root, _, _ = mounted_archive
    require_configured_archive_mount(root / "objects" / "new" / "object.parquet")
    assert list(root.iterdir()) == []


def test_existing_store_rechecks_identity_before_every_mutation(mounted_archive, tmp_path):
    root, _, record = mounted_archive
    store = FilesystemRawArchiveObjectStore(root / "objects")
    source = tmp_path / "object"
    source.write_bytes(b"immutable-test-evidence")
    checksum = hashlib.sha256(source.read_bytes()).hexdigest()
    store.put_verified(object_key="date/object.parquet", source_path=source, expected_sha256=checksum)
    record.write_text("E:ID_FS_UUID=wrong-uuid\n")
    with pytest.raises(StorageMountError, match="identity_mismatch"):
        store.delete_verified(object_key="date/object.parquet", expected_sha256=checksum)
    with pytest.raises(StorageMountError, match="identity_mismatch"):
        store.put_verified(object_key="date/new.parquet", source_path=source, expected_sha256=checksum)
    assert (root / "objects/date/object.parquet").read_bytes() == source.read_bytes()
    assert not (root / "objects/date/new.parquet").exists()
    # Recovery/restart succeeds only once the expected filesystem is restored.
    record.write_text("E:ID_FS_UUID=test-archive-uuid\n")
    recovered = FilesystemRawArchiveObjectStore(root / "objects")
    assert recovered.local_path("date/object.parquet").read_bytes() == source.read_bytes()


def test_spool_rotation_refuses_wrong_filesystem_before_creating_wal(mounted_archive):
    root, _, record = mounted_archive
    record.write_text("E:ID_FS_UUID=wrong-uuid\n")
    with pytest.raises(StorageMountError, match="identity_mismatch"):
        DurableRawSpoolSegment(
            root=root / "spool", definition_id="definition", session_id="session",
            connection_epoch=1,
        )
    assert not (root / "spool").exists()


def test_directory_mode_does_not_require_udev_or_an_existing_root(tmp_path, monkeypatch):
    monkeypatch.delenv("QT_MARKET_DATA_EXPECTED_UUID", raising=False)
    assert require_configured_archive_mount(tmp_path / "not-created") is None
    assert not (tmp_path / "not-created").exists()


@pytest.mark.parametrize("value", ["/", "relative/archive"])
def test_dedicated_mode_rejects_broad_or_relative_root(mounted_archive, monkeypatch, value):
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", value)
    with pytest.raises(StorageMountError, match="storage_mount_invalid"):
        require_configured_archive_mount()


def test_deployment_probe_reports_actionable_failure_without_creating_path(tmp_path):
    repository = Path(__file__).resolve().parents[2]
    absent = tmp_path / "not-mounted"
    result = subprocess.run(
        ["python3", str(repository / "src/core/storage_mounts.py"),
         "--path", str(absent), "--expected-uuid", "test-archive-uuid"],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode != 0
    assert "storage_mount_unavailable" in result.stderr
    assert str(absent) in result.stderr
    assert not absent.exists()
