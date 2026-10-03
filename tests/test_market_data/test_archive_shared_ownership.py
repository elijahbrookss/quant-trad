"""Explicit shared archive publication; legacy spool permissions are untouched."""
import hashlib
import os
import stat
from types import SimpleNamespace

import pytest

from core.settings import StorageSettings, _archive_shared_group
from market_data import archive


@pytest.fixture
def shared(tmp_path, monkeypatch):
    group = os.getegid()
    if group == 0:
        pytest.skip("non-root group required for ordinary ownership contract")
    monkeypatch.setattr(archive, "get_settings", lambda: SimpleNamespace(
        storage=StorageSettings(archive_shared_group_id=group)))
    root = tmp_path / "objects"
    root.mkdir()
    root.chmod(0o2770)
    source = tmp_path / "source"
    source.write_bytes(b"retained market evidence")
    source.chmod(0o600)
    return root, source, hashlib.sha256(source.read_bytes()).hexdigest()


def publish(root, source, digest):
    return archive.FilesystemRawArchiveObjectStore(root).put_verified(
        object_key="raw/session/object.parquet", source_path=source, expected_sha256=digest)


def test_shared_publication_preserves_source_and_reuses_exact_object(shared):
    root, source, digest = shared
    before = source.stat()
    ack = publish(*shared)
    target = root / ack.object_key
    assert target.read_bytes() == source.read_bytes()
    assert stat.S_IMODE(target.stat().st_mode) == 0o640
    assert target.stat().st_gid == os.getegid()
    for directory in (root, root / "raw", root / "raw/session"):
        assert stat.S_IMODE(directory.stat().st_mode) == 0o2770
    assert publish(*shared).reused_existing
    after = source.stat()
    assert (before.st_uid, before.st_gid, before.st_mode, before.st_ino) == (
        after.st_uid, after.st_gid, after.st_mode, after.st_ino)


@pytest.mark.parametrize("mode", [0o700, 0o770, 0o2777, 0o2750])
def test_shared_root_is_never_repaired(shared, mode):
    root, _, _ = shared
    root.chmod(mode)
    with pytest.raises(PermissionError, match="shared_directory_invalid"):
        publish(*shared)
    assert stat.S_IMODE(root.stat().st_mode) == mode
    assert list(root.iterdir()) == []


def test_existing_private_object_is_not_widened(shared):
    root, _, _ = shared
    ack = publish(*shared)
    target = root / ack.object_key
    target.chmod(0o600)
    before = target.stat()
    with pytest.raises(PermissionError, match="shared_object_invalid"):
        publish(*shared)
    assert target.stat().st_mode == before.st_mode
    assert target.stat().st_ino == before.st_ino


def test_existing_private_directory_and_alias_are_not_repaired(shared):
    root, _, _ = shared
    (root / "raw").mkdir(mode=0o700)
    with pytest.raises(PermissionError, match="shared_directory_invalid"):
        publish(*shared)
    assert stat.S_IMODE((root / "raw").stat().st_mode) == 0o2700
    (root / "raw").rmdir()
    (root / "other").mkdir()
    (root / "raw").symlink_to(root / "other", target_is_directory=True)
    with pytest.raises(PermissionError, match="shared_object_alias"):
        publish(*shared)


def test_private_default_does_not_share(tmp_path, monkeypatch):
    monkeypatch.setattr(archive, "get_settings", lambda: SimpleNamespace(storage=StorageSettings()))
    source = tmp_path / "source"
    source.write_bytes(b"private")
    root = tmp_path / "objects"
    ack = publish(root, source, hashlib.sha256(source.read_bytes()).hexdigest())
    assert stat.S_IMODE((root / ack.object_key).stat().st_mode) == 0o600


@pytest.mark.parametrize("value", [True, False, 0, -1, 1.2, "70.0", "", " 70", 2**32-1])
def test_invalid_shared_group_fails(value):
    with pytest.raises(ValueError, match="shared_group_invalid"):
        _archive_shared_group(value)


def test_shared_group_configuration_is_explicit():
    assert _archive_shared_group(None) is None
    assert _archive_shared_group("70") == 70
    assert StorageSettings().archive_shared_group_id is None


def test_environment_binding_uses_central_settings(monkeypatch):
    from core.settings import get_settings, clear_settings_cache
    monkeypatch.setenv("QT_ARCHIVE_SHARED_GROUP_ID", "70")
    try:
        assert get_settings(force_reload=True).storage.archive_shared_group_id == 70
        monkeypatch.setenv("QT_ARCHIVE_SHARED_GROUP_ID", "false")
        with pytest.raises(ValueError, match="shared_group_invalid"):
            get_settings(force_reload=True)
    finally:
        clear_settings_cache()
