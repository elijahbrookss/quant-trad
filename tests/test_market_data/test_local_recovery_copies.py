"""Local recovery filesystem failure boundaries; database restore is separate."""
import hashlib
import io
import json
import os
from pathlib import Path

import pytest

from core.storage_targets import StorageTarget
from market_data.archive import FilesystemRawArchiveObjectStore
from portal.backend.service.storage import recovery_copies as recovery


@pytest.fixture
def manager(tmp_path, monkeypatch):
    target_root=tmp_path/"hdd"
    target_root.mkdir()
    udev=tmp_path/"udev"
    udev.mkdir()
    device=target_root.stat().st_dev
    (udev/f"b{os.major(device)}:{os.minor(device)}").write_text("E:ID_FS_UUID=uuid-recovery-test\n")
    monkeypatch.setenv("QT_STORAGE_UDEV_ROOT",str(udev))
    target=StorageTarget("hdd","History","uuid-recovery-test",str(target_root),"hdd")
    return recovery.LocalRecoveryCopies(target=target,database_identity="123/456",
                max_bytes=1024**2,reserve_bytes=0,timeout_seconds=60)


def _generation(manager,name="copy_"+"a"*32):
    path=manager.root/name
    path.mkdir(mode=0o700)
    recovery._json_write(path/"owner.json",{
        "schema_version":recovery._VERSION,"database_identity":manager.identity,
        "filesystem_uuid":manager.target.filesystem_uuid,"name":name.removeprefix("."),
    })
    return path


def test_budget_stops_before_overwriting_and_lock_excludes_another_writer(manager):
    manager.max_bytes=3
    output=io.BytesIO()
    digest=hashlib.sha256()
    manager._write(output,b"abc",digest)
    with pytest.raises(RuntimeError,match="byte_budget"):
        manager._write(output,b"d",digest)
    assert output.getvalue()==b"abc"
    with manager.lock():
        with pytest.raises(RuntimeError,match="already_running"),manager.lock():
            pytest.fail("second lock must not be admitted")
    with manager.lock():
        pass


def test_wrong_uuid_and_elapsed_budget_refuse_writes(manager,tmp_path,monkeypatch):
    monkeypatch.setenv("QT_STORAGE_UDEV_ROOT",str(tmp_path/"missing"))
    with pytest.raises(RuntimeError,match="storage_mount"):
        manager.check()
    manager.deadline=0
    with pytest.raises(RuntimeError,match="time_budget"):
        manager.check()


def test_retirement_recovers_after_interruption_and_preserves_other_copies(manager,monkeypatch):
    old=_generation(manager)
    retained=_generation(manager,"copy_"+"b"*32)
    (old/"database.dump").write_bytes(b"old")
    (old/"objects").mkdir(mode=0o700)
    (old/"objects"/"data").write_bytes(b"archive")
    original=Path.unlink
    failed=False
    def interrupt(path,*args,**kwargs):
        nonlocal failed
        if path.name=="database.dump" and not failed:
            failed=True
            raise RuntimeError("interrupted retirement")
        return original(path,*args,**kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(Path,"unlink",interrupt)
        with pytest.raises(RuntimeError,match="interrupted retirement"):
            manager._remove(old)
    tombstone=manager.root/("."+old.name)
    assert not old.exists() and (tombstone/"owner.json").is_file()
    manager._remove(tombstone)
    assert not tombstone.exists() and retained.is_dir()


def test_unowned_or_linked_generation_never_deletes_outside_files(manager,tmp_path):
    outside=tmp_path/"outside"
    outside.write_bytes(b"preserve")
    unsafe=_generation(manager)
    (unsafe/"link").symlink_to(outside)
    with pytest.raises(RuntimeError,match="unsafe_member"):
        manager._remove(unsafe)
    assert outside.read_bytes()==b"preserve" and unsafe.is_dir()
    unowned=manager.root/(".copy_"+"c"*32)
    unowned.mkdir(mode=0o700)
    (unowned/"user-file").write_bytes(b"keep")
    with pytest.raises(FileNotFoundError):
        manager._remove(unowned)
    assert (unowned/"user-file").read_bytes()==b"keep"


def test_object_copy_verifies_bytes_and_stays_in_owned_generation(manager,tmp_path):
    objects=FilesystemRawArchiveObjectStore(tmp_path/"source")
    source=objects.root/"sample"
    source.write_bytes(b"durable archive")
    generation=_generation(manager,".copy_"+"d"*32)
    row={"object_key":"deep/inside/sample","object_sha256":hashlib.sha256(source.read_bytes()).hexdigest(),
         "byte_count":source.stat().st_size}
    (objects.root/"deep"/"inside").mkdir(parents=True)
    source.rename(objects.root/row["object_key"])
    result=manager._object(objects,row,generation)
    assert result["sha256"]==row["object_sha256"]
    assert (generation/"objects"/row["object_key"]).read_bytes()==b"durable archive"
    written = manager.written
    with pytest.raises(RuntimeError, match="duplicate_archive_key"):
        manager._object(objects,row,generation)
    assert manager.written == written
    assert (generation/"objects"/row["object_key"]).read_bytes() == b"durable archive"
    row["object_key"]="../outside"
    with pytest.raises(ValueError):
        manager._object(objects,row,generation)


def test_corrupt_source_cannot_complete_a_generation(manager,tmp_path):
    objects=FilesystemRawArchiveObjectStore(tmp_path/"source")
    (objects.root/"sample").write_bytes(b"wrong")
    generation=_generation(manager,".copy_"+"e"*32)
    with pytest.raises(RuntimeError,match="changed_or_corrupt"):
        manager._object(objects,{"object_key":"sample","object_sha256":"a"*64,"byte_count":5},generation)
    assert not (generation/"complete.json").exists()


def test_cancellation_and_resource_loss_stop_before_more_bytes(manager):
    output=io.BytesIO()
    digest=hashlib.sha256()
    manager._write(output,b"saved",digest)
    manager.cancelled=lambda:True
    with pytest.raises(RuntimeError,match="recovery_cancelled"):
        manager._write(output,b"not written",digest)
    assert output.getvalue()==b"saved"
    manager.cancelled=None
    def lost():
        raise RuntimeError("storage ownership lost")
    manager.check_resources=lost
    with pytest.raises(RuntimeError,match="ownership lost"):
        manager._write(output,b"not written",digest)
    assert output.getvalue()==b"saved"


@pytest.mark.parametrize("layout",[None,{},
    {"layout_version":"unknown","certificate_sha256":"a"*64},
    {"layout_version":"market.fact_storage_tiers.v2","certificate_sha256":"bad"}])
def test_invalid_layout_receipt_cannot_certify_a_recovery_copy(manager,layout):
    generation=_generation(manager)
    recovery._json_write(generation/"complete.json",{
        "schema_version":recovery._VERSION,"database_identity":manager.identity,
        "filesystem_uuid":manager.target.filesystem_uuid,"name":generation.name,
        "completed_at":"2026-09-19T00:00:00+00:00","storage_layout":layout})
    with pytest.raises(RuntimeError,match="completion_layout_invalid"):
        manager.completed()
    assert generation.is_dir()


def test_large_object_budget_admits_existing_target_without_allocating_inventory(manager):
    options=dict(target=manager.target,database_identity=manager.identity,
                 max_bytes=manager.max_bytes,reserve_bytes=0,timeout_seconds=60)
    larger=recovery.LocalRecoveryCopies(**options,max_objects=10_000_000)
    assert larger.max_objects==10_000_000
    with pytest.raises(ValueError,match="recovery_budget_invalid"):
        recovery.LocalRecoveryCopies(**options,max_objects=10_000_001)
