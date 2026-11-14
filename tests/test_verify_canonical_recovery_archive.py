import hashlib
import json

import pytest

from scripts.automation.verify_canonical_recovery_archive import verify_archive


def _restored_archive(tmp_path, *, object_key="subdir/segment"):
    root = tmp_path / "restore"
    archive = root / "qt-history" / "archives"
    obj = archive / "subdir" / "segment"
    obj.parent.mkdir(parents=True)
    content = b"QT retained archive object"
    obj.write_bytes(content)
    inventory = root / "qt-history" / "recovery" / ".copy_fixture" / "objects.jsonl"
    inventory.parent.mkdir(parents=True)
    inventory.write_bytes(
        json.dumps({
            "object_key": object_key,
            "bytes": len(content),
            "sha256": hashlib.sha256(content).hexdigest(),
        }).encode() + b"\n"
    )
    receipt = tmp_path / "complete.json"
    receipt.write_text(json.dumps({
        "schema_version": "qt.encrypted_recovery_pair.v1",
        "restore_endpoint": "immediate",
        "database_label": "20261114-164000F",
        "archive_snapshot": "a" * 64,
        "inventory_sha256": hashlib.sha256(inventory.read_bytes()).hexdigest(),
        "archive_objects": 1,
        "archive_bytes": len(content),
        "inventory_snapshot_path": "/qt-history/recovery/.copy_fixture/objects.jsonl",
        "archive_source_root": "/qt-history/archives",
    }))
    return root, receipt, obj, inventory


def test_verifies_exact_restored_inventory_and_contents(tmp_path):
    root, receipt, obj, inventory = _restored_archive(tmp_path)
    assert verify_archive(receipt, root) == {
        "database_label": "20261114-164000F",
        "archive_snapshot": "a" * 64,
        "inventory_sha256": hashlib.sha256(inventory.read_bytes()).hexdigest(),
        "verified_archive_objects": 1,
        "verified_archive_bytes": obj.stat().st_size,
    }


def test_accepts_empty_archive_inventory(tmp_path):
    root, receipt, _, inventory = _restored_archive(tmp_path)
    value = json.loads(receipt.read_bytes())
    value.update(archive_objects=0, archive_bytes=0, archive_source_root="/empty/archive",
                 inventory_sha256=hashlib.sha256(b"").hexdigest())
    receipt.write_text(json.dumps(value))
    inventory.write_bytes(b"")
    assert verify_archive(receipt, root)["verified_archive_objects"] == 0


@pytest.mark.parametrize("damage", ["missing", "corrupt", "inventory", "symlink"])
def test_rejects_incomplete_or_corrupt_restore(tmp_path, damage):
    root, receipt, obj, inventory = _restored_archive(tmp_path)
    if damage == "missing":
        obj.unlink()
    elif damage == "corrupt":
        obj.write_bytes(b"x" * obj.stat().st_size)
    elif damage == "inventory":
        payload = json.loads(inventory.read_bytes())
        inventory.write_bytes(json.dumps(payload, indent=None, separators=(",", ":")).encode() + b"\n")
    else:
        aliased = obj.with_name("aliased")
        obj.rename(aliased)
        try:
            obj.symlink_to(aliased)
        except OSError:
            pytest.skip("this host cannot create test symlinks")
    with pytest.raises((FileNotFoundError, ValueError)):
        verify_archive(receipt, root)


@pytest.mark.parametrize("object_key", ["../segment", "subdir//segment", "/tmp/segment"])
def test_rejects_paths_other_than_exact_relative_archive_keys(tmp_path, object_key):
    root, receipt, _, _ = _restored_archive(tmp_path, object_key=object_key)
    with pytest.raises(ValueError, match="invalid recovery path"):
        verify_archive(receipt, root)
