import hashlib
import json

import pytest

from scripts.automation.verify_canonical_recovery_archive import verify_archive, verify_logical_copy


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


@pytest.fixture
def logical_copy(tmp_path):
    identity = {
        "schema_version": "qt.local_recovery_copy.v1",
        "database_identity": "1234/5678",
        "filesystem_uuid": "source-fixture-hdd",
        "name": "copy_" + "a" * 32,
    }
    archive = tmp_path / "objects" / "archive"
    archive.parent.mkdir()
    archive.write_bytes(b"fixture archive")
    inventory = tmp_path / "objects.jsonl"
    inventory.write_bytes(json.dumps({
        "object_key": "archive",
        "bytes": archive.stat().st_size,
        "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
    }).encode() + b"\n")
    dump = tmp_path / "database.dump"
    dump.write_bytes(b"PGDMP logical fixture header")
    (tmp_path / "owner.json").write_text(json.dumps(identity))
    (tmp_path / "complete.json").write_text(json.dumps(identity | {
        "database": {"bytes": dump.stat().st_size, "sha256": hashlib.sha256(dump.read_bytes()).hexdigest()},
        "archive_objects": 1,
        "archive_bytes": archive.stat().st_size,
        "inventory_sha256": hashlib.sha256(inventory.read_bytes()).hexdigest(),
    }))
    return tmp_path


def test_logical_copy_checks_dump_and_archive_inventory(logical_copy):
    result = verify_logical_copy(logical_copy)
    assert result["schema_version"] == "qt.local_recovery_copy.v1"
    assert result["database_dump_bytes"] == (logical_copy / "database.dump").stat().st_size
    assert result["verified_archive_objects"] == 1


@pytest.mark.parametrize(("broken_file", "reason"), [
    ("objects/archive", "archive object failed size or hash"),
    ("owner.json", "mismatched logical-copy"),
])
def test_logical_copy_refuses_changed_custody_bytes(logical_copy, broken_file, reason):
    path = logical_copy / broken_file
    path.write_bytes(b"{}" if path.suffix == ".json" else b"x" * path.stat().st_size)
    with pytest.raises(ValueError, match=reason):
        verify_logical_copy(logical_copy)


def test_logical_copy_checks_database_hash_and_format(logical_copy):
    dump = logical_copy / "database.dump"
    dump.write_bytes(b"PGDMP" + b"X" * (dump.stat().st_size - 5))
    with pytest.raises(ValueError, match="database file digest or size mismatch"):
        verify_logical_copy(logical_copy)
    dump.write_bytes(b"PGSQL" + dump.read_bytes()[5:])
    with pytest.raises(ValueError, match="not a PostgreSQL custom-format dump"):
        verify_logical_copy(logical_copy)


def test_logical_copy_refuses_physical_pair_receipt(logical_copy):
    receipt = logical_copy / "complete.json"
    data = json.loads(receipt.read_text()) | {"schema_version": "qt.encrypted_recovery_pair.v1"}
    receipt.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="unrecognized or mismatched logical-copy"):
        verify_logical_copy(logical_copy)
