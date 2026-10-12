import json

import pytest

from core.storage_inventory import INVENTORY_VERSION, read_storage_inventory


def test_missing_inventory_is_unconfigured_not_a_fake_disk(tmp_path):
    assert read_storage_inventory(tmp_path / "missing.json") == ()


def test_inventory_rejects_duplicate_filesystem_capacity(tmp_path):
    path = tmp_path / "inventory.json"
    row = dict(target_id="disk_one", label="HDD", filesystem_uuid="uuid-one",
               root="/storage/disk_one", medium="hdd")
    second = {**row, "target_id": "disk_two", "root": "/storage/disk_two"}
    path.write_text(json.dumps({"schema_version": INVENTORY_VERSION, "targets": [row, second]}))
    with pytest.raises(ValueError, match="duplicate filesystem_uuid"):
        read_storage_inventory(path)


def test_prepared_inventory_is_read_without_creating_target(tmp_path):
    path = tmp_path / "inventory.json"
    root = tmp_path / "not-mounted"
    path.write_text(json.dumps({"schema_version": INVENTORY_VERSION, "targets": [
        dict(target_id="hdd", label="HDD", filesystem_uuid="uuid-hdd", root=str(root), medium="hdd"),
    ]}))
    assert read_storage_inventory(path)[0].target_id == "hdd"
    assert not root.exists()


def test_oversized_inventory_fails_explicitly(tmp_path):
    path = tmp_path / "inventory.json"
    path.write_bytes(b" " * (128 * 1024 + 1))
    with pytest.raises(ValueError, match="too large"):
        read_storage_inventory(path)
