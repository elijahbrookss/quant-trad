import hashlib
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pytest

from market_data.fact_archive import archive_evidence_hash
from scripts.automation import verify_archive_custody_subset as custody
from scripts.db.canonical_cold_recovery_suffix import _encode


@pytest.fixture
def block_tree(tmp_path):
    sysfs = tmp_path / "sys"
    index = sysfs / "dev" / "block"
    index.mkdir(parents=True)

    def add(relative, device, *, partition=False, slaves=(), physical=True):
        path = sysfs / "devices" / relative
        path.mkdir(parents=True)
        (path / "dev").write_text(device + "\n", encoding="ascii")
        if partition:
            (path / "partition").touch()
        else:
            (path / "slaves").mkdir()
            for item in slaves:
                (path / "slaves" / item.name).symlink_to(item, target_is_directory=True)
            if physical:
                (path / "device").mkdir()
        (index / device).symlink_to(path, target_is_directory=True)
        return path

    return sysfs, add


def test_physical_aliases_fail_even_when_filesystem_device_numbers_differ(block_tree):
    sysfs, add = block_tree
    add("pci0/ata0/block/sda", "8:0")
    partition = add("pci0/ata0/block/sda/sda1", "8:1", partition=True)
    add("virtual/block/dm-0", "253:0", slaves=[partition], physical=False)
    add("pci0/ata1/block/sdb", "8:16")
    add("virtual/block/loop0", "7:0", physical=False)
    for alias in ("8:1", "253:0"):
        with pytest.raises(ValueError, match="shared_physical_device"):
            custody._device_evidence(sysfs, ["8:0"], alias)
    source, receiver = custody._device_evidence(sysfs, ["253:0"], "8:16")
    assert source["253:0"][0]["device_id"] == "8:0"
    assert receiver[0]["device_id"] == "8:16"
    with pytest.raises(ValueError, match="physical_device_unresolved"):
        custody._device_evidence(sysfs, ["8:0"], "7:0")


def test_nvme_namespaces_on_one_controller_share_the_physical_unit(block_tree):
    sysfs, add = block_tree
    add("pci0/nvme/nvme0/nvme0n1", "259:0")
    add("pci0/nvme/nvme0/nvme0n2", "259:1")
    with pytest.raises(ValueError, match="shared_physical_device"):
        custody._device_evidence(sysfs, ["259:0"], "259:1")


@pytest.fixture
def inventory_case(tmp_path, block_tree, monkeypatch, request):
    sysfs, add = block_tree
    destination = tmp_path / "archive"
    (destination / "raw").mkdir(parents=True)
    info = destination.stat()
    device = f"{os.major(info.st_dev)}:{os.minor(info.st_dev)}"
    source_workspace = TemporaryDirectory(prefix="qt-archive-custody-source-", dir="/dev/shm")
    request.addfinalizer(source_workspace.cleanup)
    source_root = Path(source_workspace.name)
    original_info = source_root.stat()
    assert original_info.st_dev != info.st_dev
    original = f"{os.major(original_info.st_dev)}:{os.minor(original_info.st_dev)}"
    add("pci0/ata0/block/sda", original)
    add("pci0/ata1/block/sdb", device)

    def inspect(path, *, expected_uuid, **kwargs):
        uuid, device_id, readonly = {source_root: ("source-uuid", original, False),
                                    destination: ("fixture-uuid", device, True)}[path]
        if expected_uuid != uuid:
            raise RuntimeError("storage_mount_identity_mismatch")
        return SimpleNamespace(read_only=readonly, device_id=device_id)

    monkeypatch.setattr(custody, "inspect_filesystem", inspect)
    monkeypatch.setattr(custody.os, "statvfs", lambda _: SimpleNamespace(f_flag=os.ST_RDONLY))
    expected = {"raw/verified": b"match", "raw/bad-hash": b"left", "raw/bad-size": b"full", "raw/missing": b"only"}
    contents = {"raw/verified": b"match", "raw/bad-hash": b"lost", "raw/bad-size": b"x"}
    for key, body in contents.items():
        (destination / key).write_bytes(body)
    rows = b"".join(_encode(key, (hashlib.sha256(data).hexdigest(), len(data))) for key, data in expected.items())
    inventory = tmp_path / "catalog.jsonl"
    inventory.write_bytes(rows)
    checkpoint = {"schema": "qt.replayed_recovery_checkpoint.v1", "pair_receipt_sha256": "a" * 64,
                  "database_identity": "100/20", "storage_layout": {"layout_version": "fixture", "certificate_sha256": "b" * 64},
                  "catalog_inventory_sha256": hashlib.sha256(rows).hexdigest(), "catalog_objects": len(expected),
                  "catalog_bytes": sum(len(row) for row in expected.values())}
    checkpoint["verification_sha256"] = archive_evidence_hash(checkpoint)
    return {"inventory": inventory, "checkpoint": checkpoint, "checkpoint_sha256": checkpoint["verification_sha256"],
            "destination": destination, "destination_inode": info.st_ino, "destination_uuid": "fixture-uuid",
            "source_root": source_root, "source_inode": original_info.st_ino, "source_uuid": "source-uuid",
            "max_objects": 4, "max_verify_bytes": 9, "timeout_seconds": 10, "sysfs": sysfs}


def test_custody_counts_only_verified_exact_archive_objects_and_reports_gaps(inventory_case):
    rows = []
    proof = custody.verify_archive_custody_subset(**inventory_case, record_gap=rows.append)
    assert proof["scope"] == "archive_object_bytes" and proof["required_objects"] == 4
    assert proof["source"]["device_id"] in proof["source_block_ancestry"]
    assert [(key, status["objects"], status["bytes"]) for key, status in proof["coverage"].items()] == [
        ("verified", 1, 5), ("missing", 1, 4), ("mismatched", 2, 8),
    ]
    assert proof["verified_file_bytes_read"] == 9
    assert [(row["object_key"], row["status"], row["reason"]) for row in rows] == [
        ("raw/bad-hash", "mismatched", "sha256"), ("raw/bad-size", "mismatched", "size"),
        ("raw/missing", "missing", "absent"),
    ]
    assert proof["gap_report_sha256"] == hashlib.sha256(b"".join(custody._json_line(row) for row in rows)).hexdigest()


def test_missing_object_alias_is_a_hard_refusal(inventory_case):
    destination = inventory_case["destination"]
    (destination / "raw" / "missing").symlink_to(destination / "does-not-exist")
    with pytest.raises(ValueError, match="unsafe_object_path: raw/missing"):
        custody.verify_archive_custody_subset(**inventory_case)


def test_exact_manifest_and_byte_budgets_fail_closed(inventory_case):
    with pytest.raises(ValueError, match="verification_byte_budget_exceeded"):
        custody.verify_archive_custody_subset(**(inventory_case | {"max_verify_bytes": 8}))
    inventory_case["inventory"].write_bytes(inventory_case["inventory"].read_bytes().replace(b"raw/verified", b"raw/notfound"))
    with pytest.raises(ValueError, match="inventory_mismatch"):
        custody.verify_archive_custody_subset(**inventory_case)


def test_source_pin_cannot_be_replaced_by_an_unrelated_device_claim(inventory_case, tmp_path, monkeypatch):
    with pytest.raises(RuntimeError, match="mount_identity_mismatch"):
        custody.verify_archive_custody_subset(**(inventory_case | {"source_uuid": "wrong-uuid"}))
    with pytest.raises(ValueError, match="mount_pin_changed"):
        custody.verify_archive_custody_subset(**(inventory_case | {"source_inode": inventory_case["source_inode"] + 1}))
    alias = tmp_path / "different-path-same-device"
    alias.mkdir()
    original_inspect = custody.inspect_filesystem
    monkeypatch.setattr(custody, "inspect_filesystem", lambda path, **kw: original_inspect(
        inventory_case["destination"] if path == alias else path, **kw,
    ))
    with pytest.raises(ValueError, match="shared_physical_device"):
        custody.verify_archive_custody_subset(**(inventory_case | {
            "source_root": alias, "source_inode": alias.stat().st_ino, "source_uuid": inventory_case["destination_uuid"],
            "source_devices": ["8:0"],
        }))


def test_same_hdd_paused_receipt_requires_full_view_and_original_source_pin(inventory_case):
    options = {key: value for key, value in inventory_case.items() if not key.startswith("checkpoint")}
    checkpoint = inventory_case["checkpoint"]
    info = options["source_root"].stat()
    source_device = f"{os.major(info.st_dev)}:{os.minor(info.st_dev)}"
    receipt = {key: checkpoint[key] for key in ("database_identity", "storage_layout", "pair_receipt_sha256",
                                                "catalog_inventory_sha256", "catalog_objects", "catalog_bytes")}
    receipt.update(schema="qt.paused_archive_suffix.v1", materialize_baseline=True, timeline=1,
                   target_lsn="0/3000090", replay_lsn="0/30000F0", accepted_commit_seq=2,
                   source_archive="/qt-plaintext", source_inode=info.st_ino, device_id=source_device,
                   archive_root="/qt-suffix", catalog_inventory_path="/qt-suffix/catalog.jsonl", suffix_root="/qt-suffix/objects",
                   materialized_objects=4, materialized_bytes=17, materialized_inventory_sha256=receipt["catalog_inventory_sha256"],
                   baseline_objects_used=1, baseline_bytes_used=5, suffix_objects=3, suffix_bytes=12)

    def scope(body):
        pin = archive_evidence_hash(body)
        return {"paused_archive_receipt": body | {"verification_sha256": pin}, "paused_archive_receipt_sha256": pin}

    selected = scope(receipt)
    proof = custody.verify_archive_custody_subset(**options, **selected)
    assert proof["inventory_proof_schema"] == receipt["schema"]
    assert proof["paused_archive_receipt_sha256"] == selected["paused_archive_receipt_sha256"]
    assert proof["coverage"]["verified"]["objects"] == 1
    with pytest.raises(ValueError, match="paused_receipt_invalid"):
        custody.verify_archive_custody_subset(**options, **(selected | {
            "paused_archive_receipt": selected["paused_archive_receipt"] | {"accepted_commit_seq": 3},
        }))
    with pytest.raises(ValueError, match="paused_materialized_view_incomplete"):
        custody.verify_archive_custody_subset(**options, **scope(receipt | {"materialized_objects": 3}))
    with pytest.raises(ValueError, match="paused_source_scope_changed"):
        custody.verify_archive_custody_subset(**options, **scope(receipt | {"source_inode": info.st_ino + 1}))


def test_cli_publishes_no_partial_gap_report_on_failure(inventory_case, tmp_path, monkeypatch, capsys):
    checkpoint = tmp_path / "checkpoint.json"
    checkpoint.touch(mode=0o600)
    checkpoint.write_text(json.dumps(inventory_case["checkpoint"]), encoding="utf-8")
    output = tmp_path / "gaps.jsonl"
    verify = custody.verify_archive_custody_subset
    monkeypatch.setattr(custody, "verify_archive_custody_subset",
                        lambda **kw: verify(**kw, sysfs=inventory_case["sysfs"]))
    arguments = ["--inventory", str(inventory_case["inventory"]), "--checkpoint", str(checkpoint),
                 "--checkpoint-sha256", inventory_case["checkpoint_sha256"],
                 "--destination", str(inventory_case["destination"]), "--destination-inode", str(inventory_case["destination_inode"]),
                 "--destination-uuid", inventory_case["destination_uuid"], "--source-root", str(inventory_case["source_root"]),
                 "--source-inode", str(inventory_case["source_inode"]), "--source-uuid", inventory_case["source_uuid"],
                 "--max-objects", "4", "--timeout-seconds", "10", "--gap-report", str(output)]
    with pytest.raises(SystemExit, match="1"):
        custody.main(arguments + ["--max-verify-bytes", "8"])
    assert not output.exists() and not list(tmp_path.glob(".archive-gaps-*"))
    capsys.readouterr()
    custody.main(arguments + ["--max-verify-bytes", "9"])
    report = json.loads(capsys.readouterr().out)
    assert report["gap_report_sha256"] == hashlib.sha256(output.read_bytes()).hexdigest()
