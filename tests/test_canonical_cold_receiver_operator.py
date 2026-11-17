import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
import pytest

from scripts.automation import canonical_cold_receiver_operator as operator
from scripts.automation.canonical_cold_named_recovery import verify_signed_recovery
from scripts.automation.canonical_cold_receiver_ready import PREPARED_READY_FILENAME
from scripts.automation.storage_host_boundary import load_receipt


def _write(path, value):
    path.touch(mode=0o600)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_offer_publishes_exact_detached_catalog_without_readiness(tmp_path):
    receiving = tmp_path / "exchange"
    receiving.mkdir(mode=0o700)
    inventory = tmp_path / "catalog.jsonl"
    inventory.write_bytes(b"first\nsecond\n")
    raw = inventory.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    checkpoint = {"verification_sha256": "a" * 64, "catalog_inventory_sha256": digest}
    staged = SimpleNamespace(inventory=inventory, checkpoint=checkpoint)
    site = SimpleNamespace(receiving=receiving)
    signer = Ed25519PrivateKey.generate()
    approved = operator._publish_offer(staged, site, {"measurement_sha256": "1" * 64}, signer,
                                       plan_sha256="2" * 64, reserve_bytes=0, check=lambda: None)
    receipt = verify_signed_recovery(load_receipt(approved), signer.public_key().public_bytes_raw().hex())
    assert receipt["checkpoint"] == checkpoint
    assert receipt["catalog"] == {"filename": f"replayed-catalog-{digest}.jsonl", "bytes": len(raw), "sha256": digest}
    catalog = receiving / receipt["catalog"]["filename"]
    assert catalog.read_bytes() == raw
    assert catalog.stat().st_nlink == 1
    assert not (receiving / PREPARED_READY_FILENAME).exists()

    inventory.write_bytes(b"altered\n")
    with pytest.raises(ValueError, match="catalog_digest_changed"):
        operator._publish_offer(staged, site, {"measurement_sha256": "1" * 64}, signer,
                                plan_sha256="2" * 64, reserve_bytes=0, check=lambda: None)
    assert catalog.read_bytes() == raw


def test_private_replay_selection_requires_exact_pins(tmp_path):
    path = tmp_path / "replay-selection.json"
    baseline, preflight = "a" * 64, "b" * 64
    selection = {
        "schema": operator.REPLAY_SELECTION_VERSION,
        "baseline_verification_sha256": baseline,
        "receiver_preflight_sha256": preflight,
        "timeline": 1,
        "target_lsn": "0/4000000",
        "replay_lsn": "0/4000100",
        "prefixes": [{"after": "000000010000000000000003", "through": "000000010000000000000004",
                      "prefix_sha256": "c" * 64, "previous_prefix_sha256": None}],
    }
    _write(path, selection)
    site = SimpleNamespace(baseline_verification={"verification_sha256": baseline})
    assert operator._replay_selection(path, site, {"measurement_sha256": preflight}, lambda: None) == selection
    with pytest.raises(ValueError, match="replay_selection_scope_changed"):
        operator._replay_selection(path, site, {"measurement_sha256": "d" * 64}, lambda: None)
    _write(path, selection | {"prefixes": [selection["prefixes"][0] | {"arbitrary_hook": "command"}]})
    with pytest.raises(ValueError, match="invalid_wal_prefix"):
        operator._replay_selection(path, site, {"measurement_sha256": preflight}, lambda: None)


def test_approval_wrong_checkpoint_is_refused_before_reading_package(tmp_path):
    checkpoint = "c" * 64
    marker = {"schema": operator.APPROVAL_VERSION, "package_filename": "cutover.json", "package_sha256": "a" * 64,
              "inventory_filename": "host.json", "checkpoint_sha256": "d" * 64}
    _write(tmp_path / "approval.json", marker)
    with pytest.raises(ValueError, match="approval_checkpoint_changed"):
        operator._approval(SimpleNamespace(checkpoint={"verification_sha256": checkpoint}), None, None, tmp_path,
                           wait_seconds=1, check=lambda: None)


def test_headroom_groups_remaining_final_work_on_shared_device(monkeypatch):
    roles = ("exchange", "workspace", "pgdata")
    data_bytes, archive_bytes, segment_bytes = 100, 300, 2**20
    measurement = {
        "receiver_mounts": {name: {"path": f"/{name}", "device_id": "0:1", "block_size": 1} for name in roles},
        "requirements": {
            "exchange": {"checkpoint_bytes": 40, "entries": 5, "reserve_bytes": 20},
            "workspace": {"checkpoint_bytes": archive_bytes + 50, "entries": 8, "reserve_bytes": 10},
            "pgdata": {"checkpoint_bytes": data_bytes + segment_bytes, "entries": 10, "reserve_bytes": 15},
        },
        "database_targets": {"pgdata": {"logical_bytes": data_bytes, "entries": 9}},
        "archive_restore_bytes": archive_bytes,
        "archive_restore_files": 3,
    }
    site = SimpleNamespace(max_restart_segments=2, baseline_verification={"wal_segment_bytes": segment_bytes})
    required_bytes = 40 + 50 + 2 * segment_bytes + 5 + 5 + 2
    capacity = SimpleNamespace(f_bavail=required_bytes + 20, f_frsize=1, f_favail=12)
    observations = []

    def observed(path):
        observations.append(path)
        return capacity

    monkeypatch.setattr(operator.os, "statvfs", observed)
    report = operator._final_headroom(site, measurement)
    assert report["0:1"]["required_bytes"] == required_bytes
    assert observations == [Path("/exchange")]
    capacity.f_bavail -= 1
    with pytest.raises(RuntimeError, match="final_capacity_insufficient"):
        operator._final_headroom(site, measurement)
