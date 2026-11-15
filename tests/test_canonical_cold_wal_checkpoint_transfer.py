import hashlib
import os

import pytest

from market_data.fact_archive import archive_evidence_hash
from scripts.automation.stage_encrypted_recovery_handoff import TransferLimits, _encode
from scripts.db import canonical_cold_recovery_wal_transfer as transfer


@pytest.mark.parametrize("staging_name", (".qt-wal-a1b2c3d4", "unrelated-owner-link"))
def test_wal_copy_recovers_only_its_interrupted_private_staging_link(tmp_path, staging_name):
    source = tmp_path / "source"
    source.write_bytes(b"encrypted fixture bytes")
    receiving = tmp_path / "receiving"
    bucket = receiving / "archive" / "segment-bucket"
    bucket.mkdir(parents=True, mode=0o700)
    destination = bucket / "archive-wal"
    destination.write_bytes(source.read_bytes())
    destination.chmod(0o600)
    alias = bucket / staging_name
    os.link(destination, alias)
    options = (source, transfer._stamp(source.stat()), receiving, destination.relative_to(receiving),
               receiving.stat().st_dev, lambda: None)
    if staging_name.startswith(".qt-wal-"):
        report = transfer._copy_one(*options)
        assert report["sha256"] == hashlib.sha256(source.read_bytes()).hexdigest()
        assert destination.stat().st_nlink == 1 and not alias.exists()
    else:
        with pytest.raises(RuntimeError, match="existing_ciphertext_changed"):
            transfer._copy_one(*options)
        assert alias.is_file() and destination.stat().st_nlink == 2


def _segment(number, timeline=1):
    return f"{timeline:08X}00000000{number:08X}"


@pytest.fixture
def final_checkpoint(tmp_path):
    receiving = tmp_path / "receiving"
    receiving.mkdir(mode=0o700)

    def publish(path, body):
        encoded = _encode(body)
        path.write_bytes(encoded)
        return hashlib.sha256(encoded).hexdigest()

    pair = {"database_identity": "100/20", "database_label": "20261115-000000F",
            "archive_snapshot": "9" * 64, "storage_layout": {"layout_version": "original", "certificate_sha256": "8" * 64}}
    pair_hash = publish(receiving / "complete.json", pair)
    base = publish(receiving / "encrypted-copy.json", {"schema": "qt.encrypted_recovery_transfer.v1", "pair_receipt_sha256": pair_hash})
    baseline = {"schema": "qt.encrypted_recovery_repository_verification.v1", **pair, "pair_receipt_sha256": pair_hash,
                "anchor_sha256": None, "database_archive_id": "15-1", "wal_start": _segment(1),
                "wal_backup_stop": _segment(2), "wal_verified_through": _segment(2), "wal_segment_bytes": 1 << 24}
    baseline["verification_sha256"] = archive_evidence_hash(baseline)
    checkpoint = {
        "schema": "qt.replayed_recovery_checkpoint.v1", "database_identity": pair["database_identity"],
        "storage_layout": pair["storage_layout"], "pair_receipt_sha256": pair_hash, "archive_verification_sha256": "7" * 64,
        "catalog_inventory_sha256": "6" * 64, "catalog_objects": 1, "catalog_bytes": 8, "accepted_commit_seq": 12,
        "timeline": 1, "target_lsn": "0/84000028", "replay_lsn": "0/84000088", "checkpoint_lsn": "0/83000080",
        "redo_lsn": "0/83000028", "redo_wal_segment": _segment(0x83), "minimum_recovery_lsn": "0/84000064",
        "wal_first": _segment(1), "wal_verified_through": _segment(0x84), "wal_segment_bytes": 1 << 24,
        "wal_verification_sha256": "5" * 64, "stopped_control_sha256": "4" * 64, "pg_controldata_sha256": "3" * 64,
    }
    checkpoint["verification_sha256"] = archive_evidence_hash(checkpoint)
    anchor = {"database": {"system_identifier": "100", "database_oid": 20, "allow_connections": False},
              "storage_layout": pair["storage_layout"], "source_commit_seq": 12, "restore_name": "qt_original_" + "2" * 32,
              "anchor_lsn": "0/86000000", "wal_segment": _segment(0x85), "timeline": 1}
    anchor_path = tmp_path / "anchor.json"
    anchor_sha256 = publish(anchor_path, anchor)
    options = {"receiving": receiving, "baseline": baseline, "baseline_verification_sha256": baseline["verification_sha256"],
               "base_transfer_sha256": base, "anchor_path": anchor_path, "anchor_sha256": anchor_sha256,
               "limits": TransferLimits(seconds=5, bytes=100, files=1),
               "checkpoint": checkpoint, "checkpoint_sha256": checkpoint["verification_sha256"]}
    return options, anchor, publish


def test_final_wal_verifies_only_the_exact_tail_and_binds_original_base(final_checkpoint):
    options, anchor, publish = final_checkpoint
    options["max_restart_segments"] = 3
    base, checkpoint = options["baseline"], options["checkpoint"]
    terminal = anchor["wal_segment"]
    origin, expected = transfer._final_scope(
        base, base_transfer_sha256=options["base_transfer_sha256"], anchor_sha256=options["anchor_sha256"],
        anchor=anchor, last=terminal, checkpoint=checkpoint, checkpoint_sha256=options["checkpoint_sha256"],
        max_restart_segments=options["max_restart_segments"],
    )
    assert origin == _segment(0x84)
    assert expected["wal_backup_stop"] == _segment(2)
    assert expected["wal_verification_sha256"] == checkpoint["wal_verification_sha256"]
    assert expected["wal_transferred_through"] == terminal
    assert expected["schema"] == "qt.encrypted_recovery_wal_transfer.v2"
    assert expected["max_restart_segments"] == expected["wal_restart_segments"] == 3
    checksum = "a" * 40
    name = f"database/archive/qt/15-1/{terminal[:16]}/{terminal}-{checksum}.gz"
    cipher = options["receiving"] / name
    cipher.parent.mkdir(parents=True)
    data = b"test ciphertext"
    cipher.write_bytes(data)
    report = expected | {"encrypted_bytes": len(data), "files": [{"segment": terminal, "wal_checksum": checksum,
                            "path": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}]}
    digest = publish(options["receiving"] / "encrypted-wal-transfer.json", report)
    assert transfer.verify_final_wal(**options, expected_hash=digest) == report | {"wal_transfer_sha256": digest}
    legacy = options | {"checkpoint": None, "checkpoint_sha256": None, "max_restart_segments": None}
    with pytest.raises(ValueError, match="wal_receipt_binding_changed"):
        transfer.verify_final_wal(**legacy, expected_hash=digest)
    with pytest.raises(ValueError, match="wal_receipt_binding_changed"):
        transfer.verify_final_wal(**(options | {"max_restart_segments": 4}), expected_hash=digest)
    with pytest.raises(ValueError, match="wal_restart_range_outside_budget"):
        transfer.verify_final_wal(**(options | {"max_restart_segments": 2}), expected_hash=digest)
    cipher.write_bytes(b"test CIPHERTEXT")
    with pytest.raises(RuntimeError, match="receiving_ciphertext_changed"):
        transfer.verify_final_wal(**options, expected_hash=digest)


@pytest.mark.parametrize(("fields", "error"), (
    ({"wal_segment": _segment(0x84), "anchor_lsn": "0/840000A0"}, "wal_anchor_not_after_checkpoint"),
    ({"wal_segment": _segment(0x85, 2), "timeline": 2}, "wal_anchor_not_after_checkpoint"),
    ({"anchor_lsn": "0/86000001"}, "wal_anchor_not_after_checkpoint"),
    ({"source_commit_seq": 11}, "wal_anchor_not_after_checkpoint"),
))
def test_final_wal_rejects_invalid_checkpoint_terminal_before_ciphertext_read(final_checkpoint, fields, error):
    options, anchor, publish = final_checkpoint
    options["anchor_sha256"] = publish(options["anchor_path"], anchor | fields)
    with pytest.raises(ValueError, match=error):
        transfer.verify_final_wal(**options, expected_hash="c" * 64)


def test_final_wal_origin_requires_independently_pinned_checkpoint(final_checkpoint):
    options, _, _ = final_checkpoint
    baseline, checkpoint = options["baseline"], options["checkpoint"]
    assert transfer.final_wal_origin(baseline) == baseline["wal_backup_stop"]
    with pytest.raises(ValueError, match="checkpoint_digest_changed"):
        transfer.final_wal_origin(baseline, checkpoint=checkpoint)
    changed = checkpoint | {"wal_verification_sha256": "f" * 64}
    with pytest.raises(ValueError, match="checkpoint_digest_changed"):
        transfer.final_wal_origin(baseline, checkpoint=changed, checkpoint_sha256=options["checkpoint_sha256"])


def test_pre_stop_wal_counts_redo_range_separately_from_transfer(monkeypatch):
    segment_bytes = 1 << 24
    row = {"segment": "000000010000000100000001"}
    monkeypatch.setattr(transfer, "_source_wal_snapshot", lambda *_: row)
    options = {"control": object(), "expected_database": {}, "backup_stop": "000000010000000100000000",
               "segment_bytes": segment_bytes, "max_segments": 1, "timeout_seconds": 1}
    restart_from = _segment(0xFF)
    assert transfer.check_source_wal_capacity(**options) == 1
    assert transfer.check_source_wal_capacity(**options, restart_from=restart_from, max_restart_segments=3) == 1
    with pytest.raises(ValueError, match="wal_restart_range_outside_budget"):
        transfer.check_source_wal_capacity(**options, restart_from=restart_from, max_restart_segments=2)
    with pytest.raises(ValueError, match="wal_restart_segment_budget_invalid"):
        transfer.check_source_wal_capacity(**options, max_restart_segments=3)
