import hashlib

import pytest

from market_data.fact_archive import archive_evidence_hash
from scripts.automation.stage_encrypted_recovery_handoff import _encode
from scripts.db.canonical_cold_recovery_wal_transfer import _prefix_binding, _prefix_path
from scripts.db.canonical_cold_replayed_checkpoint import build_replayed_checkpoint
from scripts.db import canonical_cold_retained_wal as retained
from scripts.db.canonical_cold_retained_wal import capture_retained_wal


def _segment(number):
    return f"0000000100000000{number:08X}"


@pytest.fixture
def received_wal(tmp_path):
    root = tmp_path / "exchange"
    root.mkdir(mode=0o700)
    bucket = root / "database/archive/qt/15-1/0000000100000000"
    bucket.mkdir(parents=True)
    layout = {"layout_version": "market.fact_storage_tiers.v2", "certificate_sha256": "b" * 64}
    pair = {"database_identity": "123/42", "storage_layout": layout, "archive_snapshot": "c" * 64,
            "database_label": "20250101-000000F"}

    def write_json(path, content):
        raw = _encode(content)
        path.write_bytes(raw)
        return hashlib.sha256(raw).hexdigest()

    pair_sha256 = write_json(root / "complete.json", pair)
    transfer_sha256 = write_json(root / "encrypted-copy.json", {"schema": "qt.encrypted_recovery_transfer.v1",
                                                                 "pair_receipt_sha256": pair_sha256})
    files = {}
    for number in range(3, 7):
        name = _segment(number)
        checksum = hashlib.sha1(name.encode()).hexdigest()
        path = bucket / f"{name}-{checksum}.gz"
        ciphertext = f"synthetic ciphertext {name}".encode()
        path.write_bytes(ciphertext)
        files[name] = {"segment": name, "wal_checksum": checksum, "path": path.relative_to(root).as_posix(),
                       "bytes": len(ciphertext), "sha256": hashlib.sha256(ciphertext).hexdigest()}

    start = _segment(3)
    baseline = pair | {"schema": "qt.encrypted_recovery_repository_verification.v1", "pair_receipt_sha256": pair_sha256,
                       "anchor_sha256": None, "wal_start": start, "wal_backup_stop": start, "wal_verified_through": start,
                       "wal_segment_bytes": 2**24, "wal_segments": 1, "database_archive_id": "15-1",
                       "wal_inventory_sha256": archive_evidence_hash([[start, files[start]["wal_checksum"]]])}
    baseline["verification_sha256"] = archive_evidence_hash(baseline)
    prefix_scope = {"prepared_receiver_sha256": "d" * 64, "pair_receipt_sha256": pair_sha256,
                    "baseline_verification_sha256": baseline["verification_sha256"]}
    after, previous = start, None
    prefixes = []
    for number in range(4, 7):
        name = _segment(number)
        row = files[name]
        receipt = _prefix_binding(transfer_sha256, baseline["verification_sha256"]) | {
            "wal_after": after, "wal_transferred_through": name, "previous_prefix_sha256": previous,
            "encrypted_bytes": row["bytes"], "files": [row],
        }
        digest = write_json(_prefix_path(root, name), receipt)
        body = prefix_scope | {"schema": "qt.named_recovery_wal_prefix_verification.v1", "after": after, "through": name,
                               "wal_prefix_sha256": digest, "previous_prefix_sha256": previous, "segments": 1,
                               "verified_wal_sha256": archive_evidence_hash([[name, row["wal_checksum"]]])}
        prefixes.append({"body": body, "signature": "synthetic authenticated upstream signature"})
        after, previous = name, digest
    verification = prefix_scope | {"wal_first": start, "wal_verified_through": _segment(6), "wal_segment_bytes": 2**24,
                                   "prefix_batches": 3, "prefix_segments": 3}
    verification["wal_verification_sha256"] = archive_evidence_hash(
        verification | {"receipts": [archive_evidence_hash(envelope) for envelope in prefixes]},
    )

    def options(redo, native_verify):
        endpoint = {"database_identity": pair["database_identity"], "storage_layout": layout, "timeline": 1,
                    "accepted_commit_seq": 10, "target_lsn": "0/6001000", "replay_lsn": "0/6001060"}
        archive = endpoint | {"schema": "qt.paused_archive_suffix.v1", "pair_receipt_sha256": pair_sha256,
                              "catalog_inventory_sha256": "e" * 64, "catalog_objects": 0, "catalog_bytes": 0}
        archive["verification_sha256"] = archive_evidence_hash(archive)
        control = f"""pg_control version number: 1300
Database system identifier: 123
Database cluster state: shut down in recovery
Latest checkpoint location: 0/0{redo}000060
Latest checkpoint's REDO location: 0/0{redo}000028
Latest checkpoint's REDO WAL file: {_segment(redo)}
Latest checkpoint's TimeLineID: 1
Minimum recovery ending location: 0/06001060
Min recovery ending loc's timeline: 1
Bytes per WAL segment: 16777216
"""
        checkpoint = build_replayed_checkpoint(
            pair_receipt_sha256=pair_sha256, database_identity=pair["database_identity"], timeline=1,
            wal_first=start, wal_verified_through=_segment(6), wal_segment_bytes=2**24,
            wal_verification_sha256=verification["wal_verification_sha256"], paused_receipt=endpoint,
            archive_receipt=archive, stopped_control=control, stopped_control_sha256="f" * 64,
        )
        return {"receiving": root, "baseline": baseline, "base_transfer_sha256": transfer_sha256, "checkpoint": checkpoint,
                "checkpoint_sha256": checkpoint["verification_sha256"], "wal_verification": verification,
                "prefix_receipts": prefixes, "max_segments": 8, "native_verify": native_verify, "check": lambda: None}

    return root, bucket, files, options


def _native_rows(wal):
    return [[row["segment"], row["wal_checksum"]] for row in wal["files"]]


def test_retained_wal_captures_original_redo_and_allows_final_tail(received_wal):
    root, bucket, files, options = received_wal
    calls = []

    def native_verify(wal):
        calls.append(wal)
        return _native_rows(wal)

    held = capture_retained_wal(**options(3, native_verify))
    assert [row.segment for row in held.files] == list(files)
    assert calls[0]["wal_segment_bytes"] == 2**24
    assert [row[0] for row in _native_rows(calls[0])] == list(files)
    (bucket / f"{_segment(7)}-{'a' * 40}.gz").write_bytes(b"final suffix")
    held.assert_held(lambda: None)
    file = root / files[_segment(5)]["path"]
    file.write_bytes(b"x" * file.stat().st_size)
    with pytest.raises(RuntimeError, match="canonical_retained_wal_archive_changed"):
        held.assert_held(lambda: None)


def test_restart_only_reads_overlapping_prefixes_and_refuses_alias(received_wal):
    root, bucket, files, options = received_wal
    _prefix_path(root, _segment(4)).unlink()
    (root / files[_segment(4)]["path"]).unlink()
    held = capture_retained_wal(**options(5, _native_rows))
    assert [row.segment for row in held.files] == [_segment(5), _segment(6)]
    (bucket / f"{_segment(5)}-{'a' * 40}.zst").write_bytes(b"alternate candidate")
    with pytest.raises(RuntimeError, match="segment_missing_or_ambiguous"):
        held.assert_held(lambda: None)


def test_capture_refuses_base_name_ciphertext_and_native_changes(received_wal):
    root, bucket, files, options = received_wal
    original = root / files[_segment(3)]["path"]
    changed = original.rename(bucket / f"{_segment(3)}-{'a' * 40}.gz")
    with pytest.raises(ValueError, match="base_inventory_changed"):
        capture_retained_wal(**options(3, _native_rows))
    changed.rename(original)
    with pytest.raises(ValueError, match="native_verification_changed"):
        capture_retained_wal(**options(3, lambda wal: []))
    prefix = root / files[_segment(5)]["path"]
    prefix.write_bytes(b"x" * prefix.stat().st_size)
    with pytest.raises(ValueError, match="prefix_file_changed"):
        capture_retained_wal(**options(3, _native_rows))


def test_capture_refuses_changed_chain_and_restart_cap(received_wal):
    _, _, _, options = received_wal
    kwargs = options(3, _native_rows)
    with pytest.raises(ValueError, match="wal_range_outside_budget"):
        capture_retained_wal(**(kwargs | {"max_segments": 3}))
    kwargs["prefix_receipts"][0]["signature"] = "updated upstream signature bytes"
    with pytest.raises(ValueError, match="prefix_chain_changed"):
        capture_retained_wal(**kwargs)


def test_capture_final_members_checks_authenticated_names_without_reading_bytes(received_wal, monkeypatch):
    root, bucket, files, _ = received_wal
    options = {"receiving": root, "archive_id": "15-1", "check": lambda: None}

    def unexpected_read(*args, **kwargs):
        pytest.fail("final member custody opened a WAL file")

    monkeypatch.setattr(retained, "_file", unexpected_read)
    assert retained.capture_wal_members(rows=[], **options).files == ()
    row = files[_segment(6)]
    held = retained.capture_wal_members(rows=[row], **options)
    assert [(item.segment, item.path, item.wal_checksum) for item in held.files] == [(row["segment"], row["path"], row["wal_checksum"])]
    for changed in ({"path": "elsewhere"}, {"wal_checksum": "a" * 40}, {"bytes": row["bytes"] + 1}):
        with pytest.raises(ValueError, match="member_changed"):
            retained.capture_wal_members(rows=[row | changed], **options)
    with pytest.raises(ValueError, match="member_scope_invalid"):
        retained.capture_wal_members(rows=[row, row], **options)
    (bucket / f"{_segment(7)}-{'a' * 40}.gz").write_bytes(b"new segment")
    held.assert_held(options["check"])
    (bucket / f"{_segment(6)}-{'a' * 40}.gz").write_bytes(b"ambiguous existing segment")
    with pytest.raises(RuntimeError, match="segment_missing_or_ambiguous"):
        held.assert_held(options["check"])
