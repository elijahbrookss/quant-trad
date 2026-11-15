from dataclasses import replace
import hashlib
import io
import os
from types import SimpleNamespace

import pytest

from market_data.fact_archive import archive_evidence_hash
from scripts.ci import materialize_paused_archive_suffix as materializer
from scripts.db.canonical_cold_recovery_suffix import (
    PreparedArchiveBaseline, _encode, prepare_replayed_archive_baseline, verify_final_archive_suffix,
)


def _checkpoint(payload, *, count, total):
    body = {
        "schema": "qt.replayed_recovery_checkpoint.v1", "pair_receipt_sha256": "a" * 64,
        "database_identity": "12345/42",
        "storage_layout": {"layout_version": "market.fact_storage_tiers.v2", "certificate_sha256": "b" * 64},
        "catalog_inventory_sha256": hashlib.sha256(payload).hexdigest(), "catalog_objects": count, "catalog_bytes": total,
    }
    return body | {"verification_sha256": archive_evidence_hash(body)}


def _put(root, key, content):
    path = root / key
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_bytes(content)
    return hashlib.sha256(content).hexdigest(), len(content)


def test_complete_replayed_inventory_preserves_stream_order_and_verifies_receiver_bytes(tmp_path, monkeypatch):
    suffix, source, baseline_files = (tmp_path / name for name in ("suffix", "source", "baseline"))
    for path in (suffix, source, baseline_files):
        path.mkdir(mode=0o700)
    older_key, new_key = "z/base.parquet", "a/later.parquet"
    descriptors = {older_key: _put(baseline_files, older_key, b"old"), new_key: _put(source, new_key, b"newer")}
    expected = b"".join(_encode(key, descriptor) for key, descriptor in descriptors.items())
    rows = [{"object_key": key, "object_sha256": digest, "byte_count": size}
            for key, (digest, size) in descriptors.items()]
    monkeypatch.setattr(materializer, "_archive_rows", lambda *args, **kwargs: iter(rows))
    baseline = PreparedArchiveBaseline("a" * 64, "12345/42", {}, {older_key: descriptors[older_key]})
    scope = {"materialize_baseline": True}
    objects, work = materializer._scope_directory(suffix, scope)
    args = SimpleNamespace(max_objects=10, reserve_bytes=0, source_archive=source, max_work_bytes=len(expected) + 8)
    staged = work / "catalog.jsonl"
    with staged.open("xb") as manifest:
        result = materializer._copy_catalog(None, args, baseline, objects, work, lambda: None, lambda: None,
            baseline_bytes=3, baseline_source=baseline_files, catalog_inventory=manifest)
    path = materializer._publish_catalog_inventory(suffix, work, result["catalog_inventory_sha256"], lambda: None)
    assert path.read_bytes() == expected
    assert result["catalog_inventory_sha256"] == hashlib.sha256(expected).hexdigest()
    assert result["catalog_inventory_bytes"] == len(expected)
    too_small = SimpleNamespace(**(vars(args) | {"max_work_bytes": len(expected) + 7}))
    with pytest.raises(RuntimeError, match="work_byte_budget_exceeded"):
        materializer._copy_catalog(None, too_small, baseline, objects, work, lambda: None, lambda: None,
                                   baseline_bytes=3, baseline_source=baseline_files, catalog_inventory=io.BytesIO())

    # A crash between installing the manifest and removing the staging name is resumable.
    original = path.stat()
    os.link(path, staged)
    materializer._scope_directory(suffix, scope)
    staged.write_bytes(expected)
    assert materializer._publish_catalog_inventory(suffix, work, result["catalog_inventory_sha256"], lambda: None) == path
    assert path.stat().st_ino == original.st_ino and not staged.exists()

    checkpoint = _checkpoint(expected, count=2, total=8)
    detached = tmp_path / "transported-catalog.jsonl"
    detached.write_bytes(expected)
    from_source = prepare_replayed_archive_baseline(detached, checkpoint=checkpoint, max_objects=2)
    from_receiver = prepare_replayed_archive_baseline(path, checkpoint=checkpoint, max_objects=2, archive_objects=objects)
    assert from_receiver == from_source
    assert list(from_receiver.objects.items()) == list(descriptors.items())
    assert from_receiver.receipt_sha256 == baseline.receipt_sha256
    assert from_receiver.checkpoint_sha256 == checkpoint["verification_sha256"]

    (objects / new_key).write_bytes(b"wrong")
    with pytest.raises(ValueError, match="restored archive object failed size or hash check"):
        prepare_replayed_archive_baseline(path, checkpoint=checkpoint, max_objects=2, archive_objects=objects)
    assert prepare_replayed_archive_baseline(detached, checkpoint=checkpoint, max_objects=2) == from_source
    with pytest.raises(ValueError, match="checkpoint_invalid"):
        prepare_replayed_archive_baseline(detached, checkpoint=checkpoint, max_objects=1)


def test_replayed_manifest_refuses_mutated_and_noncanonical_rows(tmp_path):
    path = tmp_path / "catalog.jsonl"
    content = _encode("one.parquet", ("c" * 64, 7))
    path.write_bytes(content.replace(b"one.parquet", b"two.parquet"))
    with pytest.raises(ValueError, match="inventory_mismatch"):
        prepare_replayed_archive_baseline(path, checkpoint=_checkpoint(content, count=1, total=7), max_objects=1)

    wrong_encoding = content.replace(b'":', b'": ')
    path.write_bytes(wrong_encoding)
    with pytest.raises(ValueError, match="inventory_row_invalid"):
        prepare_replayed_archive_baseline(path, checkpoint=_checkpoint(wrong_encoding, count=1, total=7), max_objects=1)


def test_final_archive_rejects_fallback_to_the_original_pair_baseline(tmp_path):
    path = tmp_path / "catalog.jsonl"
    path.touch()
    checkpoint = _checkpoint(b"", count=0, total=0)
    baseline = prepare_replayed_archive_baseline(path, checkpoint=checkpoint, max_objects=1)
    anchor = {"restore_name": "qt_fixture"}
    evidence = {
        "archive_snapshot": "c" * 64, "schema_version": "qt.named_recovery_archive_suffix.v1",
        "anchor_sha256": archive_evidence_hash(anchor), "baseline_receipt_sha256": baseline.receipt_sha256,
        "baseline_checkpoint_sha256": baseline.checkpoint_sha256,
    }
    with pytest.raises(RuntimeError, match="suffix_binding_mismatch"):
        verify_final_archive_suffix(None, anchor, replace(baseline, checkpoint_sha256=None), evidence, tmp_path,
                                    max_objects=1, suffix_snapshot_id="c" * 64)
