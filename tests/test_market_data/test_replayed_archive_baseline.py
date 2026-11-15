from contextlib import nullcontext
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


def test_archive_pair_sizing_rounds_both_copies_and_preserves_other_capacity(tmp_path, monkeypatch):
    baseline_key, later_key = "z/base.parquet", "a/nested/later.parquet"
    baseline = PreparedArchiveBaseline("a" * 64, "12345/42", {}, {baseline_key: ("b" * 64, 1)})
    descriptors = [(baseline_key, ("b" * 64, 1)), (later_key, ("c" * 64, 4097))]
    rows = [{"object_key": key, "object_sha256": digest, "byte_count": size}
            for key, (digest, size) in descriptors]
    monkeypatch.setattr(materializer, "_archive_rows", lambda *args, **kwargs: iter(rows))
    args = SimpleNamespace(suffix=tmp_path, max_objects=4, reserve_bytes=4096, other_planned_bytes=4096,
                           other_planned_entries=3, max_work_bytes=10000, max_pair_allocated_bytes=33 * 4096)
    monkeypatch.setattr(materializer, "_filesystem_space", lambda _: (4096, 35 * 4096, 22))

    def measure():
        return materializer._measure_catalog(None, args, baseline, {}, lambda: None, lambda: None, baseline_bytes=1)

    report = measure()
    encoded = b"".join(_encode(key, descriptor) for key, descriptor in descriptors)
    assert report["admission"] == "fits_inputs"
    assert report["catalog_inventory_sha256"] == hashlib.sha256(encoded).hexdigest()
    assert report["rounded_object_data_bytes"] == 3 * 4096
    assert report["source_allocation_bound_bytes"] == 17 * 4096
    assert report["candidate_allocation_bound_bytes"] == 16 * 4096
    assert report["required_free_bytes"] == 35 * 4096
    assert report["required_free_entries"] == 22
    assert report["work_logical_bytes"] == 1 + 4097 + len(encoded)
    assert list(tmp_path.iterdir()) == []

    args.max_pair_allocated_bytes -= 1
    assert measure()["admission"] == "insufficient"
    args.max_pair_allocated_bytes += 1
    monkeypatch.setattr(materializer, "_filesystem_space", lambda _: (4096, 35 * 4096, 21))
    assert measure()["admission"] == "insufficient"

    _put(tmp_path, "objects/a/nested/later.parquet", b"x" * 4097)
    marker = tmp_path / "scope.json"
    marker.write_bytes(materializer._scope_data({}))
    monkeypatch.setattr(materializer, "_filesystem_space", lambda _: (4096, 33 * 4096, 21))
    retry = measure()
    assert retry["admission"] == "fits_inputs"
    assert retry["previously_allocated_object_bytes"] == 2 * 4096
    assert retry["previously_allocated_object_entries"] == 1


def test_paused_archive_preflight_and_integrated_refusal_leave_suffix_empty(tmp_path, monkeypatch):
    source, suffix, archive = (tmp_path / name for name in ("source", "suffix", "archive"))
    for path in (source, suffix, archive):
        path.mkdir(mode=0o700)
    key, contents = "raw/late.parquet", b"accepted raw frame"
    digest, size = _put(source, key, contents)
    baseline = PreparedArchiveBaseline("a" * 64, "12345/42", {}, {})

    def mount_identity(path):
        return materializer._identity(path.lstat())

    pair = {"archive_bytes": 0, "archive_snapshot": "snapshot", "inventory_sha256": "b" * 64}
    args = SimpleNamespace(source_archive=source, suffix=suffix, archive_root=archive, pgdata=tmp_path / "pgdata",
                           pgdata_inode=17, device_id="8:99", receipt=tmp_path / "pair.json", receipt_sha256="a" * 64,
                           preflight_only=True, max_pair_allocated_bytes=1024, target_lsn="1/3", expected_replay_lsn="1/8",
                           timeline=1, materialize_baseline=True, reserve_bytes=0, other_planned_bytes=0,
                           other_planned_entries=0, max_objects=8, max_work_bytes=10_000, timeout_seconds=5,
                           statement_timeout_seconds=1)
    statements = []
    conn = SimpleNamespace(exec_driver_sql=statements.append)
    engine = SimpleNamespace(begin=lambda: nullcontext(conn), dispose=lambda: None)
    monkeypatch.setattr(materializer, "_check_scope", lambda args: None)
    monkeypatch.setattr(materializer, "_mount_paths", lambda args: {"private": True})
    monkeypatch.setattr(materializer, "_roots", lambda args: (mount_identity(source), mount_identity(suffix)))
    monkeypatch.setattr(materializer, "_read_pair", lambda args: pair)
    monkeypatch.setattr(materializer, "_readonly_engine", lambda args, check: engine)
    monkeypatch.setattr(materializer, "_bound_statement", lambda conn, check, seconds: check())
    monkeypatch.setattr(materializer, "_probe", lambda *a, **kw: {"replay_lsn": "1/8"})
    monkeypatch.setattr(materializer.PostgresMarketDataRepository, "_current_commit_seq_with_session", lambda conn: 47)
    monkeypatch.setattr(materializer, "prepare_archive_baseline", lambda *a, **kw: baseline)
    monkeypatch.setattr(materializer, "_archive_rows", lambda *a, **kw: iter([
        {"object_key": key, "object_sha256": digest, "byte_count": size},
    ]))

    real_statvfs = os.statvfs

    def test_statvfs(path):
        fs = real_statvfs(path)
        if path == source or (path == suffix and args.preflight_only):
            return SimpleNamespace(f_flag=fs.f_flag | os.ST_RDONLY, f_frsize=fs.f_frsize,
                                   f_bavail=fs.f_bavail, f_favail=fs.f_favail,
                                   f_blocks=fs.f_blocks, f_files=fs.f_files)
        return fs

    monkeypatch.setattr(materializer.os, "statvfs", test_statvfs)
    preview = materializer.materialize(args)
    assert preview["schema"] == "qt.paused_archive_pair_preflight.v1"
    assert preview["admission"] == "insufficient" and preview["accepted_commit_seq"] == 47
    assert list(suffix.iterdir()) == []
    args.preflight_only = False
    with pytest.raises(RuntimeError, match="paused_archive_suffix_pair_budget_exceeded"):
        materializer.materialize(args)
    assert list(suffix.iterdir()) == []

    args.max_pair_allocated_bytes = preview["combined_allocation_bound_bytes"]
    copied = materializer.materialize(args)
    assert copied["archive_pair_admission"]["admission"] == "fits_inputs"
    assert copied["catalog_inventory_sha256"] == preview["catalog_inventory_sha256"]
    assert (suffix / "objects" / key).read_bytes() == contents
    assert (suffix / "catalog.jsonl").read_bytes() == _encode(key, (digest, size))
    assert statements and set(statements) == {"SET TRANSACTION READ ONLY"}


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
