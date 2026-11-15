"""Seed a private candidate through the real pinned inventory and atomic copier."""
from hashlib import sha256
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from market_data.fact_archive import archive_evidence_hash
from scripts.ci import seed_private_candidate_archive as seeder
from scripts.db.canonical_cold_recovery_suffix import _encode


pytestmark = pytest.mark.skipif(os.name != "posix", reason="requires native POSIX archive custody")
OBJECTS = {"raw/first.parquet": b"native raw bytes", "fact/second.parquet": b"native Fact bytes"}


def _receipt(path, body):
    receipt = body | {"verification_sha256": archive_evidence_hash(body)}
    data = (json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n").encode()
    path.write_bytes(data)
    path.chmod(0o600)
    return sha256(data).hexdigest()


@pytest.fixture
def scope(tmp_path, monkeypatch):
    source, destination = tmp_path / "source", tmp_path / "candidate"
    source.mkdir(mode=0o700)
    destination.mkdir(mode=0o700)
    objects = source / "objects"
    objects.mkdir(mode=0o700)
    for key, payload in OBJECTS.items():
        leaf = objects / key
        leaf.parent.mkdir(mode=0o700, exist_ok=True)
        leaf.write_bytes(payload)
        leaf.chmod(0o600)
    inventory = b"".join(_encode(key, (sha256(payload).hexdigest(), len(payload))) for key, payload in OBJECTS.items())
    (source / "catalog.jsonl").write_bytes(inventory)
    (source / "catalog.jsonl").chmod(0o600)
    st_dev = source.stat().st_dev
    device_id = f"{os.major(st_dev)}:{os.minor(st_dev)}"
    byte_count = sum(map(len, OBJECTS.values()))
    digest = sha256(inventory).hexdigest()
    common = {"database_identity": "1000/16384", "storage_layout": {
        "layout_version": "market.fact_storage_tiers.v2", "certificate_sha256": "b" * 64,
    }, "timeline": 1, "target_lsn": "A/10", "replay_lsn": "A/20", "accepted_commit_seq": 71,
        "device_id": device_id, "pgdata_inode": 1201, "pair_receipt_sha256": "c" * 64}
    suffix = tmp_path / "suffix.json"
    suffix_body = common | {
        "schema": "qt.paused_archive_suffix.v1", "materialize_baseline": True,
        "pgdata": "/original/pgdata", "source_archive": "/original/objects", "source_inode": 1202,
        "suffix_inode": source.stat().st_ino, "archive_root": "/paused/archive", "suffix_root": "/paused/archive/objects",
        "catalog_inventory_path": "/paused/archive/catalog.jsonl", "catalog_inventory_sha256": digest,
        "catalog_objects": len(OBJECTS), "catalog_bytes": byte_count, "catalog_inventory_bytes": len(inventory),
        "materialized_inventory_sha256": digest, "materialized_objects": len(OBJECTS), "materialized_bytes": byte_count,
        "archive_snapshot": "d" * 64, "baseline_inventory_sha256": "e" * 64,
        "baseline_objects_used": 1, "baseline_bytes_used": len(OBJECTS["raw/first.parquet"]),
        "suffix_objects": 1, "suffix_bytes": len(OBJECTS["fact/second.parquet"]),
        "suffix_inventory_sha256": "f" * 64, "copied_objects": 1, "reused_objects": 0,
        "copied_baseline_objects": 1, "reused_baseline_objects": 0,
        "work_logical_bytes": byte_count + len(inventory), "elapsed_seconds": 0.5,
    }
    suffix_sha = _receipt(suffix, suffix_body)
    proof = tmp_path / "clone.json"
    proof_sha = _receipt(proof, common | {
        "schema": "qt.benchmark_clone_boundary.v1", "database": "qt", "role": "qt", "role_oid": 10,
        "postmaster_started_at": "2026-11-15T08:00:00+00:00", "postmaster_epoch": "1794729600.000000",
        "pgdata_inode": 1203, "tablespace_inode": 1204, "tablespace_oid": 70715236, "socket_inode": 1205,
        "archive_inode": source.stat().st_ino, "archive_objects_inode": objects.stat().st_ino,
        "archive": {"inventory_sha256": digest, "objects": len(OBJECTS), "bytes": byte_count},
        "copy_receipt_sha256": "1" * 64, "suffix_receipt_sha256": suffix_sha,
        "copy_binding_sha256": "2" * 64, "elapsed_seconds": 0.5,
    })
    # A local tmp_path has no independent readonly bind. All file I/O, identity
    # and copy semantics remain native. Mount admission is covered by its own tests.
    def same_hdd_mounts(readonly, writable, expected_device):
        assert (readonly, writable, expected_device) == (source, destination, device_id)

    monkeypatch.setattr(seeder, "_require_isolated_mounts", same_hdd_mounts)
    native_statvfs = os.statvfs

    def receipt_mount(path):
        observed = native_statvfs(path)
        if Path(path) in (proof, suffix) or Path(path).is_relative_to(source):
            return os.statvfs_result((*observed[:8], observed.f_flag | os.ST_RDONLY, observed.f_namemax))
        return observed

    monkeypatch.setattr(os, "statvfs", receipt_mount)
    return SimpleNamespace(
        source_archive=source, destination_archive=destination, clone_proof=proof, clone_proof_sha256=proof_sha,
        suffix_receipt=suffix, suffix_receipt_sha256=suffix_sha, max_objects=10, max_copy_bytes=1_000_000,
        reserve_bytes=0, timeout_seconds=15,
    )


def test_seed_copies_exact_objects_then_resumes_without_allocating_another_copy(scope):
    source = scope.source_archive / "objects"
    candidate = scope.destination_archive / "objects"
    first = seeder.seed(scope)
    first_inodes = {}
    for key, payload in OBJECTS.items():
        leaf = candidate / key
        assert leaf.read_bytes() == source.joinpath(key).read_bytes() == payload
        assert leaf.stat().st_ino != source.joinpath(key).stat().st_ino
        assert leaf.stat().st_nlink == 1
        first_inodes[key] = leaf.stat().st_ino
    second = seeder.seed(scope)
    assert (first["copied_objects"], first["reused_objects"]) == (len(OBJECTS), 0)
    assert (second["copied_objects"], second["reused_objects"]) == (0, len(OBJECTS))
    assert {key: (candidate / key).stat().st_ino for key in OBJECTS} == first_inodes
    allocated = sum((candidate / key).stat().st_blocks * 512 for key in OBJECTS)
    for result in (first, second):
        assert result["schema"] == "qt.private_candidate_archive_seed.v1"
        assert result["archive_inode"] == scope.destination_archive.stat().st_ino
        assert result["archive_objects_inode"] == candidate.stat().st_ino
        assert result["catalog_objects"] == len(OBJECTS)
        assert result["catalog_bytes"] == sum(map(len, OBJECTS.values()))
        assert result["catalog_inventory_sha256"] == sha256((scope.source_archive / "catalog.jsonl").read_bytes()).hexdigest()
        assert result["object_file_allocated_bytes"] == allocated
        assert result["verification_sha256"] == archive_evidence_hash({
            key: value for key, value in result.items() if key != "verification_sha256"
        })


def test_seed_refuses_corrupted_existing_destination_without_repair(scope):
    seeder.seed(scope)
    key, original = next(iter(OBJECTS.items()))
    replacement = scope.destination_archive / "objects" / key
    replacement.write_bytes(b"x" * len(original))
    with pytest.raises((RuntimeError, ValueError), match="bytes_changed|checksum|mismatch"):
        seeder.seed(scope)
    assert replacement.read_bytes() == b"x" * len(original)
    assert (scope.source_archive / "objects" / key).read_bytes() == original


def test_seed_reuses_valid_objects_after_interrupted_copy(scope, monkeypatch):
    native_place = seeder._place_object
    completed = []

    def interrupted(key, descriptor, **options):
        if completed:
            raise TimeoutError("interrupted test copy")
        result = native_place(key, descriptor, **options)
        completed.append(key)
        return result

    with monkeypatch.context() as patch:
        patch.setattr(seeder, "_place_object", interrupted)
        with pytest.raises(TimeoutError, match="interrupted test copy"):
            seeder.seed(scope)
    persisted = scope.destination_archive / "objects" / completed[0]
    inode = persisted.stat().st_ino
    result = seeder.seed(scope)
    assert (result["copied_objects"], result["reused_objects"]) == (len(OBJECTS) - 1, 1)
    assert persisted.stat().st_ino == inode


def test_seed_refuses_source_alias(scope):
    first, second = OBJECTS
    leaf = scope.source_archive / "objects" / first
    leaf.unlink()
    leaf.symlink_to(scope.source_archive / "objects" / second)
    with pytest.raises((RuntimeError, ValueError), match="alias|path|namespace"):
        seeder.seed(scope)
    assert not (scope.destination_archive / "objects" / first).exists()


def test_seed_refuses_short_byte_budget(scope):
    scope.max_copy_bytes = sum(map(len, OBJECTS.values())) - 1
    with pytest.raises((RuntimeError, ValueError), match="budget|limit"):
        seeder.seed(scope)
    assert not any((scope.destination_archive / "objects" / key).exists() for key in OBJECTS)
