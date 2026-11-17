"""Build and verify the archive closure of a named pre-switch WAL point."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from types import MappingProxyType
from typing import Mapping

from core.storage_targets import StorageLocation
from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.recovery_copies import _archive_rows, _identity, _snapshot_layout, _sync
from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
from scripts.automation.stage_encrypted_recovery_handoff import _private_root, _stamp
from scripts.automation.verify_canonical_recovery_archive import (
    _encrypted_receipt, _path, _restore_root, _sha256, _sha_file, _verify_encrypted_inventory,
    _verify_inventory,
)


@dataclass(frozen=True)
class PreparedArchiveBaseline:
    receipt_sha256: str
    database_identity: str
    layout: Mapping
    objects: Mapping[str, tuple[str, int]]
    checkpoint_sha256: str | None = None


def prepare_archive_baseline(receipt_path, restored_root, *, expected_receipt_sha256, max_objects,
                             check=lambda: None):
    """Verify the independently restored baseline files before the writer fence."""
    if type(max_objects) is not int or not 1 <= max_objects <= 10_000_000:
        raise ValueError("canonical_recovery_archive_object_budget_invalid")
    receipt_path = Path(receipt_path)
    if not receipt_path.is_file() or receipt_path.stat().st_size > 65536:
        raise ValueError("canonical_recovery_baseline_receipt_invalid")
    receipt_sha256, _ = _sha_file(receipt_path)
    receipt = _encrypted_receipt(receipt_path)
    if (receipt_sha256 != expected_receipt_sha256 or type(receipt.get("archive_objects")) is not int
            or not 0 <= receipt["archive_objects"] <= max_objects):
        raise RuntimeError("canonical_recovery_baseline_receipt_or_budget_mismatch")
    restored_objects = {}

    def record(key, digest, size):
        StorageLocation("archive", key)
        restored_objects[key] = (digest, size)

    _verify_encrypted_inventory(receipt, _restore_root(Path(restored_root)), record=record, check=check)
    if _sha_file(receipt_path)[0] != receipt_sha256:
        raise RuntimeError("canonical_recovery_baseline_receipt_changed")
    return PreparedArchiveBaseline(
        receipt_sha256, receipt["database_identity"], MappingProxyType(receipt["storage_layout"]),
        MappingProxyType(restored_objects),
    )


def _encode(key, descriptor):
    digest, size = descriptor
    return (json.dumps({"object_key": key, "bytes": size, "sha256": digest},
                       sort_keys=True, separators=(",", ":")) + "\n").encode()


def prepare_replayed_archive_baseline(inventory_path, *, checkpoint, max_objects, archive_objects=None,
                                      check=lambda: None):
    """Read an authenticated checkpoint's complete catalog before the writer fence.

    The caller verifies the checkpoint with the signed readiness evidence. The
    source needs only the manifest. The receiver also verifies its exact files.
    """
    if type(max_objects) is not int or not 1 <= max_objects <= 10_000_000:
        raise ValueError("canonical_recovery_archive_object_budget_invalid")
    if not isinstance(checkpoint, Mapping):
        raise ValueError("canonical_recovery_replayed_archive_checkpoint_invalid")
    body = dict(checkpoint)
    checkpoint_hash = body.pop("verification_sha256", None)
    pair_hash, identity, layout = (body.get(key) for key in ("pair_receipt_sha256", "database_identity", "storage_layout"))
    inventory_hash, count, total = (body.get(key) for key in ("catalog_inventory_sha256", "catalog_objects", "catalog_bytes"))
    if (body.get("schema") != "qt.replayed_recovery_checkpoint.v1"
            or not all(_sha256(value) for value in (checkpoint_hash, pair_hash, inventory_hash))
            or not isinstance(identity, str) or not re.fullmatch(r"[1-9][0-9]{0,19}/[1-9][0-9]{0,9}", identity)
            or not isinstance(layout, dict) or set(layout) != {"layout_version", "certificate_sha256"}
            or not isinstance(layout["layout_version"], str) or not layout["layout_version"]
            or not _sha256(layout["certificate_sha256"])
            or type(count) is not int or not 0 <= count <= max_objects
            or type(total) is not int or not 0 <= total < 2**63
            or archive_evidence_hash(body) != checkpoint_hash):
        raise ValueError("canonical_recovery_replayed_archive_checkpoint_invalid")

    objects = read_archive_catalog_inventory(inventory_path, inventory_sha256=inventory_hash, count=count, total=total,
                                             max_objects=max_objects, check=check)
    if archive_objects is not None:
        archive, archive_identity = _private_root(Path(archive_objects))
        for key, descriptor in objects.items():
            check()
            path = _path(archive, key, absolute=False)
            if _sha_file(path, check=check) != descriptor:
                raise ValueError(f"restored archive object failed size or hash check: {key}")
        if _private_root(archive)[1] != archive_identity:
            raise RuntimeError("canonical_recovery_replayed_archive_root_changed")
    check()
    return PreparedArchiveBaseline(pair_hash, identity, MappingProxyType(layout.copy()),
                                   MappingProxyType(objects), checkpoint_hash)


def read_archive_catalog_inventory(inventory_path, *, inventory_sha256, count, total, max_objects, check):
    """Read the canonical catalog used by both paused and replayed recovery evidence."""
    if type(max_objects) is not int or not 1 <= max_objects <= 10_000_000:
        raise ValueError("canonical_recovery_archive_object_budget_invalid")
    if (not _sha256(inventory_sha256) or type(count) is not int or not 0 <= count <= max_objects
            or type(total) is not int or not 0 <= total < 2**63):
        raise ValueError("canonical_recovery_replayed_archive_inventory_mismatch")
    inventory = Path(inventory_path)
    if not inventory.is_absolute() or inventory.resolve(strict=True) != inventory:
        raise ValueError("canonical_recovery_replayed_archive_inventory_path_invalid")
    before = inventory.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        raise ValueError("canonical_recovery_replayed_archive_inventory_path_invalid")
    stamp = _stamp(before)
    objects, digest = {}, hashlib.sha256()
    bytes_seen = 0
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    with os.fdopen(os.open(inventory, flags), "rb") as stream:
        if _stamp(os.fstat(stream.fileno())) != stamp:
            raise RuntimeError("canonical_recovery_replayed_archive_inventory_changed")
        while line := stream.readline(65537):
            check()
            if len(line) > 65536 or len(objects) >= count or not line.endswith(b"\n"):
                raise ValueError("canonical_recovery_replayed_archive_inventory_row_invalid")
            row = json.loads(line)
            if not isinstance(row, dict) or set(row) != {"object_key", "sha256", "bytes"}:
                raise ValueError("canonical_recovery_replayed_archive_inventory_row_invalid")
            key, sha, size = row["object_key"], row["sha256"], row["bytes"]
            StorageLocation("archive", key)
            if (len(key) > 2048 or len(key.split("/")) > 64 or key in objects
                    or not _sha256(sha) or type(size) is not int or size <= 0
                    or line != _encode(key, (sha, size))):
                raise ValueError("canonical_recovery_replayed_archive_inventory_row_invalid")
            bytes_seen += size
            if bytes_seen > total:
                raise ValueError("canonical_recovery_replayed_archive_inventory_mismatch")
            digest.update(line)
            objects[key] = (sha, size)
        if _stamp(os.fstat(stream.fileno())) != stamp or _stamp(inventory.lstat()) != stamp:
            raise RuntimeError("canonical_recovery_replayed_archive_inventory_changed")
    if (digest.hexdigest(), len(objects), bytes_seen) != (inventory_sha256, count, total):
        raise ValueError("canonical_recovery_replayed_archive_inventory_mismatch")
    return objects


def _final_descriptors(conn, anchor, baseline, *, max_objects, check):
    if type(max_objects) is not int or not 1 <= max_objects <= 10_000_000:
        raise ValueError("canonical_recovery_archive_object_budget_invalid")
    database = anchor["database"]
    identity = f"{database['system_identifier']}/{database['database_oid']}"
    if (_identity(conn)[0] != identity or baseline.database_identity != identity
            or baseline.layout != anchor["storage_layout"]
            or _snapshot_layout(conn) != anchor["storage_layout"]
            or PostgresMarketDataRepository._current_commit_seq_with_session(conn) != anchor["source_commit_seq"]):
        raise RuntimeError("canonical_recovery_archive_boundary_mismatch")
    result = {}
    for row in _archive_rows(conn, max_objects=max_objects, check=check):
        key, digest, size = row["object_key"], row["object_sha256"], row["byte_count"]
        StorageLocation("archive", key)
        if len(key) > 2048 or len(key.split("/")) > 64 or not _sha256(digest) or type(size) is not int or size <= 0:
            raise RuntimeError("canonical_recovery_archive_descriptor_invalid")
        if key in result:
            raise RuntimeError("canonical_recovery_archive_duplicate_key")
        descriptor = (digest, size)
        if key in baseline.objects and baseline.objects[key] != descriptor:
            raise RuntimeError("canonical_recovery_archive_baseline_key_changed")
        result[key] = descriptor
    return result


def _digest(objects, check):
    digest = hashlib.sha256()
    total = 0
    for key in sorted(objects):
        check()
        descriptor = objects[key]
        digest.update(_encode(key, descriptor))
        total += descriptor[1]
    return digest.hexdigest(), total


def _copy_object(source, destination, descriptor, check):
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with os.fdopen(os.open(source, os.O_RDONLY | os.O_NOFOLLOW), "rb") as incoming:
        before = os.fstat(incoming.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size != descriptor[1]:
            raise RuntimeError("canonical_recovery_archive_source_size_or_type_changed")
        digest = hashlib.sha256()
        copied = 0
        with os.fdopen(os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as output:
            while chunk := incoming.read(1024 * 1024):
                check()
                copied += len(chunk)
                if copied > descriptor[1]:
                    raise RuntimeError("canonical_recovery_archive_source_size_or_type_changed")
                digest.update(chunk)
                output.write(chunk)
            output.flush()
            os.fsync(output.fileno())
        after = os.fstat(incoming.fileno())
        if ((digest.hexdigest(), copied) != descriptor
                or (before.st_mtime_ns, before.st_ctime_ns, before.st_size) !=
                   (after.st_mtime_ns, after.st_ctime_ns, after.st_size)):
            raise RuntimeError("canonical_recovery_archive_source_bytes_changed")


def capture_final_archive_suffix(fence, anchor, baseline, *, source_root, stage, max_objects,
                                 max_suffix_bytes, check_archive):
    """Stage only new archive bytes; encrypt the returned NUL file list separately.

    Stage must be an existing, private, empty directory. Failures leave an
    unpublished stage to be discarded. Use an independently managed Restic
    repository for the suffix because ordinary pair pruning owns its snapshots.
    """
    conn = fence.connection
    stage = Path(stage)
    if (not callable(check_archive) or anchor["database"] != fence.database or type(max_suffix_bytes) is not int
            or max_suffix_bytes < 0 or not stage.is_absolute() or stage.resolve(strict=True) != stage
            or not stage.is_dir() or stage.stat().st_uid != os.getuid()
            or stage.stat().st_mode & 0o077 or any(stage.iterdir())):
        raise ValueError("canonical_recovery_archive_stage_or_fence_invalid")

    def check():
        fence.check_clock()
        check_archive()

    fence.assert_quiet(conn)
    check()
    final = _final_descriptors(conn, anchor, baseline, max_objects=max_objects, check=check)
    suffix = {key: value for key, value in final.items() if key not in baseline.objects}
    suffix_digest, suffix_bytes = _digest(suffix, check)
    if suffix_bytes > max_suffix_bytes:
        raise RuntimeError("canonical_recovery_archive_suffix_budget_exceeded")
    final_digest, final_bytes = _digest(final, check)
    root = Path(source_root)
    if root.resolve(strict=True) != root or not root.is_dir() or stage.is_relative_to(root):
        raise ValueError("canonical_recovery_archive_source_root_invalid")
    inventory, file_list = stage / "objects.jsonl", stage / "files.list"
    directories = {stage}
    with inventory.open("xb") as output, file_list.open("xb") as listing:
        inventory.chmod(0o600)
        file_list.chmod(0o600)
        listing.write(os.fsencode(inventory) + b"\x00")
        for key in sorted(suffix):
            check()
            destination = stage / "objects" / key
            _copy_object(_path(root, key, absolute=False), destination, suffix[key], check)
            directories.update(parent for parent in destination.parents if parent.is_relative_to(stage))
            output.write(_encode(key, suffix[key]))
            listing.write(os.fsencode(destination) + b"\x00")
        for handle in (output, listing):
            handle.flush()
            os.fsync(handle.fileno())
    for directory in sorted(directories, key=lambda path: len(path.parts), reverse=True):
        check()
        _sync(directory)
    fence.assert_quiet(conn)
    check()
    result = {
        "schema_version": "qt.named_recovery_archive_suffix.v1",
        "anchor_sha256": archive_evidence_hash(anchor), "baseline_receipt_sha256": baseline.receipt_sha256,
        "final_inventory_sha256": final_digest, "final_objects": len(final), "final_bytes": final_bytes,
        "inventory_sha256": suffix_digest, "archive_objects": len(suffix), "archive_bytes": suffix_bytes,
        "inventory_snapshot_path": str(inventory), "archive_source_root": str(stage / "objects"),
    }
    if baseline.checkpoint_sha256 is not None:
        result["baseline_checkpoint_sha256"] = baseline.checkpoint_sha256
    return result


def verify_final_archive_suffix(conn, anchor, baseline, evidence, restored_root, *, max_objects,
                                suffix_snapshot_id, check=lambda: None):
    """Verify the decrypted suffix against the catalog at the isolated named restore."""
    if (not _sha256(suffix_snapshot_id) or evidence["archive_snapshot"] != suffix_snapshot_id
            or evidence["schema_version"] != "qt.named_recovery_archive_suffix.v1"
            or evidence["anchor_sha256"] != archive_evidence_hash(anchor)
            or evidence["baseline_receipt_sha256"] != baseline.receipt_sha256
            or evidence.get("baseline_checkpoint_sha256") != baseline.checkpoint_sha256):
        raise RuntimeError("canonical_recovery_archive_suffix_binding_mismatch")
    if not conn.in_transaction():
        raise RuntimeError("canonical_recovery_archive_verification_transaction_required")
    final = _final_descriptors(conn, anchor, baseline, max_objects=max_objects, check=check)
    suffix = {key: value for key, value in final.items() if key not in baseline.objects}
    digest, total = _digest(final, check)
    if (evidence["final_inventory_sha256"], evidence["final_objects"], evidence["final_bytes"]) != (
            digest, len(final), total):
        raise RuntimeError("canonical_recovery_archive_final_inventory_mismatch")
    root = _restore_root(Path(restored_root))
    inventory = _path(root, evidence["inventory_snapshot_path"], absolute=True)
    verified = set()

    def record(key, digest, size):
        if suffix.get(key) != (digest, size):
            raise RuntimeError("canonical_recovery_archive_unexpected_suffix_object")
        verified.add(key)

    _verify_inventory(root, evidence, inventory, evidence["archive_source_root"], absolute=True,
                      record=record, check=check)
    if verified != suffix.keys():
        raise RuntimeError("canonical_recovery_archive_suffix_incomplete")
    return {"archive_snapshot": suffix_snapshot_id,
            "final_inventory_sha256": digest, "final_objects": len(final), "final_bytes": total,
            "baseline_objects_used": len(final) - len(suffix), "verified_suffix_objects": len(verified)}
