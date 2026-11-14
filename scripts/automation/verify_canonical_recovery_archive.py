#!/usr/bin/env python3
"""Check the files of a recovered QT encrypted pair or legacy logical copy offline."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat


def _sha256(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _path(root: Path, value: object, *, absolute: bool) -> Path:
    if not isinstance(value, str) or "\\" in value or "\x00" in value or (os.name == "nt" and ":" in value):
        raise ValueError("invalid recovery path")
    path = PurePosixPath(value)
    if path.is_absolute() != absolute or str(path) != value or ".." in path.parts:
        raise ValueError(f"invalid recovery path: {value}")
    parts = path.parts[1:] if absolute else path.parts
    if not parts or len(parts) > 64:
        raise ValueError(f"invalid recovery path depth: {value}")
    candidate = root.joinpath(*parts)
    if candidate.resolve(strict=True) != candidate or candidate.stat().st_dev != root.stat().st_dev:
        raise ValueError(f"restored path crosses an alias or filesystem boundary: {value}")
    return candidate


def _sha_file(path: Path, *, required_prefix: bytes = b"") -> tuple[str, int]:
    digest = hashlib.sha256()
    before_open = path.lstat()
    if not stat.S_ISREG(before_open.st_mode):
        raise ValueError(f"restored object is not a regular file: {path}")
    with os.fdopen(os.open(path, os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)), "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise ValueError(f"restored object is not a regular file: {path}")
        if required_prefix:
            prefix = stream.read(len(required_prefix))
            if prefix != required_prefix:
                raise ValueError("logical-copy database file is not a PostgreSQL custom-format dump")
            digest.update(prefix)
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
        after = os.fstat(stream.fileno())
    if len({(s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
            for s in (before_open, before, after, path.lstat())}) != 1:
        raise ValueError(f"restored object changed while reading: {path}")
    return digest.hexdigest(), before.st_size


def verify_archive(receipt_path: Path, restore_target: Path) -> dict:
    """Bind the restored inventory and all listed files to the completion receipt."""
    with receipt_path.open("rb") as stream:
        encoded_receipt = stream.read(65537)
    if len(encoded_receipt) > 65536:
        raise ValueError("paired recovery receipt is too large")
    receipt = json.loads(encoded_receipt)
    if receipt.get("schema_version") != "qt.encrypted_recovery_pair.v1":
        raise ValueError("unrecognized QT encrypted recovery pair")
    if receipt.get("restore_endpoint") != "immediate" or not receipt.get("database_label"):
        raise ValueError("paired database restore identity is missing")
    if not _sha256(receipt.get("archive_snapshot")) or not _sha256(receipt.get("inventory_sha256")):
        raise ValueError("paired archive identity or inventory hash is invalid")
    root = restore_target.resolve(strict=True)
    if root.parent == root:
        raise ValueError("an isolated restore directory is required")
    inventory = _path(root, receipt["inventory_snapshot_path"], absolute=True)
    return {
        "database_label": receipt["database_label"],
        "archive_snapshot": receipt["archive_snapshot"],
        **_verify_inventory(root, receipt, inventory, receipt.get("archive_source_root"), absolute=True),
    }


def _verify_inventory(root: Path, receipt: dict, inventory: Path, archive_path: object, *, absolute: bool) -> dict:
    expected_count, expected_bytes = receipt.get("archive_objects"), receipt.get("archive_bytes")
    if type(expected_count) is not int or type(expected_bytes) is not int or min(expected_count, expected_bytes) < 0:
        raise ValueError("paired archive count or byte total is invalid")
    if not inventory.is_file():
        raise ValueError("the restored inventory is absent")
    archive = root
    if expected_count:
        archive = _path(root, archive_path, absolute=absolute)
        if not archive.is_dir():
            raise ValueError("the restored archive root is not a directory")
    digest = hashlib.sha256()
    seen = set()
    total = 0
    with inventory.open("rb") as stream:
        while line := stream.readline(65537):
            if len(line) > 65536 or len(seen) >= expected_count or not line.endswith(b"\n"):
                raise ValueError("unexpected inventory row count or line length")
            digest.update(line)
            item = json.loads(line)
            key, size, sha256 = item["object_key"], item["bytes"], item["sha256"]
            if not isinstance(key, str) or type(size) is not int or size <= 0 or not _sha256(sha256):
                raise ValueError("invalid restored archive inventory object")
            if key in seen:
                raise ValueError(f"duplicate restored archive object: {key}")
            object_path = _path(archive, key, absolute=False)
            actual_digest, actual_size = _sha_file(object_path)
            if actual_size != size or actual_digest != sha256:
                raise ValueError(f"restored archive object failed size or hash check: {key}")
            seen.add(key)
            total += size
    if len(seen) != expected_count or total != expected_bytes or digest.hexdigest() != receipt["inventory_sha256"]:
        raise ValueError("archive inventory digest, object count, or byte total does not match the pair")
    return {
        "inventory_sha256": digest.hexdigest(),
        "verified_archive_objects": len(seen),
        "verified_archive_bytes": total,
    }


def verify_logical_copy(root: Path) -> dict:
    """Check a complete legacy generation before attempting a PostgreSQL restore."""
    root = root.resolve(strict=True)
    if root.parent == root:
        raise ValueError("an isolated logical-copy generation directory is required")
    receipt_path = _path(root, "complete.json", absolute=False)
    owner_path = _path(root, "owner.json", absolute=False)
    for path in (receipt_path, owner_path):
        if not path.is_file() or path.stat().st_size > 65536:
            raise ValueError("invalid logical-copy metadata file")
    receipt = json.loads(receipt_path.read_bytes())
    owner = json.loads(owner_path.read_bytes())
    keys = ("schema_version", "name", "database_identity", "filesystem_uuid")
    if (not isinstance(receipt, dict) or not isinstance(owner, dict)
            or receipt.get("schema_version") != "qt.local_recovery_copy.v1"
            or any(not isinstance(receipt.get(key), str) or owner.get(key) != receipt[key] for key in keys)
            or re.fullmatch(r"copy_[0-9a-f]{32}", receipt["name"]) is None
            or re.fullmatch(r"[0-9]+/[0-9]+", receipt["database_identity"]) is None
            or not receipt["filesystem_uuid"]):
        raise ValueError("unrecognized or mismatched logical-copy owner/receipt")
    dump = _path(root, "database.dump", absolute=False)
    expected_dump = receipt["database"]
    if (type(expected_dump["bytes"]) is not int or expected_dump["bytes"] <= 0
            or not _sha256(expected_dump["sha256"])):
        raise ValueError("invalid logical-copy database file descriptor")
    dump_digest, dump_size = _sha_file(dump, required_prefix=b"PGDMP")
    if (dump_digest, dump_size) != (expected_dump["sha256"], expected_dump["bytes"]):
        raise ValueError("logical-copy database file digest or size mismatch")
    inventory = _path(root, "objects.jsonl", absolute=False)
    return {
        "schema_version": receipt["schema_version"],
        "database_identity": receipt["database_identity"],
        "database_dump_sha256": dump_digest,
        "database_dump_bytes": dump_size,
        **_verify_inventory(root, receipt, inventory, "objects", absolute=False),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, help="independently copied encrypted-pair complete.json")
    parser.add_argument("--restore-target", type=Path, help="isolated Restic restore root")
    parser.add_argument("--logical-copy", type=Path, help="isolated complete legacy logical generation")
    args = parser.parse_args()
    if args.logical_copy is not None:
        if args.receipt is not None or args.restore_target is not None:
            parser.error("--logical-copy is mutually exclusive with encrypted-pair options")
    elif args.receipt is None or args.restore_target is None:
        parser.error("provide --receipt and --restore-target, or --logical-copy")
    try:
        result = (verify_logical_copy(args.logical_copy) if args.logical_copy
                  else verify_archive(args.receipt, args.restore_target))
    except (OSError, ValueError, AttributeError, KeyError, TypeError) as error:
        parser.exit(1, f"archive verification failed: {error}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
